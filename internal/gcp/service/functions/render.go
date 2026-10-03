package functions

import (
	"fmt"
	"time"

	"jaiscloud/internal/gcp/eventing"
	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// FunctionJSON renders a stored Function in the wire shape matching the given
// API version. It is the single source of truth for the function shape, shared
// by the REST provider and the gRPC transport.
func FunctionJSON(v Version, project string, f functionsstore.Function) map[string]any {
	if v == V2 {
		return functionJSONV2(project, f)
	}
	return functionJSONV1(project, f)
}

// functionJSONV1 renders the Cloud Functions v1 CloudFunction shape.
func functionJSONV1(project string, f functionsstore.Function) map[string]any {
	out := map[string]any{
		"name":   resourceID(project)("cloud-function", f.Location+"/"+f.ID),
		"status": f.Status,
	}
	if !f.UpdateTime.IsZero() {
		out["updateTime"] = formatTimestamp(f.UpdateTime)
	}
	if f.Runtime != "" {
		out["runtime"] = f.Runtime
	}
	if f.EntryPoint != "" {
		out["entryPoint"] = f.EntryPoint
	}
	if f.SourceUploadURL != "" {
		out["sourceUploadUrl"] = f.SourceUploadURL
	}
	if f.SourceArchiveURL != "" {
		out["sourceArchiveUrl"] = f.SourceArchiveURL
	}
	if f.EventTrigger != nil {
		et := map[string]any{
			"eventType": f.EventTrigger.EventType,
			"resource":  f.EventTrigger.Resource,
			"service":   f.EventTrigger.Service,
		}
		if f.EventTrigger.Retry {
			// v1 signals retry by the presence of an (empty) Retry message.
			et["failurePolicy"] = map[string]any{"retry": map[string]any{}}
		}
		out["eventTrigger"] = et
	} else if f.HttpsTriggerURL != "" {
		out["httpsTrigger"] = map[string]any{
			"url":           f.HttpsTriggerURL,
			"securityLevel": "SECURE_ALWAYS",
		}
	}
	if f.EnvironmentVariables != nil {
		out["environmentVariables"] = f.EnvironmentVariables
	}
	if f.Labels != nil {
		out["labels"] = f.Labels
	}
	if f.AvailableMemoryMB > 0 {
		out["availableMemoryMb"] = f.AvailableMemoryMB
	}
	if f.Timeout != "" {
		out["timeout"] = f.Timeout
	}
	if f.Description != "" {
		out["description"] = f.Description
	}
	return out
}

// functionSourceV2 renders the v2 BuildConfig.source oneof from the stored
// source reference. A gs:// archive becomes a storageSource; a stored signed
// sourceUploadUrl becomes a sourceUploadUrl; anything else yields nil (the
// field is omitted).
func functionSourceV2(f functionsstore.Function) map[string]any {
	ref := f.SourceArchiveURL
	if !hasGSPrefix(ref) {
		if f.SourceUploadURL != "" {
			return map[string]any{"sourceUploadUrl": f.SourceUploadURL}
		}
		return nil
	}
	rest := ref[len("gs://"):]
	i := indexByte(rest, '/')
	if i <= 0 || i >= len(rest)-1 {
		return nil
	}
	return map[string]any{
		"storageSource": map[string]any{
			"bucket": rest[:i],
			"object": rest[i+1:],
		},
	}
}

// functionJSONV2 renders the Cloud Functions v2 Function shape: state,
// environment, buildConfig (runtime/entryPoint/source), serviceConfig
// (service/uri), and the shared metadata. functionJSONV1 is deliberately left
// unchanged.
func functionJSONV2(project string, f functionsstore.Function) map[string]any {
	out := map[string]any{
		"name":  resourceID(project)("cloud-function", f.Location+"/"+f.ID),
		"state": f.Status,
		// Cloud Functions v2 runs on Cloud Run (GEN_2); the field is
		// output-only and every emulated v2 function is GEN_2.
		"environment": "GEN_2",
	}
	if !f.CreateTime.IsZero() {
		out["createTime"] = formatTimestamp(f.CreateTime)
	}
	if !f.UpdateTime.IsZero() {
		out["updateTime"] = formatTimestamp(f.UpdateTime)
	}
	if f.HttpsTriggerURL != "" {
		out["url"] = f.HttpsTriggerURL
	}
	build := map[string]any{}
	if f.Runtime != "" {
		build["runtime"] = f.Runtime
	}
	if f.EntryPoint != "" {
		build["entryPoint"] = f.EntryPoint
	}
	if src := functionSourceV2(f); src != nil {
		build["source"] = src
	}
	if f.EnvironmentVariables != nil {
		build["environmentVariables"] = f.EnvironmentVariables
	}
	if len(build) > 0 {
		out["buildConfig"] = build
	}
	svc := map[string]any{
		// The backing Cloud Run service is named after the function. It is
		// output-only and always rendered (even without a trigger) so clients
		// that read serviceConfig.service get a stable value.
		"service": resourceID(project)("cloud-run-service", f.Location+"/"+f.ID),
	}
	// A deployed function's backing Cloud Run revision is derived from its
	// persisted revision counter + source hash; all traffic serves it unless the
	// 1st→2nd gen upgrade flow has redirected traffic to the Gen2 copy (FD5).
	if rev := functionRevisionName(project, f); rev != "" {
		svc["revision"] = rev
	}
	// The traffic flag is meaningful once the function has a deployed revision
	// or has entered the upgrade flow, so a redirect is observable even on a
	// function with no persisted source archive.
	if f.Revision > 0 || f.UpgradeState != "" || f.UpgradeTrafficGen2 {
		svc["allTrafficOnLatestRevision"] = !f.UpgradeTrafficGen2
	}
	if f.HttpsTriggerURL != "" {
		svc["uri"] = f.HttpsTriggerURL
	}
	if f.EnvironmentVariables != nil {
		svc["environmentVariables"] = f.EnvironmentVariables
	}
	if f.AvailableMemoryMB > 0 {
		svc["availableMemory"] = fmtIntM(f.AvailableMemoryMB)
	}
	if secs := timeoutSeconds(f.Timeout); secs > 0 {
		svc["timeoutSeconds"] = secs
	}
	// v2 ServiceConfig instance/concurrency settings (FD6). Only explicitly
	// configured values are surfaced (no synthesized defaults), so a client sees
	// exactly the configuration it set.
	if f.MinInstanceCount > 0 {
		svc["minInstanceCount"] = f.MinInstanceCount
	}
	if f.MaxInstanceCount > 0 {
		svc["maxInstanceCount"] = f.MaxInstanceCount
	}
	if f.MaxInstanceRequestConcurrency > 0 {
		svc["maxInstanceRequestConcurrency"] = f.MaxInstanceRequestConcurrency
	}
	if f.AvailableCPU != "" {
		svc["availableCpu"] = f.AvailableCPU
	}
	if len(svc) > 0 {
		out["serviceConfig"] = svc
	}
	if f.Labels != nil {
		out["labels"] = f.Labels
	}
	if f.Description != "" {
		out["description"] = f.Description
	}
	if f.EventTrigger != nil {
		et := map[string]any{"eventType": f.EventTrigger.EventType}
		if eventing.IsStorageEventType(f.EventTrigger.EventType) {
			// A v2 Cloud Storage trigger expresses its source as an event_filters
			// bucket; pubsubTopic is valid only for a Pub/Sub trigger.
			if bucket := eventing.ResourceID(f.EventTrigger.Resource); bucket != "" {
				et["eventFilters"] = []any{map[string]any{"attribute": "bucket", "value": bucket}}
			}
		} else {
			et["pubsubTopic"] = f.EventTrigger.Resource
		}
		if f.EventTrigger.RetryPolicy != "" {
			et["retryPolicy"] = f.EventTrigger.RetryPolicy
		}
		// trigger is output-only: the backing Eventarc trigger the platform
		// materializes for a Pub/Sub or Cloud Storage event trigger (FD9, FP2).
		if f.EventTrigger.Trigger != "" {
			et["trigger"] = f.EventTrigger.Trigger
		}
		out["eventTrigger"] = et
	}
	if ui := upgradeInfoJSON(project, f); ui != nil {
		out["upgradeInfo"] = ui
	}
	return out
}

// Operation is a Cloud Functions long-running operation. By default (lroMode
// disabled) a mutation completes synchronously and Done is true; when async
// timing is enabled it is stored done=false and settled lazily on read (see
// Service.settle). The operation is persisted (see store/functions) so
// operations.get/list and REST :wait can read it back. Function is the
// create/update response snapshot; it is nil for a delete (whose response is a
// google.protobuf.Empty Any).
type Operation struct {
	ID         string
	Location   string
	Verb       string // "create" | "update" | "delete" | a v2 upgrade/traffic method name
	Target     string // full function resource name
	Function   *functionsstore.Function
	Done       bool
	CreateTime time.Time
	EndTime    time.Time
}

// newOperation builds the operation for a function mutation. In the default
// synchronous mode it is done with EndTime == CreateTime, exactly as before;
// in async mode it is in flight with a zero EndTime and settle derives
// completion on read. The timestamps use the business clock and are stable once
// stored.
func (s *Service) newOperation(location, verb, target string, f *functionsstore.Function) Operation {
	t := now()
	op := Operation{ID: newUUID(), Location: location, Verb: verb, Target: target, Function: f, CreateTime: t}
	if s.lroMode.Async() {
		return op
	}
	op.Done = true
	op.EndTime = t
	return op
}

// OperationName returns the full long-running-operation resource name for op.
// v2 (and the other location-scoped services) use the standard
// projects/{p}/locations/{l}/operations/{id}; v1 Cloud Functions publishes its
// operations top-level as operations/{id} (Discovery pattern ^operations/[^/]+$).
func OperationName(v Version, project string, op Operation) string {
	if v == V1 {
		return "operations/" + op.ID
	}
	return resourceID(project)("cloud-function-operation", op.Location+"/"+op.ID)
}

// OperationJSON renders a mutation Operation as a google.longrunning.Operation
// wire map. The response is a typed Any carrying the @type discriminator gax
// clients require to unpack it: the Function for create/update, or
// google.protobuf.Empty for a delete. The response and the metadata completion
// timestamp are only present once the operation is done; real GCP omits them
// while an async operation is in flight. A done operation keeps the original
// shape exactly.
func OperationJSON(v Version, project string, op Operation) map[string]any {
	out := map[string]any{
		"name":     OperationName(v, project, op),
		"metadata": operationMetadataMap(v, op),
		"done":     op.Done,
	}
	if op.Done {
		response := anyResponse(emptyTypeURL, nil)
		if op.Function != nil {
			response = anyResponse(functionTypeFor(v), FunctionJSON(v, project, *op.Function))
		}
		out["response"] = response
	}
	return out
}

// functionTypeFor returns the google.protobuf.Any type URL of the Function
// resource for the given API version.
func functionTypeFor(v Version) string {
	if v == V2 {
		return functionTypeURLV2
	}
	return functionTypeURLV1
}

// anyResponse wraps a rendered resource body as the Any-shaped JSON a
// google.longrunning.Operation response carries, adding the required @type
// discriminator (a missing type URL makes gax fail with "Missing type url when
// parsing"). body is copied, never mutated; a nil body yields the bare @type
// object (e.g. google.protobuf.Empty).
func anyResponse(typeURL string, body map[string]any) map[string]any {
	out := make(map[string]any, len(body)+1)
	out["@type"] = typeURL
	for k, val := range body {
		out[k] = val
	}
	return out
}

// operationMetadataMap renders the version-specific OperationMetadata carried
// on a function operation. v1 uses OperationMetadataV1 ({target, type,
// updateTime}); v2 uses OperationMetadata ({target, verb, operationType,
// apiVersion, createTime, endTime}). The completion timestamp (v1 updateTime,
// v2 endTime) is only present once the operation is done: an in-flight async
// operation does not fabricate one. The done shape is unchanged.
func operationMetadataMap(v Version, op Operation) map[string]any {
	start := op.CreateTime
	if start.IsZero() {
		start = now()
	}
	end := op.EndTime
	if end.IsZero() {
		end = start
	}
	if v == V2 {
		md := map[string]any{
			"@type":         operationMetadataTypeV2,
			"createTime":    formatTimestamp(start),
			"target":        op.Target,
			"verb":          op.Verb,
			"operationType": operationTypeFor(op.Verb),
			"apiVersion":    string(V2),
		}
		if op.Done {
			md["endTime"] = formatTimestamp(end)
		}
		return md
	}
	md := map[string]any{
		"@type":  operationMetadataType,
		"target": op.Target,
		"type":   operationTypeFor(op.Verb),
	}
	if op.Done {
		md["updateTime"] = formatTimestamp(end)
	}
	return md
}

// operationTypeFor maps an operation verb to the OperationMetadata
// operationType enum value.
func operationTypeFor(verb string) string {
	switch verb {
	case "create":
		return "CREATE_FUNCTION"
	case "update":
		return "UPDATE_FUNCTION"
	case "delete":
		return "DELETE_FUNCTION"
	}
	return ""
}

// functionRevisionName renders the Cloud Run revision backing a deployed
// function — "projects/{p}/locations/{l}/services/{id}/revisions/{id}-{NNNNN}-
// {sha8}" — from its persisted revision counter and source hash. It is "" when
// the function has no deployed revision (the metadata-only case), so a function
// created without a source archive renders no revision.
func functionRevisionName(project string, f functionsstore.Function) string {
	if f.Revision <= 0 {
		return ""
	}
	rev := fmt.Sprintf("%s-%05d", f.ID, f.Revision)
	if len(f.SourceSHA256) >= 8 {
		rev += "-" + f.SourceSHA256[:8]
	}
	return resourceID(project)("cloud-run-revision", f.Location+"/"+f.ID+"/"+rev)
}

// upgradeInfoJSON renders the v2 Function.upgradeInfo (output-only) once the
// function has entered the 1st→2nd gen upgrade flow. It carries the documented
// upgradeState enum plus the Gen2 copy's build/service configuration derived
// from the persisted overrides; it is nil (the field is omitted) for a function
// that has never been through an upgrade method.
func upgradeInfoJSON(project string, f functionsstore.Function) map[string]any {
	if f.UpgradeState == "" {
		return nil
	}
	runtime := f.Runtime
	if f.UpgradeRuntime != "" {
		runtime = f.UpgradeRuntime
	}
	build := map[string]any{}
	if runtime != "" {
		build["runtime"] = runtime
	}
	if f.EntryPoint != "" {
		build["entryPoint"] = f.EntryPoint
	}
	svc := map[string]any{
		"service": resourceID(project)("cloud-run-service", f.Location+"/"+f.ID),
	}
	if f.UpgradeMaxInstances > 0 {
		svc["maxInstanceCount"] = f.UpgradeMaxInstances
	}
	ui := map[string]any{"upgradeState": f.UpgradeState}
	if len(build) > 0 {
		ui["buildConfig"] = build
	}
	if len(svc) > 0 {
		ui["serviceConfig"] = svc
	}
	return ui
}

// hasGSPrefix reports whether s starts with "gs://".
func hasGSPrefix(s string) bool {
	return len(s) >= 5 && s[:5] == "gs://"
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
