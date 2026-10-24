package functions

import (
	"fmt"
	"strings"

	"jaiscloud/internal/gcp/resource"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
)

// FunctionInput carries the caller-supplied, mutable fields of a function
// create/update in a wire-version-independent shape. It is built from a REST
// Discovery body (FunctionInputFromMap) or a proto message via protojson (the
// gRPC transport), so the core never sees NormalizedRequest or protobuf.
type FunctionInput struct {
	// Name is the optional resource name carried in a create/update body.
	Name string

	Runtime          string
	EntryPoint       string
	SourceUploadURL  string
	SourceArchiveURL string
	SourceBucket     string
	SourceObject     string
	// SourceArchive is an in-memory source archive supplied directly to the
	// core rather than referenced over GCS. It is set only by the console UI's
	// inline-source create path; FunctionInputFromMap and the gRPC transport
	// never populate it, so wire behavior is unchanged. When non-empty it takes
	// precedence over the GCS reference in resolveSource.
	SourceArchive        []byte
	EnvironmentVariables map[string]string
	Labels               map[string]string
	Description          string
	Timeout              string // canonical duration string, e.g. "60s"
	AvailableMemoryMB    int
	EventTrigger         *functionsstore.EventTrigger

	// The v2 ServiceConfig instance/concurrency settings (FD6). The Has* flags
	// record whether the request body actually carried the field, so an
	// empty-mask update cannot silently zero a stored value while an explicit
	// updateMask path still applies an explicit 0/"" (the FieldMask semantics
	// real Cloud Functions uses).
	MinInstanceCount                 int
	MaxInstanceCount                 int
	MaxInstanceRequestConcurrency    int
	AvailableCPU                     string
	HasMinInstanceCount              bool
	HasMaxInstanceCount              bool
	HasMaxInstanceRequestConcurrency bool
	HasAvailableCPU                  bool
}

// FunctionInputFromMap extracts the wire-version-appropriate fields from a
// Discovery-shaped body (a v1 Function or a v2 Function) into the unified input.
// The env-vars/event-trigger fields differ by version: v1 places them at the
// top level and uses eventTrigger.{eventType,resource,service}; v2 nests them
// under buildConfig/serviceConfig, uses
// eventTrigger.{eventType,pubsubTopic}, and expresses source as
// buildConfig.source.storageSource.
func FunctionInputFromMap(body map[string]any, v Version) FunctionInput {
	if v == V2 {
		return functionInputFromMapV2(body)
	}
	in := FunctionInput{
		Name:                 bodyString(body, "name"),
		Runtime:              bodyString(body, "runtime"),
		EntryPoint:           bodyString(body, "entryPoint"),
		SourceUploadURL:      bodyString(body, "sourceUploadUrl"),
		SourceArchiveURL:     bodyString(body, "sourceArchiveUrl"),
		EnvironmentVariables: bodyStringMap(body, "environmentVariables"),
		Labels:               bodyStringMap(body, "labels"),
		Description:          bodyString(body, "description"),
		Timeout:              bodyString(body, "timeout"),
	}
	if n, ok := body["availableMemoryMb"].(float64); ok && n > 0 {
		in.AvailableMemoryMB = int(n)
	}
	if et, ok := body["eventTrigger"].(map[string]any); ok {
		in.EventTrigger = &functionsstore.EventTrigger{
			EventType: stringOf(et["eventType"]),
			Resource:  stringOf(et["resource"]),
			Service:   stringOf(et["service"]),
		}
		// v1 expresses retry as the presence of eventTrigger.failurePolicy.retry
		// (an empty Retry message); its absence means failures are ignored.
		if fp, ok := et["failurePolicy"].(map[string]any); ok {
			if _, ok := fp["retry"]; ok {
				in.EventTrigger.Retry = true
			}
		}
	}
	return in
}

// functionInputFromMapV2 extracts a Cloud Functions v2 Function.
func functionInputFromMapV2(body map[string]any) FunctionInput {
	bc := nestedMap(body, "buildConfig")
	sc := nestedMap(body, "serviceConfig")
	in := FunctionInput{
		Name:                 bodyString(body, "name"),
		Runtime:              bodyString(bc, "runtime"),
		EntryPoint:           bodyString(bc, "entryPoint"),
		EnvironmentVariables: bodyStringMap(bc, "environmentVariables"),
		Labels:               bodyStringMap(body, "labels"),
		Description:          bodyString(body, "description"),
	}
	if in.EnvironmentVariables == nil {
		in.EnvironmentVariables = bodyStringMap(sc, "environmentVariables")
	}
	in.AvailableMemoryMB = parseMemoryMB(bodyString(sc, "availableMemory"))
	if secs, ok := sc["timeoutSeconds"].(float64); ok && secs > 0 {
		in.Timeout = durationString(int(secs))
	}
	// v2 ServiceConfig instance/concurrency settings (FD6). The Has* flags track
	// presence so an empty-mask update does not clear an unconfigured field.
	if n, ok := bodyInt(sc, "minInstanceCount"); ok {
		in.MinInstanceCount, in.HasMinInstanceCount = n, true
	}
	if n, ok := bodyInt(sc, "maxInstanceCount"); ok {
		in.MaxInstanceCount, in.HasMaxInstanceCount = n, true
	}
	if n, ok := bodyInt(sc, "maxInstanceRequestConcurrency"); ok {
		in.MaxInstanceRequestConcurrency, in.HasMaxInstanceRequestConcurrency = n, true
	}
	if s, ok := sc["availableCpu"].(string); ok {
		in.AvailableCPU, in.HasAvailableCPU = s, true
	}
	if src := nestedMap(bc, "source"); src != nil {
		if ss := nestedMap(src, "storageSource"); ss != nil {
			in.SourceBucket = bodyString(ss, "bucket")
			in.SourceObject = bodyString(ss, "object")
			if in.SourceBucket != "" && in.SourceObject != "" {
				in.SourceArchiveURL = "gs://" + in.SourceBucket + "/" + in.SourceObject
			}
			if in.SourceUploadURL == "" {
				in.SourceUploadURL = bodyString(ss, "sourceUploadUrl")
			}
		}
	}
	if et := nestedMap(body, "eventTrigger"); et != nil {
		// EventTrigger.Service models the v1 "service" hostname, not the v2
		// serviceAccountEmail, so only the event type and source are carried
		// across; storing the v2 email here would surface it under the v1
		// service field on a cross-version read.
		//
		// A v2 trigger expresses its source as an event_filters bucket (Cloud
		// Storage) or a pubsubTopic (Pub/Sub, the only event type it is valid
		// for); the unified Resource carries whichever is set.
		src := stringOf(et["pubsubTopic"])
		if bucket := eventFilterValue(et["eventFilters"], "bucket"); bucket != "" {
			src = resource.ResourceID("")("gcs-bucket-policy", bucket)
		}
		in.EventTrigger = &functionsstore.EventTrigger{
			EventType:   stringOf(et["eventType"]),
			Resource:    src,
			RetryPolicy: stringOf(et["retryPolicy"]),
		}
	}
	return in
}

// eventFilterValue returns the value of the first v2 eventFilters entry whose
// attribute matches attr. The filters arrive in the decoded JSON/protojson
// []any-of-objects shape.
func eventFilterValue(raw any, attr string) string {
	filters, _ := raw.([]any)
	for _, f := range filters {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}
		if a, _ := fm["attribute"].(string); a == attr {
			v, _ := fm["value"].(string)
			return v
		}
	}
	return ""
}

// durationString renders whole seconds as the canonical Cloud Functions
// duration string ("60s"), matching the v1 timeout field's format.
func durationString(seconds int) string {
	return fmt.Sprintf("%ds", seconds)
}

// newFunction builds the stored record for a create request.
func newFunction(project, location, id string, in FunctionInput) functionsstore.Function {
	t := now()
	f := functionsstore.Function{
		ID:                   id,
		Location:             location,
		Runtime:              in.Runtime,
		EntryPoint:           in.EntryPoint,
		SourceUploadURL:      in.SourceUploadURL,
		SourceArchiveURL:     in.SourceArchiveURL,
		EnvironmentVariables: in.EnvironmentVariables,
		Status:               "ACTIVE",
		CreateTime:           t,
		UpdateTime:           t,
		Labels:               in.Labels,
		AvailableMemoryMB:    defaultMemoryMB,
		Timeout:              defaultTimeout,
		Description:          in.Description,
	}
	if in.AvailableMemoryMB > 0 {
		f.AvailableMemoryMB = in.AvailableMemoryMB
	}
	if in.Timeout != "" {
		f.Timeout = in.Timeout
	}
	f.MinInstanceCount = in.MinInstanceCount
	f.MaxInstanceCount = in.MaxInstanceCount
	f.MaxInstanceRequestConcurrency = in.MaxInstanceRequestConcurrency
	f.AvailableCPU = in.AvailableCPU
	if in.EventTrigger != nil {
		f.EventTrigger = in.EventTrigger
	} else {
		f.HttpsTriggerURL = defaultHttpsTriggerURL(project, location, id)
	}
	return f
}

// functionUpdateFields maps every accepted updateMask path (lowerCamel or
// snake_case) to its canonical CloudFunction field name. A path absent from
// this set fails loud with Unimplemented rather than being silently ignored.
var functionUpdateFields = map[string]string{
	"runtime":               "runtime",
	"entrypoint":            "entryPoint",
	"entry_point":           "entryPoint",
	"sourceuploadurl":       "sourceUploadUrl",
	"source_upload_url":     "sourceUploadUrl",
	"sourcearchiveurl":      "sourceArchiveUrl",
	"source_archive_url":    "sourceArchiveUrl",
	"environmentvariables":  "environmentVariables",
	"environment_variables": "environmentVariables",
	"labels":                "labels",
	"description":           "description",
	"timeout":               "timeout",
	"availablememorymb":     "availableMemoryMb",
	"available_memory_mb":   "availableMemoryMb",
	"eventtrigger":          "eventTrigger",
	"event_trigger":         "eventTrigger",
	// Cloud Functions v2 nests the mutable fields under buildConfig and
	// serviceConfig; accept those mask paths so v2 clients can PATCH them.
	"buildconfig.runtime":                 "runtime",
	"buildconfig.entrypoint":              "entryPoint",
	"buildconfig.entry_point":             "entryPoint",
	"buildconfig.environmentvariables":    "environmentVariables",
	"buildconfig.environment_variables":   "environmentVariables",
	"buildconfig.source":                  "source",
	"serviceconfig.environmentvariables":  "environmentVariables",
	"serviceconfig.environment_variables": "environmentVariables",
	"serviceconfig.availablememory":       "availableMemoryMb",
	"serviceconfig.available_memory":      "availableMemoryMb",
	"serviceconfig.timeoutseconds":        "timeout",
	"serviceconfig.timeout_seconds":       "timeout",
	// v2 ServiceConfig instance/concurrency settings (FD6).
	"serviceconfig.mininstancecount":                 "minInstanceCount",
	"serviceconfig.min_instance_count":               "minInstanceCount",
	"serviceconfig.maxinstancecount":                 "maxInstanceCount",
	"serviceconfig.max_instance_count":               "maxInstanceCount",
	"serviceconfig.maxinstancerequestconcurrency":    "maxInstanceRequestConcurrency",
	"serviceconfig.max_instance_request_concurrency": "maxInstanceRequestConcurrency",
	"serviceconfig.availablecpu":                     "availableCpu",
	"serviceconfig.available_cpu":                    "availableCpu",
}

// canonicalMaskField normalizes an updateMask path to its canonical field name.
// A leading "function." (the proto request wraps the resource) is stripped, and
// the v2 nested prefixes are accepted in either the proto's snake_case
// ("build_config"/"service_config") or the JSON shape's camelCase
// ("buildConfig"/"serviceConfig"), since a FieldMask carries the path verbatim.
func canonicalMaskField(path string) (string, bool) {
	p := strings.ToLower(strings.TrimSpace(path))
	p = strings.TrimPrefix(p, "function.")
	p = strings.ReplaceAll(p, "build_config.", "buildconfig.")
	p = strings.ReplaceAll(p, "service_config.", "serviceconfig.")
	f, ok := functionUpdateFields[p]
	return f, ok
}

// maskAppliesSource reports whether an update mask selects any source field
// (an empty mask selects every mutable field). It mirrors ApplyFunctionUpdate's
// apply() checks, so the source archive is only (re)resolved when the mask
// actually lets the source change.
func maskAppliesSource(mask []string) bool {
	if len(mask) == 0 {
		return true
	}
	for _, m := range mask {
		if c, ok := canonicalMaskField(m); ok {
			switch c {
			case "source", "sourceArchiveUrl", "sourceUploadUrl":
				return true
			}
		}
	}
	return false
}

// ApplyFunctionUpdate merges an update body into f. A non-empty mask names the
// paths to overlay (masked paths take the incoming body value, unmasked paths
// retain the stored value); an empty mask overlays every mutable field present
// in the body. Every masked path is validated up front so an unsupported field
// fails loud with Unimplemented instead of being silently no-op-ed.
func ApplyFunctionUpdate(f *functionsstore.Function, in FunctionInput, mask []string) error {
	for _, m := range mask {
		if _, ok := canonicalMaskField(m); !ok {
			return model.NewProviderError("Unimplemented", "unsupported updateMask field "+m, 501)
		}
	}
	apply := func(field string) bool {
		if len(mask) == 0 {
			return true
		}
		for _, m := range mask {
			if c, ok := canonicalMaskField(m); ok && c == field {
				return true
			}
		}
		return false
	}
	if apply("runtime") && in.Runtime != "" {
		f.Runtime = in.Runtime
	}
	if apply("entryPoint") && in.EntryPoint != "" {
		f.EntryPoint = in.EntryPoint
	}
	if apply("sourceUploadUrl") && in.SourceUploadURL != "" {
		f.SourceUploadURL = in.SourceUploadURL
	}
	if apply("sourceArchiveUrl") && in.SourceArchiveURL != "" {
		f.SourceArchiveURL = in.SourceArchiveURL
	}
	if apply("environmentVariables") && in.EnvironmentVariables != nil {
		f.EnvironmentVariables = in.EnvironmentVariables
	}
	if apply("labels") && in.Labels != nil {
		f.Labels = in.Labels
	}
	if apply("description") && in.Description != "" {
		f.Description = in.Description
	}
	if apply("timeout") && in.Timeout != "" {
		f.Timeout = in.Timeout
	}
	if apply("availableMemoryMb") && in.AvailableMemoryMB > 0 {
		f.AvailableMemoryMB = in.AvailableMemoryMB
	}
	// v2 ServiceConfig instance/concurrency settings (FD6): a non-empty mask
	// selecting the path applies its value even when explicitly 0/"" (FieldMask
	// semantics); an empty mask applies only fields the body actually carried,
	// so it cannot silently clear an unconfigured field.
	if apply("minInstanceCount") && (len(mask) > 0 || in.HasMinInstanceCount) {
		f.MinInstanceCount = in.MinInstanceCount
	}
	if apply("maxInstanceCount") && (len(mask) > 0 || in.HasMaxInstanceCount) {
		f.MaxInstanceCount = in.MaxInstanceCount
	}
	if apply("maxInstanceRequestConcurrency") && (len(mask) > 0 || in.HasMaxInstanceRequestConcurrency) {
		f.MaxInstanceRequestConcurrency = in.MaxInstanceRequestConcurrency
	}
	if apply("availableCpu") && (len(mask) > 0 || in.HasAvailableCPU) {
		f.AvailableCPU = in.AvailableCPU
	}
	if apply("eventTrigger") && in.EventTrigger != nil {
		f.EventTrigger = in.EventTrigger
	}
	if apply("source") && in.SourceBucket != "" && in.SourceObject != "" {
		f.SourceArchiveURL = "gs://" + in.SourceBucket + "/" + in.SourceObject
	}
	f.UpdateTime = now()
	return nil
}
