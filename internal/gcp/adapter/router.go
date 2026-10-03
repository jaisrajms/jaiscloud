package gcp

import (
	"net/http"
	"strings"

	functionscore "jaiscloud/internal/gcp/service/functions"
	servicelogging "jaiscloud/internal/gcp/service/logging"
	runcore "jaiscloud/internal/gcp/service/run"
)

// DetectionSource indicates how the service was identified.
type DetectionSource int

const (
	SourceUnknown DetectionSource = iota
	SourcePath                    // URL path prefix matched a service
	SourceHost                    // request host matched a service (Function URL)
)

// DetectService identifies the GCP service from the HTTP request path.
// GCP has no SigV4 scope; the path is the sole reliable discriminator.
func DetectService(r *http.Request) (service string, source DetectionSource) {
	// A request addressed to a deployed function's synthesized HTTPS-trigger
	// host ({location}-{project}.cloudfunctions.net) is a data-plane invocation,
	// not a control-plane API call. It must be detected by host: the trigger
	// path (/{functionId}) is otherwise indistinguishable from a GCS raw-media
	// path and would be mis-served.
	if functionscore.IsTriggerHost(r.Host) {
		return "functions", SourceHost
	}
	// A request addressed to a deployed Cloud Run service's synthesized host
	// ({id}-{token}.{location}.run.app) is a data-plane invocation, not a
	// control-plane API call: its path is arbitrary and would otherwise be
	// mis-served as a GCS raw-media object.
	if runcore.IsInvocationHost(r.Host) {
		return "run", SourceHost
	}
	// GKE (container.googleapis.com) SDK clients address the canonical
	// /v1/projects/{project}/locations/{location}/clusters path, which collides
	// with Managed Kafka on the single emulator origin. The Host's first DNS
	// label is the discriminator: container.localhost / container.googleapis.com
	// → GKE. The path is left unchanged (the container codec accepts the
	// canonical form); Terraform/gcloud use the /container/ path prefix, claimed
	// by the descriptor's PathPrefixes below. Only /v1/ paths are matched, so
	// GCS XML/object, /storage, /v2 and health endpoints are never affected.
	if strings.HasPrefix(r.URL.Path, "/v1/") && firstHostLabel(r.Host) == "container" {
		return "container", SourceHost
	}
	p := r.URL.Path
	// SDK test clients concatenate an endpoint that may already end in "/" with
	// "/v1/..." paths, yielding a leading "//". Collapse redundant leading
	// slashes so path-prefix detection is stable.
	p = "/" + strings.TrimLeft(p, "/")
	for _, svc := range gcpServices {
		for _, prefix := range svc.PathPrefixes {
			if strings.HasPrefix(p, prefix) {
				return svc.ServiceName, SourcePath
			}
		}
	}
	// BigQuery's servicePath is bigquery/v2/ (not /v1/), so it is detected here
	// before the generic /v1/ resolver below. The /bigquery/v2/projects prefix
	// is unambiguous and cannot collide with dataproc/workflows/managedkafka
	// (all /v1/projects/...).
	if svc := detectBigQueryService(p); svc != "" {
		return svc, SourcePath
	}
	// /v1/projects/{project}/... services — resolve by resource type. This must
	// run before the raw-media fallback: a /v1/... path also has two or more
	// segments and would otherwise be mistaken for a GCS media download.
	if strings.HasPrefix(p, "/v1/") {
		if svc := detectV1Service(r.URL.EscapedPath()); svc != "" {
			return svc, SourcePath
		}
	}
	// Cloud Functions v2 only. Resolved before the raw-media fallback for the
	// same reason as /v1/... above.
	if strings.HasPrefix(p, "/v2/") {
		if svc := detectV2Service(r.URL.EscapedPath()); svc != "" {
			return svc, SourcePath
		}
	}
	// GCS raw XML API object requests use the "raw" URL form /{bucket}/{object}
	// (no JSON-API prefix): GET/HEAD downloads (the storage client derives this
	// base from the emulator endpoint) and PUT uploads. Recognise them as
	// storage when no other service prefix matched and the path has at least a
	// bucket and an object segment.
	if isRawStorageMediaPath(r) {
		return "storage", SourcePath
	}
	return "", SourceUnknown
}

// isRawStorageMediaPath reports whether r is a GCS raw XML API object request
// of the form /{bucket}/{object}: a download (GET/HEAD) or an upload (PUT,
// whether a V4 signed URL or a plain XML API PUT). Admin routes and JSON-API
// prefixes are handled elsewhere; only genuine object requests reach this
// fallback.
func isRawStorageMediaPath(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodPut:
	default:
		return false
	}
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "" || strings.HasPrefix(p, "_jaiscloud") {
		return false
	}
	// Known API prefixes own their paths (the service/version routers handle
	// them); the raw media fallback must not claim one, or a JSON metadata
	// write like PUT /storage/v1/b/{bucket}/o/{object} would be mistaken for an
	// object upload (and its gzip transport body left undecoded). The versioned
	// /v1//v2/ namespaces are reserved by the other GCP services' routers.
	for _, prefix := range []string{
		"v1/", "v2/", "bigquery/", "storage/v1/", "upload/storage/", "download/storage/",
		"resumable/upload/", "batch/storage/", "projects/",
	} {
		if strings.HasPrefix(p, prefix) {
			return false
		}
	}
	idx := strings.IndexByte(p, '/')
	return idx > 0 && idx < len(p)-1
}

// detectBigQueryService maps a BigQuery path to the "bigquery" service name.
// Two forms are accepted: the full servicePath /bigquery/v2/projects/{project}/...
// and the WithEndpoint-stripped form /projects/{project}/{datasets|jobs|queries|serviceAccount}
// (the apiary client resolves method paths against the endpoint option, which
// drops the bigquery/v2/ servicePath). Both are unambiguous — no other GCP
// service in the emulator routes bare /projects/{project}/... without a
// version segment, so this runs before the generic /v1/ resolver and the raw
// GCS media fallback without colliding.
func detectBigQueryService(path string) string {
	if strings.HasPrefix(path, "/bigquery/v2/projects/") {
		return "bigquery"
	}
	if !strings.HasPrefix(path, "/projects/") {
		return ""
	}
	rest := strings.TrimPrefix(path, "/projects/")
	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		return ""
	}
	resource := rest[idx+1:]
	for _, r := range []string{"datasets", "jobs", "queries", "serviceAccount"} {
		if resource == r || strings.HasPrefix(resource, r+"/") {
			return "bigquery"
		}
	}
	return ""
}

// detectV1Service maps a /v1/projects/{project}/... path to a service name by
// inspecting the resource-type segment(s) after the project.
func detectV1Service(path string) string {
	seg := splitEscaped(path)
	// v1 Cloud Functions operations are top-level (operations/{id}); the
	// location-scoped .../locations/{l}/operations/{id} form belongs to the
	// Workflows/Managed Kafka/etc. resource switch below.
	if len(seg) >= 2 && seg[0] == "v1" && seg[1] == "operations" {
		return "functions"
	}
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	// Cloud Datastore v1 data methods are project-segment custom verbs
	// (POST /v1/projects/{project}:lookup|runQuery|runAggregationQuery|
	// beginTransaction|commit|rollback|allocateIds|reserveIds). They share the
	// /v1/projects/{project}:verb shape with Cloud Resource Manager's project
	// custom methods, so the verb — not the resource segment — discriminates.
	// This must run before the resource-manager guard below.
	if pi >= 0 && pi+1 == len(seg)-1 && isDatastoreVerb(seg[pi+1]) {
		return "datastore"
	}
	// Cloud Resource Manager project surface: when the project segment is the
	// LAST segment there is no trailing resource type — either the bare project
	// resource (GET /v1/projects/{project} → projects.get) or a project-segment
	// custom method (POST /v1/projects/{project}:getIamPolicy|setIamPolicy|
	// testIamPermissions). This must run before the pi+2 guard below, which
	// assumes a trailing resource-type segment and would otherwise return "".
	if pi >= 0 && pi+1 == len(seg)-1 {
		return "resourcemanager"
	}
	if pi < 0 || pi+2 >= len(seg) {
		return ""
	}
	rest := seg[pi+2:]
	// Capture and strip a trailing custom-method suffix (":commit", ":runQuery",
	// ...) from the last segment so "documents:commit" detects as "documents"
	// (mirrors the JSONCodec.Decode custom-method handling).
	custom := ""
	if len(rest) > 0 {
		last := rest[len(rest)-1]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			custom = last[i+1:]
			rest[len(rest)-1] = last[:i]
		}
	}
	// IAM Service Account Credentials shares the serviceAccounts resource path
	// with IAM on a single emulator origin; its unique custom verbs
	// (generateAccessToken/generateIdToken) discriminate it, as does
	// getAllowedLocations (declared on serviceAccounts and workloadIdentityPools).
	// signBlob/signJwt are ambiguous with iam and remain owned by iam.
	if custom == "generateAccessToken" || custom == "generateIdToken" || custom == "getAllowedLocations" {
		return "iamcredentials"
	}
	// Service Usage v1: /v1/projects/{project}/services[/{service}][:verb]. The
	// bare "services" resource segment is claimed here before the generic
	// resource-type switch — and, crucially, before the GCS raw-media fallback,
	// which would otherwise serve a /v1/projects/... path as a media download
	// (a GET would be mislogged as service=storage action=ObjectsGetMedia).
	if len(rest) > 0 && rest[0] == "services" {
		return "serviceusage"
	}
	// Cloud Scheduler v1 lives under
	// /v1/projects/{project}/locations/{location}/jobs[/{job}][:verb] — detect
	// it before the generic resource-type switch (its "jobs" segment is
	// otherwise unmatched and would fall through to the unknown-service 404).
	if len(rest) > 0 && rest[0] == "locations" && len(rest) >= 3 && rest[2] == "jobs" {
		return "scheduler"
	}
	// Dataproc lives under /v1/projects/{project}/regions/{region}/{clusters|jobs|operations}
	// — detect it before the generic resource-type switch (its "operations"
	// segment would otherwise be mistaken for the Workflows LRO surface).
	if detectDataprocResourceType(rest) != "" {
		return "dataproc"
	}
	// Managed Kafka lives under /v1/projects/{project}/locations/{location}/clusters
	// — detect it before the generic resource-type switch (a topic path's "topics"
	// segment would otherwise be mistaken for the Pub/Sub topic surface).
	if detectManagedKafkaResourceType(rest) != "" {
		return "managedkafka"
	}
	// Dataproc Metastore lives under /v1/projects/{project}/locations/{location}/services
	// — detect it before the generic resource-type switch (its "services" segment
	// is otherwise unmatched; "backups"/"metadataImports"/"operations" are only
	// meaningful nested under it).
	if detectMetastoreResourceType(rest) != "" {
		return "metastore"
	}
	// Memorystore for Redis lives under /v1/projects/{project}/locations/{location}/instances
	// — detect it before the generic resource-type switch (its "locations"
	// segment would otherwise be unclaimed and fall through to the GCS raw-media
	// fallback). Location discovery (locations[/{location}]) is claimed here too:
	// no other emulator service owns those bare paths.
	if detectMemorystoreResourceType(rest) != "" {
		return "redis"
	}
	switch detectResourceType(rest) {
	case "topics", "subscriptions":
		return "pubsub"
	case "secrets":
		return "secretmanager"
	case "keyRings", "cryptoKeys", "cryptoKeyVersions":
		return "kms"
	case "serviceAccounts", "keys":
		return "iam"
	case "documents", "indexes":
		return "firestore"
	case "functions":
		return "functions"
	case "workflows", "operations":
		return "workflows"
	case "executions":
		return "workflowexecutions"
	case "triggers", "channels", "providers":
		return "eventarc"
	}
	return ""
}

// datastoreVerbs are the Cloud Datastore v1 REST data-method custom verbs. All
// are POSTs to /v1/projects/{project}:{verb}. The Discovery document's two
// Datastore Admin methods (projects:export / projects:import) are deliberately
// not claimed here — the emulator implements the data plane only; leaving them
// out means they fall through to the resource-manager guard and 404 rather than
// being mis-served.
var datastoreVerbs = map[string]bool{
	"lookup":              true,
	"runQuery":            true,
	"runAggregationQuery": true,
	"beginTransaction":    true,
	"commit":              true,
	"rollback":            true,
	"allocateIds":         true,
	"reserveIds":          true,
}

// isDatastoreVerb reports whether seg is a project segment carrying a Datastore
// custom verb ("{project}:{verb}").
func isDatastoreVerb(seg string) bool {
	i := strings.IndexByte(seg, ':')
	if i < 0 {
		return false
	}
	return datastoreVerbs[seg[i+1:]]
}

// detectV2Service maps a /v2/... path to a service name. Cloud Logging v2 and
// Cloud Functions v2 are the two emulated services that claim the /v2/
// namespace: Logging owns the entries custom methods
// (POST /v2/entries:write|:list), the descriptor catalog
// (/v2/monitoredResourceDescriptors), the {scope}/{scopeID}/logs family
// (logs.list / logs.delete), the config plane
// ({scope}/{scopeID}/{sinks,exclusions}), and logs-based metrics
// ({scope}/{scopeID}/metrics); Cloud Functions owns
// /v2/projects/{project}/locations/... (functions/operations). The two do not
// collide: Logging's log paths sit directly under a project, while Functions
// always has a locations segment next.
func detectV2Service(path string) string {
	// SDK test clients may yield a leading "//"; collapse it as DetectService does.
	path = "/" + strings.TrimLeft(path, "/")
	if !strings.HasPrefix(path, "/v2/") {
		return ""
	}
	seg := splitEscaped(path)

	// Cloud Logging entry custom methods and the global descriptor catalog.
	if len(seg) >= 2 {
		if strings.HasPrefix(seg[1], "entries:") || seg[1] == "monitoredResourceDescriptors" {
			return "logging"
		}
	}
	// Cloud Logging log listing/deletion: /v2/{scope}/{scopeID}/logs[/{logId}].
	if isLoggingLogsPath(seg) {
		return "logging"
	}
	// Cloud Logging config plane: /v2/{scope}/{scopeID}/sinks[/{id}] and
	// /v2/{scope}/{scopeID}/exclusions[/{id}].
	if isLoggingConfigPath(seg) {
		return "logging"
	}
	// Cloud Logging logs-based metrics:
	// /v2/{scope}/{scopeID}/metrics[/{metricId...}].
	if isLoggingMetricsPath(seg) {
		return "logging"
	}

	if !strings.HasPrefix(path, "/v2/projects/") {
		return ""
	}
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+2 >= len(seg) {
		return ""
	}
	rest := seg[pi+2:]
	// Strip a trailing custom-method suffix (":cancel") from the last segment
	// so it does not hide the resource type.
	if len(rest) > 0 {
		last := rest[len(rest)-1]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			rest[len(rest)-1] = last[:i]
		}
	}
	if len(rest) < 1 || rest[0] != "locations" {
		return ""
	}
	// Cloud Tasks v2 lives under
	// /v2/projects/{project}/locations/{location}/queues[/{queue}/tasks...] —
	// detect it before the Functions location switch (its "queues" segment is
	// otherwise unmatched).
	if len(rest) >= 3 && rest[2] == "queues" {
		return "tasks"
	}
	// Cloud Run v2 shares the /v2/projects/{p}/locations/{l} namespace. It owns
	// the services resource family (covers services/{svc}/revisions and the
	// service IAM custom methods) and the run-prefixed operation ids
	// ("operation-run-<uuid>"). The bare locations/{l}/operations[/{id}] family
	// is path-ambiguous with Cloud Functions on one origin and stays with
	// functions by design (documented limitation; run operations are returned
	// inline and reachable directly by their prefixed id).
	if len(rest) >= 3 && rest[2] == "services" {
		return "run"
	}
	if len(rest) >= 4 && rest[2] == "operations" && strings.HasPrefix(rest[3], "operation-run-") {
		return "run"
	}
	// locations and locations/{location} are the shared google.cloud.location
	// discovery paths; no other emulated service claims the /v2 namespace.
	if len(rest) <= 2 {
		return "functions"
	}
	switch rest[2] {
	case "functions", "operations", "runtimes":
		return "functions"
	}
	return ""
}

// isLoggingLogsPath reports whether seg is /v2/{scope}/{scopeID}/logs[/...],
// where scope is a Cloud Logging resource container (projects, organizations,
// folders, billingAccounts).
func isLoggingLogsPath(seg []string) bool {
	if len(seg) < 4 || !servicelogging.IsLogScope(seg[1]) || seg[2] == "" {
		return false
	}
	return seg[3] == "logs"
}

// isLoggingConfigPath reports whether seg is
// /v2/{scope}/{scopeID}/sinks[/{id}] or /v2/{scope}/{scopeID}/exclusions[/{id}].
func isLoggingConfigPath(seg []string) bool {
	if len(seg) < 4 || len(seg) > 5 || !servicelogging.IsLogScope(seg[1]) || seg[2] == "" {
		return false
	}
	return seg[3] == "sinks" || seg[3] == "exclusions"
}

// isLoggingMetricsPath reports whether seg is
// /v2/{scope}/{scopeID}/metrics[/{metricId...}]. A metric id may itself contain
// slashes, so any number of trailing segments is accepted.
func isLoggingMetricsPath(seg []string) bool {
	if len(seg) < 4 || !servicelogging.IsLogScope(seg[1]) || seg[2] == "" {
		return false
	}
	return seg[3] == "metrics"
}

// detectDataprocResourceType returns "clusters", "jobs", "operations", or
// "workflowTemplates" when the segments after projects/{project} form
// regions/{region}/{type}, else "".
func detectDataprocResourceType(seg []string) string {
	if len(seg) < 3 || seg[0] != "regions" {
		return ""
	}
	// A custom-method suffix ("workflowTemplates:instantiateInline") is still
	// the workflowTemplates resource type.
	resource := seg[2]
	if i := strings.IndexByte(resource, ':'); i >= 0 {
		resource = resource[:i]
	}
	switch resource {
	case "clusters", "jobs", "operations", "workflowTemplates":
		return resource
	}
	return ""
}

// detectManagedKafkaResourceType returns "clusters" when the segments after
// projects/{project} form locations/{location}/clusters, else "". The
// "locations" (vs Dataproc's "regions") segment is the distinguishing key. The
// shared locations/{location}/operations/{id} LRO path is intentionally NOT
// claimed here — it is path-ambiguous with Workflows' operations surface on a
// single host, so it remains routed to workflows and Managed Kafka returns its
// operations inline (done: true) from the cluster mutation that created them.
// The provider still registers GetOperation/ListOperations for direct dispatch.
func detectManagedKafkaResourceType(seg []string) string {
	if len(seg) < 3 || seg[0] != "locations" {
		return ""
	}
	if seg[2] == "clusters" {
		return "clusters"
	}
	return ""
}

// detectMemorystoreResourceType returns "instances" when the segments after
// projects/{project} form locations/{location}/instances, and "locations" for
// the bare location-discovery paths locations or locations/{location}. The
// "instances" segment is unique to Memorystore for Redis among the emulator's
// /v1/projects/{project}/locations/{location}/... services (workflows uses
// "workflows", functions uses "functions", KMS uses "keyRings", Managed Kafka
// uses "clusters", Dataproc Metastore uses "services"). The shared
// locations/{location}/operations/{id} LRO path is intentionally NOT claimed
// here — it is path-ambiguous with Workflows' operations surface on a single
// host, so it remains routed to workflows and Memorystore returns its
// operations inline (done: true).
func detectMemorystoreResourceType(seg []string) string {
	if len(seg) == 0 || seg[0] != "locations" {
		return ""
	}
	switch {
	case len(seg) >= 3 && seg[2] == "instances":
		return "instances"
	case len(seg) <= 2:
		return "locations"
	}
	return ""
}

// detectMetastoreResourceType returns "services" when the segments after
// projects/{project} form locations/{location}/services, else "". The
// "services" segment is unique to Dataproc Metastore among the emulator's
// /v1/projects/{project}/locations/{location}/... services (workflows uses
// "workflows", functions uses "functions", KMS uses "keyRings", Managed Kafka
// uses "clusters"). The shared locations/{location}/operations/{id} LRO path is
// intentionally NOT claimed here — it is path-ambiguous with Workflows'
// operations surface on a single host, so it remains routed to workflows.
func detectMetastoreResourceType(seg []string) string {
	if len(seg) < 3 || seg[0] != "locations" {
		return ""
	}
	if seg[2] == "services" {
		return "services"
	}
	return ""
}

// firstHostLabel returns the first DNS label of an HTTP Host header, stripping
// any port. It is the emulator's host-token discriminator for services that
// share a canonical path on one origin (currently GKE's "container").
func firstHostLabel(host string) string {
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if i := strings.IndexByte(host, '.'); i >= 0 {
		return host[:i]
	}
	return host
}
