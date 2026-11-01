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
	// Managed Kafka and Dataproc Metastore share the bare
	// /v1/projects/{project}/locations/{location}/operations[/{id}] LRO path with
	// Cloud Workflows on the single emulator origin, and the operation resource
	// name carries no service discriminator (it is
	// projects/{p}/locations/{l}/operations/{id} for every owner). Their canonical
	// hosts — managedkafka.googleapis.com / metastore.googleapis.com, or the
	// managedkafka.localhost / metastore.localhost test forms — disambiguate it:
	// an operations request to that host resolves to the owning service instead of
	// Workflows. Every other path is left to path detection, which already owns
	// the clusters/services resource segments; the default host keeps the bare
	// operations path on Workflows (that path also backs the Cloud Workflows LRO
	// surface).
	if svc := operationsHostService(firstHostLabel(r.Host), r.URL.EscapedPath()); svc != "" {
		return svc, SourceHost
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
		"upload/bigquery/", "resumable/upload/", "batch/storage/", "projects/",
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
	// The jobs.insert media-upload endpoint (load_table_from_file) lives under
	// /upload/bigquery/v2/ and /resumable/upload/bigquery/v2/, not under the
	// service's normal /bigquery/v2/ prefix. It is unambiguous — no other
	// emulated service claims /upload/bigquery/ — so claim it here before the
	// GCS raw-media fallback (a resumable chunk is a PUT).
	if isBigQueryUploadPath(path) {
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
	// SDK test clients concatenate an endpoint that may already end in "/" with
	// "/v1/..." paths, yielding a leading "//". Collapse it (mirrors
	// detectV2Service) so the top-level "/v1/operations" guard and the segment
	// scan below stay stable.
	path = "/" + strings.TrimLeft(path, "/")
	seg := splitEscaped(path)
	// IAM Service Account Credentials' getAllowedLocations is a plain trailing
	// resource segment in the real Discovery documents
	// (…/serviceAccounts/{sa}/allowedLocations and
	// …/workloadIdentityPools/{pool}/allowedLocations), not the
	// ":getAllowedLocations" custom verb the other iamcredentials methods use.
	// It shares the serviceAccounts path with IAM, so the trailing segment is
	// the discriminator (and it must run before the resource-type switch, which
	// would otherwise claim the serviceAccounts path for iam).
	if len(seg) > 0 && seg[len(seg)-1] == "allowedLocations" {
		return "iamcredentials"
	}
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
	// Cloud Resource Manager collection route: /v1/projects (the "projects"
	// segment is the last segment) is projects.list (GET) / projects.create
	// (POST). It has no project segment, so the project-last guard below does
	// not match.
	if pi >= 0 && pi == len(seg)-1 {
		return "resourcemanager"
	}
	// Cloud Resource Manager project surface: when the project segment is the
	// LAST segment there is no trailing resource type — either the bare project
	// resource (GET /v1/projects/{project} → projects.get) or a project-segment
	// custom method (POST /v1/projects/{project}:getIamPolicy|setIamPolicy|
	// testIamPermissions) / delete (DELETE) / undelete (POST). This must run
	// before the pi+2 guard below, which assumes a trailing resource-type
	// segment and would otherwise return "".
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
	// Cloud KMS locations.generateRandomBytes shares the bare
	// locations[/{location}] path with Memorystore's location discovery, which
	// is otherwise claimed below. The verb is declared only by Cloud KMS, so it
	// discriminates before the Memorystore guard.
	if custom == "generateRandomBytes" {
		return "kms"
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
	// Cloud Monitoring's REST-only v1 dashboards API
	// (/v1/projects/{project}/dashboards[/{id}]) is the only v1 surface the
	// Monitoring service owns. Claim it before the generic resource-type switch,
	// which has no dashboards arm (and before the GCS raw-media fallback).
	if len(rest) > 0 && rest[0] == "dashboards" {
		return "monitoring"
	}
	switch detectResourceType(rest) {
	case "topics", "subscriptions":
		return "pubsub"
	case "secrets":
		return "secretmanager"
	case "keyRings", "cryptoKeys", "cryptoKeyVersions", "importJobs":
		return "kms"
	case "serviceAccounts", "keys":
		return "iam"
	case "documents", "indexes", "databases", "databasesOperations", "backupSchedules", "userCreds", "fields", "backups":
		return "firestore"
	case "functions":
		return "functions"
	case "workflows", "operations":
		return "workflows"
	case "executions":
		return "workflowexecutions"
	case "triggers", "channels", "providers", "messageBuses", "enrollments",
		"pipelines", "googleApiSources", "channelConnections", "googleChannelConfig":
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
// ({scope}/{scopeID}/{sinks,exclusions}), logs-based metrics
// ({scope}/{scopeID}/metrics), and the Admin v2 plane
// ({scope}/{scopeID}/locations/{location}/{buckets,logScopes} plus the
// settings/cmekSettings records); Cloud Functions owns
// /v2/projects/{project}/locations/... (functions/operations). The two do not
// collide: Logging's log paths sit directly under a project, while Functions
// always has a locations segment next, and the Admin v2 bucket/logScopes
// families are routed explicitly before the Functions fallback.
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
	// Cloud Logging Admin v2: buckets/views/links under
	// /v2/{scope}/{scopeID}/locations/{location}..., and the per-scope
	// settings/cmekSettings records. These would otherwise be claimed by the
	// /v2/projects/{p}/locations fallback (Cloud Functions).
	if isLoggingAdminPath(seg) {
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

// isLoggingAdminPath reports whether seg is a Cloud Logging Admin v2 path:
// /v2/{scope}/{scopeID}/locations/{location}/buckets... (buckets, and the views
// and links nested under them), /v2/{scope}/{scopeID}/locations/{location}/logScopes...,
// or the per-scope settings/cmekSettings records at either the container or the
// location level.
func isLoggingAdminPath(seg []string) bool {
	if len(seg) < 4 || !servicelogging.IsLogScope(seg[1]) || seg[2] == "" {
		return false
	}
	if seg[3] == "settings" || seg[3] == "cmekSettings" {
		return len(seg) == 4
	}
	if seg[3] != "locations" || len(seg) < 6 || seg[4] == "" {
		return false
	}
	switch seg[5] {
	case "buckets", "buckets:createAsync", "logScopes", "settings", "cmekSettings":
		return true
	}
	return false
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
// The provider still registers GetOperation/ListOperations for direct dispatch,
// and the service's own host token (managedkafka.localhost /
// managedkafka.googleapis.com) routes the operations family to Managed Kafka
// instead; see operationsHostService.
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
// projects/{project} form locations/{location}/services, or "federations" for
// locations/{location}/federations, else "". The "services" and "federations"
// segments are unique to Dataproc Metastore among the emulator's
// /v1/projects/{project}/locations/{location}/... services (workflows uses
// "workflows", functions uses "functions", KMS uses "keyRings", Managed Kafka
// uses "clusters", Cloud Scheduler uses "jobs"). The shared
// locations/{location}/operations/{id} LRO path is intentionally NOT claimed
// here — it is path-ambiguous with Workflows' operations surface on a single
// host, so it remains routed to workflows (the service's own host token routes
// it to Metastore instead; see operationsHostService).
func detectMetastoreResourceType(seg []string) string {
	if len(seg) < 3 || seg[0] != "locations" {
		return ""
	}
	switch seg[2] {
	case "services", "federations":
		return seg[2]
	}
	return ""
}

// operationsHostService maps a request host token plus path to the service that
// owns the shared /v1/projects/{project}/locations/{location}/operations[/{id}]
// LRO family. Managed Kafka, Dataproc Metastore and Eventarc each address their
// operations under their own canonical host (managedkafka.googleapis.com /
// metastore.googleapis.com / eventarc.googleapis.com, first DNS label
// managedkafka/metastore/eventarc). Any other host, or a non-operations path,
// returns "" so detection falls through to the path-based resolver, which keeps
// the bare operations path on Cloud Workflows.
func operationsHostService(hostLabel, path string) string {
	switch hostLabel {
	case "managedkafka", "metastore", "eventarc":
	default:
		return ""
	}
	if !isLocationOperationsPath(path) {
		return ""
	}
	return hostLabel
}

// isLocationOperationsPath reports whether path is the shared
// /v1/projects/{project}/locations/{location}/operations[/{id}] long-running
// operations family. Only /v1/ is considered, mirroring the host-token guard on
// the sibling GKE discriminator (a non-/v1/ path is never a control-plane LRO).
func isLocationOperationsPath(path string) bool {
	if !strings.HasPrefix(path, "/v1/") {
		return false
	}
	seg := splitEscaped(path)
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+2 > len(seg) {
		return false
	}
	rest := seg[pi+2:]
	return len(rest) >= 3 && rest[0] == "locations" && rest[2] == "operations"
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
