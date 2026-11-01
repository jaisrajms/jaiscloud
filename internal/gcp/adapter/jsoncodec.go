package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// JSONCodec is the generic codec for GCP REST/JSON services routed under
// /v1/projects/{project}/... (Pub/Sub, Secret Manager, KMS, IAM). Detection
// and action derivation are data-driven; per-service providers register the
// resulting "Prefix.Action" keys.
type JSONCodec struct {
	Service string
}

func (c *JSONCodec) ServiceName() string { return c.Service }

// Decode parses a /v1/projects/{project}/... path into a NormalizedRequest.
// Params carry: project, name (full relative resource name), resourceType,
// location (KMS), body, and query parameters. The custom method (":publish",
// ":access", ...) is stripped from the last path segment and folded into the
// action.
func (c *JSONCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	// Cloud Functions v1 publishes its long-running operations top-level
	// (operations/{id}), with no projects segment (J60).
	if len(seg) >= 2 && seg[0] == "v1" && seg[1] == "operations" {
		return c.decodeTopLevelOperations(r, body, seg)
	}
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+1 >= len(seg) {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource path", 404)
	}
	rest := seg[pi+2:]

	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = seg[pi+1]
	// Carry the API version so the provider can emit the right resource shape
	// (v1 status vs v2 state/buildConfig/serviceConfig). v1 stays the default.
	apiVersion := "v1"
	if len(seg) > 0 && seg[0] == "v2" {
		apiVersion = "v2"
	}
	nr.Params["apiVersion"] = apiVersion
	queryToParams(r, nr.Params)
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	custom := ""
	if len(rest) > 0 {
		last := rest[len(rest)-1]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			custom = last[i+1:]
			rest[len(rest)-1] = last[:i]
		}
	}

	resourceType := detectResourceType(rest)
	name := strings.Join(rest, "/") // full relative resource name

	nr.Params["resourceType"] = resourceType
	nr.Params["name"] = name

	// KMS: surface the location segment for resource-name reconstruction.
	if resourceType == "keyRings" || resourceType == "cryptoKeys" {
		for i, s := range rest {
			if s == "locations" && i+1 < len(rest) {
				nr.Params["location"] = rest[i+1]
				break
			}
		}
	}

	// Cloud Functions (functions + the v2 runtimes catalog): surface the
	// location segment for store scoping / request validation.
	if resourceType == "functions" || resourceType == "runtimes" {
		for i, s := range rest {
			if s == "locations" && i+1 < len(rest) {
				nr.Params["location"] = rest[i+1]
				break
			}
		}
	}

	// Cloud Workflows (management + executions + operations): surface the
	// location segment for store scoping.
	if resourceType == "workflows" || resourceType == "executions" || resourceType == "operations" {
		for i, s := range rest {
			if s == "locations" && i+1 < len(rest) {
				nr.Params["location"] = rest[i+1]
				break
			}
		}
	}

	// Eventarc (triggers/channels/providers and the advanced surface): surface
	// the location segment for store scoping and resource-name reconstruction.
	switch resourceType {
	case "triggers", "channels", "providers", "messageBuses", "enrollments",
		"pipelines", "googleApiSources", "channelConnections", "googleChannelConfig":
		for i, s := range rest {
			if s == "locations" && i+1 < len(rest) {
				nr.Params["location"] = rest[i+1]
				break
			}
		}
	}

	// Memorystore for Redis: surface the location and instance id for store
	// scoping and resource-name reconstruction.
	if resourceType == "instances" || resourceType == "locations" {
		for i, s := range rest {
			if s == "locations" && i+1 < len(rest) {
				nr.Params["location"] = rest[i+1]
				break
			}
		}
		if resourceType == "instances" {
			if id := segmentAfter(rest, "instances"); id != "" {
				nr.Params["instanceId"] = id
			}
		}
	}

	isCollection := len(rest) > 0 && rest[len(rest)-1] == resourceType

	nr.Action = deriveAction(resourceType, isCollection, name, r.Method, custom, apiVersion)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 501)
	}
	return nr, nil
}

// Encode serialises a provider response as JSON.
func (c *JSONCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	if raw, ok := resp.Data[wire.RawJSONKey].(json.RawMessage); ok {
		return status, headers, raw
	}
	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as a GCP error envelope.
func (c *JSONCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	statusStr, _ := gcperr.Resolve(perr)
	errObj := map[string]any{
		"code":    status,
		"message": perr.Message,
		"status":  statusStr,
	}
	if details := gcpErrorDetails(perr.Details); len(details) > 0 {
		errObj["details"] = details
	}
	env := map[string]any{"error": errObj}
	out, _ := json.Marshal(env)
	return status, headers, out
}

// gcpErrorDetails renders ProviderError.Details as the GCP error envelope's
// "details" array (each entry tagged with its google.rpc @type). It currently
// models google.rpc.ErrorInfo.
func gcpErrorDetails(details []model.ErrorDetail) []map[string]any {
	out := make([]map[string]any, 0, len(details))
	for _, d := range details {
		if d.Type == "" {
			continue
		}
		entry := map[string]any{"@type": "type.googleapis.com/" + d.Type}
		if d.Reason != "" {
			entry["reason"] = d.Reason
		}
		if d.Domain != "" {
			entry["domain"] = d.Domain
		}
		if len(d.Metadata) > 0 {
			entry["metadata"] = d.Metadata
		}
		out = append(out, entry)
	}
	return out
}

// detectResourceType returns the GCP resource-type marker from the path
// segments (after projects/{project}). cryptoKeys wins over keyRings since a
// cryptoKey path also contains "keyRings"; cryptoKeyVersions wins over
// cryptoKeys since a version path also contains "cryptoKeys".
func detectResourceType(segs []string) string {
	// Memorystore for Redis: locations/{location}/instances[/{id}], plus the
	// bare location-discovery paths locations[/{location}]. The "instances"
	// marker is unique to Memorystore among the emulator's
	// /v1/projects/{p}/locations/{l}/... services, so it can be claimed before
	// the generic scan below.
	if len(segs) >= 1 && segs[0] == "locations" {
		if len(segs) >= 3 && segs[2] == "instances" {
			return "instances"
		}
		if len(segs) >= 3 && segs[2] == "backups" {
			// Firestore Admin backups: locations/{loc}/backups[/{id}]. Matched
			// by position rather than a bare segment scan, so Memorystore's
			// locations/{loc}/backupCollections/{c}/backups is not retyped as
			// Firestore.
			return "backups"
		}
		if len(segs) <= 2 {
			return "locations"
		}
	}
	// Firestore Admin: projects.databases[/{db}] is the database control plane.
	// It is claimed here only as the terminal resource — a deeper marker
	// (documents, collectionGroups, ...) owns the path and is detected below —
	// so the pre-loop length check keeps the data-plane detection intact.
	if len(segs) >= 1 && segs[0] == "databases" && len(segs) <= 2 {
		return "databases"
	}
	// Firestore Admin long-running operations:
	// projects.databases.{db}.operations[/{op}]. Claimed before the generic
	// "operations" marker, which would otherwise route the poll path to the
	// shared Workflows LRO surface (a distinct resource type keeps the shared
	// operations semantics untouched).
	if len(segs) >= 3 && segs[0] == "databases" && segs[2] == "operations" {
		return "databasesOperations"
	}
	var hasKeyRings, hasCryptoKeys, hasVersions, hasServiceAccounts bool
	var hasWorkflows, hasExecutions bool
	for _, s := range segs {
		switch s {
		case "topics":
			return "topics"
		case "subscriptions":
			return "subscriptions"
		case "secrets":
			return "secrets"
		case "documents":
			// Firestore document paths always contain the "documents" marker
			// after "databases/{db}", so it wins over the generic "keys" /
			// "serviceAccounts" cases below (a document collection could be
			// named "keys").
			return "documents"
		case "indexes":
			// Firestore composite-index admin paths
			// (.../collectionGroups/{cg}/indexes[/{id}]).
			return "indexes"
		case "backupSchedules":
			// Firestore Admin backup schedules
			// (.../databases/{db}/backupSchedules[/{id}]).
			return "backupSchedules"
		case "userCreds":
			// Firestore Admin user creds
			// (.../databases/{db}/userCreds[/{id}]).
			return "userCreds"
		case "fields":
			// Firestore Admin collection-group fields
			// (.../databases/{db}/collectionGroups/{cg}/fields[/{fieldPath}]).
			return "fields"
		case "keys":
			return "keys" // service account keys
		case "importJobs":
			// Cloud KMS import jobs (…/keyRings/{kr}/importJobs[/{id}]).
			return "importJobs"
		case "cryptoKeyVersions":
			hasVersions = true
		case "cryptoKeys":
			hasCryptoKeys = true
		case "keyRings":
			hasKeyRings = true
		case "serviceAccounts":
			hasServiceAccounts = true
		case "functions":
			return "functions"
		case "runtimes":
			// Cloud Functions v2 runtime catalog
			// (…/projects/{p}/locations/{l}/runtimes).
			return "runtimes"
		case "operations":
			// Workflows long-running operations (…/locations/{l}/operations/{id}).
			return "operations"
		case "executions":
			// Workflow executions nest under …/workflows/{w}/executions, so this
			// must win over the "workflows" marker below.
			hasExecutions = true
		case "workflows":
			hasWorkflows = true
		case "triggers":
			// Eventarc triggers (…/locations/{l}/triggers[/{t}]).
			return "triggers"
		case "channels":
			// Eventarc channels (…/locations/{l}/channels[/{c}]).
			return "channels"
		case "providers":
			// Eventarc providers (…/locations/{l}/providers[/{p}]) — read-only
			// discovery.
			return "providers"
		case "workloadIdentityPools":
			// IAM Credentials workload identity pools
			// (…/locations/{l}/workloadIdentityPools[/{pool}]). Only
			// custom-method discovery (getAllowedLocations) is modelled.
			return "workloadIdentityPools"
		case "messageBuses":
			// Eventarc message buses (…/locations/{l}/messageBuses[/{b}]).
			return "messageBuses"
		case "enrollments":
			// Eventarc enrollments (…/locations/{l}/enrollments[/{e}]).
			return "enrollments"
		case "pipelines":
			// Eventarc pipelines (…/locations/{l}/pipelines[/{p}]).
			return "pipelines"
		case "googleApiSources":
			// Eventarc Google API sources
			// (…/locations/{l}/googleApiSources[/{s}]).
			return "googleApiSources"
		case "channelConnections":
			// Eventarc channel connections
			// (…/locations/{l}/channelConnections[/{c}]).
			return "channelConnections"
		case "googleChannelConfig":
			// Eventarc Google channel config — a per-location singleton
			// (…/locations/{l}/googleChannelConfig).
			return "googleChannelConfig"
		}
	}
	if hasExecutions {
		return "executions"
	}
	if hasWorkflows {
		return "workflows"
	}
	if hasVersions {
		return "cryptoKeyVersions"
	}
	if hasCryptoKeys {
		return "cryptoKeys"
	}
	if hasKeyRings {
		return "keyRings"
	}
	if hasServiceAccounts {
		return "serviceAccounts"
	}
	return ""
}

// decodeTopLevelOperations decodes the Cloud Functions v1 top-level operations
// surface: GET /v1/operations (list) and GET /v1/operations/{id} (get). The path
// has no projects segment, so it is decoded directly instead of through the
// generic /v1/projects/... parser. The location is recovered from the persisted
// operation by the core (see functions.ParseOperationName).
func (c *JSONCodec) decodeTopLevelOperations(r *http.Request, body []byte, seg []string) (*model.NormalizedRequest, error) {
	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}, Raw: r}
	nr.Params["apiVersion"] = "v1"
	nr.Params["resourceType"] = "operations"
	nr.Params["name"] = strings.Join(seg[1:], "/") // "operations[/{id}]"
	queryToParams(r, nr.Params)
	if m, err := parseJSON(body); err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	} else if m != nil {
		nr.Params["body"] = m
	}
	if len(seg) == 2 {
		nr.Action = "ListOperations"
	} else {
		nr.Action = "GetOperation"
	}
	return nr, nil
}

// deriveAction maps (resourceType, isCollection, name, method, custom method)
// to an action name. apiVersion is "v1" or "v2"; only the Cloud Functions v2
// LRO surface differs between the two (its operation verbs are location-scoped
// and unambiguous, unlike the shared /v1 operations path).
func deriveAction(resourceType string, isCollection bool, name, method, custom, apiVersion string) string {
	// IAM Service Account Credentials' getAllowedLocations Discovery path is a
	// plain trailing resource segment (…/serviceAccounts/{sa}/allowedLocations,
	// …/workloadIdentityPools/{pool}/allowedLocations), not a custom verb. The
	// router routes those paths to iamcredentials; this derives the action its
	// provider registers. The ":getAllowedLocations" verb form (below) is still
	// accepted for older clients.
	if custom == "" && strings.HasSuffix(name, "/allowedLocations") {
		return "GetAllowedLocations"
	}
	// Cloud Functions v2 runtime catalog. `runtimes.list` is the only v2
	// runtime method real GCP declares (no get), and the resource exists only
	// in v2, so a non-list method fails loud.
	if apiVersion == "v2" && resourceType == "runtimes" {
		if isCollection && method == http.MethodGet {
			return "ListRuntimes"
		}
		return ""
	}
	// Cloud Functions v2 long-running operations. These reuse the standard
	// google.longrunning action names; the v1 path is untouched because its
	// operations route through the shared Workflows surface.
	if apiVersion == "v2" && resourceType == "operations" {
		switch {
		case custom == "cancel":
			return "CancelOperation"
		case custom == "wait":
			return "WaitOperation"
		case isCollection && method == http.MethodGet:
			return "ListOperations"
		case method == http.MethodGet:
			return "GetOperation"
		case method == http.MethodDelete:
			return "DeleteOperation"
		}
	}
	if custom != "" {
		switch resourceType {
		case "topics":
			switch custom {
			case "publish":
				return "TopicPublish"
			case "getIamPolicy":
				return "TopicGetIamPolicy"
			case "setIamPolicy":
				return "TopicSetIamPolicy"
			case "testIamPermissions":
				return "TopicTestIamPermissions"
			}
		case "subscriptions":
			switch custom {
			case "detach":
				return "SubscriptionDetach"
			case "pull":
				return "SubscriptionPull"
			case "acknowledge":
				return "SubscriptionAcknowledge"
			case "modifyAckDeadline":
				return "SubscriptionModifyAckDeadline"
			case "seek":
				return "SubscriptionSeek"
			case "modifyPushConfig":
				return "SubscriptionModifyPushConfig"
			case "getIamPolicy":
				return "SubscriptionGetIamPolicy"
			case "setIamPolicy":
				return "SubscriptionSetIamPolicy"
			case "testIamPermissions":
				return "SubscriptionTestIamPermissions"
			}
		case "secrets":
			switch custom {
			case "addVersion":
				return "AddVersion"
			case "access":
				return "Access"
			case "destroy":
				return "DestroyVersion"
			case "disable":
				return "DisableVersion"
			case "enable":
				return "EnableVersion"
			case "enableManagedRotation":
				return "EnableManagedRotation"
			case "rotateSecret":
				return "RotateSecret"
			case "getIamPolicy":
				return "GetIamPolicy"
			case "setIamPolicy":
				return "SetIamPolicy"
			case "testIamPermissions":
				return "TestIamPermissions"
			}
		case "cryptoKeys":
			switch custom {
			case "encrypt":
				return "CryptoKeyEncrypt"
			case "decrypt":
				return "CryptoKeyDecrypt"
			case "updatePrimaryVersion":
				return "CryptoKeyUpdatePrimaryVersion"
			case "getIamPolicy":
				return "CryptoKeyGetIamPolicy"
			case "setIamPolicy":
				return "CryptoKeySetIamPolicy"
			case "testIamPermissions":
				return "CryptoKeyTestIamPermissions"
			}
		case "cryptoKeyVersions":
			// Real KMS has no version-level :disable/:enable or IAM custom
			// methods: a version's state is changed through
			// cryptoKeyVersions.patch (handled by the method-based switch
			// below) and IAM is scoped to key rings, crypto keys and import
			// jobs. Any other custom verb is therefore unrecognized and returns
			// "" (fail loud).
			switch custom {
			case "destroy":
				return "CryptoKeyVersionDestroy"
			case "restore":
				return "CryptoKeyVersionRestore"
			case "asymmetricSign":
				return "CryptoKeyVersionAsymmetricSign"
			case "asymmetricDecrypt":
				return "CryptoKeyVersionAsymmetricDecrypt"
			case "macSign":
				return "CryptoKeyVersionMacSign"
			case "macVerify":
				return "CryptoKeyVersionMacVerify"
			case "rawEncrypt":
				return "CryptoKeyVersionRawEncrypt"
			case "rawDecrypt":
				return "CryptoKeyVersionRawDecrypt"
			case "import":
				return "CryptoKeyVersionImport"
			case "importTrustedKeyWrappedCryptoKeyVersion":
				return "CryptoKeyVersionImportTrusted"
			case "exportTrustedKeyWrappedCryptoKeyVersion":
				return "CryptoKeyVersionExportTrusted"
			case "decapsulate":
				return "CryptoKeyVersionDecapsulate"
			}
		case "importJobs":
			// Cloud KMS import jobs carry their own IAM policy
			// (…/keyRings/{kr}/importJobs/{id}:getIamPolicy and friends).
			switch custom {
			case "getIamPolicy":
				return "ImportJobGetIamPolicy"
			case "setIamPolicy":
				return "ImportJobSetIamPolicy"
			case "testIamPermissions":
				return "ImportJobTestIamPermissions"
			}
		case "locations":
			// Cloud KMS locations.generateRandomBytes (the only custom method
			// declared on a location resource).
			switch custom {
			case "generateRandomBytes":
				return "GenerateRandomBytes"
			}
		case "keyRings":
			switch custom {
			case "getIamPolicy":
				return "KeyRingGetIamPolicy"
			case "setIamPolicy":
				return "KeyRingSetIamPolicy"
			case "testIamPermissions":
				return "KeyRingTestIamPermissions"
			}
		case "serviceAccounts":
			switch custom {
			case "getIamPolicy":
				return "ServiceAccountGetIamPolicy"
			case "setIamPolicy":
				return "ServiceAccountSetIamPolicy"
			case "testIamPermissions":
				return "ServiceAccountTestIamPermissions"
			case "signBlob":
				return "ServiceAccountSignBlob"
			case "signJwt":
				return "ServiceAccountSignJwt"
			case "disable":
				return "ServiceAccountDisable"
			case "enable":
				return "ServiceAccountEnable"
			case "undelete":
				return "ServiceAccountUndelete"
			// IAM Service Account Credentials (iamcredentials) custom verbs.
			// detectV1Service routes these to the iamcredentials service, whose
			// provider registers "IAMCredentials.<action>".
			case "generateAccessToken":
				return "GenerateAccessToken"
			case "generateIdToken":
				return "GenerateIdToken"
			case "getAllowedLocations":
				return "GetAllowedLocations"
			}
		case "workloadIdentityPools":
			// iamcredentials projects.locations.workloadIdentityPools custom
			// verbs (only getAllowedLocations is modelled).
			switch custom {
			case "getAllowedLocations":
				return "GetAllowedLocations"
			}
		case "keys":
			// Service-account key lifecycle (projects.serviceAccounts.keys).
			switch custom {
			case "disable":
				return "ServiceAccountKeyDisable"
			case "enable":
				return "ServiceAccountKeyEnable"
			case "upload":
				return "ServiceAccountKeyUpload"
			}
		case "documents":
			// Custom methods POSTed to the "documents" collection marker:
			// documents:commit, documents:runQuery, documents:batchWrite, etc.
			switch custom {
			case "commit":
				return "Commit"
			case "runQuery":
				return "RunQuery"
			case "runAggregationQuery":
				return "RunAggregationQuery"
			case "partitionQuery":
				return "PartitionQuery"
			case "executePipeline":
				return "ExecutePipeline"
			case "batchWrite":
				return "BatchWrite"
			case "batchGet":
				return "BatchGet"
			case "beginTransaction":
				return "BeginTransaction"
			case "rollback":
				return "Rollback"
			case "listCollectionIds":
				return "ListCollectionIds"
			}
		case "userCreds":
			// Firestore Admin user-creds verbs (the resource itself is a plain
			// CRUD resource handled by the method switch below).
			switch custom {
			case "enable":
				return "EnableUserCreds"
			case "disable":
				return "DisableUserCreds"
			case "resetPassword":
				return "ResetUserPassword"
			}
		case "functions":
			switch custom {
			case "call":
				return "CallFunction"
			case "generateUploadUrl":
				return "GenerateUploadUrl"
			case "generateDownloadUrl":
				return "GenerateDownloadUrl"
			case "getIamPolicy":
				return "FunctionGetIamPolicy"
			case "setIamPolicy":
				return "FunctionSetIamPolicy"
			case "testIamPermissions":
				return "FunctionTestIamPermissions"
			// v2 1st→2nd gen upgrade / traffic control plane (custom POST
			// verbs on a function name; v2-only in real GCP).
			case "setupFunctionUpgradeConfig":
				return "SetupFunctionUpgradeConfig"
			case "redirectFunctionUpgradeTraffic":
				return "RedirectFunctionUpgradeTraffic"
			case "rollbackFunctionUpgradeTraffic":
				return "RollbackFunctionUpgradeTraffic"
			case "commitFunctionUpgrade":
				return "CommitFunctionUpgrade"
			case "commitFunctionUpgradeAsGen2":
				return "CommitFunctionUpgradeAsGen2"
			case "abortFunctionUpgrade":
				return "AbortFunctionUpgrade"
			case "detachFunction":
				return "DetachFunction"
			}
		case "triggers":
			switch custom {
			case "getIamPolicy":
				return "TriggerGetIamPolicy"
			case "setIamPolicy":
				return "TriggerSetIamPolicy"
			case "testIamPermissions":
				return "TriggerTestIamPermissions"
			}
		case "channels":
			switch custom {
			case "getIamPolicy":
				return "ChannelGetIamPolicy"
			case "setIamPolicy":
				return "ChannelSetIamPolicy"
			case "testIamPermissions":
				return "ChannelTestIamPermissions"
			}
		case "messageBuses":
			switch custom {
			case "getIamPolicy":
				return "MessageBusGetIamPolicy"
			case "setIamPolicy":
				return "MessageBusSetIamPolicy"
			case "testIamPermissions":
				return "MessageBusTestIamPermissions"
			case "listEnrollments":
				return "ListMessageBusEnrollments"
			}
		case "enrollments":
			switch custom {
			case "getIamPolicy":
				return "EnrollmentGetIamPolicy"
			case "setIamPolicy":
				return "EnrollmentSetIamPolicy"
			case "testIamPermissions":
				return "EnrollmentTestIamPermissions"
			}
		case "pipelines":
			switch custom {
			case "getIamPolicy":
				return "PipelineGetIamPolicy"
			case "setIamPolicy":
				return "PipelineSetIamPolicy"
			case "testIamPermissions":
				return "PipelineTestIamPermissions"
			}
		case "googleApiSources":
			switch custom {
			case "getIamPolicy":
				return "GoogleApiSourceGetIamPolicy"
			case "setIamPolicy":
				return "GoogleApiSourceSetIamPolicy"
			case "testIamPermissions":
				return "GoogleApiSourceTestIamPermissions"
			}
		case "channelConnections":
			switch custom {
			case "getIamPolicy":
				return "ChannelConnectionGetIamPolicy"
			case "setIamPolicy":
				return "ChannelConnectionSetIamPolicy"
			case "testIamPermissions":
				return "ChannelConnectionTestIamPermissions"
			}
		case "instances":
			switch custom {
			case "upgrade":
				return "UpgradeInstance"
			case "import":
				return "ImportInstance"
			case "export":
				return "ExportInstance"
			case "failover":
				return "FailoverInstance"
			case "rescheduleMaintenance":
				return "RescheduleMaintenance"
			}
		case "workflows":
			// Workflow revision history: GET …/workflows/{w}:listRevisions.
			switch custom {
			case "listRevisions":
				if method == http.MethodGet {
					return "ListWorkflowRevisions"
				}
			}
		case "operations":
			// The shared google.longrunning operations surface. Only Eventarc
			// declares a `:cancel` on it among the emulator's services
			// (metastore/managedkafka/run/dataproc own their operations through
			// service-specific codecs; Firestore's databasesOperations is
			// separate). A cancel on any other owner's host reaches that
			// owner's provider, which does not register CancelOperation, so it
			// still fails loud.
			switch custom {
			case "cancel":
				return "CancelOperation"
			}
		}
		// An unrecognized custom (:verb) method is never the resource's plain
		// verb. Real GCP declares no such method, so letting one fall through to
		// the method switch below silently misroutes the request — e.g.
		// POST …/secrets:zzz → Create, POST …/serviceAccounts/{sa}/keys:zzz →
		// ServiceAccountKeyCreate, POST …/databases/{db}/documents:zzz →
		// CreateDocument, or POST …/databases:restore → CreateDatabase. This
		// block is therefore the codec's single fail-loud gate for a stray verb:
		// the method-switch arms below only ever see a request with no custom
		// verb (only the databasesOperations pass-through below is exempt), so
		// they carry no per-arm `custom == ""` guard (TD24).
		//
		// The Firestore databasesOperations surface is the exception: its
		// `:cancel` is derived in the method switch below, so keep its
		// pass-through. (Cloud Functions v2's operations `:cancel`/`:wait` never
		// reach here — they return from the v2 block above.)
		if resourceType != "databasesOperations" {
			return ""
		}
	}

	switch resourceType {
	case "topics":
		switch {
		case method == http.MethodPut && !isCollection:
			return "TopicCreate"
		case method == http.MethodPatch && !isCollection:
			// topics.patch (updateMask) — the REST update the official clients
			// send; mirrors gRPC UpdateTopic.
			return "TopicUpdate"
		case isCollection && method == http.MethodGet:
			return "TopicList"
		case method == http.MethodGet:
			return "TopicGet"
		case method == http.MethodDelete:
			return "TopicDelete"
		}
	case "subscriptions":
		switch {
		case method == http.MethodPatch && !isCollection:
			return "SubscriptionUpdate"
		case method == http.MethodPut && !isCollection:
			return "SubscriptionCreate"
		case isCollection && method == http.MethodGet:
			return "SubscriptionList"
		case method == http.MethodGet:
			return "SubscriptionGet"
		case method == http.MethodDelete:
			return "SubscriptionDelete"
		}
	case "secrets":
		switch {
		case isCollection && method == http.MethodPost:
			return "Create"
		case isCollection && method == http.MethodGet:
			return "List"
		case method == http.MethodPatch:
			return "Update"
		case method == http.MethodDelete:
			return "Delete"
		case method == http.MethodGet && isSecretVersionsCollection(name):
			return "ListVersions"
		case method == http.MethodGet && strings.Contains(name, "/versions/"):
			return "GetVersion"
		case method == http.MethodGet:
			return "Get"
		}
	case "keyRings":
		switch {
		case isCollection && method == http.MethodPost:
			return "KeyRingCreate"
		case isCollection && method == http.MethodGet:
			return "KeyRingList"
		case method == http.MethodGet:
			return "KeyRingGet"
		}
	case "cryptoKeys":
		switch {
		case isCollection && method == http.MethodPost:
			return "CryptoKeyCreate"
		case isCollection && method == http.MethodGet:
			return "CryptoKeyList"
		case method == http.MethodGet:
			return "CryptoKeyGet"
		case method == http.MethodPatch:
			// cryptoKeys.patch (updateMask) — the real REST update the official
			// clients send; mirrors gRPC UpdateCryptoKey.
			return "CryptoKeyUpdate"
		case method == http.MethodDelete:
			return "CryptoKeyDelete"
		}
	case "importJobs":
		switch {
		case isCollection && method == http.MethodPost:
			return "ImportJobCreate"
		case isCollection && method == http.MethodGet:
			return "ImportJobList"
		case method == http.MethodGet:
			return "ImportJobGet"
		}
	case "cryptoKeyVersions":
		// Only non-custom requests dispatch here: an unrecognized custom verb
		// (e.g. a removed :disable/:enable or version :getIamPolicy) is refused
		// by the custom-verb block above before the method switch, so these arms
		// need no per-arm guard.
		switch {
		case method == http.MethodGet && strings.HasSuffix(name, "/publicKey"):
			return "CryptoKeyVersionGetPublicKey"
		case isCollection && method == http.MethodPost:
			return "CryptoKeyVersionCreate"
		case isCollection && method == http.MethodGet:
			return "CryptoKeyVersionList"
		case method == http.MethodPatch:
			return "CryptoKeyVersionUpdate"
		case method == http.MethodDelete:
			return "CryptoKeyVersionDelete"
		case method == http.MethodGet:
			return "CryptoKeyVersionGet"
		}
	case "serviceAccounts":
		switch {
		case isCollection && method == http.MethodPost:
			return "ServiceAccountCreate"
		case isCollection && method == http.MethodGet:
			return "ServiceAccountList"
		case method == http.MethodPatch:
			// serviceAccounts.patch (updateMask) — what the gax/Java client sends.
			return "ServiceAccountPatch"
		case method == http.MethodPut:
			// serviceAccounts.update (full replace) — real GCP serves update as PUT.
			return "ServiceAccountUpdate"
		case method == http.MethodGet:
			return "ServiceAccountGet"
		case method == http.MethodDelete:
			return "ServiceAccountDelete"
		}
	case "keys":
		switch {
		case isCollection && method == http.MethodPost:
			return "ServiceAccountKeyCreate"
		case isCollection && method == http.MethodGet:
			return "ServiceAccountKeyList"
		case method == http.MethodGet:
			return "ServiceAccountKeyGet"
		case method == http.MethodDelete:
			return "ServiceAccountKeyDelete"
		}
	case "documents":
		// Firestore: an even (positive) number of segments after "documents"
		// is a document; an odd count is a collection. Custom methods
		// (:commit, :runQuery, :batchWrite, :beginTransaction, :rollback,
		// :batchGet, :listCollectionIds) are handled by the custom switch
		// above.
		segs := segmentsAfterDocuments(name)
		isDoc := segs > 0 && segs%2 == 0
		switch {
		case method == http.MethodPost && !isDoc:
			return "CreateDocument"
		case method == http.MethodGet && isDoc:
			return "GetDocument"
		case method == http.MethodGet && !isDoc:
			// documents.list / documents.listDocuments share one wire path
			// (GET on a collection); both are served by the ListDocuments
			// handler.
			return "ListDocuments"
		case method == http.MethodPatch && isDoc:
			return "PatchDocument"
		case method == http.MethodDelete && isDoc:
			return "DeleteDocument"
		}
	case "indexes":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateIndex"
		case isCollection && method == http.MethodGet:
			return "ListIndexes"
		case method == http.MethodGet:
			return "GetIndex"
		case method == http.MethodDelete:
			return "DeleteIndex"
		}
	case "databases":
		// Firestore Admin database control plane (projects.databases). Custom
		// verbs are handled by the custom switch above (which fails loud).
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateDatabase"
		case isCollection && method == http.MethodGet:
			return "ListDatabases"
		case method == http.MethodGet:
			return "GetDatabase"
		case method == http.MethodPatch:
			return "UpdateDatabase"
		case method == http.MethodDelete:
			return "DeleteDatabase"
		}
	case "backupSchedules":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateBackupSchedule"
		case isCollection && method == http.MethodGet:
			return "ListBackupSchedules"
		case method == http.MethodGet:
			return "GetBackupSchedule"
		case method == http.MethodPatch:
			return "UpdateBackupSchedule"
		case method == http.MethodDelete:
			return "DeleteBackupSchedule"
		}
	case "userCreds":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateUserCreds"
		case isCollection && method == http.MethodGet:
			return "ListUserCreds"
		case method == http.MethodGet:
			return "GetUserCreds"
		case method == http.MethodDelete:
			return "DeleteUserCreds"
		}
	case "fields":
		switch {
		case isCollection && method == http.MethodGet:
			return "ListFields"
		case method == http.MethodGet:
			return "GetField"
		case method == http.MethodPatch:
			return "UpdateField"
		}
	case "backups":
		switch {
		case isCollection && method == http.MethodGet:
			return "ListBackups"
		case method == http.MethodGet:
			return "GetBackup"
		case method == http.MethodDelete:
			return "DeleteBackup"
		}
	case "databasesOperations":
		// Firestore Admin long-running operations:
		// projects.databases.{db}.operations[/{op}]. The resource marker is
		// "operations", so collection-ness is derived from the path rather than
		// the resource type.
		isColl := strings.HasSuffix(name, "/operations")
		switch {
		case custom == "cancel":
			return "CancelOperation"
		case isColl && method == http.MethodGet:
			return "ListOperations"
		case method == http.MethodGet:
			return "GetOperation"
		case method == http.MethodDelete:
			return "DeleteOperation"
		}
	case "functions":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateFunction"
		case isCollection && method == http.MethodGet:
			return "ListFunctions"
		case method == http.MethodPatch:
			return "UpdateFunction"
		case method == http.MethodDelete:
			return "DeleteFunction"
		case method == http.MethodGet:
			return "GetFunction"
		}
	case "workflows":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateWorkflow"
		case isCollection && method == http.MethodGet:
			return "ListWorkflows"
		case method == http.MethodPatch:
			return "UpdateWorkflow"
		case method == http.MethodDelete:
			return "DeleteWorkflow"
		case method == http.MethodGet:
			return "GetWorkflow"
		}
	case "operations":
		// Cloud Workflows' location-scoped operations surface. The Discovery
		// declares list, get and delete (no cancel), so a collection GET lists
		// and an item GET/DELETE gets/deletes. A collection-level DELETE is not
		// a declared method and stays fail-loud.
		switch {
		case isCollection && method == http.MethodGet:
			return "ListOperations"
		case method == http.MethodGet:
			return "GetOperation"
		case method == http.MethodDelete && !isCollection:
			return "DeleteOperation"
		}
	case "triggers":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateTrigger"
		case isCollection && method == http.MethodGet:
			return "ListTriggers"
		case method == http.MethodPatch:
			return "UpdateTrigger"
		case method == http.MethodDelete:
			return "DeleteTrigger"
		case method == http.MethodGet:
			return "GetTrigger"
		}
	case "channels":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateChannel"
		case isCollection && method == http.MethodGet:
			return "ListChannels"
		case method == http.MethodPatch:
			return "UpdateChannel"
		case method == http.MethodDelete:
			return "DeleteChannel"
		case method == http.MethodGet:
			return "GetChannel"
		}
	case "providers":
		switch {
		case isCollection && method == http.MethodGet:
			return "ListProviders"
		case method == http.MethodGet:
			return "GetProvider"
		}
	case "messageBuses":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateMessageBus"
		case isCollection && method == http.MethodGet:
			return "ListMessageBuses"
		case method == http.MethodPatch:
			return "UpdateMessageBus"
		case method == http.MethodDelete:
			return "DeleteMessageBus"
		case method == http.MethodGet:
			return "GetMessageBus"
		}
	case "enrollments":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateEnrollment"
		case isCollection && method == http.MethodGet:
			return "ListEnrollments"
		case method == http.MethodPatch:
			return "UpdateEnrollment"
		case method == http.MethodDelete:
			return "DeleteEnrollment"
		case method == http.MethodGet:
			return "GetEnrollment"
		}
	case "pipelines":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreatePipeline"
		case isCollection && method == http.MethodGet:
			return "ListPipelines"
		case method == http.MethodPatch:
			return "UpdatePipeline"
		case method == http.MethodDelete:
			return "DeletePipeline"
		case method == http.MethodGet:
			return "GetPipeline"
		}
	case "googleApiSources":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateGoogleApiSource"
		case isCollection && method == http.MethodGet:
			return "ListGoogleApiSources"
		case method == http.MethodPatch:
			return "UpdateGoogleApiSource"
		case method == http.MethodDelete:
			return "DeleteGoogleApiSource"
		case method == http.MethodGet:
			return "GetGoogleApiSource"
		}
	case "channelConnections":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateChannelConnection"
		case isCollection && method == http.MethodGet:
			return "ListChannelConnections"
		case method == http.MethodDelete:
			return "DeleteChannelConnection"
		case method == http.MethodGet:
			return "GetChannelConnection"
		}
	case "googleChannelConfig":
		switch method {
		case http.MethodPatch:
			return "UpdateGoogleChannelConfig"
		case http.MethodGet:
			return "GetGoogleChannelConfig"
		}
	case "instances":
		switch {
		case method == http.MethodGet && strings.HasSuffix(name, "/authString"):
			// Memorystore instances.getAuthString (GET …/instances/{i}/authString).
			// It is a sub-resource path, not a custom verb, so without this
			// guard the plain item GET would derive GetInstance and answer the
			// instance object.
			return "GetAuthString"
		case isCollection && method == http.MethodPost:
			return "CreateInstance"
		case isCollection && method == http.MethodGet:
			return "ListInstances"
		case method == http.MethodGet:
			return "GetInstance"
		case method == http.MethodPatch:
			return "UpdateInstance"
		case method == http.MethodDelete:
			return "DeleteInstance"
		}
	case "locations":
		switch {
		case isCollection && method == http.MethodGet:
			return "ListLocations"
		case method == http.MethodGet:
			return "GetLocation"
		}
	}
	return ""
}

// segmentAfter returns the path segment immediately following marker in segs,
// or "" when marker is absent or last.
func segmentAfter(segs []string, marker string) string {
	for i, s := range segs {
		if s == marker && i+1 < len(segs) {
			return segs[i+1]
		}
	}
	return ""
}

// isSecretVersionsCollection reports whether name is a secret's versions
// collection path ("secrets/{id}/versions"). It is distinct from a secret
// literally named "versions" ("secrets/versions", two segments) and from a
// single version ("secrets/{id}/versions/{v}", four segments), neither of which
// is a versions list.
func isSecretVersionsCollection(name string) bool {
	parts := strings.Split(name, "/")
	return len(parts) == 3 && parts[0] == "secrets" && parts[2] == "versions"
}

// segmentsAfterDocuments returns the number of path segments after the
// "documents" marker in a Firestore resource name (e.g.
// "databases/(default)/documents/cities/SF" → 2). An even (positive) count is a
// document; an odd count is a collection.
func segmentsAfterDocuments(name string) int {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		if p == "documents" {
			return len(parts) - i - 1
		}
	}
	return 0
}
