package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

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

	// Cloud Functions: surface the location segment for store scoping.
	if resourceType == "functions" {
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

	// Eventarc (triggers/channels/providers): surface the location segment for
	// store scoping and resource-name reconstruction.
	if resourceType == "triggers" || resourceType == "channels" || resourceType == "providers" {
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

	nr.Action = deriveAction(resourceType, isCollection, name, r.Method, custom)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
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
	statusStr := perr.Status
	if statusStr == "" {
		statusStr = gcpStatusString(status)
	}
	env := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": perr.Message,
			"status":  statusStr,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
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
		if len(segs) <= 2 {
			return "locations"
		}
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
		case "keys":
			return "keys" // service account keys
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

// deriveAction maps (resourceType, isCollection, name, method, custom method)
// to an action name.
func deriveAction(resourceType string, isCollection bool, name, method, custom string) string {
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
			}
		case "cryptoKeyVersions":
			switch custom {
			case "destroy":
				return "CryptoKeyVersionDestroy"
			case "disable":
				return "CryptoKeyVersionDisable"
			case "enable":
				return "CryptoKeyVersionEnable"
			case "asymmetricSign":
				return "CryptoKeyVersionAsymmetricSign"
			case "asymmetricDecrypt":
				return "CryptoKeyVersionAsymmetricDecrypt"
			case "macSign":
				return "CryptoKeyVersionMacSign"
			case "macVerify":
				return "CryptoKeyVersionMacVerify"
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
			}
		case "executions":
			switch custom {
			case "cancel":
				return "CancelExecution"
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
		case "instances":
			switch custom {
			case "upgrade":
				return "UpgradeInstance"
			}
		}
	}

	switch resourceType {
	case "topics":
		switch {
		case method == http.MethodPut && !isCollection:
			return "TopicCreate"
		case isCollection && method == http.MethodGet:
			return "TopicList"
		case method == http.MethodGet:
			return "TopicGet"
		case method == http.MethodDelete:
			return "TopicDelete"
		}
	case "subscriptions":
		switch {
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
		}
	case "cryptoKeyVersions":
		switch {
		case method == http.MethodGet && strings.HasSuffix(name, "/publicKey"):
			return "CryptoKeyVersionGetPublicKey"
		case isCollection && method == http.MethodPost:
			return "CryptoKeyVersionCreate"
		case isCollection && method == http.MethodGet:
			return "CryptoKeyVersionList"
		case method == http.MethodGet:
			return "CryptoKeyVersionGet"
		}
	case "serviceAccounts":
		switch {
		case isCollection && method == http.MethodPost:
			return "ServiceAccountCreate"
		case isCollection && method == http.MethodGet:
			return "ServiceAccountList"
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
	case "executions":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateExecution"
		case isCollection && method == http.MethodGet:
			return "ListExecutions"
		case method == http.MethodGet:
			return "GetExecution"
		}
	case "operations":
		switch {
		case method == http.MethodGet:
			return "GetOperation"
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
	case "instances":
		switch {
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
