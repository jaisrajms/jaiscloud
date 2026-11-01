package gcp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// This file is the closed-form derivation table for the generic REST
// `JSONCodec`: every (resource type × method × collection/item) shape the
// emulator serves, every custom (`:verb`) method it knows, and every shape real
// GCP does not define — which must fail loud (501 UnsupportedOperation) rather
// than silently dispatching as a neighbouring verb. It is written as a table
// rather than per-service assertions so a widened `switch` arm, a dropped
// `isCollection`/`custom == ""` guard or an emptied case body is caught by a
// failing row instead of surviving unnoticed (TD20).
//
// Two conventions a reader should keep in mind. First, the HTTP verb on a
// custom (`:verb`) row is documentary — `deriveAction` derives the action from
// the verb segment alone — so each row carries the verb the Discovery declares
// rather than one the codec requires. Second, a cell asserting an unsupported
// shape pins the derivation as it is today; where the shape is one real GCP
// declares, the comment names the recorded gap (TD26) instead of reading
// as the intended contract.

// decodeJSON decodes one method+path through a fresh JSONCodec.
func decodeJSON(t *testing.T, method, path string, body []byte) (*model.NormalizedRequest, error) {
	t.Helper()
	var r *http.Request
	if body == nil {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, strings.NewReader(string(body)))
	}
	return (&JSONCodec{Service: "test"}).Decode(r, body)
}

// wantUnsupported asserts that a shape is refused with 501 UnsupportedOperation
// (the codec's fail-loud contract for a recognized path with no modelled verb).
func wantUnsupported(t *testing.T, what string, nr *model.NormalizedRequest, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: derived action %q, want unsupported (501)", what, nr.Action)
		return
	}
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Errorf("%s: error = %T %v, want *model.ProviderError", what, err, err)
		return
	}
	if perr.HTTPStatus != 501 || perr.Code != "UnsupportedOperation" {
		t.Errorf("%s: error = %d %s (%q), want 501 UnsupportedOperation", what, perr.HTTPStatus, perr.Code, perr.Message)
	}
}

// jsonCodecMethodMatrix is the resource × method × level grid. A method absent
// from a want map is an unsupported shape for that level and must fail loud.
func TestJSONCodecResourceMethodMatrix(t *testing.T) {
	const (
		p   = "/v1/projects/p"
		kms = "/v1/projects/p/locations/us/keyRings/kr"
		loc = "/v1/projects/p/locations/us-central1"
		fst = "/v1/projects/p/databases/db"
	)
	resources := []struct {
		res        string
		coll, item string
		collWant   map[string]string
		itemWant   map[string]string
	}{
		// ── Pub/Sub ────────────────────────────────────────────────────────
		{res: "topics", coll: p + "/topics", item: p + "/topics/t",
			collWant: map[string]string{"GET": "TopicList", "DELETE": "TopicDelete"},
			itemWant: map[string]string{"PUT": "TopicCreate", "PATCH": "TopicUpdate", "GET": "TopicGet", "DELETE": "TopicDelete"}},
		{res: "subscriptions", coll: p + "/subscriptions", item: p + "/subscriptions/s",
			collWant: map[string]string{"GET": "SubscriptionList", "DELETE": "SubscriptionDelete"},
			itemWant: map[string]string{"PUT": "SubscriptionCreate", "PATCH": "SubscriptionUpdate", "GET": "SubscriptionGet", "DELETE": "SubscriptionDelete"}},
		// ── Secret Manager ─────────────────────────────────────────────────
		{res: "secrets", coll: p + "/secrets", item: p + "/secrets/s",
			collWant: map[string]string{"POST": "Create", "GET": "List", "PATCH": "Update", "DELETE": "Delete"},
			itemWant: map[string]string{"GET": "Get", "PATCH": "Update", "DELETE": "Delete"}},
		// ── Cloud KMS ──────────────────────────────────────────────────────
		{res: "keyRings", coll: p + "/locations/us/keyRings", item: kms,
			collWant: map[string]string{"POST": "KeyRingCreate", "GET": "KeyRingList"},
			itemWant: map[string]string{"GET": "KeyRingGet"}},
		// cryptoKeys.patch is the real REST key update (mirrors gRPC
		// UpdateCryptoKey).
		{res: "cryptoKeys", coll: kms + "/cryptoKeys", item: kms + "/cryptoKeys/k",
			collWant: map[string]string{"POST": "CryptoKeyCreate", "GET": "CryptoKeyList", "PATCH": "CryptoKeyUpdate", "DELETE": "CryptoKeyDelete"},
			itemWant: map[string]string{"GET": "CryptoKeyGet", "PATCH": "CryptoKeyUpdate", "DELETE": "CryptoKeyDelete"}},
		{res: "importJobs", coll: kms + "/importJobs", item: kms + "/importJobs/ij",
			collWant: map[string]string{"POST": "ImportJobCreate", "GET": "ImportJobList"},
			itemWant: map[string]string{"GET": "ImportJobGet"}},
		{res: "cryptoKeyVersions", coll: kms + "/cryptoKeys/k/cryptoKeyVersions", item: kms + "/cryptoKeys/k/cryptoKeyVersions/3",
			collWant: map[string]string{"POST": "CryptoKeyVersionCreate", "GET": "CryptoKeyVersionList", "PATCH": "CryptoKeyVersionUpdate", "DELETE": "CryptoKeyVersionDelete"},
			itemWant: map[string]string{"GET": "CryptoKeyVersionGet", "PATCH": "CryptoKeyVersionUpdate", "DELETE": "CryptoKeyVersionDelete"}},
		// ── IAM ────────────────────────────────────────────────────────────
		{res: "serviceAccounts", coll: p + "/serviceAccounts", item: p + "/serviceAccounts/sa@x.com",
			collWant: map[string]string{"POST": "ServiceAccountCreate", "GET": "ServiceAccountList", "PATCH": "ServiceAccountPatch", "PUT": "ServiceAccountUpdate", "DELETE": "ServiceAccountDelete"},
			itemWant: map[string]string{"GET": "ServiceAccountGet", "PATCH": "ServiceAccountPatch", "PUT": "ServiceAccountUpdate", "DELETE": "ServiceAccountDelete"}},
		{res: "keys", coll: p + "/serviceAccounts/sa@x.com/keys", item: p + "/serviceAccounts/sa@x.com/keys/kid1",
			collWant: map[string]string{"POST": "ServiceAccountKeyCreate", "GET": "ServiceAccountKeyList", "DELETE": "ServiceAccountKeyDelete"},
			itemWant: map[string]string{"GET": "ServiceAccountKeyGet", "DELETE": "ServiceAccountKeyDelete"}},
		{res: "workloadIdentityPools", coll: loc + "/workloadIdentityPools", item: loc + "/workloadIdentityPools/pool",
			collWant: map[string]string{}, itemWant: map[string]string{}},
		// ── Firestore ──────────────────────────────────────────────────────
		{res: "documents", coll: fst + "/documents/cities", item: fst + "/documents/cities/SF",
			collWant: map[string]string{"POST": "CreateDocument", "GET": "ListDocuments"},
			itemWant: map[string]string{"GET": "GetDocument", "PATCH": "PatchDocument", "DELETE": "DeleteDocument"}},
		{res: "indexes", coll: fst + "/collectionGroups/cg/indexes", item: fst + "/collectionGroups/cg/indexes/i",
			collWant: map[string]string{"POST": "CreateIndex", "GET": "ListIndexes", "DELETE": "DeleteIndex"},
			itemWant: map[string]string{"GET": "GetIndex", "DELETE": "DeleteIndex"}},
		{res: "databases", coll: p + "/databases", item: p + "/databases/db",
			collWant: map[string]string{"POST": "CreateDatabase", "GET": "ListDatabases", "PATCH": "UpdateDatabase", "DELETE": "DeleteDatabase"},
			itemWant: map[string]string{"GET": "GetDatabase", "PATCH": "UpdateDatabase", "DELETE": "DeleteDatabase"}},
		{res: "backupSchedules", coll: fst + "/backupSchedules", item: fst + "/backupSchedules/1",
			collWant: map[string]string{"POST": "CreateBackupSchedule", "GET": "ListBackupSchedules", "PATCH": "UpdateBackupSchedule", "DELETE": "DeleteBackupSchedule"},
			itemWant: map[string]string{"GET": "GetBackupSchedule", "PATCH": "UpdateBackupSchedule", "DELETE": "DeleteBackupSchedule"}},
		{res: "userCreds", coll: fst + "/userCreds", item: fst + "/userCreds/uc",
			collWant: map[string]string{"POST": "CreateUserCreds", "GET": "ListUserCreds", "DELETE": "DeleteUserCreds"},
			itemWant: map[string]string{"GET": "GetUserCreds", "DELETE": "DeleteUserCreds"}},
		{res: "fields", coll: fst + "/collectionGroups/cg/fields", item: fst + "/collectionGroups/cg/fields/f",
			collWant: map[string]string{"GET": "ListFields", "PATCH": "UpdateField"},
			itemWant: map[string]string{"GET": "GetField", "PATCH": "UpdateField"}},
		{res: "backups", coll: p + "/locations/us/backups", item: p + "/locations/us/backups/b",
			collWant: map[string]string{"GET": "ListBackups", "DELETE": "DeleteBackup"},
			itemWant: map[string]string{"GET": "GetBackup", "DELETE": "DeleteBackup"}},
		{res: "databasesOperations", coll: fst + "/operations", item: fst + "/operations/op1",
			collWant: map[string]string{"GET": "ListOperations", "DELETE": "DeleteOperation"},
			itemWant: map[string]string{"GET": "GetOperation", "DELETE": "DeleteOperation"}},
		// ── Cloud Functions / Workflows ────────────────────────────────────
		{res: "functions", coll: loc + "/functions", item: loc + "/functions/f",
			collWant: map[string]string{"POST": "CreateFunction", "GET": "ListFunctions", "PATCH": "UpdateFunction", "DELETE": "DeleteFunction"},
			itemWant: map[string]string{"GET": "GetFunction", "PATCH": "UpdateFunction", "DELETE": "DeleteFunction"}},
		{res: "workflows", coll: loc + "/workflows", item: loc + "/workflows/w",
			collWant: map[string]string{"POST": "CreateWorkflow", "GET": "ListWorkflows", "PATCH": "UpdateWorkflow", "DELETE": "DeleteWorkflow"},
			itemWant: map[string]string{"GET": "GetWorkflow", "PATCH": "UpdateWorkflow", "DELETE": "DeleteWorkflow"}},
		{res: "operations", coll: loc + "/operations", item: loc + "/operations/op1",
			collWant: map[string]string{"GET": "ListOperations"},
			itemWant: map[string]string{"GET": "GetOperation", "DELETE": "DeleteOperation"}},
		// ── Eventarc ───────────────────────────────────────────────────────
		{res: "triggers", coll: loc + "/triggers", item: loc + "/triggers/t",
			collWant: map[string]string{"POST": "CreateTrigger", "GET": "ListTriggers", "PATCH": "UpdateTrigger", "DELETE": "DeleteTrigger"},
			itemWant: map[string]string{"GET": "GetTrigger", "PATCH": "UpdateTrigger", "DELETE": "DeleteTrigger"}},
		{res: "channels", coll: loc + "/channels", item: loc + "/channels/c",
			collWant: map[string]string{"POST": "CreateChannel", "GET": "ListChannels", "PATCH": "UpdateChannel", "DELETE": "DeleteChannel"},
			itemWant: map[string]string{"GET": "GetChannel", "PATCH": "UpdateChannel", "DELETE": "DeleteChannel"}},
		{res: "providers", coll: loc + "/providers", item: loc + "/providers/pubsub.googleapis.com",
			collWant: map[string]string{"GET": "ListProviders"},
			itemWant: map[string]string{"GET": "GetProvider"}},
		{res: "messageBuses", coll: loc + "/messageBuses", item: loc + "/messageBuses/mb",
			collWant: map[string]string{"POST": "CreateMessageBus", "GET": "ListMessageBuses", "PATCH": "UpdateMessageBus", "DELETE": "DeleteMessageBus"},
			itemWant: map[string]string{"GET": "GetMessageBus", "PATCH": "UpdateMessageBus", "DELETE": "DeleteMessageBus"}},
		{res: "enrollments", coll: loc + "/enrollments", item: loc + "/enrollments/e",
			collWant: map[string]string{"POST": "CreateEnrollment", "GET": "ListEnrollments", "PATCH": "UpdateEnrollment", "DELETE": "DeleteEnrollment"},
			itemWant: map[string]string{"GET": "GetEnrollment", "PATCH": "UpdateEnrollment", "DELETE": "DeleteEnrollment"}},
		{res: "pipelines", coll: loc + "/pipelines", item: loc + "/pipelines/pl",
			collWant: map[string]string{"POST": "CreatePipeline", "GET": "ListPipelines", "PATCH": "UpdatePipeline", "DELETE": "DeletePipeline"},
			itemWant: map[string]string{"GET": "GetPipeline", "PATCH": "UpdatePipeline", "DELETE": "DeletePipeline"}},
		{res: "googleApiSources", coll: loc + "/googleApiSources", item: loc + "/googleApiSources/s",
			collWant: map[string]string{"POST": "CreateGoogleApiSource", "GET": "ListGoogleApiSources", "PATCH": "UpdateGoogleApiSource", "DELETE": "DeleteGoogleApiSource"},
			itemWant: map[string]string{"GET": "GetGoogleApiSource", "PATCH": "UpdateGoogleApiSource", "DELETE": "DeleteGoogleApiSource"}},
		{res: "channelConnections", coll: loc + "/channelConnections", item: loc + "/channelConnections/cc",
			collWant: map[string]string{"POST": "CreateChannelConnection", "GET": "ListChannelConnections", "DELETE": "DeleteChannelConnection"},
			itemWant: map[string]string{"GET": "GetChannelConnection", "DELETE": "DeleteChannelConnection"}},
		{res: "googleChannelConfig", coll: loc + "/googleChannelConfig",
			collWant: map[string]string{"GET": "GetGoogleChannelConfig", "PATCH": "UpdateGoogleChannelConfig"}},
		// ── Memorystore / common location discovery ────────────────────────
		{res: "instances", coll: loc + "/instances", item: loc + "/instances/i",
			collWant: map[string]string{"POST": "CreateInstance", "GET": "ListInstances", "PATCH": "UpdateInstance", "DELETE": "DeleteInstance"},
			itemWant: map[string]string{"GET": "GetInstance", "PATCH": "UpdateInstance", "DELETE": "DeleteInstance"}},
		{res: "locations", coll: p + "/locations", item: p + "/locations/us",
			collWant: map[string]string{"GET": "ListLocations"},
			itemWant: map[string]string{"GET": "GetLocation"}},
	}
	for _, r := range resources {
		levels := []struct {
			name string
			path string
			want map[string]string
		}{{"collection", r.coll, r.collWant}}
		if r.item != "" {
			levels = append(levels, struct {
				name string
				path string
				want map[string]string
			}{"item", r.item, r.itemWant})
		}
		for _, lv := range levels {
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
				what := r.res + " " + lv.name + " " + method
				nr, err := decodeJSON(t, method, lv.path, nil)
				want, supported := lv.want[method]
				if !supported {
					wantUnsupported(t, what+" ("+lv.path+")", nr, err)
					continue
				}
				if err != nil {
					t.Errorf("%s (%s): %v", what, lv.path, err)
					continue
				}
				if nr.Action != want {
					t.Errorf("%s (%s): action = %q, want %q", what, lv.path, nr.Action, want)
				}
			}
		}
	}
}

// TestJSONCodecCustomVerbMatrix pins every custom (`:verb`) method the codec
// derives. A custom verb is a distinct API surface from the plain resource
// verbs, so each one is asserted explicitly rather than inferred from its
// method switch.
func TestJSONCodecCustomVerbMatrix(t *testing.T) {
	const (
		p     = "/v1/projects/p"
		kms   = p + "/locations/us/keyRings/kr"
		loc   = p + "/locations/us-central1"
		fst   = p + "/databases/db"
		v2loc = "/v2/projects/p/locations/us-central1"
	)
	cases := []struct{ method, path, want string }{
		// Pub/Sub. The verb is documentary for a custom method — the codec
		// derives the action from the `:verb` alone — but it is the verb the
		// Discovery declares, so a future reader is not misled.
		{"POST", p + "/topics/t:publish", "TopicPublish"},
		{"GET", p + "/topics/t:getIamPolicy", "TopicGetIamPolicy"},
		{"POST", p + "/topics/t:setIamPolicy", "TopicSetIamPolicy"},
		{"POST", p + "/topics/t:testIamPermissions", "TopicTestIamPermissions"},
		{"POST", p + "/subscriptions/s:pull", "SubscriptionPull"},
		{"POST", p + "/subscriptions/s:acknowledge", "SubscriptionAcknowledge"},
		{"POST", p + "/subscriptions/s:modifyAckDeadline", "SubscriptionModifyAckDeadline"},
		{"POST", p + "/subscriptions/s:detach", "SubscriptionDetach"},
		{"POST", p + "/subscriptions/s:seek", "SubscriptionSeek"},
		{"POST", p + "/subscriptions/s:modifyPushConfig", "SubscriptionModifyPushConfig"},
		{"GET", p + "/subscriptions/s:getIamPolicy", "SubscriptionGetIamPolicy"},
		{"POST", p + "/subscriptions/s:setIamPolicy", "SubscriptionSetIamPolicy"},
		{"POST", p + "/subscriptions/s:testIamPermissions", "SubscriptionTestIamPermissions"},
		// Secret Manager (version lifecycle + IAM).
		{"POST", p + "/secrets/s:addVersion", "AddVersion"},
		{"GET", p + "/secrets/s/versions/1:access", "Access"},
		{"POST", p + "/secrets/s/versions/1:destroy", "DestroyVersion"},
		{"POST", p + "/secrets/s/versions/1:disable", "DisableVersion"},
		{"POST", p + "/secrets/s/versions/1:enable", "EnableVersion"},
		{"POST", p + "/secrets/s:enableManagedRotation", "EnableManagedRotation"},
		{"POST", p + "/secrets/s:rotateSecret", "RotateSecret"},
		{"GET", p + "/secrets/s:getIamPolicy", "GetIamPolicy"},
		{"POST", p + "/secrets/s:setIamPolicy", "SetIamPolicy"},
		{"POST", p + "/secrets/s:testIamPermissions", "TestIamPermissions"},
		// Cloud KMS.
		{"POST", kms + "/cryptoKeys/k:encrypt", "CryptoKeyEncrypt"},
		{"POST", kms + "/cryptoKeys/k:decrypt", "CryptoKeyDecrypt"},
		{"POST", kms + "/cryptoKeys/k:updatePrimaryVersion", "CryptoKeyUpdatePrimaryVersion"},
		{"GET", kms + "/cryptoKeys/k:getIamPolicy", "CryptoKeyGetIamPolicy"},
		{"POST", kms + "/cryptoKeys/k:setIamPolicy", "CryptoKeySetIamPolicy"},
		{"POST", kms + "/cryptoKeys/k:testIamPermissions", "CryptoKeyTestIamPermissions"},
		{"GET", kms + ":getIamPolicy", "KeyRingGetIamPolicy"},
		{"POST", kms + ":setIamPolicy", "KeyRingSetIamPolicy"},
		{"POST", kms + ":testIamPermissions", "KeyRingTestIamPermissions"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:destroy", "CryptoKeyVersionDestroy"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:restore", "CryptoKeyVersionRestore"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:asymmetricSign", "CryptoKeyVersionAsymmetricSign"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:asymmetricDecrypt", "CryptoKeyVersionAsymmetricDecrypt"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:macSign", "CryptoKeyVersionMacSign"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:macVerify", "CryptoKeyVersionMacVerify"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:rawEncrypt", "CryptoKeyVersionRawEncrypt"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:rawDecrypt", "CryptoKeyVersionRawDecrypt"},
		{"GET", kms + "/importJobs/j:getIamPolicy", "ImportJobGetIamPolicy"},
		{"POST", kms + "/importJobs/j:setIamPolicy", "ImportJobSetIamPolicy"},
		{"POST", kms + "/importJobs/j:testIamPermissions", "ImportJobTestIamPermissions"},
		{"POST", loc + ":generateRandomBytes", "GenerateRandomBytes"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions:import", "CryptoKeyVersionImport"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions:importTrustedKeyWrappedCryptoKeyVersion", "CryptoKeyVersionImportTrusted"},
		{"GET", kms + "/cryptoKeys/k/cryptoKeyVersions/3:exportTrustedKeyWrappedCryptoKeyVersion", "CryptoKeyVersionExportTrusted"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:decapsulate", "CryptoKeyVersionDecapsulate"},
		{"GET", kms + "/cryptoKeys/k/cryptoKeyVersions/3/publicKey", "CryptoKeyVersionGetPublicKey"},
		// IAM service accounts + keys.
		{"POST", p + "/serviceAccounts/sa@x.com:getIamPolicy", "ServiceAccountGetIamPolicy"},
		{"POST", p + "/serviceAccounts/sa@x.com:setIamPolicy", "ServiceAccountSetIamPolicy"},
		{"POST", p + "/serviceAccounts/sa@x.com:testIamPermissions", "ServiceAccountTestIamPermissions"},
		{"POST", p + "/serviceAccounts/sa@x.com:signBlob", "ServiceAccountSignBlob"},
		{"POST", p + "/serviceAccounts/sa@x.com:signJwt", "ServiceAccountSignJwt"},
		{"POST", p + "/serviceAccounts/sa@x.com:disable", "ServiceAccountDisable"},
		{"POST", p + "/serviceAccounts/sa@x.com:enable", "ServiceAccountEnable"},
		{"POST", p + "/serviceAccounts/sa@x.com:undelete", "ServiceAccountUndelete"},
		// IAM Credentials (iamcredentials) custom verbs. The `:getAllowedLocations`
		// form below is not a Discovery method (the real shape is the trailing
		// `allowedLocations` segment, pinned next); the codec accepts it for older
		// clients.
		{"POST", p + "/serviceAccounts/sa@x.com:generateAccessToken", "GenerateAccessToken"},
		{"POST", p + "/serviceAccounts/sa@x.com:generateIdToken", "GenerateIdToken"},
		{"POST", p + "/serviceAccounts/sa@x.com:getAllowedLocations", "GetAllowedLocations"},
		{"POST", loc + "/workloadIdentityPools/pool:getAllowedLocations", "GetAllowedLocations"},
		// The Discovery shape of getAllowedLocations is a plain trailing segment.
		{"GET", p + "/serviceAccounts/sa@x.com/allowedLocations", "GetAllowedLocations"},
		{"GET", loc + "/workloadIdentityPools/pool/allowedLocations", "GetAllowedLocations"},
		{"POST", p + "/serviceAccounts/sa@x.com/keys/kid1:disable", "ServiceAccountKeyDisable"},
		{"POST", p + "/serviceAccounts/sa@x.com/keys/kid1:enable", "ServiceAccountKeyEnable"},
		// keys:upload is a custom verb on the keys *collection* (the name is the
		// service account), so it must not fall through to ServiceAccountKeyCreate.
		{"POST", p + "/serviceAccounts/sa@x.com/keys:upload", "ServiceAccountKeyUpload"},
		// Firestore data-plane custom methods on the `documents` marker.
		{"POST", fst + "/documents:commit", "Commit"},
		{"POST", fst + "/documents:partitionQuery", "PartitionQuery"},
		{"POST", fst + "/documents:executePipeline", "ExecutePipeline"},
		{"POST", fst + "/documents:runQuery", "RunQuery"},
		{"POST", fst + "/documents:runAggregationQuery", "RunAggregationQuery"},
		{"POST", fst + "/documents:batchWrite", "BatchWrite"},
		{"POST", fst + "/documents:batchGet", "BatchGet"},
		{"POST", fst + "/documents:beginTransaction", "BeginTransaction"},
		{"POST", fst + "/documents:rollback", "Rollback"},
		{"POST", fst + "/documents:listCollectionIds", "ListCollectionIds"},
		// Firestore Admin user creds.
		{"POST", fst + "/userCreds/uc:enable", "EnableUserCreds"},
		{"POST", fst + "/userCreds/uc:disable", "DisableUserCreds"},
		{"POST", fst + "/userCreds/uc:resetPassword", "ResetUserPassword"},
		// Cloud Functions v1.
		{"POST", loc + "/functions/f:call", "CallFunction"},
		{"POST", loc + "/functions:generateUploadUrl", "GenerateUploadUrl"},
		{"POST", loc + "/functions/f:generateDownloadUrl", "GenerateDownloadUrl"},
		{"GET", loc + "/functions/f:getIamPolicy", "FunctionGetIamPolicy"},
		{"POST", loc + "/functions/f:setIamPolicy", "FunctionSetIamPolicy"},
		{"POST", loc + "/functions/f:testIamPermissions", "FunctionTestIamPermissions"},
		// Cloud Functions v1→v2 upgrade / traffic control plane.
		{"POST", v2loc + "/functions/f:setupFunctionUpgradeConfig", "SetupFunctionUpgradeConfig"},
		{"POST", v2loc + "/functions/f:redirectFunctionUpgradeTraffic", "RedirectFunctionUpgradeTraffic"},
		{"POST", v2loc + "/functions/f:rollbackFunctionUpgradeTraffic", "RollbackFunctionUpgradeTraffic"},
		{"POST", v2loc + "/functions/f:commitFunctionUpgrade", "CommitFunctionUpgrade"},
		{"POST", v2loc + "/functions/f:commitFunctionUpgradeAsGen2", "CommitFunctionUpgradeAsGen2"},
		{"POST", v2loc + "/functions/f:abortFunctionUpgrade", "AbortFunctionUpgrade"},
		{"POST", v2loc + "/functions/f:detachFunction", "DetachFunction"},
		// Eventarc: IAM on every resource, plus the message-bus enrolment list.
		{"GET", loc + "/triggers/t:getIamPolicy", "TriggerGetIamPolicy"},
		{"POST", loc + "/triggers/t:setIamPolicy", "TriggerSetIamPolicy"},
		{"POST", loc + "/triggers/t:testIamPermissions", "TriggerTestIamPermissions"},
		{"GET", loc + "/channels/c:getIamPolicy", "ChannelGetIamPolicy"},
		{"POST", loc + "/channels/c:setIamPolicy", "ChannelSetIamPolicy"},
		{"POST", loc + "/channels/c:testIamPermissions", "ChannelTestIamPermissions"},
		{"GET", loc + "/messageBuses/mb:getIamPolicy", "MessageBusGetIamPolicy"},
		{"POST", loc + "/messageBuses/mb:setIamPolicy", "MessageBusSetIamPolicy"},
		{"POST", loc + "/messageBuses/mb:testIamPermissions", "MessageBusTestIamPermissions"},
		{"GET", loc + "/messageBuses/mb:listEnrollments", "ListMessageBusEnrollments"},
		{"GET", loc + "/enrollments/e:getIamPolicy", "EnrollmentGetIamPolicy"},
		{"POST", loc + "/enrollments/e:setIamPolicy", "EnrollmentSetIamPolicy"},
		{"POST", loc + "/enrollments/e:testIamPermissions", "EnrollmentTestIamPermissions"},
		{"GET", loc + "/pipelines/pl:getIamPolicy", "PipelineGetIamPolicy"},
		{"POST", loc + "/pipelines/pl:setIamPolicy", "PipelineSetIamPolicy"},
		{"POST", loc + "/pipelines/pl:testIamPermissions", "PipelineTestIamPermissions"},
		{"GET", loc + "/googleApiSources/s:getIamPolicy", "GoogleApiSourceGetIamPolicy"},
		{"POST", loc + "/googleApiSources/s:setIamPolicy", "GoogleApiSourceSetIamPolicy"},
		{"POST", loc + "/googleApiSources/s:testIamPermissions", "GoogleApiSourceTestIamPermissions"},
		{"GET", loc + "/channelConnections/cc:getIamPolicy", "ChannelConnectionGetIamPolicy"},
		{"POST", loc + "/channelConnections/cc:setIamPolicy", "ChannelConnectionSetIamPolicy"},
		{"POST", loc + "/channelConnections/cc:testIamPermissions", "ChannelConnectionTestIamPermissions"},
		// Memorystore Redis upgrade.
		{"POST", loc + "/instances/i:upgrade", "UpgradeInstance"},
		{"POST", loc + "/instances/i:import", "ImportInstance"},
		{"POST", loc + "/instances/i:export", "ExportInstance"},
		{"POST", loc + "/instances/i:failover", "FailoverInstance"},
		{"POST", loc + "/instances/i:rescheduleMaintenance", "RescheduleMaintenance"},
		{"GET", loc + "/instances/i/authString", "GetAuthString"},
		// The shared operations surface's Eventarc `:cancel` (the only owner of
		// the shared path that declares one).
		{"POST", loc + "/operations/op1:cancel", "CancelOperation"},
		// Workflow revision history.
		{"GET", loc + "/workflows/w:listRevisions", "ListWorkflowRevisions"},
	}
	for _, tc := range cases {
		nr, err := decodeJSON(t, tc.method, tc.path, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.want {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.want)
		}
	}
}

// TestJSONCodecUnsupportedShapes is the negative half of the derivation table:
// an unknown custom verb on an item or collection path, a documented-but-not
// modelled surface, and a version-gated resource must all fail loud rather than
// fall through to the plain resource verb of the same path.
//
// A stray custom verb is refused on both the *item* and the *collection* path,
// whatever the resource's plain create verb is: TD24 closed the fail-open where
// an unknown `:verb` on a collection path fell through to the resource's plain
// create verb (`POST /v1/projects/p/secrets:zzz` → `Create`,
// `POST …/serviceAccounts/{sa}/keys:zzz` → `ServiceAccountKeyCreate`,
// `POST …/databases/{db}/documents:zzz` → `CreateDocument`). The collection
// rows below pin that, and the `custom == ""` guards on the cryptoKeys /
// cryptoKeyVersions arms pin the same for the PATCH/DELETE item verbs.
//
// The Firestore `databasesOperations` surface keeps its pass-through: a
// `databasesOperations :cancel` still derives `CancelOperation` (asserted in
// TestFirestoreAdminDecode). Cloud Functions v2's operations `:cancel`/`:wait`
// are derived above the custom-verb block and are asserted in TestFunctionsV2Decode;
// an *unknown* verb on either operations surface is refused like any other.
func TestJSONCodecUnsupportedShapes(t *testing.T) {
	const (
		p   = "/v1/projects/p"
		kms = p + "/locations/us/keyRings/kr"
		loc = p + "/locations/us-central1"
		fst = p + "/databases/db"
	)
	cases := []struct{ method, path string }{
		// Unknown custom verbs must not dispatch as the plain resource verb.
		{"POST", p + "/topics/t:zzz"},
		{"POST", p + "/topics:zzz"},
		{"POST", p + "/subscriptions/s:zzz"},
		{"POST", p + "/secrets/s:zzz"},
		{"PUT", p + "/secrets/s/versions"},
		{"PUT", p + "/secrets/s/versions/1"},
		{"POST", kms + "/cryptoKeys/k:zzz"},
		{"PATCH", kms + "/cryptoKeys/k:zzz"},
		{"DELETE", kms + "/cryptoKeys/k:zzz"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions:zzz"},
		{"PUT", kms + "/cryptoKeys/k/cryptoKeyVersions:zzz"},
		{"GET", kms + "/cryptoKeys/k/cryptoKeyVersions:zzz"},
		{"GET", kms + "/cryptoKeys/k/cryptoKeyVersions/3:zzz"},
		{"POST", kms + "/cryptoKeys/k/cryptoKeyVersions/3:zzz"},
		{"PATCH", kms + "/cryptoKeys/k/cryptoKeyVersions/3:zzz"},
		{"DELETE", kms + "/cryptoKeys/k/cryptoKeyVersions/3:zzz"},
		{"GET", kms + "/cryptoKeys/k/cryptoKeyVersions/3/publicKey:zzz"},
		{"PUT", kms + "/cryptoKeys/k/cryptoKeyVersions/3/publicKey"},
		{"POST", p + "/serviceAccounts/sa@x.com:zzz"},
		{"POST", loc + "/triggers/t:zzz"},
		{"POST", loc + "/functions/f:zzz"},
		// A stray verb on the getAllowedLocations Discovery shape must not be
		// waved through by the trailing-segment check.
		{"GET", p + "/serviceAccounts/sa@x.com/allowedLocations:zzz"},
		{"GET", loc + "/workloadIdentityPools/pool/allowedLocations:zzz"},
		// A stray custom verb on a *collection* path must never dispatch as the
		// resource's plain create verb (TD24). Every modelled family rejects it
		// now, not just the topic/subscription/keyRing ones.
		{"POST", p + "/secrets:zzz"},
		{"POST", p + "/serviceAccounts:zzz"},
		{"POST", p + "/serviceAccounts/sa@x.com/keys:zzz"},
		{"POST", kms + "/keyRings:zzz"},
		{"POST", kms + "/cryptoKeys:zzz"},
		{"POST", kms + "/importJobs:zzz"},
		{"POST", fst + "/documents:zzz"},
		{"POST", fst + "/documents/cities:zzz"},
		{"POST", fst + "/indexes:zzz"},
		{"POST", fst + "/collectionGroups/cg/indexes:zzz"},
		{"POST", fst + "/backupSchedules:zzz"},
		{"POST", fst + "/userCreds:zzz"},
		{"POST", loc + "/functions:zzz"},
		{"POST", loc + "/workflows:zzz"},
		{"POST", loc + "/triggers:zzz"},
		{"POST", loc + "/channels:zzz"},
		{"POST", loc + "/messageBuses:zzz"},
		{"POST", loc + "/enrollments:zzz"},
		{"POST", loc + "/pipelines:zzz"},
		{"POST", loc + "/googleApiSources:zzz"},
		{"POST", loc + "/channelConnections:zzz"},
		{"POST", loc + "/instances:zzz"},
		// A stray verb paired with a mutating method is refused too, rather
		// than deriving the item PATCH/DELETE verb for a collection path.
		{"PATCH", p + "/secrets:zzz"},
		{"DELETE", p + "/secrets:zzz"},
		{"PATCH", loc + "/instances:zzz"},
		// An unknown verb on the shared operations surfaces is refused too: v1
		// operations has list/get/delete only, and Firestore databasesOperations
		// only `:cancel`.
		{"GET", loc + "/operations/op1:zzz"},
		{"POST", loc + "/operations/op1:zzz"},
		{"POST", loc + "/operations:zzz"},
		{"POST", fst + "/operations/op1:zzz"},
		{"POST", "/v2/projects/p/locations/us-central1/operations/op1:zzz"},
		// This is an item path, so it exercises the unknown-verb fall-through
		// rather than the `databases` custom arm that fails loud; the
		// collection-level data-plane verbs (databases:restore/clone) are
		// asserted in TestFirestoreAdminDecode.
		{"POST", p + "/databases/db:zzz"},
		// Version-gated and unmodelled surfaces.
		{"GET", p + "/locations/us/runtimes"},                       // runtimes only exist in /v2
		{"POST", loc + "/runtimes"},                                 // v2 runtimes has list only
		{"GET", loc + "/runtimes/rt"},                               // v2 runtimes has no get
		{"POST", "/v2/projects/p/locations/us-central1/runtimes"},   // v2 runtimes has list only
		{"GET", "/v2/projects/p/locations/us-central1/runtimes/rt"}, // v2 runtimes has no get
		{"POST", "/v2/projects/p/locations/us-central1/operations"}, // v2 operations has list/get/delete only
		{"POST", loc + "/operations"},                               // /v1 operations has list/get/delete only (delete is item-only)
		{"DELETE", loc + "/operations"},                             // no collection-level delete is declared
		{"POST", p + "/locations/us/backupCollections/bc/backups"},  // memorystore backups are not modelled
	}
	for _, tc := range cases {
		nr, err := decodeJSON(t, tc.method, tc.path, nil)
		wantUnsupported(t, tc.method+" "+tc.path, nr, err)
	}

	// A path with no projects segment at all is a 404 (never a panic or a
	// silently empty project).
	nr, err := decodeJSON(t, "GET", "/v1/organizations/o", nil)
	if err == nil {
		t.Fatalf("GET /v1/organizations/o: action %q, want 404", nr.Action)
	}
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.HTTPStatus != 404 {
		t.Fatalf("GET /v1/organizations/o: error = %v, want 404", err)
	}

	// /v2/operations is not the Cloud Functions v1 top-level operations surface.
	if _, err := decodeJSON(t, "GET", "/v2/operations", nil); err == nil {
		t.Fatalf("GET /v2/operations: decoded, want 404")
	}

	// A malformed body is refused on both the generic and the top-level
	// operations surface.
	for _, path := range []string{p + "/secrets", "/v1/operations"} {
		nr, err := decodeJSON(t, "POST", path, []byte("{"))
		if err == nil {
			t.Errorf("POST %s (malformed body): action %q, want 400", path, nr.Action)
			continue
		}
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.HTTPStatus != 400 || perr.Code != "InvalidRequest" {
			t.Errorf("POST %s (malformed body): error = %v, want 400 InvalidRequest", path, err)
		}
	}
}

// TestJSONCodecMalformedPaths pins the path parser's failure contract: a path
// with no project, a marker with no value, or an empty resource tail must fail
// loud with the documented status and must never panic or pick a later
// `projects` segment.
func TestJSONCodecMalformedPaths(t *testing.T) {
	cases := []struct {
		path       string
		wantStatus int
		wantAction string
	}{
		// No `projects` marker at all.
		{"/v1", 404, ""},
		{"/v1/organizations/o", 404, ""},
		// A `projects` marker with no project value.
		{"/v1/projects", 404, ""},
		// A project but no resource tail: decoded, then unsupported.
		{"/v1/projects/p", 501, ""},
		// `projects` is not required to be the second segment.
		{"/projects/p/secrets", 0, "List"},
		// The first `projects` marker wins (a resource literally named
		// "projects" must not become the project).
		{"/v1/projects/p/secrets/projects", 0, "Get"},
	}
	for _, tc := range cases {
		nr, err := decodeJSON(t, "GET", tc.path, nil)
		if tc.wantStatus == 0 {
			if err != nil {
				t.Errorf("GET %s: %v", tc.path, err)
				continue
			}
			if nr.Action != tc.wantAction {
				t.Errorf("GET %s: action = %q, want %q", tc.path, nr.Action, tc.wantAction)
			}
			continue
		}
		if err == nil {
			t.Errorf("GET %s: action = %q, want %d", tc.path, nr.Action, tc.wantStatus)
			continue
		}
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.HTTPStatus != tc.wantStatus {
			t.Errorf("GET %s: error = %v, want HTTP %d", tc.path, err, tc.wantStatus)
		}
	}
}

// TestJSONCodecFirestoreDocumentLevels pins the Firestore segment parity rule:
// an even (positive) number of segments after `documents` is a document, an odd
// count is a collection, and the bare marker is the collection-level list.
func TestJSONCodecFirestoreDocumentLevels(t *testing.T) {
	const fst = "/v1/projects/p/databases/db"
	cases := []struct {
		method, path, want string
	}{
		{"GET", fst + "/documents", "ListDocuments"},               // 0 segments after the marker
		{"GET", fst + "/documents/cities", "ListDocuments"},        // 1 → collection
		{"POST", fst + "/documents/cities", "CreateDocument"},      // 1 → collection
		{"PATCH", fst + "/documents/cities", ""},                   // no collection-level patch
		{"DELETE", fst + "/documents/cities", ""},                  // no collection-level delete
		{"GET", fst + "/documents/cities/SF", "GetDocument"},       // 2 → document
		{"PATCH", fst + "/documents/cities/SF", "PatchDocument"},   // 2 → document
		{"DELETE", fst + "/documents/cities/SF", "DeleteDocument"}, // 2 → document
		{"POST", fst + "/documents/cities/SF", ""},                 // no document-level create
		{"GET", fst + "/documents/cities/SF/sub", "ListDocuments"}, // 3 → subcollection
	}
	for _, tc := range cases {
		nr, err := decodeJSON(t, tc.method, tc.path, nil)
		if tc.want == "" {
			wantUnsupported(t, tc.method+" "+tc.path, nr, err)
			continue
		}
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.want {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.want)
		}
	}
}

// TestJSONCodecTopLevelOperationsParams pins the Cloud Functions v1 top-level
// operations surface: the version is always v1, the persisted name keeps the
// `operations/...` prefix, and the collection/item pair derives list/get.
func TestJSONCodecTopLevelOperationsParams(t *testing.T) {
	cases := []struct{ path, wantName, wantAction string }{
		{"/v1/operations", "operations", "ListOperations"},
		{"/v1/operations/op1", "operations/op1", "GetOperation"},
	}
	for _, tc := range cases {
		nr, err := decodeJSON(t, "GET", tc.path, nil)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		if nr.Params["resourceType"] != "operations" || nr.Params["apiVersion"] != "v1" {
			t.Errorf("GET %s: resourceType=%v apiVersion=%v, want operations/v1", tc.path, nr.Params["resourceType"], nr.Params["apiVersion"])
		}
		if nr.Params["name"] != tc.wantName {
			t.Errorf("GET %s: name = %v, want %q", tc.path, nr.Params["name"], tc.wantName)
		}
		if nr.Action != tc.wantAction {
			t.Errorf("GET %s: action = %q, want %q", tc.path, nr.Action, tc.wantAction)
		}
	}
}

// TestJSONCodecDecodeParams pins what Decode folds into the normalized request:
// project/name/resourceType/apiVersion, the location segment each family needs
// (including the first-locations-wins rule), the instance id, query parameters
// and the JSON body.
func TestJSONCodecDecodeParams(t *testing.T) {
	const p = "/v1/projects/p"

	t.Run("defaults", func(t *testing.T) {
		nr, err := decodeJSON(t, "GET", p+"/secrets/s?updateMask=etag&pageSize=10", nil)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if nr.Service != "test" {
			t.Errorf("service = %q, want test", nr.Service)
		}
		if nr.Params["project"] != "p" {
			t.Errorf("project = %v, want p", nr.Params["project"])
		}
		if nr.Params["apiVersion"] != "v1" {
			t.Errorf("apiVersion = %v, want v1", nr.Params["apiVersion"])
		}
		if nr.Params["resourceType"] != "secrets" {
			t.Errorf("resourceType = %v, want secrets", nr.Params["resourceType"])
		}
		if nr.Params["name"] != "secrets/s" {
			t.Errorf("name = %v, want secrets/s", nr.Params["name"])
		}
		if nr.Params["updateMask"] != "etag" || nr.Params["pageSize"] != "10" {
			t.Errorf("query params = %v, want updateMask/pageSize folded", nr.Params)
		}
		if _, ok := nr.Params["body"]; ok {
			t.Errorf("body = %v, want absent for an empty body", nr.Params["body"])
		}
	})

	t.Run("v2 apiVersion", func(t *testing.T) {
		nr, err := decodeJSON(t, "GET", "/v2/projects/p/locations/us-central1/functions/f", nil)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if nr.Params["apiVersion"] != "v2" {
			t.Errorf("apiVersion = %v, want v2", nr.Params["apiVersion"])
		}
	})

	t.Run("json body folded", func(t *testing.T) {
		nr, err := decodeJSON(t, "PUT", p+"/topics/t?topicId=t", []byte(`{"name":"t","labels":{"a":"b"}}`))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		body, ok := nr.Params["body"].(map[string]any)
		if !ok {
			t.Fatalf("body = %T %v, want map", nr.Params["body"], nr.Params["body"])
		}
		if body["name"] != "t" {
			t.Errorf("body.name = %v, want t", body["name"])
		}
		if nr.Params["topicId"] != "t" {
			t.Errorf("topicId = %v, want t", nr.Params["topicId"])
		}
	})

	// Each /locations/{l}/... family surfaces its location, and the first
	// `locations` segment wins over a later resource literally named
	// "locations".
	t.Run("location extraction", func(t *testing.T) {
		cases := []struct {
			path  string
			loc   string
			extra map[string]any
		}{
			{p + "/locations/us/keyRings", "us", nil},
			{p + "/locations/us/keyRings/kr/cryptoKeys/k", "us", nil},
			{p + "/locations/us/keyRings/locations/cryptoKeys/k", "us", nil},
			{p + "/locations/us-central1/functions/f", "us-central1", nil},
			{p + "/locations/us-central1/functions/locations/sub", "us-central1", nil},
			{p + "/locations/us-central1/workflows/w", "us-central1", nil},
			{p + "/locations/l1/operations/locations/sub", "l1", nil},
			{p + "/locations/us-central1/triggers/t", "us-central1", nil},
			{p + "/locations/us-central1/triggers/locations/x", "us-central1", nil},
			{p + "/locations/us-central1/instances/i", "us-central1", map[string]any{"instanceId": "i"}},
			{p + "/locations/us-central1/instances/locations/sub", "us-central1", map[string]any{"instanceId": "locations"}},
			{p + "/locations/us", "us", nil},
		}
		for _, tc := range cases {
			nr, err := decodeJSON(t, "GET", tc.path, nil)
			if err != nil {
				t.Errorf("%s: %v", tc.path, err)
				continue
			}
			if nr.Params["location"] != tc.loc {
				t.Errorf("%s: location = %v, want %q", tc.path, nr.Params["location"], tc.loc)
			}
			for k, v := range tc.extra {
				if nr.Params[k] != v {
					t.Errorf("%s: %s = %v, want %v", tc.path, k, nr.Params[k], v)
				}
			}
		}
	})
}

// TestJSONCodecEncodeDefaults pins the response half of the codec: the status
// default, the JSON content type, raw-JSON passthrough and the marshal-failure
// envelope.
func TestJSONCodecEncodeDefaults(t *testing.T) {
	c := &JSONCodec{Service: "test"}

	status, headers, body := c.Encode(nil, &model.ProviderResponse{Data: map[string]any{"name": "t"}})
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200 for an unset HTTPStatus", status)
	}
	if got := headers.Get("Content-Type"); got != "application/json; charset=UTF-8" {
		t.Errorf("Content-Type = %q, want application/json; charset=UTF-8", got)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if decoded["name"] != "t" {
		t.Errorf("body = %s, want the marshalled Data", body)
	}

	status, _, _ = c.Encode(nil, &model.ProviderResponse{HTTPStatus: http.StatusCreated, Data: map[string]any{}})
	if status != http.StatusCreated {
		t.Errorf("status = %d, want the explicit 201", status)
	}

	raw := json.RawMessage(`{"raw":true}`)
	status, _, body = c.Encode(nil, &model.ProviderResponse{Data: map[string]any{wire.RawJSONKey: raw}})
	if status != http.StatusOK || string(body) != string(raw) {
		t.Errorf("raw passthrough = (%d, %s), want (200, %s)", status, body, raw)
	}

	status, _, body = c.Encode(nil, &model.ProviderResponse{Data: map[string]any{"bad": make(chan int)}})
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 when Data cannot be marshalled", status)
	}
	if !strings.Contains(string(body), `"code":500`) {
		t.Errorf("body = %s, want the INTERNAL encode-failure envelope", body)
	}
}

// TestJSONCodecEncodeErrorEnvelope pins the GCP error envelope: the status
// default, the code/status/message triple, and the conditional details array.
func TestJSONCodecEncodeErrorEnvelope(t *testing.T) {
	decode := func(t *testing.T, body []byte) map[string]any {
		t.Helper()
		var env struct {
			Error map[string]any `json:"error"`
		}
		if err := json.Unmarshal(body, &env); err != nil {
			t.Fatalf("unmarshal %s: %v", body, err)
		}
		return env.Error
	}

	c := &JSONCodec{Service: "test"}
	status, headers, body := c.EncodeError(nil, model.NewProviderError("NotFound", "no such secret", http.StatusNotFound))
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
	if got := headers.Get("Content-Type"); got != "application/json; charset=UTF-8" {
		t.Errorf("Content-Type = %q", got)
	}
	got := decode(t, body)
	if got["code"] != float64(404) || got["status"] != "NOT_FOUND" || got["message"] != "no such secret" {
		t.Errorf("error = %v, want {code:404,status:NOT_FOUND,message:no such secret}", got)
	}
	if _, ok := got["details"]; ok {
		t.Errorf("details = %v, want absent when there are none", got["details"])
	}

	// An unset HTTPStatus defaults to 500 while the google.rpc status still
	// resolves from the code/status table.
	status, _, body = c.EncodeError(nil, model.NewProviderError("", "boom", 0))
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want the 500 default", status)
	}
	got = decode(t, body)
	if got["code"] != float64(500) || got["status"] != "INTERNAL" {
		t.Errorf("error = %v, want {code:500,status:INTERNAL}", got)
	}

	perr := model.NewProviderError("InvalidArgument", "bad ack ids", http.StatusBadRequest)
	perr.Details = []model.ErrorDetail{{
		Type:     "google.rpc.ErrorInfo",
		Reason:   "EXACTLY_ONCE_ACKID_FAILURE",
		Domain:   "pubsub.googleapis.com",
		Metadata: map[string]string{"ack-1": "PERMANENT_FAILURE_INVALID_ACK_ID"},
	}}
	_, _, body = c.EncodeError(nil, perr)
	got = decode(t, body)
	details, ok := got["details"].([]any)
	if !ok || len(details) != 1 {
		t.Fatalf("details = %v, want one entry", got["details"])
	}
}

// TestGCPErrorDetailsTable pins the detail renderer: an untyped entry is
// skipped rather than rendered with an empty @type, and reason/domain/metadata
// are only emitted when set.
func TestGCPErrorDetailsTable(t *testing.T) {
	if got := gcpErrorDetails(nil); len(got) != 0 {
		t.Errorf("gcpErrorDetails(nil) = %v, want empty", got)
	}
	if got := gcpErrorDetails([]model.ErrorDetail{{Type: ""}}); len(got) != 0 {
		t.Errorf("untyped detail = %v, want it skipped", got)
	}

	got := gcpErrorDetails([]model.ErrorDetail{
		{},
		{Type: "google.rpc.ErrorInfo"},
		{Type: "google.rpc.ErrorInfo", Reason: "R", Domain: "d.example.com", Metadata: map[string]string{"k": "v"}},
	})
	if len(got) != 2 {
		t.Fatalf("details = %v, want the untyped entry dropped and the other two kept", got)
	}
	if got[0]["@type"] != "type.googleapis.com/google.rpc.ErrorInfo" {
		t.Errorf("@type = %v", got[0]["@type"])
	}
	for _, key := range []string{"reason", "domain", "metadata"} {
		if _, ok := got[0][key]; ok {
			t.Errorf("bare detail carries %s = %v, want it omitted", key, got[0][key])
		}
	}
	if got[1]["reason"] != "R" || got[1]["domain"] != "d.example.com" {
		t.Errorf("detail = %v", got[1])
	}
	if md, ok := got[1]["metadata"].(map[string]string); !ok || md["k"] != "v" {
		t.Errorf("metadata = %v, want the map", got[1]["metadata"])
	}
}

// TestDetectResourceTypeTable pins the resource-type derivation, including the
// precedence rules (cryptoKeyVersions > cryptoKeys > keyRings, documents > keys,
// executions > workflows) and the positional guards that keep Memorystore paths
// out of the Firestore buckets.
func TestDetectResourceTypeTable(t *testing.T) {
	cases := []struct {
		name string
		segs []string
		want string
	}{
		{"empty", nil, ""},
		{"unknown", []string{"nonsense", "x"}, ""},
		{"topics", []string{"topics", "t"}, "topics"},
		{"subscriptions", []string{"subscriptions", "s"}, "subscriptions"},
		{"secret versions stay secrets", []string{"secrets", "s", "versions", "1"}, "secrets"},
		{"documents beat keys", []string{"databases", "db", "documents", "keys", "k"}, "documents"},
		{"databases bare", []string{"databases"}, "databases"},
		{"databases item", []string{"databases", "db"}, "databases"},
		{"databases too deep", []string{"databases", "db", "x"}, ""},
		{"databasesOperations", []string{"databases", "db", "operations"}, "databasesOperations"},
		{"databasesOperations item", []string{"databases", "db", "operations", "op1"}, "databasesOperations"},
		{"backupSchedules", []string{"databases", "db", "backupSchedules", "1"}, "backupSchedules"},
		{"userCreds", []string{"databases", "db", "userCreds", "uc"}, "userCreds"},
		{"fields", []string{"databases", "db", "collectionGroups", "cg", "fields", "f"}, "fields"},
		{"indexes", []string{"databases", "db", "collectionGroups", "cg", "indexes", "i"}, "indexes"},
		{"executions beat workflows", []string{"locations", "l", "workflows", "w", "executions", "e"}, "executions"},
		{"workflows", []string{"locations", "l", "workflows", "w"}, "workflows"},
		{"cryptoKeyVersions beat cryptoKeys", []string{"locations", "l", "keyRings", "kr", "cryptoKeys", "k", "cryptoKeyVersions", "3"}, "cryptoKeyVersions"},
		{"cryptoKeys beat keyRings", []string{"locations", "l", "keyRings", "kr", "cryptoKeys", "k"}, "cryptoKeys"},
		{"keyRings", []string{"locations", "l", "keyRings", "kr"}, "keyRings"},
		{"importJobs", []string{"locations", "l", "keyRings", "kr", "importJobs", "ij"}, "importJobs"},
		{"serviceAccounts", []string{"serviceAccounts", "sa"}, "serviceAccounts"},
		{"keys", []string{"serviceAccounts", "sa", "keys", "k"}, "keys"},
		{"functions", []string{"locations", "l", "functions", "f"}, "functions"},
		{"runtimes", []string{"locations", "l", "runtimes"}, "runtimes"},
		{"operations", []string{"locations", "l", "operations", "op1"}, "operations"},
		{"a locations-prefixed operations path is not a firestore operation", []string{"locations", "l", "operations"}, "operations"},
		{"triggers", []string{"locations", "l", "triggers", "t"}, "triggers"},
		{"channels", []string{"locations", "l", "channels", "c"}, "channels"},
		{"providers", []string{"locations", "l", "providers", "pubsub.googleapis.com"}, "providers"},
		{"messageBuses", []string{"locations", "l", "messageBuses", "mb"}, "messageBuses"},
		{"enrollments", []string{"locations", "l", "enrollments", "e"}, "enrollments"},
		{"pipelines", []string{"locations", "l", "pipelines", "pl"}, "pipelines"},
		{"googleApiSources", []string{"locations", "l", "googleApiSources", "s"}, "googleApiSources"},
		{"channelConnections", []string{"locations", "l", "channelConnections", "cc"}, "channelConnections"},
		{"googleChannelConfig", []string{"locations", "l", "googleChannelConfig"}, "googleChannelConfig"},
		{"workloadIdentityPools", []string{"locations", "l", "workloadIdentityPools", "pool"}, "workloadIdentityPools"},
		{"instances are positional", []string{"locations", "l", "instances", "i"}, "instances"},
		{"backups are positional", []string{"locations", "l", "backups", "b"}, "backups"},
		{"memorystore backups are not firestore backups", []string{"locations", "l", "backupCollections", "bc", "backups", "b"}, ""},
		{"locations bare", []string{"locations"}, "locations"},
		{"locations item", []string{"locations", "us"}, "locations"},
		{"locations too deep", []string{"locations", "us", "x"}, ""},
	}
	for _, tc := range cases {
		if got := detectResourceType(tc.segs); got != tc.want {
			t.Errorf("%s: detectResourceType(%v) = %q, want %q", tc.name, tc.segs, got, tc.want)
		}
	}
}

// TestJSONCodecPathHelpers pins the three package-private path helpers the codec
// derives its collection/document distinction from.
func TestJSONCodecPathHelpers(t *testing.T) {
	if got := segmentAfter([]string{"a", "instances", "i"}, "instances"); got != "i" {
		t.Errorf("segmentAfter = %q, want i", got)
	}
	if got := segmentAfter([]string{"a", "b"}, "instances"); got != "" {
		t.Errorf("segmentAfter(absent) = %q, want empty", got)
	}
	// A marker in final position has no following segment.
	if got := segmentAfter([]string{"a", "instances"}, "instances"); got != "" {
		t.Errorf("segmentAfter(final) = %q, want empty", got)
	}

	secretVersions := []struct {
		name string
		want bool
	}{
		{"secrets/s/versions", true},
		{"secrets/versions", false},       // a secret literally named "versions"
		{"secrets/s/1", false},            // a single version
		{"secrets/s/versions/1", false},   // a single version below the collection
		{"topics/t/versions", false},      // another service's three-part path
		{"secrets/s/versions/1/x", false}, // deeper than a versions collection
	}
	for _, tc := range secretVersions {
		if got := isSecretVersionsCollection(tc.name); got != tc.want {
			t.Errorf("isSecretVersionsCollection(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}

	segmentCounts := []struct {
		name string
		want int
	}{
		{"databases/(default)/documents", 0},
		{"databases/(default)/documents/cities", 1},
		{"databases/(default)/documents/cities/SF", 2},
		{"databases/(default)/documents/cities/SF/sub", 3},
		{"databases/(default)/collectionGroups/cg", 0},
	}
	for _, tc := range segmentCounts {
		if got := segmentsAfterDocuments(tc.name); got != tc.want {
			t.Errorf("segmentsAfterDocuments(%q) = %d, want %d", tc.name, got, tc.want)
		}
	}
}
