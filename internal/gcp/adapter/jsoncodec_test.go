package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestJSONCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		// Secret Manager
		{"POST", "/v1/projects/p/secrets?secretId=s", "Create"},
		{"GET", "/v1/projects/p/secrets", "List"},
		{"GET", "/v1/projects/p/secrets/s", "Get"},
		{"PATCH", "/v1/projects/p/secrets/s", "Update"},
		{"DELETE", "/v1/projects/p/secrets/s", "Delete"},
		{"POST", "/v1/projects/p/secrets/s:addVersion", "AddVersion"},
		{"POST", "/v1/projects/p/secrets/s/versions/1:access", "Access"},
		{"GET", "/v1/projects/p/secrets/s/versions", "ListVersions"},
		{"GET", "/v1/projects/p/secrets/s/versions/1", "GetVersion"},
		{"GET", "/v1/projects/p/secrets/versions", "Get"},
		{"GET", "/v1/projects/p/secrets/s:getIamPolicy", "GetIamPolicy"},
		{"POST", "/v1/projects/p/secrets/s:setIamPolicy", "SetIamPolicy"},
		{"POST", "/v1/projects/p/secrets/s:testIamPermissions", "TestIamPermissions"},
		// Pub/Sub
		{"PUT", "/v1/projects/p/topics/t", "TopicCreate"},
		{"PATCH", "/v1/projects/p/topics/t", "TopicUpdate"},
		{"GET", "/v1/projects/p/topics", "TopicList"},
		{"GET", "/v1/projects/p/topics/t", "TopicGet"},
		{"DELETE", "/v1/projects/p/topics/t", "TopicDelete"},
		{"POST", "/v1/projects/p/topics/t:publish", "TopicPublish"},
		{"GET", "/v1/projects/p/topics/t:getIamPolicy", "TopicGetIamPolicy"},
		{"POST", "/v1/projects/p/topics/t:setIamPolicy", "TopicSetIamPolicy"},
		{"POST", "/v1/projects/p/topics/t:testIamPermissions", "TopicTestIamPermissions"},
		{"PUT", "/v1/projects/p/subscriptions/s", "SubscriptionCreate"},
		{"GET", "/v1/projects/p/subscriptions", "SubscriptionList"},
		{"PATCH", "/v1/projects/p/subscriptions/s", "SubscriptionUpdate"},
		{"POST", "/v1/projects/p/subscriptions/s:pull", "SubscriptionPull"},
		{"POST", "/v1/projects/p/subscriptions/s:acknowledge", "SubscriptionAcknowledge"},
		{"GET", "/v1/projects/p/subscriptions/s:getIamPolicy", "SubscriptionGetIamPolicy"},
		{"POST", "/v1/projects/p/subscriptions/s:setIamPolicy", "SubscriptionSetIamPolicy"},
		{"POST", "/v1/projects/p/subscriptions/s:testIamPermissions", "SubscriptionTestIamPermissions"},
		// KMS
		{"POST", "/v1/projects/p/locations/us/keyRings?keyRingId=kr", "KeyRingCreate"},
		{"GET", "/v1/projects/p/locations/us/keyRings", "KeyRingList"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr", "KeyRingGet"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr:getIamPolicy", "KeyRingGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr:setIamPolicy", "KeyRingSetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr:testIamPermissions", "KeyRingTestIamPermissions"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys?cryptoKeyId=k", "CryptoKeyCreate"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys", "CryptoKeyList"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:encrypt", "CryptoKeyEncrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:decrypt", "CryptoKeyDecrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:updatePrimaryVersion", "CryptoKeyUpdatePrimaryVersion"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:getIamPolicy", "CryptoKeyGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:setIamPolicy", "CryptoKeySetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:testIamPermissions", "CryptoKeyTestIamPermissions"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions", "CryptoKeyVersionCreate"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions", "CryptoKeyVersionList"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3", "CryptoKeyVersionGet"},
		{"PATCH", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3", "CryptoKeyVersionUpdate"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:destroy", "CryptoKeyVersionDestroy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:asymmetricSign", "CryptoKeyVersionAsymmetricSign"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:asymmetricDecrypt", "CryptoKeyVersionAsymmetricDecrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:macSign", "CryptoKeyVersionMacSign"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:macVerify", "CryptoKeyVersionMacVerify"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3/publicKey", "CryptoKeyVersionGetPublicKey"},
		// Real KMS has no version-level :disable/:enable or IAM: these custom
		// verbs are asserted to fail loud in TestKMSVersionNonGCPVerbs.
		// IAM
		{"POST", "/v1/projects/p/serviceAccounts", "ServiceAccountCreate"},
		{"GET", "/v1/projects/p/serviceAccounts", "ServiceAccountList"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com", "ServiceAccountGet"},
		{"DELETE", "/v1/projects/p/serviceAccounts/sa@example.com", "ServiceAccountDelete"},
		{"PATCH", "/v1/projects/p/serviceAccounts/sa@example.com", "ServiceAccountPatch"},
		{"PUT", "/v1/projects/p/serviceAccounts/sa@example.com", "ServiceAccountUpdate"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com:getIamPolicy", "ServiceAccountGetIamPolicy"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:setIamPolicy", "ServiceAccountSetIamPolicy"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:testIamPermissions", "ServiceAccountTestIamPermissions"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:signBlob", "ServiceAccountSignBlob"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:signJwt", "ServiceAccountSignJwt"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:disable", "ServiceAccountDisable"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:enable", "ServiceAccountEnable"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:undelete", "ServiceAccountUndelete"},
		// IAM Credentials (iamcredentials) unique custom verbs.
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:generateAccessToken", "GenerateAccessToken"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:generateIdToken", "GenerateIdToken"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com:getAllowedLocations", "GetAllowedLocations"},
		{"POST", "/v1/projects/p/locations/us-central1/workloadIdentityPools/pool:getAllowedLocations", "GetAllowedLocations"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com/keys", "ServiceAccountKeyCreate"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com/keys", "ServiceAccountKeyList"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com/keys/kid1", "ServiceAccountKeyGet"},
		{"DELETE", "/v1/projects/p/serviceAccounts/sa@example.com/keys/kid1", "ServiceAccountKeyDelete"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com/keys/kid1:disable", "ServiceAccountKeyDisable"},
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com/keys/kid1:enable", "ServiceAccountKeyEnable"},
		// Cloud Functions
		{"POST", "/v1/projects/p/locations/us-central1/functions", "CreateFunction"},
		{"GET", "/v1/projects/p/locations/us-central1/functions", "ListFunctions"},
		{"GET", "/v1/projects/p/locations/us-central1/functions/f", "GetFunction"},
		{"PATCH", "/v1/projects/p/locations/us-central1/functions/f", "UpdateFunction"},
		{"DELETE", "/v1/projects/p/locations/us-central1/functions/f", "DeleteFunction"},
		{"POST", "/v1/projects/p/locations/us-central1/functions/f:call", "CallFunction"},
		{"POST", "/v1/projects/p/locations/us-central1/functions:generateUploadUrl", "GenerateUploadUrl"},
		{"POST", "/v1/projects/p/locations/us-central1/functions/f:generateDownloadUrl", "GenerateDownloadUrl"},
		{"GET", "/v1/projects/p/locations/us-central1/functions/f:getIamPolicy", "FunctionGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us-central1/functions/f:setIamPolicy", "FunctionSetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us-central1/functions/f:testIamPermissions", "FunctionTestIamPermissions"},
		// Eventarc
		{"POST", "/v1/projects/p/locations/us-central1/triggers", "CreateTrigger"},
		{"GET", "/v1/projects/p/locations/us-central1/triggers", "ListTriggers"},
		{"GET", "/v1/projects/p/locations/us-central1/triggers/t", "GetTrigger"},
		{"PATCH", "/v1/projects/p/locations/us-central1/triggers/t", "UpdateTrigger"},
		{"DELETE", "/v1/projects/p/locations/us-central1/triggers/t", "DeleteTrigger"},
		{"GET", "/v1/projects/p/locations/us-central1/triggers/t:getIamPolicy", "TriggerGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us-central1/triggers/t:setIamPolicy", "TriggerSetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us-central1/triggers/t:testIamPermissions", "TriggerTestIamPermissions"},
		{"POST", "/v1/projects/p/locations/us-central1/channels", "CreateChannel"},
		{"GET", "/v1/projects/p/locations/us-central1/channels", "ListChannels"},
		{"GET", "/v1/projects/p/locations/us-central1/channels/c", "GetChannel"},
		{"PATCH", "/v1/projects/p/locations/us-central1/channels/c", "UpdateChannel"},
		{"DELETE", "/v1/projects/p/locations/us-central1/channels/c", "DeleteChannel"},
		{"GET", "/v1/projects/p/locations/us-central1/providers", "ListProviders"},
		{"GET", "/v1/projects/p/locations/us-central1/providers/pubsub.googleapis.com", "GetProvider"},
		// Eventarc advanced surface (message buses / enrollments / pipelines /
		// Google API sources / channel connections / Google channel config).
		{"POST", "/v1/projects/p/locations/us-central1/messageBuses", "CreateMessageBus"},
		{"GET", "/v1/projects/p/locations/us-central1/messageBuses", "ListMessageBuses"},
		{"GET", "/v1/projects/p/locations/us-central1/messageBuses/mb", "GetMessageBus"},
		{"PATCH", "/v1/projects/p/locations/us-central1/messageBuses/mb", "UpdateMessageBus"},
		{"DELETE", "/v1/projects/p/locations/us-central1/messageBuses/mb", "DeleteMessageBus"},
		{"GET", "/v1/projects/p/locations/us-central1/messageBuses/mb:listEnrollments", "ListMessageBusEnrollments"},
		{"GET", "/v1/projects/p/locations/us-central1/messageBuses/mb:getIamPolicy", "MessageBusGetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us-central1/messageBuses/mb:setIamPolicy", "MessageBusSetIamPolicy"},
		{"POST", "/v1/projects/p/locations/us-central1/messageBuses/mb:testIamPermissions", "MessageBusTestIamPermissions"},
		{"POST", "/v1/projects/p/locations/us-central1/enrollments", "CreateEnrollment"},
		{"GET", "/v1/projects/p/locations/us-central1/enrollments", "ListEnrollments"},
		{"GET", "/v1/projects/p/locations/us-central1/enrollments/e", "GetEnrollment"},
		{"PATCH", "/v1/projects/p/locations/us-central1/enrollments/e", "UpdateEnrollment"},
		{"DELETE", "/v1/projects/p/locations/us-central1/enrollments/e", "DeleteEnrollment"},
		{"POST", "/v1/projects/p/locations/us-central1/pipelines", "CreatePipeline"},
		{"GET", "/v1/projects/p/locations/us-central1/pipelines", "ListPipelines"},
		{"GET", "/v1/projects/p/locations/us-central1/pipelines/pl", "GetPipeline"},
		{"PATCH", "/v1/projects/p/locations/us-central1/pipelines/pl", "UpdatePipeline"},
		{"DELETE", "/v1/projects/p/locations/us-central1/pipelines/pl", "DeletePipeline"},
		{"POST", "/v1/projects/p/locations/us-central1/googleApiSources", "CreateGoogleApiSource"},
		{"GET", "/v1/projects/p/locations/us-central1/googleApiSources", "ListGoogleApiSources"},
		{"GET", "/v1/projects/p/locations/us-central1/googleApiSources/s", "GetGoogleApiSource"},
		{"PATCH", "/v1/projects/p/locations/us-central1/googleApiSources/s", "UpdateGoogleApiSource"},
		{"DELETE", "/v1/projects/p/locations/us-central1/googleApiSources/s", "DeleteGoogleApiSource"},
		{"POST", "/v1/projects/p/locations/us-central1/channelConnections", "CreateChannelConnection"},
		{"GET", "/v1/projects/p/locations/us-central1/channelConnections", "ListChannelConnections"},
		{"GET", "/v1/projects/p/locations/us-central1/channelConnections/cc", "GetChannelConnection"},
		{"DELETE", "/v1/projects/p/locations/us-central1/channelConnections/cc", "DeleteChannelConnection"},
		{"GET", "/v1/projects/p/locations/us-central1/channelConnections/cc:getIamPolicy", "ChannelConnectionGetIamPolicy"},
		{"GET", "/v1/projects/p/locations/us-central1/googleChannelConfig", "GetGoogleChannelConfig"},
		{"PATCH", "/v1/projects/p/locations/us-central1/googleChannelConfig", "UpdateGoogleChannelConfig"},
		// Cloud Functions v1 top-level operations (J60).
		{"GET", "/v1/operations", "ListOperations"},
		{"GET", "/v1/operations/op1", "GetOperation"},
	}
	for _, tc := range cases {
		codec := &JSONCodec{Service: "test"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}
}

func TestWorkflowsListRevisions(t *testing.T) {
	// The GET :listRevisions custom method routes to ListWorkflowRevisions; a
	// non-GET request is not a real method and must fail loud (404) rather than
	// fall through to GetWorkflow and return the workflow.
	codec := &JSONCodec{Service: "workflows"}
	path := "/v1/projects/p/locations/us-central1/workflows/w:listRevisions"
	nr, err := codec.Decode(httptest.NewRequest("GET", path, nil), nil)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	if nr.Action != "ListWorkflowRevisions" {
		t.Errorf("GET action = %q, want ListWorkflowRevisions", nr.Action)
	}
	if _, err := codec.Decode(httptest.NewRequest("POST", path, nil), nil); err == nil {
		t.Errorf("POST %s: expected unsupported (404)", path)
	}
}

// TestKMSVersionNonGCPVerbs pins that the emulator no longer invents KMS
// cryptoKeyVersions :disable/:enable custom methods or version-level IAM: real
// KMS changes a version's state through cryptoKeyVersions.patch and scopes IAM
// to key rings/crypto keys, so these request shapes must 404, not silently
// fall through to the plain version verbs.
func TestKMSVersionNonGCPVerbs(t *testing.T) {
	codec := &JSONCodec{Service: "kms"}
	paths := []struct{ method, path string }{
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:disable"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:enable"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:getIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:setIamPolicy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:testIamPermissions"},
	}
	for _, tc := range paths {
		if _, err := codec.Decode(httptest.NewRequest(tc.method, tc.path, nil), nil); err == nil {
			t.Errorf("%s %s: decoded without error, want unsupported-operation", tc.method, tc.path)
		}
	}
}

func TestFunctionsLocationsCodec(t *testing.T) {
	// The Cloud Functions codec derives location-discovery actions from the
	// shared locations resource type; the bare path itself is claimed by the
	// Memorystore detector (see TestDetectV1Service) because the emulator host
	// cannot disambiguate the two clients, and both return the same
	// google.cloud.location.Location records.
	codec := &JSONCodec{Service: "functions"}
	for _, tc := range []struct{ path, action string }{
		{"/v1/projects/p/locations", "ListLocations"},
		{"/v1/projects/p/locations/us-central1", "GetLocation"},
	} {
		nr, err := codec.Decode(httptest.NewRequest("GET", tc.path, nil), nil)
		if err != nil {
			t.Fatalf("%s: %v", tc.path, err)
		}
		if nr.Action != tc.action {
			t.Errorf("%s: action = %q, want %q", tc.path, nr.Action, tc.action)
		}
		if nr.Service != "functions" {
			t.Errorf("%s: service = %q, want functions", tc.path, nr.Service)
		}
	}
}

func TestDetectV1Service(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/topics/t":                                                  "pubsub",
		"/v1/projects/p/subscriptions/s":                                           "pubsub",
		"/v1/projects/p/secrets/s":                                                 "secretmanager",
		"/v1/projects/p/locations/us/keyRings/kr":                                  "kms",
		"/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3": "kms",
		"/v1/projects/p/serviceAccounts/sa@x.com":                                  "iam",
		// IAM Credentials owns the unique generate* verbs; signBlob/signJwt stay
		// on iam (shared path, single origin).
		"/v1/projects/p/serviceAccounts/sa@x.com:generateAccessToken":                         "iamcredentials",
		"/v1/projects/p/serviceAccounts/sa@x.com:generateIdToken":                             "iamcredentials",
		"/v1/projects/p/serviceAccounts/sa@x.com:getAllowedLocations":                         "iamcredentials",
		"/v1/projects/p/locations/us-central1/workloadIdentityPools/pool:getAllowedLocations": "iamcredentials",
		"/v1/projects/p/serviceAccounts/sa@x.com:disable":                                     "iam",
		"/v1/projects/p/serviceAccounts/sa@x.com:undelete":                                    "iam",
		"/v1/projects/p/serviceAccounts/sa@x.com:signBlob":                                    "iam",
		// Cloud Functions v1 top-level operations (J60): no projects segment.
		"/v1/operations":     "functions",
		"/v1/operations/op1": "functions",
		"/v1/projects/p/locations/us-central1/functions/f": "functions",
		// The v2 runtime catalog is not a v1 surface.
		"/v1/projects/p/locations/us-central1/runtimes": "",
		// Cloud Workflows management + executions share the path shape; the
		// executions segment claims the workflowexecutions service.
		"/v1/projects/p/locations/us-central1/workflows/w":                     "workflows",
		"/v1/projects/p/locations/us-central1/workflows/w/executions":          "workflowexecutions",
		"/v1/projects/p/locations/us-central1/workflows/w/executions/e":        "workflowexecutions",
		"/v1/projects/p/locations/us-central1/workflows/w/executions/e:cancel": "workflowexecutions",
		"/v1/projects/p/locations/us-central1/clusters":                        "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c":                      "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c/topics":               "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c/topics/t":             "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c/consumerGroups":       "managedkafka",
		"/v1/projects/p/locations/us-central1/triggers/t":                      "eventarc",
		"/v1/projects/p/locations/us-central1/channels/c":                      "eventarc",
		"/v1/projects/p/locations/us-central1/providers/pubsub.googleapis.com": "eventarc",
		"/v1/projects/p/locations/us-central1/messageBuses/mb":                 "eventarc",
		"/v1/projects/p/locations/us-central1/enrollments/e":                   "eventarc",
		"/v1/projects/p/locations/us-central1/pipelines/pl":                    "eventarc",
		"/v1/projects/p/locations/us-central1/googleApiSources/s":              "eventarc",
		"/v1/projects/p/locations/us-central1/channelConnections/cc":           "eventarc",
		"/v1/projects/p/locations/us-central1/googleChannelConfig":             "eventarc",
		// Service Usage v1 (services collection + service custom verbs).
		"/v1/projects/p/services":                            "serviceusage",
		"/v1/projects/p/services/run.googleapis.com":         "serviceusage",
		"/v1/projects/p/services:batchEnable":                "serviceusage",
		"/v1/projects/p/services/run.googleapis.com:enable":  "serviceusage",
		"/v1/projects/p/services/run.googleapis.com:disable": "serviceusage",
		// Firestore Admin control plane (REST under the firestore service).
		"/v1/projects/p/databases":                                 "firestore",
		"/v1/projects/p/databases/db":                              "firestore",
		"/v1/projects/p/databases/db/documents/c/d":                "firestore",
		"/v1/projects/p/databases/db/collectionGroups/cg/indexes":  "firestore",
		"/v1/projects/p/databases/db/collectionGroups/cg/fields":   "firestore",
		"/v1/projects/p/databases/db/collectionGroups/cg/fields/f": "firestore",
		"/v1/projects/p/databases/db/backupSchedules":              "firestore",
		"/v1/projects/p/databases/db/backupSchedules/1":            "firestore",
		"/v1/projects/p/databases/db/userCreds":                    "firestore",
		"/v1/projects/p/databases/db/userCreds/uc:enable":          "firestore",
		"/v1/projects/p/databases/db/operations":                   "firestore",
		"/v1/projects/p/databases/db/operations/op1":               "firestore",
		"/v1/projects/p/databases/db/operations/op1:cancel":        "firestore", "/v1/projects/p/locations/us/backups": "firestore",
		"/v1/projects/p/locations/us/backups/b": "firestore",
		// Cloud KMS locations.generateRandomBytes shares the bare locations path
		// with Memorystore's location discovery; the verb discriminates.
		"/v1/projects/p/locations/us-central1:generateRandomBytes": "kms",
		// Cloud Resource Manager v1 project surface (project segment is last),
		// including the collection route.
		"/v1/projects":                      "resourcemanager",
		"/v1/projects/p":                    "resourcemanager",
		"/v1/projects/p:getIamPolicy":       "resourcemanager",
		"/v1/projects/p:setIamPolicy":       "resourcemanager",
		"/v1/projects/p:testIamPermissions": "resourcemanager",
		"/v1/projects/p:undelete":           "resourcemanager",
		"/storage/v1/b/bkt/o":               "",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
		}
	}
}

// TestFirestoreAdminDecode covers the Firestore Admin REST control-plane paths:
// the five resource families decode to their registry actions, and the
// data-plane / disaster-recovery custom verbs fail loud rather than dispatch as
// CreateDatabase.
func TestFirestoreAdminDecode(t *testing.T) {
	cases := []struct{ method, path, action string }{
		{"POST", "/v1/projects/p/databases?databaseId=db", "CreateDatabase"},
		{"GET", "/v1/projects/p/databases", "ListDatabases"},
		{"GET", "/v1/projects/p/databases/db", "GetDatabase"},
		{"PATCH", "/v1/projects/p/databases/db?updateMask=concurrencyMode", "UpdateDatabase"},
		{"DELETE", "/v1/projects/p/databases/db", "DeleteDatabase"},
		{"POST", "/v1/projects/p/databases/db/backupSchedules", "CreateBackupSchedule"},
		{"GET", "/v1/projects/p/databases/db/backupSchedules", "ListBackupSchedules"},
		{"GET", "/v1/projects/p/databases/db/backupSchedules/1", "GetBackupSchedule"},
		{"PATCH", "/v1/projects/p/databases/db/backupSchedules/1?updateMask=retention", "UpdateBackupSchedule"},
		{"DELETE", "/v1/projects/p/databases/db/backupSchedules/1", "DeleteBackupSchedule"},
		{"POST", "/v1/projects/p/databases/db/userCreds?userCredsId=uc", "CreateUserCreds"},
		{"GET", "/v1/projects/p/databases/db/userCreds", "ListUserCreds"},
		{"GET", "/v1/projects/p/databases/db/userCreds/uc", "GetUserCreds"},
		{"POST", "/v1/projects/p/databases/db/userCreds/uc:enable", "EnableUserCreds"},
		{"POST", "/v1/projects/p/databases/db/userCreds/uc:disable", "DisableUserCreds"},
		{"POST", "/v1/projects/p/databases/db/userCreds/uc:resetPassword", "ResetUserPassword"},
		{"DELETE", "/v1/projects/p/databases/db/userCreds/uc", "DeleteUserCreds"},
		{"GET", "/v1/projects/p/databases/db/collectionGroups/cg/fields", "ListFields"},
		{"GET", "/v1/projects/p/databases/db/collectionGroups/cg/fields/f", "GetField"},
		{"PATCH", "/v1/projects/p/databases/db/collectionGroups/cg/fields/f?updateMask=indexConfig", "UpdateField"},
		{"GET", "/v1/projects/p/locations/us/backups", "ListBackups"},
		{"GET", "/v1/projects/p/locations/us/backups/b", "GetBackup"},
		{"DELETE", "/v1/projects/p/locations/us/backups/b", "DeleteBackup"},
		{"GET", "/v1/projects/p/databases/db/operations", "ListOperations"},
		{"GET", "/v1/projects/p/databases/db/operations/op1", "GetOperation"},
		{"DELETE", "/v1/projects/p/databases/db/operations/op1", "DeleteOperation"},
		{"POST", "/v1/projects/p/databases/db/operations/op1:cancel", "CancelOperation"},
	}
	for _, tc := range cases {
		c := &JSONCodec{Service: "firestore"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := c.Decode(r, nil)
		if err != nil {
			t.Errorf("Decode(%s %s): %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("Decode(%s %s) action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}

	// Data-plane / disaster-recovery verbs are not modelled: fail loud instead of
	// dispatching the collection-level POSTs as CreateDatabase.
	for _, p := range []string{
		"/v1/projects/p/databases:restore",
		"/v1/projects/p/databases:clone",
		"/v1/projects/p/databases/db:exportDocuments",
		"/v1/projects/p/databases/db:importDocuments",
		"/v1/projects/p/databases/db:bulkDeleteDocuments",
	} {
		r := httptest.NewRequest("POST", p, nil)
		if _, err := (&JSONCodec{Service: "firestore"}).Decode(r, nil); err == nil {
			t.Errorf("Decode(%q) succeeded, want a fail-loud error", p)
		}
	}
}

func TestJSONCodecSubscriptionDetachRouting(t *testing.T) {
	c := &JSONCodec{Service: "pubsub"}
	r := httptest.NewRequest("POST", "/v1/projects/p/subscriptions/s:detach", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "SubscriptionDetach" {
		t.Fatalf("expected SubscriptionDetach, got %q", nr.Action)
	}
}

// TestFunctionsV2Decode pins the Cloud Functions v2 path→action mapping and
// that the API version is carried on the request (v1 stays the default).
func TestFunctionsV2Decode(t *testing.T) {
	c := &JSONCodec{Service: "functions"}
	cases := []struct {
		method, path, action string
	}{
		{"POST", "/v2/projects/p/locations/us-central1/functions", "CreateFunction"},
		{"GET", "/v2/projects/p/locations/us-central1/functions", "ListFunctions"},
		{"GET", "/v2/projects/p/locations/-/functions", "ListFunctions"},
		{"GET", "/v2/projects/p/locations/us-central1/functions/f", "GetFunction"},
		{"PATCH", "/v2/projects/p/locations/us-central1/functions/f", "UpdateFunction"},
		{"DELETE", "/v2/projects/p/locations/us-central1/functions/f", "DeleteFunction"},
		{"POST", "/v2/projects/p/locations/us-central1/functions:generateUploadUrl", "GenerateUploadUrl"},
		{"GET", "/v2/projects/p/locations/us-central1/runtimes", "ListRuntimes"},
		{"GET", "/v2/projects/p/locations/us-central1/operations", "ListOperations"},
		{"GET", "/v2/projects/p/locations/us-central1/operations/op1", "GetOperation"},
		{"POST", "/v2/projects/p/locations/us-central1/operations/op1:cancel", "CancelOperation"},
		{"POST", "/v2/projects/p/locations/us-central1/operations/op1:wait", "WaitOperation"},
		{"DELETE", "/v2/projects/p/locations/us-central1/operations/op1", "DeleteOperation"},
		{"GET", "/v2/projects/p/locations", "ListLocations"},
		{"GET", "/v2/projects/p/locations/us-central1", "GetLocation"},
		// v2 1st→2nd gen upgrade / traffic control plane (custom POST verbs).
		{"POST", "/v2/projects/p/locations/us-central1/functions/f:setupFunctionUpgradeConfig", "SetupFunctionUpgradeConfig"},
		{"POST", "/v2/projects/p/locations/us-central1/functions/f:redirectFunctionUpgradeTraffic", "RedirectFunctionUpgradeTraffic"},
		{"POST", "/v2/projects/p/locations/us-central1/functions/f:rollbackFunctionUpgradeTraffic", "RollbackFunctionUpgradeTraffic"},
		{"POST", "/v2/projects/p/locations/us-central1/functions/f:commitFunctionUpgrade", "CommitFunctionUpgrade"},
		{"POST", "/v2/projects/p/locations/us-central1/functions/f:commitFunctionUpgradeAsGen2", "CommitFunctionUpgradeAsGen2"},
		{"POST", "/v2/projects/p/locations/us-central1/functions/f:abortFunctionUpgrade", "AbortFunctionUpgrade"},
		{"POST", "/v2/projects/p/locations/us-central1/functions/f:detachFunction", "DetachFunction"},
	}
	for _, tc := range cases {
		nr, err := c.Decode(httptest.NewRequest(tc.method, tc.path, nil), nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if nr.Params["apiVersion"] != "v2" {
			t.Errorf("%s %s: apiVersion = %v, want v2", tc.method, tc.path, nr.Params["apiVersion"])
		}
		if tc.path != "/v2/projects/p/locations" && nr.Params["location"] == nil {
			t.Errorf("%s %s: missing location param", tc.method, tc.path)
		}
	}

	// v1 defaults to apiVersion v1 and derives the v1 actions unchanged.
	nr, err := c.Decode(httptest.NewRequest("GET", "/v1/projects/p/locations/us-central1/functions/f", nil), nil)
	if err != nil {
		t.Fatalf("v1 decode: %v", err)
	}
	if nr.Params["apiVersion"] != "v1" || nr.Action != "GetFunction" {
		t.Fatalf("v1 decode: apiVersion=%v action=%q, want v1/GetFunction", nr.Params["apiVersion"], nr.Action)
	}
}

func TestDetectV2Service(t *testing.T) {
	cases := map[string]string{
		"/v2/projects/p/locations/us-central1/functions":                   "functions",
		"/v2/projects/p/locations/-/functions":                             "functions",
		"/v2/projects/p/locations/us-central1/functions/f":                 "functions",
		"/v2/projects/p/locations/us-central1/functions:generateUploadUrl": "functions",
		"/v2/projects/p/locations/us-central1/runtimes":                    "functions",
		"/v2/projects/p/locations/us-central1/operations":                  "functions",
		"/v2/projects/p/locations/us-central1/operations/op1":              "functions",
		"/v2/projects/p/locations/us-central1/operations/op1:cancel":       "functions",
		"/v2/projects/p/locations":                                         "functions",
		"/v2/projects/p/locations/us-central1":                             "functions",
		"/v2/projects/p/sinks":                                             "logging",
		"/v2/projects/p/sinks/my-sink":                                     "logging",
		"/v2/projects/p/exclusions":                                        "logging",
		"/v2/organizations/12/exclusions/e":                                "logging",
		"/v2/folders/9/sinks/s":                                            "logging",
		"/v2/billingAccounts/b/sinks":                                      "logging",
		"/v2/projects/p/metrics":                                           "logging",
		"/v2/projects/p/metrics/my-metric":                                 "logging",
		"/v2/organizations/12/metrics/m":                                   "logging",
		"/v2/projects/p/other/x":                                           "",
		"/v1/projects/p/locations/us-central1/functions/f":                 "",
		"/storage/v1/b/bkt/o":                                              "",
	}
	for path, want := range cases {
		if got := detectV2Service(path); got != want {
			t.Errorf("detectV2Service(%q) = %q, want %q", path, got, want)
		}
	}
	if got, _ := DetectService(httptest.NewRequest("GET", "/v2/projects/p/locations/-/functions", nil)); got != "functions" {
		t.Errorf("DetectService(v2 functions) = %q, want functions", got)
	}
}
