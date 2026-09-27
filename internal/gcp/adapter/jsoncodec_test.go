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
		{"GET", "/v1/projects/p/topics", "TopicList"},
		{"GET", "/v1/projects/p/topics/t", "TopicGet"},
		{"DELETE", "/v1/projects/p/topics/t", "TopicDelete"},
		{"POST", "/v1/projects/p/topics/t:publish", "TopicPublish"},
		{"GET", "/v1/projects/p/topics/t:getIamPolicy", "TopicGetIamPolicy"},
		{"POST", "/v1/projects/p/topics/t:setIamPolicy", "TopicSetIamPolicy"},
		{"POST", "/v1/projects/p/topics/t:testIamPermissions", "TopicTestIamPermissions"},
		{"PUT", "/v1/projects/p/subscriptions/s", "SubscriptionCreate"},
		{"GET", "/v1/projects/p/subscriptions", "SubscriptionList"},
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
		{"POST", "/v1/projects/p/serviceAccounts/sa@example.com/keys", "ServiceAccountKeyCreate"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com/keys", "ServiceAccountKeyList"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com/keys/kid1", "ServiceAccountKeyGet"},
		{"DELETE", "/v1/projects/p/serviceAccounts/sa@example.com/keys/kid1", "ServiceAccountKeyDelete"},
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

func TestWorkflowsListRevisionsUnsupported(t *testing.T) {
	// Workflow revision history is not modelled: the REST path must fail loud
	// (404) rather than fall through to GetWorkflow and return the workflow.
	codec := &JSONCodec{Service: "workflows"}
	for _, method := range []string{"GET", "POST"} {
		path := "/v1/projects/p/locations/us-central1/workflows/w:listRevisions"
		if _, err := codec.Decode(httptest.NewRequest(method, path, nil), nil); err == nil {
			t.Errorf("%s %s: expected unsupported (404)", method, path)
		}
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
		"/v1/projects/p/locations/us-central1/functions/f":                         "functions",
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
		// Service Usage v1 (services collection + service custom verbs).
		"/v1/projects/p/services":                            "serviceusage",
		"/v1/projects/p/services/run.googleapis.com":         "serviceusage",
		"/v1/projects/p/services:batchEnable":                "serviceusage",
		"/v1/projects/p/services/run.googleapis.com:enable":  "serviceusage",
		"/v1/projects/p/services/run.googleapis.com:disable": "serviceusage",
		// Cloud Resource Manager v1 project surface (project segment is last).
		"/v1/projects/p":                    "resourcemanager",
		"/v1/projects/p:getIamPolicy":       "resourcemanager",
		"/v1/projects/p:setIamPolicy":       "resourcemanager",
		"/v1/projects/p:testIamPermissions": "resourcemanager",
		"/storage/v1/b/bkt/o":               "",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
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
		{"GET", "/v2/projects/p/locations/us-central1/operations", "ListOperations"},
		{"GET", "/v2/projects/p/locations/us-central1/operations/op1", "GetOperation"},
		{"POST", "/v2/projects/p/locations/us-central1/operations/op1:cancel", "CancelOperation"},
		{"DELETE", "/v2/projects/p/locations/us-central1/operations/op1", "DeleteOperation"},
		{"GET", "/v2/projects/p/locations", "ListLocations"},
		{"GET", "/v2/projects/p/locations/us-central1", "GetLocation"},
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
