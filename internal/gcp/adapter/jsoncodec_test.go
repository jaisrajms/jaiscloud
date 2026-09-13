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
		{"GET", "/v1/projects/p/secrets/s/versions/1", "GetVersion"},
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
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys?cryptoKeyId=k", "CryptoKeyCreate"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys", "CryptoKeyList"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:encrypt", "CryptoKeyEncrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:decrypt", "CryptoKeyDecrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k:updatePrimaryVersion", "CryptoKeyUpdatePrimaryVersion"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions", "CryptoKeyVersionCreate"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions", "CryptoKeyVersionList"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3", "CryptoKeyVersionGet"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:destroy", "CryptoKeyVersionDestroy"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:disable", "CryptoKeyVersionDisable"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:enable", "CryptoKeyVersionEnable"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:asymmetricSign", "CryptoKeyVersionAsymmetricSign"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:asymmetricDecrypt", "CryptoKeyVersionAsymmetricDecrypt"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:macSign", "CryptoKeyVersionMacSign"},
		{"POST", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3:macVerify", "CryptoKeyVersionMacVerify"},
		{"GET", "/v1/projects/p/locations/us/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/3/publicKey", "CryptoKeyVersionGetPublicKey"},
		// IAM
		{"POST", "/v1/projects/p/serviceAccounts", "ServiceAccountCreate"},
		{"GET", "/v1/projects/p/serviceAccounts", "ServiceAccountList"},
		{"GET", "/v1/projects/p/serviceAccounts/sa@example.com", "ServiceAccountGet"},
		{"DELETE", "/v1/projects/p/serviceAccounts/sa@example.com", "ServiceAccountDelete"},
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
		"/v1/projects/p/locations/us-central1/clusters":                            "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c":                          "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c/topics":                   "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c/topics/t":                 "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c/consumerGroups":           "managedkafka",
		"/v1/projects/p/locations/us-central1/triggers/t":                          "eventarc",
		"/v1/projects/p/locations/us-central1/channels/c":                          "eventarc",
		"/v1/projects/p/locations/us-central1/providers/pubsub.googleapis.com":     "eventarc",
		"/storage/v1/b/bkt/o":                                                      "",
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
