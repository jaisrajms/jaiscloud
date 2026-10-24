package iam

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// queryNR returns a key-list request carrying a raw query string, which is how
// the repeated keyTypes parameter reaches the provider (the shared
// adapter.queryToParams keeps only the first value).
func queryNR(email, rawQuery string) *model.NormalizedRequest {
	nr := &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     map[string]any{},
		ResourceID: resource.ResourceID("proj"),
	}
	nr.Params["name"] = "serviceAccounts/" + email + "/keys"
	if rawQuery != "" {
		req, err := http.NewRequest(http.MethodGet, "/v1/projects/proj/serviceAccounts/"+email+"/keys?"+rawQuery, nil)
		if err != nil {
			panic(err)
		}
		nr.Raw = req
	}
	return nr
}

// keyFixture builds one service account with two user-managed keys and a
// system-managed signing key (created lazily by signBlob).
func keyFixture(t *testing.T) (*Provider, string) {
	t.Helper()
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create SA: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"
	for i := 0; i < 2; i++ {
		if _, err := p.ServiceAccountKeyCreate(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys"})); err != nil {
			t.Fatalf("key create %d: %v", i, err)
		}
	}
	sign := newNR(map[string]any{
		"name": "serviceAccounts/" + email,
		"body": map[string]any{"payload": "aGVsbG8="},
	})
	if _, err := p.ServiceAccountSignBlob(ctx, sign); err != nil {
		t.Fatalf("signBlob: %v", err)
	}
	return p, email
}

func listKeyTypes(t *testing.T, p *Provider, nr *model.NormalizedRequest) map[string]int {
	t.Helper()
	resp, err := p.ServiceAccountKeyList(context.Background(), nr)
	if err != nil {
		t.Fatalf("key list: %v", err)
	}
	keys, _ := resp.Data["keys"].([]any)
	counts := map[string]int{}
	for _, item := range keys {
		m, _ := item.(map[string]any)
		counts[m["keyType"].(string)]++
	}
	return counts
}

func TestServiceAccountKeyListKeyTypes(t *testing.T) {
	p, email := keyFixture(t)

	all := listKeyTypes(t, p, queryNR(email, ""))
	if all["USER_MANAGED"] != 2 || all["SYSTEM_MANAGED"] != 1 {
		t.Fatalf("unfiltered list = %v, want 2 user-managed + 1 system-managed", all)
	}

	cases := []struct {
		name  string
		query string
		want  map[string]int
	}{
		{"user managed (repeated form)", "keyTypes=USER_MANAGED", map[string]int{"USER_MANAGED": 2}},
		{"system managed (repeated form)", "keyTypes=SYSTEM_MANAGED", map[string]int{"SYSTEM_MANAGED": 1}},
		{"both repeated values", "keyTypes=USER_MANAGED&keyTypes=SYSTEM_MANAGED", map[string]int{"USER_MANAGED": 2, "SYSTEM_MANAGED": 1}},
		{"both comma joined", "keyTypes=USER_MANAGED%2CSYSTEM_MANAGED", map[string]int{"USER_MANAGED": 2, "SYSTEM_MANAGED": 1}},
		{"empty value is no filter", "keyTypes=", map[string]int{"USER_MANAGED": 2, "SYSTEM_MANAGED": 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := listKeyTypes(t, p, queryNR(email, tc.query))
			for typ, n := range got {
				if tc.want[typ] != n {
					t.Errorf("%s = %d, want %d", typ, n, tc.want[typ])
				}
			}
			for typ, n := range tc.want {
				if got[typ] != n {
					t.Errorf("%s = %d, want %d", typ, got[typ], n)
				}
			}
		})
	}

	// A request without a raw query still honours a decoded Params value (the
	// console UI path).
	nr := queryNR(email, "")
	nr.Params["keyTypes"] = "SYSTEM_MANAGED"
	if got := listKeyTypes(t, p, nr); got["SYSTEM_MANAGED"] != 1 || got["USER_MANAGED"] != 0 {
		t.Errorf("Params keyTypes list = %v, want only the system-managed key", got)
	}
}

// TestServiceAccountKeyListKeyTypesLegacyRecord covers a stored key written
// before the type was modelled: it reads back as user-managed.
func TestServiceAccountKeyListKeyTypesLegacyRecord(t *testing.T) {
	p, email := keyFixture(t)
	ctx := context.Background()
	legacy := map[string]any{
		"email":      email,
		"keyId":      "legacy",
		"algorithm":  "KEY_ALG_RSA_2048",
		"validAfter": "2026-01-01T00:00:00Z",
	}
	data, _ := json.Marshal(legacy)
	if err := p.resources.Create(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccountKey, ID: "legacy", Data: data,
	}); err != nil {
		t.Fatalf("seed legacy key: %v", err)
	}

	got := listKeyTypes(t, p, queryNR(email, "keyTypes=USER_MANAGED"))
	if got["USER_MANAGED"] != 3 || got["SYSTEM_MANAGED"] != 0 {
		t.Errorf("user-managed list = %v, want the legacy record counted as user-managed", got)
	}
	if got := listKeyTypes(t, p, queryNR(email, "keyTypes=SYSTEM_MANAGED")); got["SYSTEM_MANAGED"] != 1 {
		t.Errorf("system-managed list = %v, want only the signing key", got)
	}
}

func TestServiceAccountKeyListKeyTypesInvalid(t *testing.T) {
	p, email := keyFixture(t)
	cases := []struct{ name, query string }{
		{"unspecified", "keyTypes=KEY_TYPE_UNSPECIFIED"},
		{"duplicate repeated", "keyTypes=USER_MANAGED&keyTypes=USER_MANAGED"},
		{"duplicate comma joined", "keyTypes=USER_MANAGED%2CUSER_MANAGED"},
		{"unknown value", "keyTypes=GOOGLE_MANAGED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.ServiceAccountKeyList(context.Background(), queryNR(email, tc.query))
			if err == nil {
				t.Fatalf("expected INVALID_ARGUMENT for %s", tc.query)
			}
			perr, ok := err.(*model.ProviderError)
			if !ok {
				t.Fatalf("error type = %T, want *model.ProviderError", err)
			}
			if perr.HTTPStatus != 400 || perr.Code != "InvalidArgument" {
				t.Errorf("error = %d/%s, want 400/InvalidArgument", perr.HTTPStatus, perr.Code)
			}
		})
	}

	// Validation precedes the resource lookup, as it does on real GCP.
	missing := queryNR("ghost@proj.iam.gserviceaccount.com", "keyTypes=KEY_TYPE_UNSPECIFIED")
	if _, err := p.ServiceAccountKeyList(context.Background(), missing); err == nil {
		t.Fatal("expected INVALID_ARGUMENT for a bad enum on a missing service account")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 400 {
		t.Fatalf("error = %v, want 400 INVALID_ARGUMENT before the 404", err)
	}

	// A missing service account with no filter still 404s.
	if _, err := p.ServiceAccountKeyList(context.Background(), queryNR("ghost@proj.iam.gserviceaccount.com", "")); err == nil {
		t.Fatal("expected NotFound for a missing service account")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 404 {
		t.Fatalf("error = %v, want 404 NotFound", err)
	}
}

// TestServiceAccountKeyListResponseShape pins the per-operation field scoping
// from the Discovery document (already covered by keys_test for its happy path;
// this asserts the list side precisely).
func TestServiceAccountKeyListResponseShape(t *testing.T) {
	p, email := keyFixture(t)
	resp, err := p.ServiceAccountKeyList(context.Background(), queryNR(email, ""))
	if err != nil {
		t.Fatalf("key list: %v", err)
	}
	for _, item := range resp.Data["keys"].([]any) {
		m, _ := item.(map[string]any)
		for _, field := range []string{"privateKeyType", "privateKeyData", "publicKeyData", "keyId"} {
			if _, present := m[field]; present {
				t.Errorf("keys.list must not emit %s: %v", field, m[field])
			}
		}
		if m["validBeforeTime"] != nonExpiringKeyTime {
			t.Errorf("validBeforeTime = %v, want %s", m["validBeforeTime"], nonExpiringKeyTime)
		}
		if m["keyOrigin"] == "" || m["keyType"] == "" {
			t.Errorf("key must carry keyOrigin/keyType: %v", m)
		}
	}
}

// TestKeyTypeFilterParsing covers the decoder directly, including the
// url.Values shape the official clients produce.
func TestKeyTypeFilterParsing(t *testing.T) {
	nr := &model.NormalizedRequest{
		Params: map[string]any{},
		Raw:    &http.Request{URL: &url.URL{RawQuery: "keyTypes=SYSTEM_MANAGED&keyTypes=USER_MANAGED"}},
	}
	got, err := keyTypeFilter(nr)
	if err != nil {
		t.Fatalf("keyTypeFilter: %v", err)
	}
	if len(got) != 2 || got[0] != "SYSTEM_MANAGED" || got[1] != "USER_MANAGED" {
		t.Errorf("keyTypeFilter = %v, want [SYSTEM_MANAGED USER_MANAGED]", got)
	}
}
