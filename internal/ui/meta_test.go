package ui

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"

	"jaiscloud/internal/config"
)

// accountsRegistrar is a Registrar that also contributes enumerable accounts,
// like the GCP registrar backed by the project registry.
type accountsRegistrar struct {
	stubRegistrar
	accounts []string
}

func (a accountsRegistrar) Accounts(context.Context) []string { return a.accounts }

func newAccountsCfg() *config.Config {
	return &config.Config{AccountID: "default-proj", ExtraAccounts: []string{"extra-proj"}}
}

func decodeAccounts(t *testing.T, cfg *config.Config, reg Registrar) []string {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/ui/v1/meta/accounts", nil)
	rec := httptest.NewRecorder()
	buildAccountsHandler(cfg, reg)(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp AccountsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.Accounts
}

func TestAccountsHandler_ConfiguredOnly(t *testing.T) {
	got := decodeAccounts(t, newAccountsCfg(), stubRegistrar{})
	want := []string{"default-proj", "extra-proj"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("accounts = %v, want %v", got, want)
	}
}

func TestAccountsHandler_UnionsProvider(t *testing.T) {
	reg := accountsRegistrar{accounts: []string{"created-proj", "extra-proj", ""}}
	got := decodeAccounts(t, newAccountsCfg(), reg)
	// The provider's created project is added, the duplicate + empty dropped,
	// and the whole list is sorted.
	want := []string{"created-proj", "default-proj", "extra-proj"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("accounts = %v, want %v", got, want)
	}
}

func TestMergeAccounts(t *testing.T) {
	got := mergeAccounts([]string{"b", "a", "b", ""}, []string{"c", "a", ""})
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mergeAccounts = %v, want %v", got, want)
	}
}
