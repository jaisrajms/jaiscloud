package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"jaiscloud/internal/store"
)

// Internal callers (event-source poller, SNS, ...) look a function up by name
// only. The memory store matches scope exactly, so loadConfig must fall back to
// a cross-scope scan when no scope is given.
func TestLoadConfigWithoutScopeFindsScopedFunction(t *testing.T) {
	st := store.NewMemoryResourceStore()
	data, _ := json.Marshal(functionConfig{FunctionName: "f"})
	if err := st.Upsert(context.Background(), "000000000000", "us-east-1", store.ResourceEntry{Type: resTypeFunction, ID: "f", Data: data}); err != nil {
		t.Fatal(err)
	}
	p := &FunctionProvider{resources: st}

	cfg, err := p.loadConfig(context.Background(), "", "", "f")
	if err != nil || cfg.FunctionName != "f" {
		t.Fatalf("unscoped lookup = %+v, %v", cfg, err)
	}
	if _, err := p.loadConfig(context.Background(), "", "", "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a missing function must stay not-found, got %v", err)
	}
	// An explicit scope is still exact: another account must not see it.
	if _, err := p.loadConfig(context.Background(), "111111111111", "us-east-1", "f"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("scoped lookup must stay exact, got %v", err)
	}
}
