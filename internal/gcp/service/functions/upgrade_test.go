package functions

import (
	"context"
	"errors"
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newUpgradeService returns a core over an empty in-memory store with a
// function already created (no source, so it stays metadata-only).
func newUpgradeService(t *testing.T) (*Service, context.Context) {
	t.Helper()
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	in := FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{"runtime": "nodejs20", "entryPoint": "handler"},
	}, V2)
	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "f1", in, V2); err != nil {
		t.Fatalf("create: %v", err)
	}
	return s, ctx
}

func assertUpgradeState(t *testing.T, s *Service, ctx context.Context, want string) functionsstore.Function {
	t.Helper()
	f, err := s.GetFunction(ctx, "proj", "us-central1", "f1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if f.UpgradeState != want {
		t.Fatalf("upgradeState = %q, want %q", f.UpgradeState, want)
	}
	return f
}

// TestUpgradeSetupRedirectRollback covers the happy-path traffic lifecycle:
// setup captures the Gen2 overrides without moving traffic, redirect moves
// traffic, and rollback moves it back.
func TestUpgradeSetupRedirectRollback(t *testing.T) {
	s, ctx := newUpgradeService(t)

	if _, err := s.SetupFunctionUpgradeConfig(ctx, "proj", "us-central1", "f1", UpgradeConfig{Runtime: "nodejs22", MaxInstanceCount: 5}, V2); err != nil {
		t.Fatalf("setup: %v", err)
	}
	f := assertUpgradeState(t, s, ctx, UpgradeStateSetupSuccessful)
	if f.UpgradeRuntime != "nodejs22" || f.UpgradeMaxInstances != 5 {
		t.Fatalf("setup overrides = %q/%d", f.UpgradeRuntime, f.UpgradeMaxInstances)
	}
	if f.UpgradeTrafficGen2 {
		t.Fatal("setup must not move traffic")
	}
	ui := FunctionJSON(V2, "proj", f)["upgradeInfo"].(map[string]any)
	if ui["upgradeState"] != UpgradeStateSetupSuccessful {
		t.Fatalf("upgradeInfo = %+v", ui)
	}

	op, err := s.RedirectFunctionUpgradeTraffic(ctx, "proj", "us-central1", "f1", V2)
	if err != nil {
		t.Fatalf("redirect: %v", err)
	}
	f = assertUpgradeState(t, s, ctx, UpgradeStateRedirectSuccessful)
	if !f.UpgradeTrafficGen2 {
		t.Fatal("redirect did not move traffic to Gen2")
	}
	// The operation is persisted and readable.
	if _, err := s.GetOperationJSON(ctx, "proj", OperationName(V2, "proj", op), V2); err != nil {
		t.Fatalf("redirect op not persisted: %v", err)
	}

	if _, err := s.RollbackFunctionUpgradeTraffic(ctx, "proj", "us-central1", "f1", V2); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	f = assertUpgradeState(t, s, ctx, UpgradeStateSetupSuccessful)
	if f.UpgradeTrafficGen2 {
		t.Fatal("rollback did not revert traffic")
	}
}

// TestUpgradeCommitAndAbort covers the terminal transitions.
func TestUpgradeCommitAndAbort(t *testing.T) {
	s, ctx := newUpgradeService(t)
	setup := func() {
		t.Helper()
		if _, err := s.SetupFunctionUpgradeConfig(ctx, "proj", "us-central1", "f1", UpgradeConfig{Runtime: "nodejs22"}, V2); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if _, err := s.RedirectFunctionUpgradeTraffic(ctx, "proj", "us-central1", "f1", V2); err != nil {
			t.Fatalf("redirect: %v", err)
		}
	}

	// commit folds the Gen2 runtime in and clears the in-flight state.
	setup()
	if _, err := s.CommitFunctionUpgrade(ctx, "proj", "us-central1", "f1", V2); err != nil {
		t.Fatalf("commit: %v", err)
	}
	f := assertUpgradeState(t, s, ctx, "")
	if f.Runtime != "nodejs22" || !f.UpgradeTrafficGen2 {
		t.Fatalf("commit did not finalize Gen2 config: runtime=%q traffic=%v", f.Runtime, f.UpgradeTrafficGen2)
	}

	// abort discards the Gen2 copy and reverts the overrides.
	if _, err := s.SetupFunctionUpgradeConfig(ctx, "proj", "us-central1", "f1", UpgradeConfig{Runtime: "python312"}, V2); err != nil {
		t.Fatalf("re-setup: %v", err)
	}
	if _, err := s.RedirectFunctionUpgradeTraffic(ctx, "proj", "us-central1", "f1", V2); err != nil {
		t.Fatalf("re-redirect: %v", err)
	}
	if _, err := s.AbortFunctionUpgrade(ctx, "proj", "us-central1", "f1", V2); err != nil {
		t.Fatalf("abort: %v", err)
	}
	f = assertUpgradeState(t, s, ctx, "")
	if f.UpgradeRuntime != "" || f.UpgradeMaxInstances != 0 || f.UpgradeTrafficGen2 {
		t.Fatalf("abort left upgrade state behind: %+v", f)
	}
}

// TestUpgradeCommitAsGen2AndDetach covers the remaining terminal methods.
func TestUpgradeCommitAsGen2AndDetach(t *testing.T) {
	s, ctx := newUpgradeService(t)
	if _, err := s.SetupFunctionUpgradeConfig(ctx, "proj", "us-central1", "f1", UpgradeConfig{}, V2); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := s.RedirectFunctionUpgradeTraffic(ctx, "proj", "us-central1", "f1", V2); err != nil {
		t.Fatalf("redirect: %v", err)
	}
	if _, err := s.CommitFunctionUpgradeAsGen2(ctx, "proj", "us-central1", "f1", V2); err != nil {
		t.Fatalf("commitAsGen2: %v", err)
	}
	assertUpgradeState(t, s, ctx, UpgradeStateCommitAsGen2Successful)

	if _, err := s.DetachFunction(ctx, "proj", "us-central1", "f1", V2); err != nil {
		t.Fatalf("detach: %v", err)
	}
	f := assertUpgradeState(t, s, ctx, "")
	if f.UpgradeTrafficGen2 {
		t.Fatal("detach left traffic on the Gen2 copy")
	}
}

// TestUpgradeInvalidTransitions covers the precondition and not-found paths.
func TestUpgradeInvalidTransitions(t *testing.T) {
	s, ctx := newUpgradeService(t)

	// redirect before setup is a precondition failure (HTTP 400, gRPC
	// FAILED_PRECONDITION).
	_, err := s.RedirectFunctionUpgradeTraffic(ctx, "proj", "us-central1", "f1", V2)
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "FailedPrecondition" || perr.HTTPStatus != 400 {
		t.Fatalf("redirect before setup: %v", err)
	}
	// rollback/commit without a redirect are also rejected.
	if _, err := s.RollbackFunctionUpgradeTraffic(ctx, "proj", "us-central1", "f1", V2); err == nil {
		t.Fatal("rollback before setup should fail")
	}
	if _, err := s.CommitFunctionUpgrade(ctx, "proj", "us-central1", "f1", V2); err == nil {
		t.Fatal("commit before redirect should fail")
	}

	// An unknown function is NotFound.
	_, err = s.SetupFunctionUpgradeConfig(ctx, "proj", "us-central1", "missing", UpgradeConfig{}, V2)
	assertNotFound(t, err)
}

// TestUpgradeInputFromMap extracts the setupFunctionUpgradeConfig overrides.
func TestUpgradeInputFromMap(t *testing.T) {
	cfg := UpgradeInputFromMap(map[string]any{
		"buildConfigOverrides":   map[string]any{"runtime": "nodejs22"},
		"serviceConfigOverrides": map[string]any{"maxInstanceCount": float64(10)},
	})
	if cfg.Runtime != "nodejs22" || cfg.MaxInstanceCount != 10 {
		t.Fatalf("cfg = %+v", cfg)
	}
	if empty := UpgradeInputFromMap(nil); empty.Runtime != "" || empty.MaxInstanceCount != 0 {
		t.Fatalf("empty cfg = %+v", empty)
	}
}
