package tasks

import (
	"context"
	"testing"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/model"
)

const testKMSKey = "projects/p/locations/global/keyRings/ring/cryptoKeys/key"

func TestCmekConfigLifecycle(t *testing.T) {
	ctx := context.Background()
	core, _, _ := newEngineCore(t)

	// Default: no key configured, no error (Discovery: "By default there is no
	// kms_key configured").
	cfg, err := core.GetCmekConfig(ctx, "p", "l")
	if err != nil {
		t.Fatalf("GetCmekConfig(default): %v", err)
	}
	if cfg.KmsKey != "" {
		t.Fatalf("default kmsKey = %q, want empty", cfg.KmsKey)
	}

	// Update sets it; the value round-trips.
	upd, err := core.UpdateCmekConfig(ctx, "p", "l", testKMSKey)
	if err != nil {
		t.Fatalf("UpdateCmekConfig: %v", err)
	}
	if upd.KmsKey != testKMSKey {
		t.Fatalf("UpdateCmekConfig kmsKey = %q", upd.KmsKey)
	}
	if got, err := core.GetCmekConfig(ctx, "p", "l"); err != nil || got.KmsKey != testKMSKey {
		t.Fatalf("GetCmekConfig = %+v, %v", got, err)
	}

	// Per-location isolation: another location in the same project has no key.
	if got, err := core.GetCmekConfig(ctx, "p", "other"); err != nil || got.KmsKey != "" {
		t.Fatalf("GetCmekConfig(other location) = %+v, %v", got, err)
	}

	// An empty kmsKey clears the config.
	if _, err := core.UpdateCmekConfig(ctx, "p", "l", ""); err != nil {
		t.Fatalf("UpdateCmekConfig(clear): %v", err)
	}
	if got, err := core.GetCmekConfig(ctx, "p", "l"); err != nil || got.KmsKey != "" {
		t.Fatalf("GetCmekConfig after clear = %+v, %v", got, err)
	}

	// A non-KMS-key resource name is INVALID_ARGUMENT.
	for _, bad := range []string{
		"key",
		"projects/p/locations/l/keyRings/ring",
		"projects/p/locations/l/keyRings/ring/cryptoKeys",
		"projects/p/keyRings/ring/cryptoKeys/key",
		"//cloudkms.googleapis.com/projects/p/locations/l/keyRings/ring/cryptoKeys/key",
	} {
		_, err := core.UpdateCmekConfig(ctx, "p", "l", bad)
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Fatalf("UpdateCmekConfig(%q) err = %v, want InvalidArgument/400", bad, err)
		}
	}

	// Reset clears the stored config.
	if _, err := core.UpdateCmekConfig(ctx, "p", "l", testKMSKey); err != nil {
		t.Fatalf("UpdateCmekConfig: %v", err)
	}
	core.Reset(ctx)
	if got, err := core.GetCmekConfig(ctx, "p", "l"); err != nil || got.KmsKey != "" {
		t.Fatalf("GetCmekConfig after Reset = %+v, %v", got, err)
	}
}

// TestCmekConfigPersistsThroughStore checks the store record itself (the memory
// store also backs the snapshot), including the empty default.
func TestCmekConfigPersistsThroughStore(t *testing.T) {
	ctx := context.Background()
	s := tasksstore.NewMemoryStore()
	if got, err := s.GetCmekConfig(ctx, "p", "l"); err != nil || got.KmsKey != "" {
		t.Fatalf("GetCmekConfig(default) = %+v, %v", got, err)
	}
	if err := s.SetCmekConfig(ctx, "p", "l", tasksstore.CmekConfig{KmsKey: testKMSKey}); err != nil {
		t.Fatalf("SetCmekConfig: %v", err)
	}
	if got, _ := s.GetCmekConfig(ctx, "p", "l"); got.KmsKey != testKMSKey || got.ProjectID != "p" || got.Location != "l" {
		t.Fatalf("GetCmekConfig = %+v", got)
	}
	if err := s.DeleteCmekConfig(ctx, "p", "l"); err != nil {
		t.Fatalf("DeleteCmekConfig: %v", err)
	}
	if got, _ := s.GetCmekConfig(ctx, "p", "l"); got.KmsKey != "" {
		t.Fatalf("DeleteCmekConfig left %+v", got)
	}
}
