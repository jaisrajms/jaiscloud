package dataproc

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

// TestClusterRecreateWhileDeleting pins the Terraform replace edge on both
// sides of the ready delay:
//
//   - while the old incarnation is still DELETING (the delay has not elapsed)
//     the name really is taken, so a create answers ALREADY_EXISTS — the
//     emulator must not pretend the delete already happened;
//   - once the delete is due, a create settles the state machine, removes the
//     old row and succeeds — never ALREADY_EXISTS for a name the delete has
//     released.
func TestClusterRecreateWhileDeleting(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newStateProvider(t, 30*time.Minute)
	ctx := context.Background()
	const name = "replace-me"

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", name, gceInput()); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	// Settle CREATING -> RUNNING first: a replace always starts from a live
	// cluster.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Hour)})
	if _, err := p.GetCluster(ctx, "proj", "us-central1", name); err != nil {
		t.Fatalf("GetCluster (settle): %v", err)
	}
	if _, err := p.DeleteCluster(ctx, "proj", "us-central1", name); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}

	// The delete is not due yet (30m delay, no time has passed since it
	// entered DELETING): the old cluster still owns the name.
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", name, gceInput()); err == nil {
		t.Fatal("CreateCluster while DELETING and not due = success, want ALREADY_EXISTS")
	}

	// Once the delay has elapsed the create must settle the delete and succeed.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(2 * time.Hour)})
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", name, gceInput()); err != nil {
		t.Fatalf("CreateCluster after the delete became due = %v, want success", err)
	}
	if _, err := p.GetCluster(ctx, "proj", "us-central1", name); err != nil {
		t.Fatalf("GetCluster after recreate: %v", err)
	}
}
