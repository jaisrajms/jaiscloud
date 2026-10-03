package firestore

import (
	"context"
	"fmt"
	"testing"
)

// TestReset_ClearsChangeFeed verifies that Reset wipes the change-feed
// history and sequence, not just transaction read-sets. Before this fix,
// ChangesSince kept replaying pre-reset events (referencing documents the
// just-reset document store no longer has) because Reset only cleared
// readSets.
func TestReset_ClearsChangeFeed(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.Service.CreateDocument(ctx, "proj", "(default)", "documents/cities", "SF", nil); err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}
	if seq := p.CurrentSeq(); seq == 0 {
		t.Fatalf("expected non-zero seq after a write, got %d", seq)
	}
	if changes := p.ChangesSince(0); len(changes) != 1 {
		t.Fatalf("expected 1 change before reset, got %d", len(changes))
	}

	p.Reset(ctx)

	if seq := p.CurrentSeq(); seq != 0 {
		t.Fatalf("expected seq reset to 0, got %d", seq)
	}
	if changes := p.ChangesSince(0); len(changes) != 0 {
		t.Fatalf("expected no changes after reset, got %d: %+v", len(changes), changes)
	}

	// A resume token computed from the pre-reset seq must not replay
	// anything either, now that history has been wiped.
	if changes := p.ChangesSince(1); len(changes) != 0 {
		t.Fatalf("expected no changes replaying a pre-reset seq, got %d: %+v", len(changes), changes)
	}
}

// TestChangeFeedCapsAndAdvancesFloor verifies the change-feed retains only the
// most recent retention() events, drops the oldest, and advances the floor past
// the evicted history.
func TestChangeFeedCapsAndAdvancesFloor(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	p.changeRetentionOverride = 3

	for i := 0; i < 5; i++ {
		if _, err := p.Service.CreateDocument(ctx, "proj", "(default)", "documents/cities",
			fmt.Sprintf("d%d", i), nil); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	if got := p.CurrentSeq(); got != 5 {
		t.Fatalf("CurrentSeq = %d, want 5", got)
	}
	// Five events written, only the last three (seq 3, 4, 5) retained, so the
	// evicted floor is seq 2.
	if got := p.ChangeFloor(); got != 2 {
		t.Fatalf("ChangeFloor = %d, want 2", got)
	}

	got := p.ChangesSince(2)
	if len(got) != 3 || got[0].Seq != 3 || got[2].Seq != 5 {
		t.Fatalf("ChangesSince(2) = %+v, want seqs [3 4 5]", got)
	}
	// A token below the floor is not replayable; one at the floor is.
	if !p.ReplayableFrom(2) {
		t.Fatal("ReplayableFrom(2) = false, want true (token at the floor)")
	}
	if p.ReplayableFrom(1) {
		t.Fatal("ReplayableFrom(1) = true, want false (token below the floor)")
	}
	// ChangesSince cannot reconstruct the evicted prefix.
	if got := p.ChangesSince(0); len(got) != 3 || got[0].Seq != 3 {
		t.Fatalf("ChangesSince(0) = %+v, want retained seqs starting at 3", got)
	}
}

// TestResetClearsChangeFloor verifies Reset resets the floor alongside the log
// and sequence, so a pre-reset token is not treated as replayable.
func TestResetClearsChangeFloor(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	p.changeRetentionOverride = 2

	for i := 0; i < 5; i++ {
		if _, err := p.Service.CreateDocument(ctx, "proj", "(default)", "documents/cities",
			fmt.Sprintf("d%d", i), nil); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}
	if p.ChangeFloor() == 0 {
		t.Fatal("expected a non-zero floor before Reset")
	}

	p.Reset(ctx)

	if got := p.ChangeFloor(); got != 0 {
		t.Fatalf("ChangeFloor after Reset = %d, want 0", got)
	}
	if p.ReplayableFrom(1) {
		t.Fatal("a pre-reset token must not be replayable after Reset")
	}
}
