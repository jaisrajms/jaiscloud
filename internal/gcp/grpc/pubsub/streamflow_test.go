package pubsub

import (
	"testing"

	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
)

func TestStreamFlowControlClaimBudget(t *testing.T) {
	// Unlimited when the client sets 0 or omits the limit.
	if got := newStreamFlowControl(0, 0).claimBudget(100); got != 100 {
		t.Fatalf("unlimited claimBudget = %d, want 100", got)
	}

	fc := newStreamFlowControl(1, 0)
	if got := fc.claimBudget(100); got != 1 {
		t.Fatalf("maxMsgs=1 empty claimBudget = %d, want 1", got)
	}
	if !fc.reserve("m1", 10, false) {
		t.Fatal("reserve m1 = false, want true")
	}
	if got := fc.claimBudget(100); got != 0 {
		t.Fatalf("maxMsgs=1 saturated claimBudget = %d, want 0", got)
	}
	// An ack frees the slot and delivery resumes.
	fc.release("m1")
	if got := fc.claimBudget(100); got != 1 {
		t.Fatalf("maxMsgs=1 after release claimBudget = %d, want 1", got)
	}

	// A larger limit clamps the batch to the remaining slots.
	fc = newStreamFlowControl(5, 0)
	if !fc.reserve("a", 1, false) || !fc.reserve("b", 1, false) {
		t.Fatal("reserve a,b = false, want true")
	}
	if got := fc.claimBudget(100); got != 3 {
		t.Fatalf("maxMsgs=5 with 2 outstanding claimBudget = %d, want 3", got)
	}
	if got := fc.claimBudget(2); got != 2 {
		t.Fatalf("batch smaller than remaining claimBudget = %d, want 2", got)
	}
}

func TestStreamFlowControlReserveBytes(t *testing.T) {
	fc := newStreamFlowControl(0, 10)
	if !fc.reserve("m1", 6, false) {
		t.Fatal("reserve m1 (6 bytes) = false, want true")
	}
	if fc.reserve("m2", 6, false) {
		t.Fatal("reserve m2 (would total 12 > 10) = true, want false")
	}
	if fc.bytesExhausted() {
		t.Fatal("bytesExhausted with 6/10 bytes = true, want false")
	}
	// Idempotent redelivery of an already-outstanding message stays admitted
	// without double-counting bytes.
	if !fc.reserve("m1", 6, false) {
		t.Fatal("re-reserve m1 = false, want true")
	}
	fc.release("m1")
	if fc.bytesExhausted() {
		t.Fatal("bytesExhausted after release = true, want false")
	}

	// An oversized message is admitted once when nothing is outstanding, so it
	// cannot deadlock the stream.
	fc = newStreamFlowControl(0, 5)
	if !fc.reserve("big", 100, true) {
		t.Fatal("reserve oversized with allowOverflow = false, want true")
	}
	if !fc.bytesExhausted() {
		t.Fatal("bytesExhausted after oversized reserve = false, want true")
	}
	if fc.reserve("next", 1, false) {
		t.Fatal("reserve after oversized = true, want false")
	}
}

func TestStreamFlowControlReleaseIdempotent(t *testing.T) {
	fc := newStreamFlowControl(2, 100)
	fc.reserve("m1", 10, false)
	fc.release("m1")
	fc.release("m1") // double release must not underflow the byte counter
	if fc.outstandingCount() != 0 || fc.bytesExhausted() {
		t.Fatalf("after double release count=%d bytes=%d, want 0/0", fc.outstandingCount(), fc.bytes)
	}
	if !fc.reserve("m1", 10, false) {
		t.Fatal("re-reserve after double release = false, want true")
	}
}

func TestStreamFlowControlReconcileReleasesAckedElsewhere(t *testing.T) {
	fc := newStreamFlowControl(2, 0)
	fc.reserve("a", 1, false)
	fc.reserve("b", 1, false)

	// Only "a" still exists in the store: "b" was acked out of band.
	fc.reconcile([]pubsubstore.Message{{MessageID: "a"}})

	if got := fc.outstandingCount(); got != 1 {
		t.Fatalf("after reconcile count = %d, want 1", got)
	}
	ids := fc.outstandingIDs()
	if len(ids) != 1 || ids[0] != "a" {
		t.Fatalf("after reconcile outstanding = %v, want [a]", ids)
	}
}
