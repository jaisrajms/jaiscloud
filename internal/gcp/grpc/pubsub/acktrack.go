package pubsub

import (
	"context"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
)

// ackReconcileInterval is how often a subscription's ack state is reconciled
// against the message store. It is intentionally short: it is the upper bound on
// how long a message acked out of band (e.g. restored by Seek, or acked on a
// stream that has since gone away) can linger as a stale tombstone.
const ackReconcileInterval = 250 * time.Millisecond

// ackTombstoneTTL bounds how long an acknowledged message ID is remembered. A
// tombstone only has to outlive the window between a stream claiming a message
// and sending it, so a few seconds is plenty; the TTL exists purely to bound
// memory on a long-lived, high-throughput stream.
const ackTombstoneTTL = time.Minute

// ackRegistry is the subscription-scoped authority for post-ack delivery state,
// shared by every StreamingPull stream on a subscription (and fed by the unary
// Acknowledge RPC). The message store deletes a message when it is acked, but a
// stream that already claimed the message into its send batch has a copy in
// memory. Without a shared, cross-stream view of "this message has been acked"
// two streams on the same subscription can both deliver it. The registry closes
// that window: any ack marks a tombstone by message ID, every stream drops
// tombstones from its next send batch, and a background reconciler keeps the
// tombstones honest against the store on a short poll interval (so a Seek that
// restores a message clears its tombstone) and bounds their lifetime.
//
// The registry is deliberately not per-stream — all streams on a subscription
// share one instance — while each stream keeps its own fast path (it still
// applies acks/modify-deadlines straight to the store and releases its local
// flow-control budget immediately).
//
// Scope and known limitations:
//   - The registry is fed by the gRPC service's ack paths (every StreamingPull
//     stream and the unary Acknowledge RPC). An ack that reaches the store via
//     another transport (REST) is not tombstoned synchronously; the stream's
//     claimed copy can therefore still be sent until the store's own
//     modify-deadline/ack-deadline state catches up. The task scope is ack
//     state across StreamingPull streams, which this covers.
//   - The ackId is base64url("subscription/messageID") and is stable across
//     redeliveries (no wire change), so the emulator cannot tell a superseded
//     ackId from the latest one. Real exactly-once rejects acks with a
//     superseded/expired ackId; the emulator treats a repeat as an idempotent
//     no-op. Acking is still guaranteed not to redeliver the message.
type ackRegistry struct {
	sub      string
	messages pubsubstore.Messages

	mu      sync.Mutex
	acked   map[string]time.Time // messageID -> wall time the tombstone was set
	refs    int                  // active StreamingPull streams
	stop    chan struct{}
	stopped chan struct{}
}

func newAckRegistry(sub string, messages pubsubstore.Messages) *ackRegistry {
	return &ackRegistry{
		sub:      sub,
		messages: messages,
		acked:    map[string]time.Time{},
	}
}

// acquire registers an active stream. The first stream starts the reconcile
// loop; the last one stops it.
func (r *ackRegistry) acquire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs++
	if r.refs == 1 {
		r.stop = make(chan struct{})
		r.stopped = make(chan struct{})
		go r.loop(r.stop, r.stopped)
	}
}

// release deregisters an active stream and stops the reconcile loop only when
// the last stream leaves. The stop channel is captured and nil'd under the lock
// so only the final release ever closes it (closing on an intermediate release
// would stop the loop while other streams are still live, then panic on the
// next close).
func (r *ackRegistry) release() {
	r.mu.Lock()
	if r.refs == 0 {
		r.mu.Unlock()
		return
	}
	r.refs--
	var stop chan struct{}
	if r.refs == 0 {
		stop = r.stop
		r.stop = nil
	}
	r.mu.Unlock()
	if stop != nil {
		close(stop)
	}
}

// markAcked records that messageID has been acknowledged on this subscription
// (by any stream or the unary Acknowledge RPC). It is idempotent — ack IDs are
// deduplicated by the message they decode to, so a repeated ack is a no-op.
//
// Tombstones only matter while at least one stream can hold a claimed copy, so
// they are recorded only while a stream is active; with no active stream there
// is nothing to reconcile and the map would otherwise grow without bound on a
// unary Pull+Acknowledge workload (the reconcile loop is only running while
// streams are active, so it could not prune it either).
func (r *ackRegistry) markAcked(messageID string) {
	r.mu.Lock()
	if r.refs > 0 {
		r.acked[messageID] = clock.RealNow()
	}
	r.mu.Unlock()
}

// isAcked reports whether messageID has already been acknowledged on this
// subscription and must therefore not be delivered again.
func (r *ackRegistry) isAcked(messageID string) bool {
	r.mu.Lock()
	_, ok := r.acked[messageID]
	r.mu.Unlock()
	return ok
}

// loop reconciles the ack state against the store until the last stream leaves.
func (r *ackRegistry) loop(stop, stopped chan struct{}) {
	defer close(stopped)
	t := time.NewTicker(ackReconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), longPollTimeout)
			r.reconcile(ctx)
			cancel()
		}
	}
}

// reconcile aligns the tombstone set with the authoritative store:
//
//   - a tombstoned message that is present in the store again (Seek restored it)
//     is deliverable, so its tombstone is cleared;
//   - tombstones older than ackTombstoneTTL are pruned to bound memory.
//
// It never resurrects a message that is merely absent from the store: absence is
// the normal state after an ack, and the stream fast path already applies the
// deletion to the store.
func (r *ackRegistry) reconcile(ctx context.Context) {
	live, err := r.messages.List(ctx, r.sub)
	if err != nil {
		return
	}
	present := make(map[string]struct{}, len(live))
	for _, m := range live {
		present[m.MessageID] = struct{}{}
	}
	now := clock.RealNow()
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, at := range r.acked {
		if _, ok := present[id]; ok {
			// The message was restored (Seek to a snapshot/time) and is
			// deliverable again: drop the stale tombstone.
			delete(r.acked, id)
			continue
		}
		if now.Sub(at) > ackTombstoneTTL {
			delete(r.acked, id)
		}
	}
}

// ackCount returns the number of live tombstones (test/diagnostics helper).
func (r *ackRegistry) ackCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.acked)
}
