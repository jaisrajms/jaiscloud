package pubsub

import (
	"sync"

	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
)

// streamFlowControl tracks the messages a single StreamingPull has sent to its
// client that have not yet been acked or nacked, and throttles the stream's
// send loop to the client-declared max_outstanding_messages /
// max_outstanding_bytes. A limit <= 0 means unlimited (matching Pub/Sub), in
// which case claimBudget reserves nothing and reserve always admits, so the
// stream keeps its pre-flow-control behavior.
//
// The outstanding set is keyed by message ID, which makes release idempotent
// (a double ack cannot underflow the counters) and prevents a redelivered
// message from being counted twice. It is scoped to the stream: the shared
// message store has no per-stream claim ownership, so an ack delivered over a
// different stream is folded in by reconcile (see below) rather than by
// release.
type streamFlowControl struct {
	mu          sync.Mutex
	maxMsgs     int64
	maxBytes    int64
	outstanding map[string]int // messageID -> delivered size in bytes
	bytes       int64
	wake        chan struct{}
}

func newStreamFlowControl(maxMsgs, maxBytes int64) *streamFlowControl {
	return &streamFlowControl{
		maxMsgs:     maxMsgs,
		maxBytes:    maxBytes,
		outstanding: map[string]int{},
		wake:        make(chan struct{}, 1),
	}
}

// claimBudget returns how many messages the stream may claim in one poll given
// the message-count limit and the requested batch size. It returns 0 when the
// client's max_outstanding_messages is already reached, the batch when there is
// room for at least that many (or no limit is set), and the remaining slots
// otherwise.
func (fc *streamFlowControl) claimBudget(batch int) int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.maxMsgs <= 0 {
		return batch
	}
	remaining := int(fc.maxMsgs) - len(fc.outstanding)
	if remaining <= 0 {
		return 0
	}
	if remaining > batch {
		return batch
	}
	return remaining
}

// bytesExhausted reports whether the byte limit is already spent, so no further
// message can be admitted without exceeding it.
func (fc *streamFlowControl) bytesExhausted() bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.maxBytes > 0 && fc.bytes >= fc.maxBytes
}

// reserve records a message as outstanding if it fits within both limits.
// allowOverflow admits a message that alone exceeds max_outstanding_bytes, used
// only when nothing is outstanding so an oversized message cannot stall the
// stream forever. It returns false when the message must not be sent.
func (fc *streamFlowControl) reserve(msgID string, size int, allowOverflow bool) bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if _, ok := fc.outstanding[msgID]; ok {
		return true // already outstanding (redelivery): don't double-count
	}
	if fc.maxMsgs > 0 && int64(len(fc.outstanding)) >= fc.maxMsgs {
		return false
	}
	if fc.maxBytes > 0 && fc.bytes+int64(size) > fc.maxBytes && !allowOverflow {
		return false
	}
	fc.outstanding[msgID] = size
	fc.bytes += int64(size)
	return true
}

// release drops a message from the outstanding set after an ack or nack. It is
// idempotent and wakes the send loop so a freed slot resumes delivery.
func (fc *streamFlowControl) release(msgID string) {
	fc.mu.Lock()
	size, ok := fc.outstanding[msgID]
	if ok {
		delete(fc.outstanding, msgID)
		fc.bytes -= int64(size)
	}
	fc.mu.Unlock()
	if ok {
		fc.signal()
	}
}

// outstandingCount returns the number of messages currently outstanding.
func (fc *streamFlowControl) outstandingCount() int {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return len(fc.outstanding)
}

// outstandingIDs snapshots the outstanding message IDs.
func (fc *streamFlowControl) outstandingIDs() []string {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	ids := make([]string, 0, len(fc.outstanding))
	for id := range fc.outstanding {
		ids = append(ids, id)
	}
	return ids
}

// reconcile releases outstanding messages that are no longer present in the
// store — i.e. acked by the client over a different stream or through the
// unary Acknowledge RPC. Bounding this leak to one poll interval is what keeps
// the stream from wedging forever when acks arrive out of band.
func (fc *streamFlowControl) reconcile(stored []pubsubstore.Message) {
	if fc.outstandingCount() == 0 {
		return
	}
	live := make(map[string]struct{}, len(stored))
	for _, m := range stored {
		live[m.MessageID] = struct{}{}
	}
	for _, id := range fc.outstandingIDs() {
		if _, ok := live[id]; !ok {
			fc.release(id)
		}
	}
}

// signal wakes a send loop blocked on the flow-control limit without blocking
// when a wake is already pending.
func (fc *streamFlowControl) signal() {
	select {
	case fc.wake <- struct{}{}:
	default:
	}
}
