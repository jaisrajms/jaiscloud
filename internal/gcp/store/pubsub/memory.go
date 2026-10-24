package pubsub

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryMessages is an in-memory Messages store.
type MemoryMessages struct {
	mu       sync.RWMutex
	messages map[string]map[string]Message // queue (subscription ID) → messageID → message
	// sortedIDs caches, per queue, message IDs in ascending PublishTime order.
	// Put/Delete invalidate a queue's entry (they change membership/order);
	// claim-state mutations (Pull/UpdateDeliveryAttempt/ModifyAckDeadline/
	// BumpDeliveryVersions only touch VisibleAt/DeliveryAttempt/DeliveryVersion,
	// never PublishTime) do not, so repeated
	// polling against an unchanged queue — the common Pub/Sub access pattern —
	// avoids re-sorting the queue on every call.
	sortedIDs map[string][]string
	seq       atomic.Int64 // monotonic message-ID counter
}

// NewMemoryMessages returns an empty in-memory message store.
func NewMemoryMessages() *MemoryMessages {
	return &MemoryMessages{
		messages:  make(map[string]map[string]Message),
		sortedIDs: make(map[string][]string),
	}
}

// orderedIDsLocked returns messageIDs for a queue in ascending PublishTime
// order, building and caching them if the cache was invalidated. Callers must
// hold s.mu for writing.
func (s *MemoryMessages) orderedIDsLocked(queue string) []string {
	if ids, ok := s.sortedIDs[queue]; ok {
		return ids
	}
	msgs := s.messages[queue]
	ids := make([]string, 0, len(msgs))
	for id := range msgs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ti, tj := msgs[ids[i]].PublishTime, msgs[ids[j]].PublishTime
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return ids[i] < ids[j] // stable tie-break; message IDs are monotonic
	})
	s.sortedIDs[queue] = ids
	return ids
}

// NextID returns the next monotonic message ID for this process.
func (s *MemoryMessages) NextID(_ context.Context) (string, error) {
	return strconv.FormatInt(s.seq.Add(1), 10), nil
}

func (s *MemoryMessages) Put(_ context.Context, m Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := m.queueKey()
	if s.messages[q] == nil {
		s.messages[q] = make(map[string]Message)
	}
	s.messages[q][m.MessageID] = m
	delete(s.sortedIDs, q)
	return nil
}

func (s *MemoryMessages) List(_ context.Context, queue string) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs := s.messages[queue]
	ids := s.orderedIDsLocked(queue)
	result := make([]Message, 0, len(ids))
	for _, id := range ids {
		result = append(result, msgs[id])
	}
	return result, nil
}

// Pull atomically claims eligible messages (mirrors SQS Receive: skip delayed/
// in-flight/acked, gate ordering-key groups, then claim under the mutex).
//
// Ordering keys follow the real-GCP pull contract: every available message for
// a key is returned in one batch, in publish order, while a key that already
// has an outstanding (delivered, unacked) message is withheld entirely (only one
// outstanding batch per key at a time). Claiming a message here makes it
// outstanding, so the remainder of a key's backlog is gated on the next pull
// until this batch is acked or its deadline lapses.
func (s *MemoryMessages) Pull(_ context.Context, queue string, maxMessages, ackDeadlineSec, retentionSec int, now time.Time) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := s.messages[queue]
	ids := s.orderedIDsLocked(queue)

	// FIFO: the set of ordering keys with a pre-existing outstanding (delivered,
	// unacked, unexpired) message. Their next batch must wait. A retained acked
	// message is not outstanding and does not gate.
	inFlightGroups := map[string]bool{}
	for _, m := range msgs {
		if m.OrderingKey != "" && !m.Acked && !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			inFlightGroups[m.OrderingKey] = true
		}
	}

	var out []Message
	for _, id := range ids {
		if len(out) >= maxMessages {
			break
		}
		m := msgs[id]
		// Retention: skip (and drop) expired messages.
		if retentionSec > 0 && now.Sub(m.PublishTime) > duration(retentionSec) {
			delete(msgs, id)
			delete(s.sortedIDs, queue)
			continue
		}
		// Acknowledged ordered messages are retained but never redelivered here.
		if m.Acked {
			continue
		}
		// In-flight: still within its ack deadline.
		if !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			continue
		}
		// FIFO: skip if an earlier batch for the same ordering key is outstanding.
		if m.OrderingKey != "" && inFlightGroups[m.OrderingKey] {
			continue
		}
		// Claim. Deliberately do NOT gate the key after claiming: the rest of
		// this key's available messages belong to the same batch.
		m.VisibleAt = now.Add(duration(ackDeadlineSec))
		m.DeliveryAttempt++
		m.DeliveryVersion++
		s.messages[queue][m.MessageID] = m
		out = append(out, m)
	}
	return out, nil
}

func (s *MemoryMessages) Delete(_ context.Context, queue, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msgs, ok := s.messages[queue]; ok {
		if _, existed := msgs[messageID]; existed {
			delete(msgs, messageID)
			delete(s.sortedIDs, queue)
		}
	}
	return nil
}

// Acknowledge applies an ack under the GCP ordered-delivery contract. An
// unordered message is deleted; a keyed message is retained and acked in key
// order (see the interface doc).
func (s *MemoryMessages) Acknowledge(_ context.Context, queue, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs, ok := s.messages[queue]
	if !ok {
		return nil
	}
	m, ok := msgs[messageID]
	if !ok {
		return nil
	}
	if m.OrderingKey == "" {
		delete(msgs, messageID)
		delete(s.sortedIDs, queue)
		return nil
	}
	if m.Acked {
		// Already acknowledged (an at-least-once ack retry): idempotent.
		return nil
	}
	s.ackKeyedLocked(queue, m)
	return nil
}

// keyedGroupLocked returns a key's message IDs in publish order (ascending
// PublishTime, then message ID as a stable tie-break). Callers must hold s.mu.
func (s *MemoryMessages) keyedGroupLocked(queue, key string) []string {
	msgs := s.messages[queue]
	var group []string
	for _, id := range s.orderedIDsLocked(queue) {
		if msgs[id].OrderingKey == key {
			group = append(group, id)
		}
	}
	return group
}

// ackKeyedLocked applies the ordered ack of target: a later ack is held while an
// earlier message for the key is unacked, otherwise it is acked and cascades
// through any held later acks. Callers must hold s.mu.
func (s *MemoryMessages) ackKeyedLocked(queue string, target Message) {
	group := s.keyedGroupLocked(queue, target.OrderingKey)
	ti := -1
	for i, id := range group {
		if id == target.MessageID {
			ti = i
			break
		}
	}
	if ti < 0 {
		return
	}
	msgs := s.messages[queue]
	for i := 0; i < ti; i++ {
		if !msgs[group[i]].Acked {
			// An earlier message is still unacknowledged: hold this ack.
			cur := msgs[target.MessageID]
			cur.AckPending = true
			msgs[target.MessageID] = cur
			return
		}
	}
	cur := msgs[target.MessageID]
	cur.Acked = true
	cur.AckPending = false
	msgs[target.MessageID] = cur
	// Apply any acks that were held behind the message just acked.
	for i := ti + 1; i < len(group); i++ {
		mm := msgs[group[i]]
		if !mm.AckPending {
			break
		}
		mm.Acked = true
		mm.AckPending = false
		msgs[group[i]] = mm
	}
}

// redeliverKeyedFromLocked clears the acked/pending state of target and every
// later message for the key and makes them immediately deliverable. This is the
// real-GCP negative-ack / deadline-expiry contract: redelivery of a message
// redelivers all subsequent messages for the key, even acknowledged ones.
// Callers must hold s.mu.
func (s *MemoryMessages) redeliverKeyedFromLocked(queue string, target Message) {
	group := s.keyedGroupLocked(queue, target.OrderingKey)
	msgs := s.messages[queue]
	start := false
	for _, id := range group {
		if id == target.MessageID {
			start = true
		}
		if !start {
			continue
		}
		mm := msgs[id]
		mm.Acked = false
		mm.AckPending = false
		mm.VisibleAt = timeZero()
		msgs[id] = mm
	}
}

func (s *MemoryMessages) UpdateDeliveryAttempt(_ context.Context, queue, messageID string, attempt int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs, ok := s.messages[queue]
	if !ok {
		return nil
	}
	m, ok := msgs[messageID]
	if !ok {
		return nil
	}
	m.DeliveryAttempt = attempt
	msgs[messageID] = m
	return nil
}

// BumpDeliveryVersions advances each named message's ack-ID version so a
// previously issued ack ID no longer names the current delivery. A Seek uses it
// to invalidate outstanding ack IDs; DeliveryAttempt is left untouched.
func (s *MemoryMessages) BumpDeliveryVersions(_ context.Context, queue string, messageIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs, ok := s.messages[queue]
	if !ok {
		return nil
	}
	for _, id := range messageIDs {
		m, ok := msgs[id]
		if !ok {
			continue
		}
		m.DeliveryVersion++
		msgs[id] = m
	}
	return nil
}

// ModifyAckDeadline resets the visibility deadline for each ack ID ("queue/messageID").
func (s *MemoryMessages) ModifyAckDeadline(_ context.Context, queue string, ackIDs []string, seconds int, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ackID := range ackIDs {
		msgID := ackID
		if i := lastSlash(ackID); i >= 0 {
			msgID = ackID[i+1:]
		}
		m, ok := s.messages[queue][msgID]
		if !ok {
			continue
		}
		if seconds == 0 {
			// A nack (or deadline reset) redelivers a keyed message and every
			// later message for its key, even acknowledged ones.
			if m.OrderingKey != "" {
				s.redeliverKeyedFromLocked(queue, m)
				continue
			}
			m.VisibleAt = timeZero()
			s.messages[queue][msgID] = m
			continue
		}
		if m.Acked {
			// A retained acked ordered message is not outstanding; extending its
			// deadline must not resurface it.
			continue
		}
		m.VisibleAt = now.Add(duration(seconds))
		s.messages[queue][msgID] = m
	}
	return nil
}

func (s *MemoryMessages) Reset(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = make(map[string]map[string]Message)
	s.sortedIDs = make(map[string][]string)
}

func duration(sec int) time.Duration { return time.Duration(sec) * time.Second }
func timeZero() time.Time            { return time.Time{} }
func lastSlash(s string) int         { return strings.LastIndex(s, "/") }
