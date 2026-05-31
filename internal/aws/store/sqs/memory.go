package sqs

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"jaiscloud/internal/clock"
)

const defaultRetentionSecs = 345600 // 4 days

// dedupEntry stores deduplication metadata for a FIFO message.
type dedupEntry struct {
	expiry    time.Time
	messageID string
}

// queueData holds per-queue messages and the configured retention period.
type queueData struct {
	messages      []*SQSMessage
	retentionSecs int
}

func (q *queueData) retention() int {
	if q.retentionSecs <= 0 {
		return defaultRetentionSecs
	}
	return q.retentionSecs
}

// MemoryMessageStore is an in-memory SQSMessageStore.
// Messages are stored in per-queue slices; FIFO ordering is preserved.
// A background goroutine removes expired messages every 10 seconds.
type MemoryMessageStore struct {
	mu     sync.Mutex
	queues map[string]*queueData // queueURL → queue state

	// FIFO deduplication: dedup key → entry (expiry + original messageID)
	dedup map[string]dedupEntry

	// FIFO sequence counter per queue
	seqCounter map[string]int64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewMemoryMessageStore() *MemoryMessageStore {
	ctx, cancel := context.WithCancel(context.Background())
	s := &MemoryMessageStore{
		queues:     make(map[string]*queueData),
		dedup:      make(map[string]dedupEntry),
		seqCounter: make(map[string]int64),
		cancel:     cancel,
	}
	s.wg.Add(1)
	go s.retentionWorker(ctx)
	return s
}

func (s *MemoryMessageStore) retentionWorker(ctx context.Context) {
	defer s.wg.Done()
	// Real wall time: infrastructure cleanup ticker. The sweep interval is a real
	// duration, not a simulated one — we want it to fire every 10s of actual time
	// regardless of what the emulated clock says.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.removeExpiredMessages()
		}
	}
}

func (s *MemoryMessageStore) removeExpiredMessages() {
	now := clock.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.queues {
		threshold := now.Add(-time.Duration(q.retention()) * time.Second)
		keep := q.messages[:0]
		for _, m := range q.messages {
			if !m.SentAt.Before(threshold) {
				keep = append(keep, m)
			}
		}
		q.messages = keep
	}
}

func (s *MemoryMessageStore) getOrCreateQueue(queueURL string) *queueData {
	q, ok := s.queues[queueURL]
	if !ok {
		q = &queueData{}
		s.queues[queueURL] = q
	}
	return q
}

func (s *MemoryMessageStore) Send(ctx context.Context, account, region string, msg SQSMessage) (dedupMessageID, sequenceNumber string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// FIFO deduplication: reject duplicates within 5-minute window.
	// Scope: "messageGroup" → key includes GroupID; default → queue-wide.
	if msg.DeduplicationID != "" {
		var dedupKey string
		if msg.DedupScope == "messageGroup" {
			dedupKey = msg.QueueURL + ":" + msg.GroupID + ":" + msg.DeduplicationID
		} else {
			dedupKey = msg.QueueURL + ":" + msg.DeduplicationID
		}
		if entry, ok := s.dedup[dedupKey]; ok && clock.Now().Before(entry.expiry) {
			return entry.messageID, "", nil // return original MessageID to caller
		}
		s.dedup[dedupKey] = dedupEntry{expiry: clock.Now().Add(5 * time.Minute), messageID: msg.MessageID}
	}

	// Assign FIFO sequence number
	if msg.GroupID != "" {
		s.seqCounter[msg.QueueURL]++
		msg.SequenceNumber = fmt.Sprintf("%020d", s.seqCounter[msg.QueueURL])
	}

	msg.MD5OfBody = fmt.Sprintf("%x", md5.Sum([]byte(msg.Body)))
	msg.ReceiptHandle = newHandle()

	// SQS stamps SentTimestamp on the broker side at accept time; the API
	// provides no way for callers to set it. Overwrite unconditionally so any
	// caller-supplied value is ignored, matching AWS semantics.
	msg.SentAt = clock.Now()

	cp := msg // copy to avoid caller mutation
	q := s.getOrCreateQueue(msg.QueueURL)
	q.messages = append(q.messages, &cp)
	return "", msg.SequenceNumber, nil
}

func (s *MemoryMessageStore) Receive(ctx context.Context, account, region, queueURL string, maxMessages int, now time.Time) ([]SQSMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queues[queueURL]
	if q == nil {
		return nil, nil
	}
	msgs := q.messages
	var result []SQSMessage

	// FIFO: track in-flight groups to preserve ordering
	inFlightGroups := map[string]bool{}
	for _, m := range msgs {
		if m.GroupID != "" && !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			inFlightGroups[m.GroupID] = true
		}
	}

	for _, m := range msgs {
		if len(result) >= maxMessages {
			break
		}
		// Skip messages not yet visible (delayed or in-flight)
		if !m.DelayUntil.IsZero() && now.Before(m.DelayUntil) {
			continue
		}
		if !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			continue
		}
		// FIFO: skip if an earlier message in the same group is in-flight
		if m.GroupID != "" && inFlightGroups[m.GroupID] {
			continue
		}

		// Assign a fresh receipt handle on each receive
		m.ReceiptHandle = newHandle()
		m.ReceiveCount++
		now2 := now
		if m.FirstReceivedAt == nil {
			m.FirstReceivedAt = &now2
		}
		// Temporarily mark as invisible (caller sets actual timeout via ChangeVisibility)
		m.VisibleAt = now.Add(30 * time.Second)

		if m.GroupID != "" {
			inFlightGroups[m.GroupID] = true
		}

		result = append(result, *m)
	}

	return result, nil
}

func (s *MemoryMessageStore) Delete(ctx context.Context, account, region, queueURL, receiptHandle string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queues[queueURL]
	if q == nil {
		return fmt.Errorf("receipt handle not found")
	}
	msgs := q.messages
	for i, m := range msgs {
		if m.ReceiptHandle == receiptHandle {
			q.messages = append(msgs[:i], msgs[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("receipt handle not found")
}

func (s *MemoryMessageStore) ChangeVisibility(ctx context.Context, account, region, queueURL, receiptHandle string, timeoutSec int, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queues[queueURL]
	if q == nil {
		return fmt.Errorf("receipt handle not found")
	}
	for _, m := range q.messages {
		if m.ReceiptHandle == receiptHandle {
			if timeoutSec == 0 {
				m.VisibleAt = time.Time{} // immediately visible
			} else {
				m.VisibleAt = now.Add(time.Duration(timeoutSec) * time.Second)
			}
			return nil
		}
	}
	return fmt.Errorf("receipt handle not found")
}

func (s *MemoryMessageStore) Purge(ctx context.Context, account, region, queueURL string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q := s.queues[queueURL]; q != nil {
		q.messages = nil
	}
	return nil
}

func (s *MemoryMessageStore) GetApproximateCounts(ctx context.Context, account, region, queueURL string, now time.Time) (visible, notVisible, delayed int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := s.queues[queueURL]
	if q == nil {
		return
	}
	for _, m := range q.messages {
		if !m.DelayUntil.IsZero() && now.Before(m.DelayUntil) {
			delayed++
		} else if !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			notVisible++
		} else {
			visible++
		}
	}
	return
}

func (s *MemoryMessageStore) Peek(_ context.Context, _, _, queueURL string, offset, limit int) ([]SQSMessage, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[queueURL]
	if q == nil {
		return nil, 0, nil
	}
	if limit <= 0 {
		limit = 100
	}
	msgs := q.messages
	total := len(msgs)
	if offset >= total {
		return nil, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	result := make([]SQSMessage, end-offset)
	for i := range end - offset {
		result[i] = *msgs[offset+i]
	}
	return result, total, nil
}

func (s *MemoryMessageStore) SetQueueRetention(_ context.Context, _, _, queueURL string, retentionSecs int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.getOrCreateQueue(queueURL)
	q.retentionSecs = retentionSecs
	return nil
}

func (s *MemoryMessageStore) Reset(ctx context.Context) {
	s.cancel()
	s.wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.queues = make(map[string]*queueData)
	s.dedup = make(map[string]dedupEntry)
	s.seqCounter = make(map[string]int64)
	s.cancel = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go s.retentionWorker(ctx)
}

func newHandle() string {
	return fmt.Sprintf("%x", rand.Int63())
}

// ─── Snapshotter ─────────────────────────────────────────────────────────────

type sqsDedupEntrySnap struct {
	Expiry    time.Time `json:"expiry"`
	MessageID string    `json:"message_id"`
}

type sqsQueueDataSnap struct {
	Messages      []*SQSMessage `json:"messages"`
	RetentionSecs int           `json:"retention_secs"`
}

type sqsMemSnap struct {
	Queues     map[string]*sqsQueueDataSnap `json:"queues"`
	Dedup      map[string]sqsDedupEntrySnap `json:"dedup"`
	SeqCounter map[string]int64             `json:"seq_counter"`
}

func (s *MemoryMessageStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.queues {
		if len(q.messages) > 0 {
			return false, nil
		}
	}
	return true, nil
}

func (s *MemoryMessageStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dedup := make(map[string]sqsDedupEntrySnap, len(s.dedup))
	for k, v := range s.dedup {
		dedup[k] = sqsDedupEntrySnap{Expiry: v.expiry, MessageID: v.messageID}
	}
	queues := make(map[string]*sqsQueueDataSnap, len(s.queues))
	for k, v := range s.queues {
		queues[k] = &sqsQueueDataSnap{Messages: v.messages, RetentionSecs: v.retentionSecs}
	}
	return json.NewEncoder(w).Encode(sqsMemSnap{Queues: queues, Dedup: dedup, SeqCounter: s.seqCounter})
}

func (s *MemoryMessageStore) Restore(_ context.Context, r io.Reader) error {
	var snap sqsMemSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queues := make(map[string]*queueData, len(snap.Queues))
	for k, v := range snap.Queues {
		if v == nil {
			continue
		}
		queues[k] = &queueData{messages: v.Messages, retentionSecs: v.RetentionSecs}
	}
	s.queues = queues
	dedup := make(map[string]dedupEntry, len(snap.Dedup))
	for k, v := range snap.Dedup {
		dedup[k] = dedupEntry{expiry: v.Expiry, messageID: v.MessageID}
	}
	s.dedup = dedup
	if snap.SeqCounter != nil {
		s.seqCounter = snap.SeqCounter
	} else {
		s.seqCounter = make(map[string]int64)
	}
	return nil
}
