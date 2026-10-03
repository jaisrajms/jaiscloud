package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"time"
)

type pubsubSnap struct {
	Messages []Message `json:"messages"`
	Seq      int64     `json:"seq"`
}

func (s *MemoryMessages) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.messages) == 0, nil
}

func (s *MemoryMessages) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	msgs := make([]Message, 0)
	for _, m := range s.messages {
		for _, msg := range m {
			msgs = append(msgs, msg)
		}
	}
	return json.NewEncoder(w).Encode(pubsubSnap{Messages: msgs, Seq: s.seq.Load()})
}

func (s *MemoryMessages) Restore(_ context.Context, r io.Reader) error {
	var snap pubsubSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	messages := make(map[string]map[string]Message)
	for _, m := range snap.Messages {
		q := m.queueKey()
		if messages[q] == nil {
			messages[q] = make(map[string]Message)
		}
		messages[q][m.MessageID] = m
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = messages
	s.seq.Store(snap.Seq)
	return nil
}

func (s *PostgresMessages) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_pubsub_messages`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresMessages) Snapshot(ctx context.Context, w io.Writer) error {
	rows, err := s.pool.Query(ctx, `SELECT topic, subscription, message_id, data, attributes, publish_time, delivery_attempt, ordering_key, visible_at, kms_key_name, wrapped_dek FROM jc_pubsub_messages ORDER BY subscription, message_id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	msgs := make([]Message, 0)
	for rows.Next() {
		var m Message
		var attrs []byte
		var visibleAt *time.Time
		if err := rows.Scan(&m.Topic, &m.Subscription, &m.MessageID, &m.Data, &attrs, &m.PublishTime, &m.DeliveryAttempt, &m.OrderingKey, &visibleAt, &m.KmsKeyName, &m.WrappedDEK); err != nil {
			return err
		}
		json.Unmarshal(attrs, &m.Attributes)
		if visibleAt != nil {
			m.VisibleAt = *visibleAt
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(pubsubSnap{Messages: msgs})
}

func (s *PostgresMessages) Restore(ctx context.Context, r io.Reader) error {
	var snap pubsubSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_pubsub_messages`); err != nil {
		return err
	}
	for _, m := range snap.Messages {
		attrs, _ := json.Marshal(m.Attributes)
		if _, err := tx.Exec(ctx, `INSERT INTO jc_pubsub_messages (topic, subscription, message_id, data, attributes, publish_time, delivery_attempt, ordering_key, visible_at, kms_key_name, wrapped_dek) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			m.Topic, m.queueKey(), m.MessageID, m.Data, json.RawMessage(attrs), m.PublishTime, m.DeliveryAttempt, m.OrderingKey, nullableTime(m.VisibleAt), m.KmsKeyName, m.WrappedDEK); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
