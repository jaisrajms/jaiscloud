package pubsub

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresMessages implements Messages against jc_pubsub_messages.
type PostgresMessages struct {
	pool *pgxpool.Pool
}

// NewPostgresMessages returns a Postgres-backed message store.
func NewPostgresMessages(pool *pgxpool.Pool) *PostgresMessages {
	return &PostgresMessages{pool: pool}
}

// NextID allocates the next message ID from the jc_pubsub_msg_seq sequence,
// keeping IDs monotonic across restarts (unlike a process-local counter).
func (s *PostgresMessages) NextID(ctx context.Context) (string, error) {
	var id int64
	if err := s.pool.QueryRow(ctx, `SELECT nextval('jc_pubsub_msg_seq')`).Scan(&id); err != nil {
		return "", err
	}
	return strconv.FormatInt(id, 10), nil
}

func (s *PostgresMessages) Put(ctx context.Context, m Message) error {
	if m.PublishTime.IsZero() {
		m.PublishTime = clock.Now()
	}
	attrs, _ := json.Marshal(m.Attributes)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_pubsub_messages (topic, message_id, data, attributes, publish_time, delivery_attempt, ordering_key, visible_at, kms_key_name, wrapped_dek)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (topic, message_id) DO UPDATE
			SET data=$3, attributes=$4, publish_time=$5, delivery_attempt=$6, ordering_key=$7, visible_at=$8, kms_key_name=$9, wrapped_dek=$10
	`, m.Topic, m.MessageID, m.Data, json.RawMessage(attrs), m.PublishTime, m.DeliveryAttempt, m.OrderingKey, nullableTime(m.VisibleAt), m.KmsKeyName, m.WrappedDEK)
	return err
}

func (s *PostgresMessages) List(ctx context.Context, topic string) ([]Message, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT message_id, data, attributes, publish_time, delivery_attempt, ordering_key, visible_at, kms_key_name, wrapped_dek
		FROM jc_pubsub_messages WHERE topic=$1 ORDER BY publish_time
	`, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Message
	for rows.Next() {
		var m Message
		var attrs []byte
		var visibleAt *time.Time
		if err := rows.Scan(&m.MessageID, &m.Data, &attrs, &m.PublishTime, &m.DeliveryAttempt, &m.OrderingKey, &visibleAt, &m.KmsKeyName, &m.WrappedDEK); err != nil {
			return nil, err
		}
		json.Unmarshal(attrs, &m.Attributes)
		m.Topic = topic
		if visibleAt != nil {
			m.VisibleAt = *visibleAt
		}
		result = append(result, m)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].PublishTime.Before(result[j].PublishTime) })
	return result, rows.Err()
}

// Pull atomically claims eligible messages using FOR UPDATE SKIP LOCKED
// (mirrors SQS Receive), applying the same orderingKey FIFO gating as the
// memory store: only the earliest eligible message in each ordering-key group
// is claimed in a single pull.
func (s *PostgresMessages) Pull(ctx context.Context, topic string, maxMessages, ackDeadlineSec, retentionSec int, now time.Time) ([]Message, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Ordering-key groups that currently have an earlier in-flight message
	// (claimed but within its ack deadline) gate delivery of later messages.
	inFlightGroups := map[string]bool{}
	krows, err := tx.Query(ctx, `
		SELECT DISTINCT ordering_key FROM jc_pubsub_messages
		WHERE topic = $1 AND ordering_key <> '' AND visible_at > $2
	`, topic, now)
	if err != nil {
		return nil, err
	}
	for krows.Next() {
		var k string
		if err := krows.Scan(&k); err != nil {
			krows.Close()
			return nil, err
		}
		inFlightGroups[k] = true
	}
	krows.Close()
	if err := krows.Err(); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, `
		SELECT message_id, data, attributes, publish_time, delivery_attempt, ordering_key, kms_key_name, wrapped_dek
		FROM jc_pubsub_messages
		WHERE topic = $1
		  AND (visible_at IS NULL OR visible_at <= $2)
		  AND publish_time > $2 - make_interval(secs => $3)
		ORDER BY publish_time
		FOR UPDATE SKIP LOCKED
	`, topic, now, retentionSec)
	if err != nil {
		return nil, err
	}
	var candidates []Message
	for rows.Next() {
		var m Message
		var attrs []byte
		if err := rows.Scan(&m.MessageID, &m.Data, &attrs, &m.PublishTime, &m.DeliveryAttempt, &m.OrderingKey, &m.KmsKeyName, &m.WrappedDEK); err != nil {
			rows.Close()
			return nil, err
		}
		json.Unmarshal(attrs, &m.Attributes)
		m.Topic = topic
		candidates = append(candidates, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var out []Message
	for _, m := range candidates {
		if len(out) >= maxMessages {
			break
		}
		// FIFO: skip if an earlier message in the same ordering-key group is in-flight.
		if m.OrderingKey != "" && inFlightGroups[m.OrderingKey] {
			continue
		}
		m.VisibleAt = now.Add(time.Duration(ackDeadlineSec) * time.Second)
		m.DeliveryAttempt++
		if m.OrderingKey != "" {
			inFlightGroups[m.OrderingKey] = true
		}
		out = append(out, m)
	}

	for _, m := range out {
		if _, err := tx.Exec(ctx, `
			UPDATE jc_pubsub_messages SET visible_at=$2, delivery_attempt=$3
			WHERE topic=$1 AND message_id=$4
		`, topic, m.VisibleAt, m.DeliveryAttempt, m.MessageID); err != nil {
			return nil, err
		}
	}
	return out, tx.Commit(ctx)
}

func (s *PostgresMessages) Delete(ctx context.Context, topic, messageID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_pubsub_messages WHERE topic=$1 AND message_id=$2`, topic, messageID)
	return err
}

func (s *PostgresMessages) UpdateDeliveryAttempt(ctx context.Context, topic, messageID string, attempt int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jc_pubsub_messages SET delivery_attempt=$3 WHERE topic=$1 AND message_id=$2
	`, topic, messageID, attempt)
	return err
}

// ModifyAckDeadline resets the visibility deadline for each ack ID ("topic/messageID").
func (s *PostgresMessages) ModifyAckDeadline(ctx context.Context, topic string, ackIDs []string, seconds int, now time.Time) error {
	for _, ackID := range ackIDs {
		msgID := ackID
		if i := strings.LastIndex(ackID, "/"); i >= 0 {
			msgID = ackID[i+1:]
		}
		var visibleAt *time.Time
		if seconds > 0 {
			t := now.Add(time.Duration(seconds) * time.Second)
			visibleAt = &t
		}
		if _, err := s.pool.Exec(ctx, `UPDATE jc_pubsub_messages SET visible_at=$2 WHERE topic=$1 AND message_id=$3`, topic, visibleAt, msgID); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresMessages) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_pubsub_messages`)
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
