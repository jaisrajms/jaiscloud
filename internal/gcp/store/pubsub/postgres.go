package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresMessages implements Messages against jc_pubsub_messages. Rows are
// keyed by delivery queue: the subscription ID for fanned-out messages (see
// Message.Subscription).
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
		INSERT INTO jc_pubsub_messages (topic, subscription, message_id, data, attributes, publish_time, delivery_attempt, delivery_version, ordering_key, visible_at, acked, ack_pending, kms_key_name, wrapped_dek)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (subscription, message_id) DO UPDATE
			SET topic=$1, data=$4, attributes=$5, publish_time=$6, delivery_attempt=$7, delivery_version=$8, ordering_key=$9, visible_at=$10, acked=$11, ack_pending=$12, kms_key_name=$13, wrapped_dek=$14
	`, m.Topic, m.queueKey(), m.MessageID, m.Data, json.RawMessage(attrs), m.PublishTime, m.DeliveryAttempt, m.DeliveryVersion, m.OrderingKey, nullableTime(m.VisibleAt), m.Acked, m.AckPending, m.KmsKeyName, m.WrappedDEK)
	return err
}

func (s *PostgresMessages) List(ctx context.Context, queue string) ([]Message, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT topic, message_id, data, attributes, publish_time, delivery_attempt, delivery_version, ordering_key, visible_at, acked, ack_pending, kms_key_name, wrapped_dek
		FROM jc_pubsub_messages WHERE subscription=$1 ORDER BY publish_time, message_id
	`, queue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Message
	for rows.Next() {
		var m Message
		var attrs []byte
		var visibleAt *time.Time
		if err := rows.Scan(&m.Topic, &m.MessageID, &m.Data, &attrs, &m.PublishTime, &m.DeliveryAttempt, &m.DeliveryVersion, &m.OrderingKey, &visibleAt, &m.Acked, &m.AckPending, &m.KmsKeyName, &m.WrappedDEK); err != nil {
			return nil, err
		}
		json.Unmarshal(attrs, &m.Attributes)
		m.Subscription = queue
		if visibleAt != nil {
			m.VisibleAt = *visibleAt
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// Pull atomically claims eligible messages using FOR UPDATE SKIP LOCKED
// (mirrors SQS Receive), applying the same orderingKey contract as the memory
// store: every available message for a key is claimed in one batch, in publish
// order, while a key that already has an outstanding (unacked, unexpired)
// message is withheld entirely.
func (s *PostgresMessages) Pull(ctx context.Context, queue string, maxMessages, ackDeadlineSec, retentionSec int, now time.Time) ([]Message, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// Ordering-key groups that currently have an outstanding message (claimed,
	// unacked, within its ack deadline) gate delivery of later messages.
	inFlightGroups := map[string]bool{}
	krows, err := tx.Query(ctx, `
		SELECT DISTINCT ordering_key FROM jc_pubsub_messages
		WHERE subscription = $1 AND ordering_key <> '' AND acked = false AND visible_at > $2
	`, queue, now)
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
		SELECT topic, message_id, data, attributes, publish_time, delivery_attempt, delivery_version, ordering_key, kms_key_name, wrapped_dek
		FROM jc_pubsub_messages
		WHERE subscription = $1
		  AND acked = false
		  AND (visible_at IS NULL OR visible_at <= $2)
		  AND ($3 <= 0 OR publish_time > $2 - make_interval(secs => $3))
		ORDER BY publish_time, message_id
		FOR UPDATE SKIP LOCKED
	`, queue, now, retentionSec)
	if err != nil {
		return nil, err
	}
	var candidates []Message
	for rows.Next() {
		var m Message
		var attrs []byte
		if err := rows.Scan(&m.Topic, &m.MessageID, &m.Data, &attrs, &m.PublishTime, &m.DeliveryAttempt, &m.DeliveryVersion, &m.OrderingKey, &m.KmsKeyName, &m.WrappedDEK); err != nil {
			rows.Close()
			return nil, err
		}
		json.Unmarshal(attrs, &m.Attributes)
		m.Subscription = queue
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
		// FIFO: skip a key that already has an outstanding batch.
		if m.OrderingKey != "" && inFlightGroups[m.OrderingKey] {
			continue
		}
		m.VisibleAt = now.Add(time.Duration(ackDeadlineSec) * time.Second)
		m.DeliveryAttempt++
		m.DeliveryVersion++
		out = append(out, m)
	}

	for _, m := range out {
		if _, err := tx.Exec(ctx, `
			UPDATE jc_pubsub_messages SET visible_at=$2, delivery_attempt=$3, delivery_version=$4
			WHERE subscription=$1 AND message_id=$5
		`, queue, m.VisibleAt, m.DeliveryAttempt, m.DeliveryVersion, m.MessageID); err != nil {
			return nil, err
		}
	}
	return out, tx.Commit(ctx)
}

// Acknowledge applies an ack under the GCP ordered-delivery contract, mirroring
// the memory store: an unordered message is deleted, a keyed message is retained
// and acked in key order (held while an earlier message is unacked, cascading
// through held later acks otherwise).
func (s *PostgresMessages) Acknowledge(ctx context.Context, queue, messageID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var key string
	var alreadyAcked bool
	if err := tx.QueryRow(ctx, `
		SELECT ordering_key, acked FROM jc_pubsub_messages
		WHERE subscription=$1 AND message_id=$2 FOR UPDATE
	`, queue, messageID).Scan(&key, &alreadyAcked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tx.Commit(ctx)
		}
		return err
	}
	if key != "" && alreadyAcked {
		// Already acknowledged (an at-least-once ack retry): idempotent.
		return tx.Commit(ctx)
	}
	if key == "" {
		if _, err := tx.Exec(ctx, `DELETE FROM jc_pubsub_messages WHERE subscription=$1 AND message_id=$2`, queue, messageID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	// Hold the ack while any earlier message for the key is unacknowledged.
	var blockers int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM jc_pubsub_messages
		WHERE subscription=$1 AND ordering_key=$2 AND acked = false
		  AND (publish_time, message_id) < (SELECT publish_time, message_id FROM jc_pubsub_messages WHERE subscription=$1 AND message_id=$3)
	`, queue, key, messageID).Scan(&blockers); err != nil {
		return err
	}
	if blockers > 0 {
		_, err := tx.Exec(ctx, `UPDATE jc_pubsub_messages SET ack_pending=true WHERE subscription=$1 AND message_id=$2`, queue, messageID)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `UPDATE jc_pubsub_messages SET acked=true, ack_pending=false WHERE subscription=$1 AND message_id=$2`, queue, messageID); err != nil {
		return err
	}
	// Apply acks held behind the message just acked, in key order, until the
	// first message that is not pending.
	rows, err := tx.Query(ctx, `
		SELECT message_id, ack_pending FROM jc_pubsub_messages
		WHERE subscription=$1 AND ordering_key=$2
		  AND (publish_time, message_id) > (SELECT publish_time, message_id FROM jc_pubsub_messages WHERE subscription=$1 AND message_id=$3)
		ORDER BY publish_time, message_id
	`, queue, key, messageID)
	if err != nil {
		return err
	}
	var pending []string
	stop := false
	for rows.Next() {
		var id string
		var pend bool
		if err := rows.Scan(&id, &pend); err != nil {
			rows.Close()
			return err
		}
		if !pend || stop {
			stop = true
			continue
		}
		pending = append(pending, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range pending {
		if _, err := tx.Exec(ctx, `UPDATE jc_pubsub_messages SET acked=true, ack_pending=false WHERE subscription=$1 AND message_id=$2`, queue, id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *PostgresMessages) Delete(ctx context.Context, queue, messageID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_pubsub_messages WHERE subscription=$1 AND message_id=$2`, queue, messageID)
	return err
}

func (s *PostgresMessages) UpdateDeliveryAttempt(ctx context.Context, queue, messageID string, attempt int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jc_pubsub_messages SET delivery_attempt=$3 WHERE subscription=$1 AND message_id=$2
	`, queue, messageID, attempt)
	return err
}

// BumpDeliveryVersions advances each named message's ack-ID version so a
// previously issued ack ID no longer names the current delivery. A Seek uses it
// to invalidate outstanding ack IDs; DeliveryAttempt (dead-letter accounting) is
// left untouched.
func (s *PostgresMessages) BumpDeliveryVersions(ctx context.Context, queue string, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE jc_pubsub_messages SET delivery_version = delivery_version + 1
		WHERE subscription=$1 AND message_id = ANY($2)
	`, queue, messageIDs)
	return err
}

// ModifyAckDeadline resets the visibility deadline for each ack ID
// ("queue/messageID"). A seconds==0 reset is a nack: it redelivers a keyed
// message and every later message for its key, even acknowledged ones, so it
// clears their acked/pending state and makes them immediately visible.
func (s *PostgresMessages) ModifyAckDeadline(ctx context.Context, queue string, ackIDs []string, seconds int, now time.Time) error {
	for _, ackID := range ackIDs {
		msgID := ackID
		if i := strings.LastIndex(ackID, "/"); i >= 0 {
			msgID = ackID[i+1:]
		}
		if seconds == 0 {
			var key string
			err := s.pool.QueryRow(ctx, `
				SELECT ordering_key FROM jc_pubsub_messages
				WHERE subscription=$1 AND message_id=$2
			`, queue, msgID).Scan(&key)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			if key == "" {
				if _, err := s.pool.Exec(ctx, `UPDATE jc_pubsub_messages SET visible_at=NULL WHERE subscription=$1 AND message_id=$2`, queue, msgID); err != nil {
					return err
				}
				continue
			}
			if _, err := s.pool.Exec(ctx, `
				UPDATE jc_pubsub_messages SET acked=false, ack_pending=false, visible_at=NULL
				WHERE subscription=$1 AND ordering_key=$2
				  AND (publish_time, message_id) >= (SELECT publish_time, message_id FROM jc_pubsub_messages WHERE subscription=$1 AND message_id=$3)
			`, queue, key, msgID); err != nil {
				return err
			}
			continue
		}
		t := now.Add(time.Duration(seconds) * time.Second)
		if _, err := s.pool.Exec(ctx, `UPDATE jc_pubsub_messages SET visible_at=$3 WHERE subscription=$1 AND message_id=$2 AND acked=false`, queue, msgID, t); err != nil {
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
