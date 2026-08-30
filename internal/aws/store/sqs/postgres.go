package sqs

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSQSMessageStore implements SQSMessageStore against PostgreSQL.
// MessageRetentionPeriod is read directly from jc_resources (where QueueProvider
// persists it on CreateQueue/SetQueueAttributes) — no separate retention table.
// A background eviction worker deletes expired messages every 10 seconds.
type PostgresSQSMessageStore struct {
	pool *pgxpool.Pool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewPostgresSQSMessageStore(pool *pgxpool.Pool) *PostgresSQSMessageStore {
	ctx, cancel := context.WithCancel(context.Background())
	s := &PostgresSQSMessageStore{pool: pool, cancel: cancel}
	s.wg.Add(1)
	go s.retentionWorker(ctx)
	return s
}

// Shutdown stops the retention eviction worker.
func (s *PostgresSQSMessageStore) Shutdown() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *PostgresSQSMessageStore) retentionWorker(ctx context.Context) {
	defer s.wg.Done()
	// Real wall time: infrastructure cleanup ticker — fires every 10s of actual
	// wall time regardless of the emulated clock state.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Resolve retention via the control-plane resource entry, falling
			// back to AWS's 4-day default for queues without a row (e.g. if the
			// resource entry was deleted out-of-band).
			// The retention worker doesn't know the account/region context — it
			// does a global sweep across all accounts. This is acceptable for
			// background eviction.
			if _, err := s.pool.Exec(ctx, `
				DELETE FROM jc_sqs_messages m
				WHERE m.sent_at < now() - make_interval(secs => COALESCE(
				    (SELECT (data->>'MessageRetentionPeriod')::int
				     FROM jc_resources
				     WHERE resource_type = 'sqs_queues' AND id = m.queue_url
				       AND account_id = m.account_id AND region = m.region),
				    345600
				))
			`); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("sqs retention sweep failed", "err", err)
			}
		}
	}
}

func (s *PostgresSQSMessageStore) Send(ctx context.Context, account, region string, msg SQSMessage) (dedupMessageID, sequenceNumber string, err error) {
	// FIFO deduplication.
	if msg.DeduplicationID != "" {
		dedupKey := account + ":" + region + ":" + msg.QueueURL + ":" + msg.GroupID + ":" + msg.DeduplicationID
		var origID string
		scanErr := s.pool.QueryRow(ctx, `
			SELECT message_id FROM jc_sqs_dedup
			WHERE account_id=$1 AND region=$2 AND dedup_key=$3 AND expires_at > now()
		`, account, region, dedupKey).Scan(&origID)
		if scanErr == nil {
			return origID, "", nil // duplicate
		}
		if !errors.Is(scanErr, pgx.ErrNoRows) {
			return "", "", fmt.Errorf("dedup check: %w", scanErr)
		}
		// Record dedup entry (upsert).
		_, err = s.pool.Exec(ctx, `
			INSERT INTO jc_sqs_dedup (account_id, region, dedup_key, message_id, expires_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (account_id, region, dedup_key) DO UPDATE
				SET message_id=$4, expires_at=$5
		`, account, region, dedupKey, msg.MessageID, clock.Now().Add(5*time.Minute))
		if err != nil {
			return "", "", fmt.Errorf("dedup upsert: %w", err)
		}
	}

	// Assign FIFO sequence number via DB sequence so it's monotonic across instances.
	if msg.GroupID != "" {
		err = s.pool.QueryRow(ctx, `SELECT nextval('jc_sqs_fifo_seq')`).Scan(&sequenceNumber)
		if err != nil {
			// Fallback: use timestamp-based number if sequence not present.
			sequenceNumber = fmt.Sprintf("%020d", clock.Now().UnixNano())
		} else {
			// Format as 20-digit decimal string to match AWS format.
			var n int64
			fmt.Sscanf(sequenceNumber, "%d", &n)
			sequenceNumber = fmt.Sprintf("%020d", n)
		}
		msg.SequenceNumber = sequenceNumber
	}

	msg.MD5OfBody = fmt.Sprintf("%x", md5.Sum([]byte(msg.Body)))
	msg.ReceiptHandle = newHandle()

	// SQS stamps SentTimestamp on the broker side at accept time; the API
	// provides no way for callers to set it. Overwrite unconditionally so any
	// caller-supplied value is ignored, matching AWS semantics.
	msg.SentAt = clock.Now()

	var delayUntil *time.Time
	if !msg.DelayUntil.IsZero() {
		delayUntil = &msg.DelayUntil
	}

	attrsJSON, marshalErr := json.Marshal(msg.MessageAttributes)
	if marshalErr != nil {
		return "", "", fmt.Errorf("marshal message attributes: %w", marshalErr)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_sqs_messages
			(account_id, region, id, queue_url, receipt_handle, body, md5_of_body, group_id, dedup_id,
			 sequence_number, sent_at, delay_until, receive_count, msg_attributes)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (account_id, region, queue_url, id) DO NOTHING
	`, account, region, msg.MessageID, msg.QueueURL, msg.ReceiptHandle, msg.Body, msg.MD5OfBody,
		msg.GroupID, msg.DeduplicationID, msg.SequenceNumber,
		msg.SentAt, delayUntil, 0, attrsJSON)
	if err != nil {
		return "", "", fmt.Errorf("insert message: %w", err)
	}
	return "", msg.SequenceNumber, nil
}

func (s *PostgresSQSMessageStore) Receive(ctx context.Context, account, region, queueURL string, maxMessages int, now time.Time) ([]SQSMessage, error) {
	// SELECT ... FOR UPDATE SKIP LOCKED — safe for concurrent consumers.
	//
	// Lazy retention filter: exclude messages older than the queue's
	// MessageRetentionPeriod as resolved from jc_resources. A scalar subquery
	// (not a JOIN) is required because Postgres forbids FOR UPDATE SKIP LOCKED
	// on the nullable side of an outer join (SQLSTATE 0A000).
	rows, err := s.pool.Query(ctx, `
		SELECT id, receipt_handle, body, md5_of_body, group_id, dedup_id,
		       sequence_number, sent_at, delay_until, visible_at,
		       receive_count, first_received_at, msg_attributes
		FROM jc_sqs_messages
		WHERE account_id = $1
		  AND region = $2
		  AND queue_url = $3
		  AND (delay_until IS NULL OR delay_until <= $4)
		  AND (visible_at IS NULL OR visible_at <= $4)
		  AND sent_at > $4 - make_interval(secs => COALESCE(
		      (SELECT (data->>'MessageRetentionPeriod')::int
		       FROM jc_resources
		       WHERE resource_type = 'sqs_queues' AND id = $3
		         AND account_id = $1 AND region = $2),
		      345600
		  ))
		ORDER BY sent_at
		LIMIT $5
		FOR UPDATE SKIP LOCKED
	`, account, region, queueURL, now, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("receive query: %w", err)
	}
	defer rows.Close()

	var msgs []SQSMessage
	for rows.Next() {
		var m SQSMessage
		var delayUntil, visibleAt *time.Time
		var firstReceivedAt *time.Time
		var attrsJSON []byte
		err := rows.Scan(&m.MessageID, &m.ReceiptHandle, &m.Body, &m.MD5OfBody,
			&m.GroupID, &m.DeduplicationID, &m.SequenceNumber,
			&m.SentAt, &delayUntil, &visibleAt,
			&m.ReceiveCount, &firstReceivedAt, &attrsJSON)
		if err != nil {
			return nil, fmt.Errorf("receive scan: %w", err)
		}
		if len(attrsJSON) > 0 && string(attrsJSON) != "null" {
			_ = json.Unmarshal(attrsJSON, &m.MessageAttributes)
		}
		if delayUntil != nil {
			m.DelayUntil = *delayUntil
		}
		if visibleAt != nil {
			m.VisibleAt = *visibleAt
		}
		m.FirstReceivedAt = firstReceivedAt
		m.QueueURL = queueURL
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Update receipt handle, receive count, visible_at for each fetched message.
	for i := range msgs {
		handle := newHandle()
		msgs[i].ReceiveCount++
		msgs[i].ReceiptHandle = handle
		visibleAt := now.Add(30 * time.Second)
		msgs[i].VisibleAt = visibleAt

		var firstRecv *time.Time
		if msgs[i].FirstReceivedAt == nil {
			firstRecv = &now
			msgs[i].FirstReceivedAt = firstRecv
		}

		_, err := s.pool.Exec(ctx, `
			UPDATE jc_sqs_messages
			SET receipt_handle=$1, receive_count=receive_count+1, visible_at=$2,
			    first_received_at=COALESCE(first_received_at,$3)
			WHERE account_id=$4 AND region=$5 AND id=$6 AND queue_url=$7
		`, handle, visibleAt, now, account, region, msgs[i].MessageID, msgs[i].QueueURL)
		if err != nil {
			return nil, fmt.Errorf("update receive state: %w", err)
		}
	}
	return msgs, nil
}

func (s *PostgresSQSMessageStore) Delete(ctx context.Context, account, region, queueURL, receiptHandle string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_sqs_messages
		WHERE account_id=$1 AND region=$2 AND queue_url=$3 AND receipt_handle=$4
	`, account, region, queueURL, receiptHandle)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("receipt handle not found")
	}
	return nil
}

func (s *PostgresSQSMessageStore) ChangeVisibility(ctx context.Context, account, region, queueURL, receiptHandle string, timeoutSec int, now time.Time) error {
	var visibleAt *time.Time
	if timeoutSec > 0 {
		t := now.Add(time.Duration(timeoutSec) * time.Second)
		visibleAt = &t
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE jc_sqs_messages
		SET visible_at=$1
		WHERE account_id=$2 AND region=$3 AND queue_url=$4 AND receipt_handle=$5
	`, visibleAt, account, region, queueURL, receiptHandle)
	return err
}

func (s *PostgresSQSMessageStore) Purge(ctx context.Context, account, region, queueURL string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_sqs_messages WHERE account_id=$1 AND region=$2 AND queue_url=$3`, account, region, queueURL)
	return err
}

func (s *PostgresSQSMessageStore) GetApproximateCounts(ctx context.Context, account, region, queueURL string, now time.Time) (visible, notVisible, delayed int, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			CASE
				WHEN delay_until IS NOT NULL AND delay_until > $4 THEN 'delayed'
				WHEN visible_at IS NOT NULL AND visible_at > $4 THEN 'not_visible'
				ELSE 'visible'
			END AS state,
			count(*) AS cnt
		FROM jc_sqs_messages
		WHERE account_id=$1 AND region=$2 AND queue_url=$3
		  AND sent_at > $4 - make_interval(secs => COALESCE(
		      (SELECT (data->>'MessageRetentionPeriod')::int
		       FROM jc_resources
		       WHERE resource_type = 'sqs_queues' AND id = $3
		         AND account_id = $1 AND region = $2),
		      345600
		  ))
		GROUP BY state
	`, account, region, queueURL, now)
	if err != nil {
		return 0, 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var cnt int
		if err := rows.Scan(&state, &cnt); err != nil {
			return 0, 0, 0, err
		}
		switch state {
		case "visible":
			visible = cnt
		case "not_visible":
			notVisible = cnt
		case "delayed":
			delayed = cnt
		}
	}
	return visible, notVisible, delayed, rows.Err()
}

// SetQueueRetention is a no-op for the postgres store: MessageRetentionPeriod
func (s *PostgresSQSMessageStore) Peek(ctx context.Context, account, region, queueURL string, offset, limit int) ([]SQSMessage, int, error) {
	if limit <= 0 {
		limit = 100
	}
	var total int
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM jc_sqs_messages WHERE account_id=$1 AND region=$2 AND queue_url=$3`,
		account, region, queueURL,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("peek count: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, receipt_handle, body, md5_of_body, group_id, dedup_id,
		       sequence_number, sent_at, delay_until, visible_at,
		       receive_count, first_received_at, msg_attributes
		FROM jc_sqs_messages
		WHERE account_id = $1 AND region = $2 AND queue_url = $3
		ORDER BY sent_at
		LIMIT $4 OFFSET $5
	`, account, region, queueURL, limit, offset)
	if err != nil {
		return nil, total, fmt.Errorf("peek query: %w", err)
	}
	defer rows.Close()

	var msgs []SQSMessage
	for rows.Next() {
		var m SQSMessage
		var delayUntil, visibleAt *time.Time
		var firstReceivedAt *time.Time
		var attrsJSON []byte
		if err := rows.Scan(&m.MessageID, &m.ReceiptHandle, &m.Body, &m.MD5OfBody,
			&m.GroupID, &m.DeduplicationID, &m.SequenceNumber,
			&m.SentAt, &delayUntil, &visibleAt,
			&m.ReceiveCount, &firstReceivedAt, &attrsJSON); err != nil {
			return nil, total, fmt.Errorf("peek scan: %w", err)
		}
		if len(attrsJSON) > 0 && string(attrsJSON) != "null" {
			_ = json.Unmarshal(attrsJSON, &m.MessageAttributes)
		}
		if delayUntil != nil {
			m.DelayUntil = *delayUntil
		}
		if visibleAt != nil {
			m.VisibleAt = *visibleAt
		}
		m.FirstReceivedAt = firstReceivedAt
		m.QueueURL = queueURL
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, total, fmt.Errorf("peek rows: %w", err)
	}
	return msgs, total, nil
}

// is read directly from jc_resources, where QueueProvider persists it on
// CreateQueue / SetQueueAttributes. The single source of truth lives there.
func (s *PostgresSQSMessageStore) SetQueueRetention(_ context.Context, _, _, _ string, _ int) error {
	return nil
}

func (s *PostgresSQSMessageStore) Reset(ctx context.Context) {
	s.pool.Exec(ctx, `DELETE FROM jc_sqs_messages`)
	s.pool.Exec(ctx, `DELETE FROM jc_sqs_dedup`)
}

func (s *PostgresSQSMessageStore) ResetScope(account, region string) {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_sqs_messages WHERE account_id=$1 AND region=$2`, account, region)
	s.pool.Exec(ctx, `DELETE FROM jc_sqs_dedup    WHERE account_id=$1 AND region=$2`, account, region)
}

func (s *PostgresSQSMessageStore) ResetAccount(account string) {
	ctx := context.Background()
	s.pool.Exec(ctx, `DELETE FROM jc_sqs_messages WHERE account_id=$1`, account)
	s.pool.Exec(ctx, `DELETE FROM jc_sqs_dedup    WHERE account_id=$1`, account)
}

// ─── Snapshotter ─────────────────────────────────────────────────────────────

type pgSQSMsgRow struct {
	AccountID       string     `json:"account_id"`
	Region          string     `json:"region"`
	QueueURL        string     `json:"queue_url"`
	ID              string     `json:"id"`
	ReceiptHandle   string     `json:"receipt_handle"`
	Body            string     `json:"body"`
	MD5OfBody       string     `json:"md5_of_body"`
	MsgAttributes   []byte     `json:"msg_attributes"`
	GroupID         string     `json:"group_id"`
	DedupID         string     `json:"dedup_id"`
	SequenceNumber  string     `json:"sequence_number"`
	VisibleAt       *time.Time `json:"visible_at"`
	SentAt          time.Time  `json:"sent_at"`
	DelayUntil      *time.Time `json:"delay_until"`
	ReceiveCount    int        `json:"receive_count"`
	FirstReceivedAt *time.Time `json:"first_received_at"`
}

type pgSQSDedupRow struct {
	AccountID string    `json:"account_id"`
	Region    string    `json:"region"`
	DedupKey  string    `json:"dedup_key"`
	MessageID string    `json:"message_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type pgSQSSnap struct {
	Messages []pgSQSMsgRow   `json:"messages"`
	Dedup    []pgSQSDedupRow `json:"dedup"`
}

func (s *PostgresSQSMessageStore) IsEmpty(ctx context.Context) (bool, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM jc_sqs_messages`).Scan(&count)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

func (s *PostgresSQSMessageStore) Snapshot(ctx context.Context, w io.Writer) error {
	var snap pgSQSSnap

	rows, err := s.pool.Query(ctx, `
		SELECT account_id, region, queue_url, id, receipt_handle, body, md5_of_body,
		       msg_attributes, group_id, dedup_id, sequence_number,
		       visible_at, sent_at, delay_until, receive_count, first_received_at
		FROM jc_sqs_messages
		ORDER BY account_id, region, queue_url, sent_at
	`)
	if err != nil {
		return fmt.Errorf("snapshot sqs_messages query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var m pgSQSMsgRow
		if err := rows.Scan(
			&m.AccountID, &m.Region, &m.QueueURL, &m.ID, &m.ReceiptHandle,
			&m.Body, &m.MD5OfBody, &m.MsgAttributes, &m.GroupID, &m.DedupID,
			&m.SequenceNumber, &m.VisibleAt, &m.SentAt, &m.DelayUntil,
			&m.ReceiveCount, &m.FirstReceivedAt,
		); err != nil {
			return fmt.Errorf("snapshot sqs_messages scan: %w", err)
		}
		snap.Messages = append(snap.Messages, m)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	drows, err := s.pool.Query(ctx, `
		SELECT account_id, region, dedup_key, message_id, expires_at
		FROM jc_sqs_dedup
		ORDER BY account_id, region, dedup_key
	`)
	if err != nil {
		return fmt.Errorf("snapshot sqs_dedup query: %w", err)
	}
	defer drows.Close()
	for drows.Next() {
		var d pgSQSDedupRow
		if err := drows.Scan(&d.AccountID, &d.Region, &d.DedupKey, &d.MessageID, &d.ExpiresAt); err != nil {
			return fmt.Errorf("snapshot sqs_dedup scan: %w", err)
		}
		snap.Dedup = append(snap.Dedup, d)
	}
	if err := drows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(snap)
}

func (s *PostgresSQSMessageStore) Restore(ctx context.Context, r io.Reader) error {
	var snap pgSQSSnap
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("restore sqs begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM jc_sqs_messages`); err != nil {
		return fmt.Errorf("restore sqs truncate messages: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_sqs_dedup`); err != nil {
		return fmt.Errorf("restore sqs truncate dedup: %w", err)
	}

	for _, m := range snap.Messages {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_sqs_messages
				(account_id, region, queue_url, id, receipt_handle, body, md5_of_body,
				 msg_attributes, group_id, dedup_id, sequence_number,
				 visible_at, sent_at, delay_until, receive_count, first_received_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		`, m.AccountID, m.Region, m.QueueURL, m.ID, m.ReceiptHandle,
			m.Body, m.MD5OfBody, json.RawMessage(m.MsgAttributes),
			m.GroupID, m.DedupID, m.SequenceNumber,
			m.VisibleAt, m.SentAt, m.DelayUntil, m.ReceiveCount, m.FirstReceivedAt,
		); err != nil {
			return fmt.Errorf("restore sqs insert message: %w", err)
		}
	}

	for _, d := range snap.Dedup {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_sqs_dedup (account_id, region, dedup_key, message_id, expires_at)
			VALUES ($1,$2,$3,$4,$5)
		`, d.AccountID, d.Region, d.DedupKey, d.MessageID, d.ExpiresAt,
		); err != nil {
			return fmt.Errorf("restore sqs insert dedup: %w", err)
		}
	}

	return tx.Commit(ctx)
}
