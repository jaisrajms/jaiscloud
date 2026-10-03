package eventarc

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_eventarc_triggers /
// jc_eventarc_channels.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// --- Triggers ---

func (s *PostgresStore) CreateTrigger(ctx context.Context, projectID, location string, t Trigger) error {
	if t.CreateTime.IsZero() {
		t.CreateTime = clock.Now()
	}
	if t.UpdateTime.IsZero() {
		t.UpdateTime = t.CreateTime
	}
	labels, _ := json.Marshal(t.Labels)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_eventarc_triggers
			(project_id, location, trigger_id, config, labels, uid, etag, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, projectID, location, t.Name, nullableJSONRaw(t.Config, "{}"), nullableJSONRaw(labels, "{}"), t.UID, t.Etag, t.CreateTime, t.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanTrigger(row pgx.Row) (Trigger, error) {
	var t Trigger
	var config, labels []byte
	err := row.Scan(&t.ProjectID, &t.Location, &t.Name, &config, &labels, &t.UID, &t.Etag, &t.CreateTime, &t.UpdateTime)
	if err != nil {
		return Trigger{}, err
	}
	t.Config = json.RawMessage(config)
	json.Unmarshal(labels, &t.Labels)
	return t, nil
}

func (s *PostgresStore) GetTrigger(ctx context.Context, projectID, location, id string) (Trigger, error) {
	t, err := scanTrigger(s.pool.QueryRow(ctx, `
		SELECT project_id, location, trigger_id, config, labels, uid, etag, create_time, update_time
		FROM jc_eventarc_triggers WHERE project_id=$1 AND location=$2 AND trigger_id=$3
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Trigger{}, ErrNoSuchTrigger
	}
	return t, err
}

// UpdateTriggerAtomic mirrors MemoryStore's version: a Serializable transaction
// with SELECT ... FOR UPDATE row-locks the trigger for the duration of mutate,
// so a concurrent UpdateTriggerAtomic on the same trigger blocks until this
// transaction commits or rolls back, instead of racing to silently overwrite
// this call's write. See store/firestore/postgres.go's Commit for the same
// convention.
func (s *PostgresStore) UpdateTriggerAtomic(ctx context.Context, projectID, location, id string, mutate func(Trigger) (Trigger, error)) (Trigger, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Trigger{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanTrigger(tx.QueryRow(ctx, `
		SELECT project_id, location, trigger_id, config, labels, uid, etag, create_time, update_time
		FROM jc_eventarc_triggers WHERE project_id=$1 AND location=$2 AND trigger_id=$3 FOR UPDATE
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Trigger{}, ErrNoSuchTrigger
	}
	if err != nil {
		return Trigger{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Trigger{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_eventarc_triggers SET config=$4, labels=$5, uid=$6, etag=$7, update_time=$8
		WHERE project_id=$1 AND location=$2 AND trigger_id=$3
	`, projectID, location, id, nullableJSONRaw(next.Config, "{}"), nullableJSONRaw(labels, "{}"), next.UID, next.Etag, next.UpdateTime)
	if err != nil {
		return Trigger{}, err
	}
	if tag.RowsAffected() == 0 {
		return Trigger{}, ErrNoSuchTrigger
	}
	if err := tx.Commit(ctx); err != nil {
		return Trigger{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	return next, nil
}

func (s *PostgresStore) DeleteTrigger(ctx context.Context, projectID, location, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_eventarc_triggers WHERE project_id=$1 AND location=$2 AND trigger_id=$3`, projectID, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchTrigger
	}
	return nil
}

// DeleteTriggerAtomic mirrors MemoryStore's version: a Serializable transaction
// with SELECT ... FOR UPDATE row-locks the trigger for the duration of guard,
// so an etag precondition checked in guard is evaluated against a snapshot no
// concurrent update can invalidate before the DELETE commits.
func (s *PostgresStore) DeleteTriggerAtomic(ctx context.Context, projectID, location, id string, guard func(Trigger) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	current, err := scanTrigger(tx.QueryRow(ctx, `
		SELECT project_id, location, trigger_id, config, labels, uid, etag, create_time, update_time
		FROM jc_eventarc_triggers WHERE project_id=$1 AND location=$2 AND trigger_id=$3 FOR UPDATE
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoSuchTrigger
	}
	if err != nil {
		return err
	}
	if err := guard(current); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM jc_eventarc_triggers WHERE project_id=$1 AND location=$2 AND trigger_id=$3`, projectID, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchTrigger
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ListTriggers(ctx context.Context, projectID, location string) ([]Trigger, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, trigger_id, config, labels, uid, etag, create_time, update_time
		FROM jc_eventarc_triggers WHERE project_id=$1 AND location=$2 ORDER BY trigger_id
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Trigger
	for rows.Next() {
		t, err := scanTrigger(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

func (s *PostgresStore) ListTriggersAllLocations(ctx context.Context, projectID string) ([]Trigger, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, trigger_id, config, labels, uid, etag, create_time, update_time
		FROM jc_eventarc_triggers WHERE project_id=$1 ORDER BY location, trigger_id
	`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Trigger
	for rows.Next() {
		t, err := scanTrigger(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Location != result[j].Location {
			return result[i].Location < result[j].Location
		}
		return result[i].Name < result[j].Name
	})
	return result, rows.Err()
}

// --- Channels ---

func (s *PostgresStore) CreateChannel(ctx context.Context, projectID, location string, c Channel) error {
	if c.CreateTime.IsZero() {
		c.CreateTime = clock.Now()
	}
	if c.UpdateTime.IsZero() {
		c.UpdateTime = c.CreateTime
	}
	labels, _ := json.Marshal(c.Labels)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_eventarc_channels
			(project_id, location, channel_id, config, labels, uid, etag, activation_token, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`, projectID, location, c.Name, nullableJSONRaw(c.Config, "{}"), nullableJSONRaw(labels, "{}"), c.UID, c.Etag, c.ActivationToken, c.CreateTime, c.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

func scanChannel(row pgx.Row) (Channel, error) {
	var c Channel
	var config, labels []byte
	err := row.Scan(&c.ProjectID, &c.Location, &c.Name, &config, &labels, &c.UID, &c.Etag, &c.ActivationToken, &c.CreateTime, &c.UpdateTime)
	if err != nil {
		return Channel{}, err
	}
	c.Config = json.RawMessage(config)
	json.Unmarshal(labels, &c.Labels)
	return c, nil
}

func (s *PostgresStore) GetChannel(ctx context.Context, projectID, location, id string) (Channel, error) {
	c, err := scanChannel(s.pool.QueryRow(ctx, `
		SELECT project_id, location, channel_id, config, labels, uid, etag, activation_token, create_time, update_time
		FROM jc_eventarc_channels WHERE project_id=$1 AND location=$2 AND channel_id=$3
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Channel{}, ErrNoSuchChannel
	}
	return c, err
}

// UpdateChannelAtomic mirrors MemoryStore's version: a Serializable transaction
// with SELECT ... FOR UPDATE row-locks the channel for the duration of mutate,
// so a concurrent UpdateChannelAtomic on the same channel blocks until this
// transaction commits or rolls back. See store/firestore/postgres.go's Commit.
func (s *PostgresStore) UpdateChannelAtomic(ctx context.Context, projectID, location, id string, mutate func(Channel) (Channel, error)) (Channel, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Channel{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanChannel(tx.QueryRow(ctx, `
		SELECT project_id, location, channel_id, config, labels, uid, etag, activation_token, create_time, update_time
		FROM jc_eventarc_channels WHERE project_id=$1 AND location=$2 AND channel_id=$3 FOR UPDATE
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Channel{}, ErrNoSuchChannel
	}
	if err != nil {
		return Channel{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Channel{}, err
	}

	labels, _ := json.Marshal(next.Labels)
	tag, err := tx.Exec(ctx, `
		UPDATE jc_eventarc_channels SET config=$4, labels=$5, uid=$6, etag=$7, activation_token=$8, update_time=$9
		WHERE project_id=$1 AND location=$2 AND channel_id=$3
	`, projectID, location, id, nullableJSONRaw(next.Config, "{}"), nullableJSONRaw(labels, "{}"), next.UID, next.Etag, next.ActivationToken, next.UpdateTime)
	if err != nil {
		return Channel{}, err
	}
	if tag.RowsAffected() == 0 {
		return Channel{}, ErrNoSuchChannel
	}
	if err := tx.Commit(ctx); err != nil {
		return Channel{}, err
	}
	next.ProjectID = projectID
	next.Location = location
	return next, nil
}

func (s *PostgresStore) DeleteChannel(ctx context.Context, projectID, location, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_eventarc_channels WHERE project_id=$1 AND location=$2 AND channel_id=$3`, projectID, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchChannel
	}
	return nil
}

// DeleteChannelAtomic mirrors DeleteTriggerAtomic for channels.
func (s *PostgresStore) DeleteChannelAtomic(ctx context.Context, projectID, location, id string, guard func(Channel) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	current, err := scanChannel(tx.QueryRow(ctx, `
		SELECT project_id, location, channel_id, config, labels, uid, etag, activation_token, create_time, update_time
		FROM jc_eventarc_channels WHERE project_id=$1 AND location=$2 AND channel_id=$3 FOR UPDATE
	`, projectID, location, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoSuchChannel
	}
	if err != nil {
		return err
	}
	if err := guard(current); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM jc_eventarc_channels WHERE project_id=$1 AND location=$2 AND channel_id=$3`, projectID, location, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNoSuchChannel
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) ListChannels(ctx context.Context, projectID, location string) ([]Channel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, location, channel_id, config, labels, uid, etag, activation_token, create_time, update_time
		FROM jc_eventarc_channels WHERE project_id=$1 AND location=$2 ORDER BY channel_id
	`, projectID, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Channel
	for rows.Next() {
		c, err := scanChannel(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, rows.Err()
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_eventarc_triggers`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_eventarc_channels`)
}

// nullableJSONRaw returns a json.RawMessage for a JSONB column, substituting the
// given empty default ("{}" or "[]") for a nil/null value so NOT NULL columns
// receive valid JSON instead of SQL NULL.
func nullableJSONRaw(b []byte, empty string) any {
	if len(b) == 0 || string(b) == "null" {
		return json.RawMessage(empty)
	}
	return json.RawMessage(b)
}
