package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against the jc_monitoring_* tables. The
// monitoring migration (gcpstore.MigrationFS, migration 023) must have run
// before use.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore returns a Postgres-backed Store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func jsonb(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return b
}

// ─── metric descriptors ───────────────────────────────────────────────────────

func scanDescriptor(scan func(...any) error) (MetricDescriptor, error) {
	var d MetricDescriptor
	var labels, mrt []byte
	if err := scan(&d.Type, &d.MetricKind, &d.ValueType, &d.Unit, &d.Description, &d.DisplayName, &labels, &mrt); err != nil {
		return MetricDescriptor{}, err
	}
	if len(labels) > 0 {
		_ = json.Unmarshal(labels, &d.Labels)
	}
	if len(mrt) > 0 {
		_ = json.Unmarshal(mrt, &d.MonitoredResourceTypes)
	}
	return d, nil
}

const descriptorCols = "type, metric_kind, value_type, unit, description, display_name, labels, monitored_resource_types"

func (s *PostgresStore) CreateMetricDescriptor(ctx context.Context, project string, d MetricDescriptor) (MetricDescriptor, error) {
	existing, err := s.GetMetricDescriptor(ctx, project, d.Type)
	switch {
	case err == nil:
		d = mergeMetricDescriptor(existing, d)
	case errors.Is(err, ErrMetricDescriptorNotFound):
		// new descriptor, keep d as-is
	default:
		return MetricDescriptor{}, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO jc_monitoring_metric_descriptors
			(project_id, type, metric_kind, value_type, unit, description, display_name, labels, monitored_resource_types)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (project_id, type) DO UPDATE SET
			metric_kind=EXCLUDED.metric_kind, value_type=EXCLUDED.value_type, unit=EXCLUDED.unit,
			description=EXCLUDED.description, display_name=EXCLUDED.display_name,
			labels=EXCLUDED.labels, monitored_resource_types=EXCLUDED.monitored_resource_types
	`, project, d.Type, d.MetricKind, d.ValueType, d.Unit, d.Description, d.DisplayName,
		jsonb(d.Labels), jsonb(d.MonitoredResourceTypes))
	if err != nil {
		return MetricDescriptor{}, fmt.Errorf("monitoring CreateMetricDescriptor: %w", err)
	}
	return d, nil
}

func (s *PostgresStore) GetMetricDescriptor(ctx context.Context, project, metricType string) (MetricDescriptor, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+descriptorCols+` FROM jc_monitoring_metric_descriptors
		WHERE project_id=$1 AND type=$2
	`, project, metricType)
	d, err := scanDescriptor(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return MetricDescriptor{}, ErrMetricDescriptorNotFound
	}
	return d, err
}

func (s *PostgresStore) ListMetricDescriptors(ctx context.Context, project string) ([]MetricDescriptor, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+descriptorCols+` FROM jc_monitoring_metric_descriptors
		WHERE project_id=$1 ORDER BY type
	`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]MetricDescriptor, 0)
	for rows.Next() {
		d, err := scanDescriptor(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

func (s *PostgresStore) DeleteMetricDescriptor(ctx context.Context, project, metricType string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_monitoring_metric_descriptors WHERE project_id=$1 AND type=$2
	`, project, metricType)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrMetricDescriptorNotFound
	}
	return nil
}

// ─── time series ──────────────────────────────────────────────────────────────

func scanTimeSeries(scan func(...any) error) (TimeSeries, error) {
	var ts TimeSeries
	var metricLabels, resourceLabels, points []byte
	if err := scan(&ts.MetricType, &metricLabels, &ts.ResourceType, &resourceLabels, &ts.MetricKind, &ts.ValueType, &ts.Unit, &points); err != nil {
		return TimeSeries{}, err
	}
	if len(metricLabels) > 0 {
		_ = json.Unmarshal(metricLabels, &ts.MetricLabels)
	}
	if len(resourceLabels) > 0 {
		_ = json.Unmarshal(resourceLabels, &ts.ResourceLabels)
	}
	if len(points) > 0 {
		_ = json.Unmarshal(points, &ts.Points)
	}
	return ts, nil
}

const seriesCols = "metric_type, metric_labels, resource_type, resource_labels, metric_kind, value_type, unit, points"

func (s *PostgresStore) CreateTimeSeries(ctx context.Context, project string, ts TimeSeries) error {
	key := seriesKey(ts)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_monitoring_time_series
			(project_id, series_key, metric_type, metric_labels, resource_type, resource_labels, metric_kind, value_type, unit, points)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (project_id, series_key) DO UPDATE SET points = jc_monitoring_time_series.points || EXCLUDED.points
	`, project, key, ts.MetricType, jsonb(ts.MetricLabels), ts.ResourceType, jsonb(ts.ResourceLabels),
		ts.MetricKind, ts.ValueType, ts.Unit, jsonb(ts.Points))
	if err != nil {
		return fmt.Errorf("monitoring CreateTimeSeries: %w", err)
	}
	return nil
}

func (s *PostgresStore) ListTimeSeries(ctx context.Context, project string) ([]TimeSeries, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+seriesCols+` FROM jc_monitoring_time_series
		WHERE project_id=$1 ORDER BY metric_type, resource_type, series_key
	`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]TimeSeries, 0)
	for rows.Next() {
		ts, err := scanTimeSeries(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, ts)
	}
	return result, rows.Err()
}

// ─── alert policies ───────────────────────────────────────────────────────────

func scanAlertPolicy(scan func(...any) error) (AlertPolicy, error) {
	var p AlertPolicy
	var documentation, conditions, notificationChannels, userLabels []byte
	if err := scan(&p.ID, &p.DisplayName, &p.Combiner, &p.Enabled, &documentation, &conditions, &notificationChannels, &userLabels); err != nil {
		return AlertPolicy{}, err
	}
	if len(documentation) > 0 && string(documentation) != "null" {
		p.Documentation = json.RawMessage(documentation)
	}
	if len(conditions) > 0 {
		_ = json.Unmarshal(conditions, &p.Conditions)
	}
	if len(notificationChannels) > 0 {
		_ = json.Unmarshal(notificationChannels, &p.NotificationChannels)
	}
	if len(userLabels) > 0 {
		_ = json.Unmarshal(userLabels, &p.UserLabels)
	}
	return p, nil
}

const policyCols = "id, display_name, combiner, enabled, documentation, conditions, notification_channels, user_labels"

func (s *PostgresStore) insertPolicy(ctx context.Context, project string, p AlertPolicy) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_monitoring_alert_policies
			(project_id, id, display_name, combiner, enabled, documentation, conditions, notification_channels, user_labels)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, project, p.ID, p.DisplayName, p.Combiner, p.Enabled, jsonb(p.Documentation),
		jsonb(p.Conditions), jsonb(p.NotificationChannels), jsonb(p.UserLabels))
	return err
}

func (s *PostgresStore) CreateAlertPolicy(ctx context.Context, project string, p AlertPolicy) error {
	err := s.insertPolicy(ctx, project, p)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlertPolicyExists
		}
		return fmt.Errorf("monitoring CreateAlertPolicy: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetAlertPolicy(ctx context.Context, project, id string) (AlertPolicy, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+policyCols+` FROM jc_monitoring_alert_policies WHERE project_id=$1 AND id=$2
	`, project, id)
	p, err := scanAlertPolicy(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return AlertPolicy{}, ErrAlertPolicyNotFound
	}
	return p, err
}

func (s *PostgresStore) ListAlertPolicies(ctx context.Context, project string) ([]AlertPolicy, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+policyCols+` FROM jc_monitoring_alert_policies WHERE project_id=$1 ORDER BY id
	`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]AlertPolicy, 0)
	for rows.Next() {
		p, err := scanAlertPolicy(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

func (s *PostgresStore) UpdateAlertPolicy(ctx context.Context, project string, p AlertPolicy) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_monitoring_alert_policies
		SET display_name=$3, combiner=$4, enabled=$5, documentation=$6, conditions=$7, notification_channels=$8, user_labels=$9
		WHERE project_id=$1 AND id=$2
	`, project, p.ID, p.DisplayName, p.Combiner, p.Enabled, jsonb(p.Documentation),
		jsonb(p.Conditions), jsonb(p.NotificationChannels), jsonb(p.UserLabels))
	if err != nil {
		return fmt.Errorf("monitoring UpdateAlertPolicy: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrAlertPolicyNotFound
	}
	return nil
}

// UpdateAlertPolicyAtomic mirrors MemoryStore's version: a Serializable
// transaction with SELECT ... FOR UPDATE row-locks the policy for the
// duration of mutate, so a concurrent UpdateAlertPolicyAtomic on the same
// policy blocks until this transaction commits or rolls back, instead of
// racing to silently overwrite this call's write. See
// store/firestore/postgres.go's Commit for the same convention.
func (s *PostgresStore) UpdateAlertPolicyAtomic(ctx context.Context, project, id string, mutate func(AlertPolicy) (AlertPolicy, error)) (AlertPolicy, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return AlertPolicy{}, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		SELECT `+policyCols+` FROM jc_monitoring_alert_policies WHERE project_id=$1 AND id=$2 FOR UPDATE
	`, project, id)
	current, err := scanAlertPolicy(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return AlertPolicy{}, ErrAlertPolicyNotFound
	}
	if err != nil {
		return AlertPolicy{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return AlertPolicy{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jc_monitoring_alert_policies
		SET display_name=$3, combiner=$4, enabled=$5, documentation=$6, conditions=$7, notification_channels=$8, user_labels=$9
		WHERE project_id=$1 AND id=$2
	`, project, id, next.DisplayName, next.Combiner, next.Enabled, jsonb(next.Documentation),
		jsonb(next.Conditions), jsonb(next.NotificationChannels), jsonb(next.UserLabels))
	if err != nil {
		return AlertPolicy{}, fmt.Errorf("monitoring UpdateAlertPolicyAtomic: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return AlertPolicy{}, ErrAlertPolicyNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return AlertPolicy{}, err
	}
	next.ID = id
	return next, nil
}

func (s *PostgresStore) DeleteAlertPolicy(ctx context.Context, project, id string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_monitoring_alert_policies WHERE project_id=$1 AND id=$2
	`, project, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrAlertPolicyNotFound
	}
	return nil
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_monitoring_metric_descriptors`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_monitoring_time_series`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_monitoring_alert_policies`)
}
