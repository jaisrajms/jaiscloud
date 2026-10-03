package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against the jc_monitoring_* tables. The
// monitoring migrations (gcpstore.MigrationFS, 023 onward, including 045 for
// services + service-level objectives) must have run before use.
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
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_monitoring_notification_channels`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_monitoring_incidents`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_monitoring_service_level_objectives`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_monitoring_services`)
}

// ─── notification channels ────────────────────────────────────────────────────

func scanNotificationChannel(scan func(...any) error) (NotificationChannel, error) {
	var c NotificationChannel
	var labels, userLabels []byte
	if err := scan(&c.ID, &c.Type, &c.DisplayName, &c.Description, &labels, &userLabels, &c.Enabled, &c.VerificationStatus, &c.CreateTime, &c.UpdateTime); err != nil {
		return NotificationChannel{}, err
	}
	if len(labels) > 0 {
		_ = json.Unmarshal(labels, &c.Labels)
	}
	if len(userLabels) > 0 {
		_ = json.Unmarshal(userLabels, &c.UserLabels)
	}
	return c, nil
}

const channelCols = "id, type, display_name, description, labels, user_labels, enabled, verification_status, create_time, update_time"

func (s *PostgresStore) CreateNotificationChannel(ctx context.Context, project string, c NotificationChannel) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_monitoring_notification_channels
			(project_id, id, type, display_name, description, labels, user_labels, enabled, verification_status, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, project, c.ID, c.Type, c.DisplayName, c.Description, jsonb(c.Labels), jsonb(c.UserLabels),
		c.Enabled, c.VerificationStatus, c.CreateTime, c.UpdateTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrNotificationChannelExists
		}
		return fmt.Errorf("monitoring CreateNotificationChannel: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetNotificationChannel(ctx context.Context, project, id string) (NotificationChannel, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+channelCols+` FROM jc_monitoring_notification_channels WHERE project_id=$1 AND id=$2
	`, project, id)
	c, err := scanNotificationChannel(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return NotificationChannel{}, ErrNotificationChannelNotFound
	}
	return c, err
}

func (s *PostgresStore) ListNotificationChannels(ctx context.Context, project string) ([]NotificationChannel, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+channelCols+` FROM jc_monitoring_notification_channels WHERE project_id=$1 ORDER BY id
	`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]NotificationChannel, 0)
	for rows.Next() {
		c, err := scanNotificationChannel(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *PostgresStore) UpdateNotificationChannelAtomic(ctx context.Context, project, id string, mutate func(NotificationChannel) (NotificationChannel, error)) (NotificationChannel, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return NotificationChannel{}, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		SELECT `+channelCols+` FROM jc_monitoring_notification_channels WHERE project_id=$1 AND id=$2 FOR UPDATE
	`, project, id)
	current, err := scanNotificationChannel(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return NotificationChannel{}, ErrNotificationChannelNotFound
	}
	if err != nil {
		return NotificationChannel{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return NotificationChannel{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jc_monitoring_notification_channels
		SET type=$3, display_name=$4, description=$5, labels=$6, user_labels=$7, enabled=$8, verification_status=$9, update_time=$10
		WHERE project_id=$1 AND id=$2
	`, project, id, next.Type, next.DisplayName, next.Description, jsonb(next.Labels), jsonb(next.UserLabels),
		next.Enabled, next.VerificationStatus, next.UpdateTime)
	if err != nil {
		return NotificationChannel{}, fmt.Errorf("monitoring UpdateNotificationChannelAtomic: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return NotificationChannel{}, ErrNotificationChannelNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return NotificationChannel{}, err
	}
	next.ID = id
	return next, nil
}

func (s *PostgresStore) DeleteNotificationChannel(ctx context.Context, project, id string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_monitoring_notification_channels WHERE project_id=$1 AND id=$2
	`, project, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotificationChannelNotFound
	}
	return nil
}

// ─── incidents ────────────────────────────────────────────────────────────────

func scanIncident(scan func(...any) error) (Incident, error) {
	var inc Incident
	var notifications []byte
	if err := scan(&inc.ID, &inc.ProjectID, &inc.PolicyID, &inc.ConditionName, &inc.State, &inc.StartedAt, &inc.EndedAt, &inc.Reason, &notifications); err != nil {
		return Incident{}, err
	}
	if len(notifications) > 0 {
		_ = json.Unmarshal(notifications, &inc.Notifications)
	}
	return inc, nil
}

const incidentCols = "id, project_id, policy_id, condition_name, state, started_at, ended_at, reason, notifications"

func (s *PostgresStore) CreateIncident(ctx context.Context, inc Incident) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var openID string
	err = tx.QueryRow(ctx, `
		SELECT id FROM jc_monitoring_incidents
		WHERE project_id=$1 AND policy_id=$2 AND state=$3
		LIMIT 1
	`, inc.ProjectID, inc.PolicyID, IncidentOpen).Scan(&openID)
	switch {
	case err == nil:
		return ErrIncidentExists
	case errors.Is(err, pgx.ErrNoRows):
		// no open incident; proceed
	default:
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO jc_monitoring_incidents
			(project_id, id, policy_id, condition_name, state, started_at, ended_at, reason, notifications)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, inc.ProjectID, inc.ID, inc.PolicyID, inc.ConditionName, inc.State, inc.StartedAt, inc.EndedAt, inc.Reason, jsonb(inc.Notifications)); err != nil {
		return fmt.Errorf("monitoring CreateIncident: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) GetIncident(ctx context.Context, project, id string) (Incident, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+incidentCols+` FROM jc_monitoring_incidents WHERE project_id=$1 AND id=$2
	`, project, id)
	inc, err := scanIncident(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Incident{}, ErrIncidentNotFound
	}
	return inc, err
}

func (s *PostgresStore) ListIncidents(ctx context.Context, project string) ([]Incident, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+incidentCols+` FROM jc_monitoring_incidents WHERE project_id=$1 ORDER BY started_at, id
	`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Incident, 0)
	for rows.Next() {
		inc, err := scanIncident(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, inc)
	}
	return result, rows.Err()
}

func (s *PostgresStore) FindOpenIncident(ctx context.Context, project, policyID string) (Incident, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+incidentCols+` FROM jc_monitoring_incidents
		WHERE project_id=$1 AND policy_id=$2 AND state=$3
		ORDER BY started_at LIMIT 1
	`, project, policyID, IncidentOpen)
	inc, err := scanIncident(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Incident{}, ErrIncidentNotFound
	}
	return inc, err
}

func (s *PostgresStore) UpdateIncidentAtomic(ctx context.Context, project, id string, mutate func(Incident) (Incident, error)) (Incident, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Incident{}, err
	}
	defer tx.Rollback(ctx)
	row := tx.QueryRow(ctx, `
		SELECT `+incidentCols+` FROM jc_monitoring_incidents WHERE project_id=$1 AND id=$2 FOR UPDATE
	`, project, id)
	current, err := scanIncident(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Incident{}, ErrIncidentNotFound
	}
	if err != nil {
		return Incident{}, err
	}
	next, err := mutate(current)
	if err != nil {
		return Incident{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE jc_monitoring_incidents
		SET condition_name=$3, state=$4, started_at=$5, ended_at=$6, reason=$7, notifications=$8
		WHERE project_id=$1 AND id=$2
	`, project, id, next.ConditionName, next.State, next.StartedAt, next.EndedAt, next.Reason, jsonb(next.Notifications)); err != nil {
		return Incident{}, fmt.Errorf("monitoring UpdateIncidentAtomic: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Incident{}, err
	}
	next.ID = id
	return next, nil
}

// ─── services ─────────────────────────────────────────────────────────────────

func scanService(scan func(...any) error) (Service, error) {
	var svc Service
	var identifier, basicService, telemetry, userLabels []byte
	if err := scan(&svc.ID, &svc.DisplayName, &identifier, &basicService, &telemetry, &userLabels); err != nil {
		return Service{}, err
	}
	if len(identifier) > 0 && string(identifier) != "null" {
		svc.Identifier = json.RawMessage(identifier)
	}
	if len(basicService) > 0 && string(basicService) != "null" {
		svc.BasicService = json.RawMessage(basicService)
	}
	if len(telemetry) > 0 && string(telemetry) != "null" {
		svc.Telemetry = json.RawMessage(telemetry)
	}
	if len(userLabels) > 0 {
		_ = json.Unmarshal(userLabels, &svc.UserLabels)
	}
	return svc, nil
}

const serviceCols = "id, display_name, identifier, basic_service, telemetry, user_labels"

func (s *PostgresStore) CreateService(ctx context.Context, project string, svc Service) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_monitoring_services
			(project_id, id, display_name, identifier, basic_service, telemetry, user_labels)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, project, svc.ID, svc.DisplayName, jsonb(svc.Identifier), jsonb(svc.BasicService), jsonb(svc.Telemetry), jsonb(svc.UserLabels))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrServiceExists
		}
		return fmt.Errorf("monitoring CreateService: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetService(ctx context.Context, project, id string) (Service, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+serviceCols+` FROM jc_monitoring_services WHERE project_id=$1 AND id=$2
	`, project, id)
	svc, err := scanService(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Service{}, ErrServiceNotFound
	}
	return svc, err
}

func (s *PostgresStore) ListServices(ctx context.Context, project string) ([]Service, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+serviceCols+` FROM jc_monitoring_services WHERE project_id=$1 ORDER BY id
	`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]Service, 0)
	for rows.Next() {
		svc, err := scanService(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, svc)
	}
	return result, rows.Err()
}

func (s *PostgresStore) UpdateServiceAtomic(ctx context.Context, project, id string, mutate func(Service) (Service, error)) (Service, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Service{}, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		SELECT `+serviceCols+` FROM jc_monitoring_services WHERE project_id=$1 AND id=$2 FOR UPDATE
	`, project, id)
	current, err := scanService(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return Service{}, ErrServiceNotFound
	}
	if err != nil {
		return Service{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return Service{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jc_monitoring_services
		SET display_name=$3, identifier=$4, basic_service=$5, telemetry=$6, user_labels=$7
		WHERE project_id=$1 AND id=$2
	`, project, id, next.DisplayName, jsonb(next.Identifier), jsonb(next.BasicService), jsonb(next.Telemetry), jsonb(next.UserLabels))
	if err != nil {
		return Service{}, fmt.Errorf("monitoring UpdateServiceAtomic: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return Service{}, ErrServiceNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return Service{}, err
	}
	next.ID = id
	return next, nil
}

func (s *PostgresStore) DeleteService(ctx context.Context, project, id string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		DELETE FROM jc_monitoring_service_level_objectives WHERE project_id=$1 AND service_id=$2
	`, project, id); err != nil {
		return fmt.Errorf("monitoring DeleteService: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM jc_monitoring_services WHERE project_id=$1 AND id=$2
	`, project, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrServiceNotFound
	}
	return tx.Commit(ctx)
}

// ─── service level objectives ─────────────────────────────────────────────────

func scanServiceLevelObjective(scan func(...any) error) (ServiceLevelObjective, error) {
	var slo ServiceLevelObjective
	var indicator, userLabels []byte
	var rollingNanos int64
	if err := scan(&slo.ID, &slo.ServiceID, &slo.DisplayName, &indicator, &slo.Goal, &rollingNanos, &slo.CalendarPeriod, &userLabels); err != nil {
		return ServiceLevelObjective{}, err
	}
	if len(indicator) > 0 && string(indicator) != "null" {
		slo.ServiceLevelIndicator = json.RawMessage(indicator)
	}
	slo.RollingPeriod = time.Duration(rollingNanos)
	if len(userLabels) > 0 {
		_ = json.Unmarshal(userLabels, &slo.UserLabels)
	}
	return slo, nil
}

const sloCols = "id, service_id, display_name, service_level_indicator, goal, rolling_period_nanos, calendar_period, user_labels"

func (s *PostgresStore) CreateServiceLevelObjective(ctx context.Context, project string, slo ServiceLevelObjective) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_monitoring_service_level_objectives
			(project_id, service_id, id, display_name, service_level_indicator, goal, rolling_period_nanos, calendar_period, user_labels)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, project, slo.ServiceID, slo.ID, slo.DisplayName, jsonb(slo.ServiceLevelIndicator), slo.Goal,
		int64(slo.RollingPeriod), slo.CalendarPeriod, jsonb(slo.UserLabels))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrServiceLevelObjectiveExists
		}
		return fmt.Errorf("monitoring CreateServiceLevelObjective: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetServiceLevelObjective(ctx context.Context, project, serviceID, id string) (ServiceLevelObjective, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT `+sloCols+` FROM jc_monitoring_service_level_objectives
		WHERE project_id=$1 AND service_id=$2 AND id=$3
	`, project, serviceID, id)
	slo, err := scanServiceLevelObjective(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return ServiceLevelObjective{}, ErrServiceLevelObjectiveNotFound
	}
	return slo, err
}

func (s *PostgresStore) ListServiceLevelObjectives(ctx context.Context, project, serviceID string) ([]ServiceLevelObjective, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+sloCols+` FROM jc_monitoring_service_level_objectives
		WHERE project_id=$1 AND service_id=$2 ORDER BY id
	`, project, serviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ServiceLevelObjective, 0)
	for rows.Next() {
		slo, err := scanServiceLevelObjective(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, slo)
	}
	return result, rows.Err()
}

func (s *PostgresStore) UpdateServiceLevelObjectiveAtomic(ctx context.Context, project, serviceID, id string, mutate func(ServiceLevelObjective) (ServiceLevelObjective, error)) (ServiceLevelObjective, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return ServiceLevelObjective{}, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		SELECT `+sloCols+` FROM jc_monitoring_service_level_objectives
		WHERE project_id=$1 AND service_id=$2 AND id=$3 FOR UPDATE
	`, project, serviceID, id)
	current, err := scanServiceLevelObjective(row.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return ServiceLevelObjective{}, ErrServiceLevelObjectiveNotFound
	}
	if err != nil {
		return ServiceLevelObjective{}, err
	}

	next, err := mutate(current)
	if err != nil {
		return ServiceLevelObjective{}, err
	}

	tag, err := tx.Exec(ctx, `
		UPDATE jc_monitoring_service_level_objectives
		SET display_name=$4, service_level_indicator=$5, goal=$6, rolling_period_nanos=$7, calendar_period=$8, user_labels=$9
		WHERE project_id=$1 AND service_id=$2 AND id=$3
	`, project, serviceID, id, next.DisplayName, jsonb(next.ServiceLevelIndicator), next.Goal,
		int64(next.RollingPeriod), next.CalendarPeriod, jsonb(next.UserLabels))
	if err != nil {
		return ServiceLevelObjective{}, fmt.Errorf("monitoring UpdateServiceLevelObjectiveAtomic: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ServiceLevelObjective{}, ErrServiceLevelObjectiveNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return ServiceLevelObjective{}, err
	}
	next.ID = id
	next.ServiceID = serviceID
	return next, nil
}

func (s *PostgresStore) DeleteServiceLevelObjective(ctx context.Context, project, serviceID, id string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jc_monitoring_service_level_objectives
		WHERE project_id=$1 AND service_id=$2 AND id=$3
	`, project, serviceID, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrServiceLevelObjectiveNotFound
	}
	return nil
}

// ListProjects returns the distinct projects that hold any monitoring state.
func (s *PostgresStore) ListProjects(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project_id FROM jc_monitoring_metric_descriptors
		UNION SELECT project_id FROM jc_monitoring_time_series
		UNION SELECT project_id FROM jc_monitoring_alert_policies
		UNION SELECT project_id FROM jc_monitoring_notification_channels
		UNION SELECT project_id FROM jc_monitoring_incidents
		UNION SELECT project_id FROM jc_monitoring_services
		UNION SELECT project_id FROM jc_monitoring_service_level_objectives
		ORDER BY project_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]string, 0)
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}
