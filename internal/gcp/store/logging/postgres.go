package logging

import (
	"context"
	"encoding/json"
	"errors"

	"jaiscloud/internal/clock"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements Store against jc_log_entries.
type PostgresStore struct {
	pool *pgxpool.Pool
	adminRecords
}

// NewPostgresStore returns a Postgres-backed store.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	s := &PostgresStore{pool: pool}
	s.backend = s
	return s
}

// ─── admin backend ────────────────────────────────────────────────────────────

func (s *PostgresStore) putAdmin(ctx context.Context, collection, scope, id string, rec adminRecord) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_log_admin_records (collection, scope, id, data)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (collection, scope, id) DO UPDATE SET data=EXCLUDED.data
	`, collection, scope, id, nullableJSON(json.RawMessage(rec.Data)))
	return err
}

func (s *PostgresStore) getAdmin(ctx context.Context, collection, scope, id string) (adminRecord, bool, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `
		SELECT data FROM jc_log_admin_records WHERE collection=$1 AND scope=$2 AND id=$3
	`, collection, scope, id).Scan(&data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return adminRecord{}, false, nil
		}
		return adminRecord{}, false, err
	}
	return adminRecord{Data: append(json.RawMessage(nil), data...)}, true, nil
}

func (s *PostgresStore) listAdmin(ctx context.Context, collection, scope string) (map[string]adminRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, data FROM jc_log_admin_records WHERE collection=$1 AND scope=$2 ORDER BY id
	`, collection, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]adminRecord{}
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		out[id] = adminRecord{Data: append(json.RawMessage(nil), data...)}
	}
	return out, rows.Err()
}

func (s *PostgresStore) deleteAdmin(ctx context.Context, collection, scope, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_log_admin_records WHERE collection=$1 AND scope=$2 AND id=$3`, collection, scope, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func nullableJSON(v any) any {
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}

func (s *PostgresStore) Write(ctx context.Context, scope string, e LogEntry) error {
	if e.Timestamp.IsZero() {
		e.Timestamp = clock.Now()
	}
	if e.ReceiveTimestamp.IsZero() {
		e.ReceiveTimestamp = e.Timestamp
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jc_log_entries
			(project_id, log_name, resource_type, resource_labels, severity, payload_type, text_payload, json_payload, timestamp, receive_timestamp, insert_id, labels)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	`, scope, e.LogName, e.ResourceType, nullableJSON(e.ResourceLabels), e.Severity, e.PayloadType, e.TextPayload,
		nullableJSON(e.JsonPayload), e.Timestamp, e.ReceiveTimestamp, e.InsertID, nullableJSON(e.Labels))
	return err
}

func (s *PostgresStore) List(ctx context.Context, scope string) ([]LogEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, log_name, resource_type, resource_labels, severity, payload_type, text_payload, json_payload, timestamp, receive_timestamp, insert_id, labels
		FROM jc_log_entries WHERE project_id=$1 ORDER BY timestamp, id
	`, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []LogEntry
	for rows.Next() {
		var e LogEntry
		var jsonPayload, resourceLabels, labels []byte
		if err := rows.Scan(&e.ID, &e.LogName, &e.ResourceType, &resourceLabels, &e.Severity, &e.PayloadType, &e.TextPayload, &jsonPayload, &e.Timestamp, &e.ReceiveTimestamp, &e.InsertID, &labels); err != nil {
			return nil, err
		}
		if len(jsonPayload) > 0 {
			json.Unmarshal(jsonPayload, &e.JsonPayload)
		}
		if len(resourceLabels) > 0 {
			json.Unmarshal(resourceLabels, &e.ResourceLabels)
		}
		if len(labels) > 0 {
			json.Unmarshal(labels, &e.Labels)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (s *PostgresStore) ListLogs(ctx context.Context, scope string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT log_name FROM jc_log_entries WHERE project_id=$1 ORDER BY log_name
	`, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		result = append(result, name)
	}
	return result, rows.Err()
}

func (s *PostgresStore) DeleteLog(ctx context.Context, scope, logName string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM jc_log_entries WHERE project_id=$1 AND log_name=$2`, scope, logName)
	return err
}

// ─── sinks ────────────────────────────────────────────────────────────────────

func (s *PostgresStore) CreateSink(ctx context.Context, scope string, sink LogSink) error {
	exclusions, err := json.Marshal(nonNilExclusions(sink.Exclusions))
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO jc_log_sinks
			(project_id, name, destination, filter, description, disabled, exclusions, writer_identity, include_children,
			 intercept_children, output_version_format, bigquery_options, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (project_id, name) DO NOTHING
	`, scope, sink.Name, sink.Destination, sink.Filter, sink.Description, sink.Disabled, exclusions, sink.WriterIdentity, sink.IncludeChildren,
		sink.InterceptChildren, sink.OutputVersionFormat, sinkBigQueryOptionsArg(sink.BigQueryOptions), sink.CreateTime, sink.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrSinkExists
	}
	return nil
}

func (s *PostgresStore) GetSink(ctx context.Context, scope, name string) (LogSink, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT name, destination, filter, description, disabled, exclusions, writer_identity, include_children,
		       intercept_children, output_version_format, bigquery_options, create_time, update_time
		FROM jc_log_sinks WHERE project_id=$1 AND name=$2
	`, scope, name)
	return scanSink(row)
}

func (s *PostgresStore) ListSinks(ctx context.Context, scope string) ([]LogSink, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, destination, filter, description, disabled, exclusions, writer_identity, include_children,
		       intercept_children, output_version_format, bigquery_options, create_time, update_time
		FROM jc_log_sinks WHERE project_id=$1 ORDER BY name
	`, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []LogSink
	for rows.Next() {
		sink, err := scanSink(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, sink)
	}
	return result, rows.Err()
}

func (s *PostgresStore) UpdateSink(ctx context.Context, scope string, sink LogSink) error {
	exclusions, err := json.Marshal(nonNilExclusions(sink.Exclusions))
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_log_sinks
		SET destination=$3, filter=$4, description=$5, disabled=$6, exclusions=$7, writer_identity=$8, include_children=$9,
		    intercept_children=$10, output_version_format=$11, bigquery_options=$12, update_time=$13
		WHERE project_id=$1 AND name=$2
	`, scope, sink.Name, sink.Destination, sink.Filter, sink.Description, sink.Disabled, exclusions, sink.WriterIdentity, sink.IncludeChildren,
		sink.InterceptChildren, sink.OutputVersionFormat, sinkBigQueryOptionsArg(sink.BigQueryOptions), sink.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrSinkNotFound
	}
	return nil
}

func (s *PostgresStore) DeleteSink(ctx context.Context, scope, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_log_sinks WHERE project_id=$1 AND name=$2`, scope, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrSinkNotFound
	}
	return nil
}

// sinkScanner is the shared row scan target for GetSink/ListSinks (pgx.Row and
// pgx.Rows both satisfy it).
type sinkScanner interface {
	Scan(dest ...any) error
}

func scanSink(row sinkScanner) (LogSink, error) {
	var sink LogSink
	var exclusions, bigQueryOptions []byte
	err := row.Scan(&sink.Name, &sink.Destination, &sink.Filter, &sink.Description, &sink.Disabled,
		&exclusions, &sink.WriterIdentity, &sink.IncludeChildren,
		&sink.InterceptChildren, &sink.OutputVersionFormat, &bigQueryOptions, &sink.CreateTime, &sink.UpdateTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return LogSink{}, ErrSinkNotFound
		}
		return LogSink{}, err
	}
	if len(exclusions) > 0 {
		_ = json.Unmarshal(exclusions, &sink.Exclusions)
	}
	if len(bigQueryOptions) > 0 && string(bigQueryOptions) != "null" {
		_ = json.Unmarshal(bigQueryOptions, &sink.BigQueryOptions)
	}
	return sink, nil
}

func nonNilExclusions(in []LogExclusion) []LogExclusion {
	if in == nil {
		return []LogExclusion{}
	}
	return in
}

// sinkBigQueryOptionsArg renders a sink's BigQueryOptions for the JSONB column:
// SQL NULL when absent, so a scan of an empty column leaves the pointer nil.
func sinkBigQueryOptionsArg(o *LogBigQueryOptions) any {
	if o == nil {
		return nil
	}
	return nullableJSON(o)
}

// ─── exclusions ───────────────────────────────────────────────────────────────

func (s *PostgresStore) CreateExclusion(ctx context.Context, scope string, e LogExclusion) error {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO jc_log_exclusions (project_id, name, description, filter, disabled, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (project_id, name) DO NOTHING
	`, scope, e.Name, e.Description, e.Filter, e.Disabled, e.CreateTime, e.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrExclusionExists
	}
	return nil
}

func (s *PostgresStore) GetExclusion(ctx context.Context, scope, name string) (LogExclusion, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT name, description, filter, disabled, create_time, update_time
		FROM jc_log_exclusions WHERE project_id=$1 AND name=$2
	`, scope, name)
	return scanExclusion(row)
}

func (s *PostgresStore) ListExclusions(ctx context.Context, scope string) ([]LogExclusion, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, description, filter, disabled, create_time, update_time
		FROM jc_log_exclusions WHERE project_id=$1 ORDER BY name
	`, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []LogExclusion
	for rows.Next() {
		e, err := scanExclusion(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (s *PostgresStore) UpdateExclusion(ctx context.Context, scope string, e LogExclusion) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_log_exclusions
		SET description=$3, filter=$4, disabled=$5, update_time=$6
		WHERE project_id=$1 AND name=$2
	`, scope, e.Name, e.Description, e.Filter, e.Disabled, e.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrExclusionNotFound
	}
	return nil
}

func (s *PostgresStore) DeleteExclusion(ctx context.Context, scope, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_log_exclusions WHERE project_id=$1 AND name=$2`, scope, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrExclusionNotFound
	}
	return nil
}

func scanExclusion(row sinkScanner) (LogExclusion, error) {
	var e LogExclusion
	err := row.Scan(&e.Name, &e.Description, &e.Filter, &e.Disabled, &e.CreateTime, &e.UpdateTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return LogExclusion{}, ErrExclusionNotFound
		}
		return LogExclusion{}, err
	}
	return e, nil
}

// ─── metrics ──────────────────────────────────────────────────────────────────

func (s *PostgresStore) CreateMetric(ctx context.Context, scope string, m LogMetric) error {
	labelExtractors, descriptor, bucketOptions, err := metricJSON(m)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO jc_log_metrics
			(project_id, name, description, filter, disabled, bucket_name, value_extractor, label_extractors, bucket_options, descriptor, create_time, update_time)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (project_id, name) DO NOTHING
	`, scope, m.Name, m.Description, m.Filter, m.Disabled, m.BucketName, m.ValueExtractor, labelExtractors, bucketOptions, descriptor, m.CreateTime, m.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrMetricExists
	}
	return nil
}

func (s *PostgresStore) GetMetric(ctx context.Context, scope, name string) (LogMetric, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT name, description, filter, disabled, bucket_name, value_extractor, label_extractors, bucket_options, descriptor, create_time, update_time
		FROM jc_log_metrics WHERE project_id=$1 AND name=$2
	`, scope, name)
	return scanMetric(row)
}

func (s *PostgresStore) ListMetrics(ctx context.Context, scope string) ([]LogMetric, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, description, filter, disabled, bucket_name, value_extractor, label_extractors, bucket_options, descriptor, create_time, update_time
		FROM jc_log_metrics WHERE project_id=$1 ORDER BY name
	`, scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []LogMetric
	for rows.Next() {
		m, err := scanMetric(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func (s *PostgresStore) UpdateMetric(ctx context.Context, scope string, m LogMetric) error {
	labelExtractors, descriptor, bucketOptions, err := metricJSON(m)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jc_log_metrics
		SET description=$3, filter=$4, disabled=$5, bucket_name=$6, value_extractor=$7, label_extractors=$8, bucket_options=$9, descriptor=$10, update_time=$11
		WHERE project_id=$1 AND name=$2
	`, scope, m.Name, m.Description, m.Filter, m.Disabled, m.BucketName, m.ValueExtractor, labelExtractors, bucketOptions, descriptor, m.UpdateTime)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrMetricNotFound
	}
	return nil
}

func (s *PostgresStore) DeleteMetric(ctx context.Context, scope, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM jc_log_metrics WHERE project_id=$1 AND name=$2`, scope, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrMetricNotFound
	}
	return nil
}

// metricJSON marshals a metric's JSONB columns. An empty label_extractors map
// is stored as `{}` (never NULL) so the column default and decode agree; a nil
// bucket_options is stored as SQL NULL (via nullableJSON, which also handles the
// typed-nil case).
func metricJSON(m LogMetric) (labelExtractors, descriptor []byte, bucketOptions any, err error) {
	extractors := m.LabelExtractors
	if extractors == nil {
		extractors = map[string]string{}
	}
	if labelExtractors, err = json.Marshal(extractors); err != nil {
		return nil, nil, nil, err
	}
	if descriptor, err = json.Marshal(m.Descriptor); err != nil {
		return nil, nil, nil, err
	}
	if len(m.BucketOptions) > 0 {
		bucketOptions = m.BucketOptions
	}
	return labelExtractors, descriptor, bucketOptions, nil
}

func scanMetric(row sinkScanner) (LogMetric, error) {
	var m LogMetric
	var labelExtractors, bucketOptions, descriptor []byte
	err := row.Scan(&m.Name, &m.Description, &m.Filter, &m.Disabled, &m.BucketName, &m.ValueExtractor,
		&labelExtractors, &bucketOptions, &descriptor, &m.CreateTime, &m.UpdateTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return LogMetric{}, ErrMetricNotFound
		}
		return LogMetric{}, err
	}
	if len(labelExtractors) > 0 {
		_ = json.Unmarshal(labelExtractors, &m.LabelExtractors)
	}
	if len(bucketOptions) > 0 {
		m.BucketOptions = append([]byte(nil), bucketOptions...)
	}
	if len(descriptor) > 0 {
		_ = json.Unmarshal(descriptor, &m.Descriptor)
	}
	return m, nil
}

func (s *PostgresStore) Reset(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_log_entries`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_log_sinks`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_log_exclusions`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_log_metrics`)
	_, _ = s.pool.Exec(ctx, `DELETE FROM jc_log_admin_records`)
}
