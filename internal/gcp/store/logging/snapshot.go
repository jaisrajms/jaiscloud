package logging

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

// memorySnapshot is the current on-disk shape of the memory-store snapshot. A
// legacy snapshot (a bare scope→entries map, before sinks/exclusions existed)
// is still accepted by Restore.
type memorySnapshot struct {
	Entries    map[string][]LogEntry              `json:"entries"`
	Sinks      map[string]map[string]LogSink      `json:"sinks,omitempty"`
	Exclusions map[string]map[string]LogExclusion `json:"exclusions,omitempty"`
	Metrics    map[string]map[string]LogMetric    `json:"metrics,omitempty"`
	Admin      map[string]map[string]adminRecord  `json:"admin,omitempty"`
}

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries) == 0 && len(s.sinks) == 0 && len(s.exclusions) == 0 && len(s.metrics) == 0 && len(s.admin) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(memorySnapshot{
		Entries:    s.entries,
		Sinks:      s.sinks,
		Exclusions: s.exclusions,
		Metrics:    s.metrics,
		Admin:      s.admin,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}
	var snap memorySnapshot
	if _, ok := probe["entries"]; ok {
		if err := json.Unmarshal(data, &snap); err != nil {
			return err
		}
	} else {
		// Legacy shape: a bare scope→entries map.
		if err := json.Unmarshal(data, &snap.Entries); err != nil {
			return err
		}
	}
	if snap.Entries == nil {
		snap.Entries = make(map[string][]LogEntry)
	}
	if snap.Sinks == nil {
		snap.Sinks = make(map[string]map[string]LogSink)
	}
	if snap.Exclusions == nil {
		snap.Exclusions = make(map[string]map[string]LogExclusion)
	}
	if snap.Metrics == nil {
		snap.Metrics = make(map[string]map[string]LogMetric)
	}
	if snap.Admin == nil {
		snap.Admin = make(map[string]map[string]adminRecord)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = snap.Entries
	s.sinks = snap.Sinks
	s.exclusions = snap.Exclusions
	s.metrics = snap.Metrics
	s.admin = snap.Admin
	var maxID int64
	for scope, entries := range snap.Entries {
		for i := range entries {
			// Backfill receive_timestamp for a snapshot written before it was
			// tracked, so a restored entry never surfaces a zero receive time.
			if entries[i].ReceiveTimestamp.IsZero() {
				entries[i].ReceiveTimestamp = entries[i].Timestamp
			}
			if entries[i].ID > maxID {
				maxID = entries[i].ID
			}
		}
		snap.Entries[scope] = entries
	}
	s.nextID = maxID + 1
	return nil
}

// --- Postgres store ---

// reseatSequenceSQL re-seats the jc_log_entries id sequence to the current max
// id after a restore inserts explicit id values, so the next BIGSERIAL Write
// does not collide with a restored id. COALESCE(MAX(id),1) keeps the sequence
// valid on an empty table (MAX would otherwise be NULL and setval would fail).
const reseatSequenceSQL = `SELECT setval(pg_get_serial_sequence('jc_log_entries','id'), COALESCE(MAX(id),1)) FROM jc_log_entries`

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM jc_log_entries)
		     + (SELECT count(*) FROM jc_log_sinks)
		     + (SELECT count(*) FROM jc_log_exclusions)
		     + (SELECT count(*) FROM jc_log_metrics)
		     + (SELECT count(*) FROM jc_log_admin_records)
	`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

// pgSnapshot is the Postgres snapshot shape: entries plus the sink/exclusion
// registries. Sinks/Exclusions are omitted when empty, so a snapshot written by
// the pre-sink emulator still restores.
type pgSnapshot struct {
	Entries []struct {
		ProjectID string   `json:"projectId"`
		Entry     LogEntry `json:"entry"`
	} `json:"entries"`
	Sinks []struct {
		ProjectID string  `json:"projectId"`
		Sink      LogSink `json:"sink"`
	} `json:"sinks,omitempty"`
	Exclusions []struct {
		ProjectID string       `json:"projectId"`
		Exclusion LogExclusion `json:"exclusion"`
	} `json:"exclusions,omitempty"`
	Metrics []struct {
		ProjectID string    `json:"projectId"`
		Metric    LogMetric `json:"metric"`
	} `json:"metrics,omitempty"`
	Admin []struct {
		Collection string      `json:"collection"`
		Scope      string      `json:"scope"`
		ID         string      `json:"id"`
		Record     adminRecord `json:"record"`
	} `json:"admin,omitempty"`
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	var snap pgSnapshot
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, id, log_name, resource_type, resource_labels, severity, payload_type, text_payload, json_payload, timestamp, receive_timestamp, insert_id, labels
		FROM jc_log_entries ORDER BY project_id, id
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r struct {
			ProjectID string   `json:"projectId"`
			Entry     LogEntry `json:"entry"`
		}
		var jsonPayload, resourceLabels, labels []byte
		if err := rows.Scan(&r.ProjectID, &r.Entry.ID, &r.Entry.LogName, &r.Entry.ResourceType, &resourceLabels, &r.Entry.Severity, &r.Entry.PayloadType, &r.Entry.TextPayload, &jsonPayload, &r.Entry.Timestamp, &r.Entry.ReceiveTimestamp, &r.Entry.InsertID, &labels); err != nil {
			rows.Close()
			return err
		}
		if len(jsonPayload) > 0 {
			json.Unmarshal(jsonPayload, &r.Entry.JsonPayload)
		}
		if len(resourceLabels) > 0 {
			json.Unmarshal(resourceLabels, &r.Entry.ResourceLabels)
		}
		if len(labels) > 0 {
			json.Unmarshal(labels, &r.Entry.Labels)
		}
		snap.Entries = append(snap.Entries, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	sinkRows, err := s.pool.Query(ctx, `
		SELECT project_id, name, destination, filter, description, disabled, exclusions, writer_identity, include_children,
		       intercept_children, output_version_format, bigquery_options, create_time, update_time
		FROM jc_log_sinks ORDER BY project_id, name
	`)
	if err != nil {
		return err
	}
	for sinkRows.Next() {
		var r struct {
			ProjectID string  `json:"projectId"`
			Sink      LogSink `json:"sink"`
		}
		var exclusions, bigQueryOptions []byte
		if err := sinkRows.Scan(&r.ProjectID, &r.Sink.Name, &r.Sink.Destination, &r.Sink.Filter, &r.Sink.Description,
			&r.Sink.Disabled, &exclusions, &r.Sink.WriterIdentity, &r.Sink.IncludeChildren,
			&r.Sink.InterceptChildren, &r.Sink.OutputVersionFormat, &bigQueryOptions, &r.Sink.CreateTime, &r.Sink.UpdateTime); err != nil {
			sinkRows.Close()
			return err
		}
		if len(exclusions) > 0 {
			_ = json.Unmarshal(exclusions, &r.Sink.Exclusions)
		}
		if len(bigQueryOptions) > 0 && string(bigQueryOptions) != "null" {
			_ = json.Unmarshal(bigQueryOptions, &r.Sink.BigQueryOptions)
		}
		snap.Sinks = append(snap.Sinks, r)
	}
	sinkRows.Close()
	if err := sinkRows.Err(); err != nil {
		return err
	}

	exclRows, err := s.pool.Query(ctx, `
		SELECT project_id, name, description, filter, disabled, create_time, update_time
		FROM jc_log_exclusions ORDER BY project_id, name
	`)
	if err != nil {
		return err
	}
	for exclRows.Next() {
		var r struct {
			ProjectID string       `json:"projectId"`
			Exclusion LogExclusion `json:"exclusion"`
		}
		if err := exclRows.Scan(&r.ProjectID, &r.Exclusion.Name, &r.Exclusion.Description, &r.Exclusion.Filter,
			&r.Exclusion.Disabled, &r.Exclusion.CreateTime, &r.Exclusion.UpdateTime); err != nil {
			exclRows.Close()
			return err
		}
		snap.Exclusions = append(snap.Exclusions, r)
	}
	exclRows.Close()
	if err := exclRows.Err(); err != nil {
		return err
	}

	metricRows, err := s.pool.Query(ctx, `
		SELECT project_id, name, description, filter, disabled, bucket_name, value_extractor, label_extractors, bucket_options, descriptor, create_time, update_time
		FROM jc_log_metrics ORDER BY project_id, name
	`)
	if err != nil {
		return err
	}
	for metricRows.Next() {
		var r struct {
			ProjectID string    `json:"projectId"`
			Metric    LogMetric `json:"metric"`
		}
		var labelExtractors, bucketOptions, descriptor []byte
		if err := metricRows.Scan(&r.ProjectID, &r.Metric.Name, &r.Metric.Description, &r.Metric.Filter, &r.Metric.Disabled,
			&r.Metric.BucketName, &r.Metric.ValueExtractor, &labelExtractors, &bucketOptions, &descriptor,
			&r.Metric.CreateTime, &r.Metric.UpdateTime); err != nil {
			metricRows.Close()
			return err
		}
		if len(labelExtractors) > 0 {
			_ = json.Unmarshal(labelExtractors, &r.Metric.LabelExtractors)
		}
		if len(bucketOptions) > 0 {
			r.Metric.BucketOptions = append([]byte(nil), bucketOptions...)
		}
		if len(descriptor) > 0 {
			_ = json.Unmarshal(descriptor, &r.Metric.Descriptor)
		}
		snap.Metrics = append(snap.Metrics, r)
	}
	metricRows.Close()
	if err := metricRows.Err(); err != nil {
		return err
	}

	adminRows, err := s.pool.Query(ctx, `
		SELECT collection, scope, id, data FROM jc_log_admin_records ORDER BY collection, scope, id
	`)
	if err != nil {
		return err
	}
	for adminRows.Next() {
		var r struct {
			Collection string      `json:"collection"`
			Scope      string      `json:"scope"`
			ID         string      `json:"id"`
			Record     adminRecord `json:"record"`
		}
		var data []byte
		if err := adminRows.Scan(&r.Collection, &r.Scope, &r.ID, &data); err != nil {
			adminRows.Close()
			return err
		}
		r.Record.Data = append(json.RawMessage(nil), data...)
		snap.Admin = append(snap.Admin, r)
	}
	adminRows.Close()
	if err := adminRows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(snap)
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap pgSnapshot
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_log_entries`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_log_sinks`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_log_exclusions`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_log_metrics`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_log_admin_records`); err != nil {
		return err
	}
	for _, r := range snap.Entries {
		// Backfill receive_timestamp for a snapshot written before it was
		// tracked.
		receiveTimestamp := r.Entry.ReceiveTimestamp
		if receiveTimestamp.IsZero() {
			receiveTimestamp = r.Entry.Timestamp
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_log_entries
				(project_id, id, log_name, resource_type, resource_labels, severity, payload_type, text_payload, json_payload, timestamp, receive_timestamp, insert_id, labels)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		`, r.ProjectID, r.Entry.ID, r.Entry.LogName, r.Entry.ResourceType, nullableJSON(r.Entry.ResourceLabels), r.Entry.Severity, r.Entry.PayloadType,
			r.Entry.TextPayload, nullableJSON(r.Entry.JsonPayload), r.Entry.Timestamp, receiveTimestamp, r.Entry.InsertID, nullableJSON(r.Entry.Labels)); err != nil {
			return err
		}
	}
	for _, r := range snap.Sinks {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_log_sinks
				(project_id, name, destination, filter, description, disabled, exclusions, writer_identity, include_children,
				 intercept_children, output_version_format, bigquery_options, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		`, r.ProjectID, r.Sink.Name, r.Sink.Destination, r.Sink.Filter, r.Sink.Description, r.Sink.Disabled,
			nullableJSON(nonNilExclusions(r.Sink.Exclusions)), r.Sink.WriterIdentity, r.Sink.IncludeChildren,
			r.Sink.InterceptChildren, r.Sink.OutputVersionFormat, sinkBigQueryOptionsArg(r.Sink.BigQueryOptions),
			r.Sink.CreateTime, r.Sink.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Exclusions {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_log_exclusions (project_id, name, description, filter, disabled, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
		`, r.ProjectID, r.Exclusion.Name, r.Exclusion.Description, r.Exclusion.Filter, r.Exclusion.Disabled,
			r.Exclusion.CreateTime, r.Exclusion.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Metrics {
		labelExtractors, descriptor, bucketOptions, jerr := metricJSON(r.Metric)
		if jerr != nil {
			return jerr
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_log_metrics
				(project_id, name, description, filter, disabled, bucket_name, value_extractor, label_extractors, bucket_options, descriptor, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		`, r.ProjectID, r.Metric.Name, r.Metric.Description, r.Metric.Filter, r.Metric.Disabled, r.Metric.BucketName,
			r.Metric.ValueExtractor, labelExtractors, bucketOptions, descriptor, r.Metric.CreateTime, r.Metric.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Admin {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_log_admin_records (collection, scope, id, data)
			VALUES ($1,$2,$3,$4)
		`, r.Collection, r.Scope, r.ID, nullableJSON(json.RawMessage(r.Record.Data))); err != nil {
			return err
		}
	}
	// Re-seat the id sequence so the next BIGSERIAL Write does not collide with
	// a restored explicit id value.
	if _, err := tx.Exec(ctx, reseatSequenceSQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
