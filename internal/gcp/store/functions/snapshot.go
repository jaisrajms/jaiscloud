package functions

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.functions) == 0 && len(s.operations) == 0 && len(s.deliveries) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"functions": s.functions, "operations": s.operations, "deliveries": s.deliveries,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Functions  map[string]map[string]Function  `json:"functions"`
		Operations map[string]map[string]Operation `json:"operations"`
		Deliveries map[string]map[string]Delivery  `json:"deliveries"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Functions == nil {
		snap.Functions = map[string]map[string]Function{}
	}
	if snap.Operations == nil {
		snap.Operations = map[string]map[string]Operation{}
	}
	if snap.Deliveries == nil {
		snap.Deliveries = map[string]map[string]Delivery{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.functions = snap.Functions
	s.operations = snap.Operations
	s.deliveries = snap.Deliveries
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_functions`).Scan(&n); err != nil {
		return false, err
	}
	if n != 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_functions_operations`).Scan(&n); err != nil {
		return false, err
	}
	if n != 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_functions_deliveries`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type functionRow struct {
		ProjectID string   `json:"projectId"`
		Function  Function `json:"function"`
	}
	functions := make([]functionRow, 0)
	rows, err := s.pool.Query(ctx, `
		SELECT project_id, function_id, location, runtime, entry_point, source_upload_url, source_archive_url,
		       https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
		       available_memory_mb, timeout, description, source_sha256, source_size, source_blob_key,
		       revision, upgrade_state, upgrade_runtime, upgrade_max_instances, upgrade_traffic_gen2,
		       min_instance_count, max_instance_count, max_instance_request_concurrency, available_cpu
		FROM jc_functions ORDER BY project_id, location, function_id
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r functionRow
		var eventTrigger, env, labels []byte
		if err := rows.Scan(&r.ProjectID, &r.Function.ID, &r.Function.Location, &r.Function.Runtime, &r.Function.EntryPoint,
			&r.Function.SourceUploadURL, &r.Function.SourceArchiveURL, &r.Function.HttpsTriggerURL, &eventTrigger, &env,
			&r.Function.Status, &r.Function.CreateTime, &r.Function.UpdateTime, &labels, &r.Function.AvailableMemoryMB,
			&r.Function.Timeout, &r.Function.Description,
			&r.Function.SourceSHA256, &r.Function.SourceSize, &r.Function.SourceBlobKey,
			&r.Function.Revision, &r.Function.UpgradeState, &r.Function.UpgradeRuntime,
			&r.Function.UpgradeMaxInstances, &r.Function.UpgradeTrafficGen2,
			&r.Function.MinInstanceCount, &r.Function.MaxInstanceCount,
			&r.Function.MaxInstanceRequestConcurrency, &r.Function.AvailableCPU); err != nil {
			rows.Close()
			return err
		}
		if len(eventTrigger) > 0 {
			json.Unmarshal(eventTrigger, &r.Function.EventTrigger)
		}
		json.Unmarshal(env, &r.Function.EnvironmentVariables)
		json.Unmarshal(labels, &r.Function.Labels)
		functions = append(functions, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	type operationRow struct {
		ProjectID string    `json:"projectId"`
		Operation Operation `json:"operation"`
	}
	operations := make([]operationRow, 0)
	orows, err := s.pool.Query(ctx, `
		SELECT project_id, operation_id, location, done, verb, target, function, create_time, end_time
		FROM jc_functions_operations ORDER BY project_id, location, operation_id
	`)
	if err != nil {
		return err
	}
	for orows.Next() {
		var r operationRow
		var function []byte
		if err := orows.Scan(&r.ProjectID, &r.Operation.ID, &r.Operation.Location, &r.Operation.Done, &r.Operation.Verb,
			&r.Operation.Target, &function, &r.Operation.CreateTime, &r.Operation.EndTime); err != nil {
			orows.Close()
			return err
		}
		if len(function) > 0 {
			var f Function
			if err := json.Unmarshal(function, &f); err != nil {
				orows.Close()
				return err
			}
			r.Operation.Function = &f
		}
		operations = append(operations, r)
	}
	orows.Close()
	if err := orows.Err(); err != nil {
		return err
	}

	type deliveryRow struct {
		ProjectID string   `json:"projectId"`
		Delivery  Delivery `json:"delivery"`
	}
	deliveries := make([]deliveryRow, 0)
	drows, err := s.pool.Query(ctx, `
		SELECT project_id, delivery_id, location, function_id, source, event_type, resource, event_id,
		       data, attributes, attempts, status, error, result, dead_letter_topic, create_time, update_time
		FROM jc_functions_deliveries ORDER BY project_id, location, delivery_id
	`)
	if err != nil {
		return err
	}
	for drows.Next() {
		var r deliveryRow
		var attrs []byte
		if err := drows.Scan(&r.ProjectID, &r.Delivery.ID, &r.Delivery.Location, &r.Delivery.FunctionID,
			&r.Delivery.Source, &r.Delivery.EventType, &r.Delivery.Resource, &r.Delivery.EventID, &r.Delivery.Data,
			&attrs, &r.Delivery.Attempts, &r.Delivery.Status, &r.Delivery.Error, &r.Delivery.Result,
			&r.Delivery.DeadLetterTopic, &r.Delivery.CreateTime, &r.Delivery.UpdateTime); err != nil {
			drows.Close()
			return err
		}
		if len(attrs) > 0 {
			json.Unmarshal(attrs, &r.Delivery.Attributes)
		}
		deliveries = append(deliveries, r)
	}
	drows.Close()
	if err := drows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"functions": functions, "operations": operations, "deliveries": deliveries,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Functions []struct {
			ProjectID string   `json:"projectId"`
			Function  Function `json:"function"`
		} `json:"functions"`
		Operations []struct {
			ProjectID string    `json:"projectId"`
			Operation Operation `json:"operation"`
		} `json:"operations"`
		Deliveries []struct {
			ProjectID string   `json:"projectId"`
			Delivery  Delivery `json:"delivery"`
		} `json:"deliveries"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_functions`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_functions_operations`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_functions_deliveries`); err != nil {
		return err
	}
	for _, r := range snap.Functions {
		env, _ := json.Marshal(r.Function.EnvironmentVariables)
		labels, _ := json.Marshal(r.Function.Labels)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_functions
				(project_id, location, function_id, runtime, entry_point, source_upload_url, source_archive_url,
				 https_trigger_url, event_trigger, environment_variables, status, create_time, update_time, labels,
				 available_memory_mb, timeout, description, source_sha256, source_size, source_blob_key,
				 revision, upgrade_state, upgrade_runtime, upgrade_max_instances, upgrade_traffic_gen2,
				 min_instance_count, max_instance_count, max_instance_request_concurrency, available_cpu)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)
		`, r.ProjectID, r.Function.Location, r.Function.ID, r.Function.Runtime, r.Function.EntryPoint,
			r.Function.SourceUploadURL, r.Function.SourceArchiveURL, r.Function.HttpsTriggerURL, nullableJSON(r.Function.EventTrigger),
			json.RawMessage(env), r.Function.Status, r.Function.CreateTime, r.Function.UpdateTime, json.RawMessage(labels),
			r.Function.AvailableMemoryMB, r.Function.Timeout, r.Function.Description,
			r.Function.SourceSHA256, r.Function.SourceSize, r.Function.SourceBlobKey,
			r.Function.Revision, r.Function.UpgradeState, r.Function.UpgradeRuntime,
			r.Function.UpgradeMaxInstances, r.Function.UpgradeTrafficGen2,
			r.Function.MinInstanceCount, r.Function.MaxInstanceCount,
			r.Function.MaxInstanceRequestConcurrency, r.Function.AvailableCPU); err != nil {
			return err
		}
	}
	for _, r := range snap.Operations {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_functions_operations
				(project_id, location, operation_id, done, verb, target, function, create_time, end_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Operation.Location, r.Operation.ID, r.Operation.Done, r.Operation.Verb,
			r.Operation.Target, nullableJSON(r.Operation.Function), r.Operation.CreateTime, r.Operation.EndTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Deliveries {
		attrs, _ := json.Marshal(r.Delivery.Attributes)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_functions_deliveries
				(project_id, location, delivery_id, function_id, source, event_type, resource, event_id,
				 data, attributes, attempts, status, error, result, dead_letter_topic, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		`, r.ProjectID, r.Delivery.Location, r.Delivery.ID, r.Delivery.FunctionID, r.Delivery.Source,
			r.Delivery.EventType, r.Delivery.Resource, r.Delivery.EventID, r.Delivery.Data, json.RawMessage(attrs),
			r.Delivery.Attempts, r.Delivery.Status, r.Delivery.Error, r.Delivery.Result, r.Delivery.DeadLetterTopic,
			r.Delivery.CreateTime, r.Delivery.UpdateTime); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
