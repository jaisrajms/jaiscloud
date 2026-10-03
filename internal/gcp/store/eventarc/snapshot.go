package eventarc

import (
	"context"
	"encoding/json"
	"io"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.triggers) == 0 && len(s.channels) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"triggers": s.triggers,
		"channels": s.channels,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Triggers map[string]map[string]Trigger `json:"triggers"`
		Channels map[string]map[string]Channel `json:"channels"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Triggers == nil {
		snap.Triggers = map[string]map[string]Trigger{}
	}
	if snap.Channels == nil {
		snap.Channels = map[string]map[string]Channel{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.triggers = snap.Triggers
	s.channels = snap.Channels
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_eventarc_triggers`).Scan(&n); err != nil {
		return false, err
	}
	if n > 0 {
		return false, nil
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM jc_eventarc_channels`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type triggerRow struct {
		ProjectID string  `json:"projectId"`
		Trigger   Trigger `json:"trigger"`
	}
	type channelRow struct {
		ProjectID string  `json:"projectId"`
		Channel   Channel `json:"channel"`
	}

	triggers := make([]triggerRow, 0)
	channels := make([]channelRow, 0)

	trows, err := s.pool.Query(ctx, `
		SELECT project_id, location, trigger_id, config, labels, uid, etag, create_time, update_time
		FROM jc_eventarc_triggers ORDER BY project_id, location, trigger_id
	`)
	if err != nil {
		return err
	}
	for trows.Next() {
		var r triggerRow
		var config, labels []byte
		if err := trows.Scan(&r.ProjectID, &r.Trigger.Location, &r.Trigger.Name, &config, &labels,
			&r.Trigger.UID, &r.Trigger.Etag, &r.Trigger.CreateTime, &r.Trigger.UpdateTime); err != nil {
			trows.Close()
			return err
		}
		r.Trigger.Config = json.RawMessage(config)
		json.Unmarshal(labels, &r.Trigger.Labels)
		triggers = append(triggers, r)
	}
	trows.Close()
	if err := trows.Err(); err != nil {
		return err
	}

	crows, err := s.pool.Query(ctx, `
		SELECT project_id, location, channel_id, config, labels, uid, etag, activation_token, create_time, update_time
		FROM jc_eventarc_channels ORDER BY project_id, location, channel_id
	`)
	if err != nil {
		return err
	}
	for crows.Next() {
		var r channelRow
		var config, labels []byte
		if err := crows.Scan(&r.ProjectID, &r.Channel.Location, &r.Channel.Name, &config, &labels,
			&r.Channel.UID, &r.Channel.Etag, &r.Channel.ActivationToken, &r.Channel.CreateTime, &r.Channel.UpdateTime); err != nil {
			crows.Close()
			return err
		}
		r.Channel.Config = json.RawMessage(config)
		json.Unmarshal(labels, &r.Channel.Labels)
		channels = append(channels, r)
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"triggers": triggers,
		"channels": channels,
	})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Triggers []struct {
			ProjectID string  `json:"projectId"`
			Trigger   Trigger `json:"trigger"`
		} `json:"triggers"`
		Channels []struct {
			ProjectID string  `json:"projectId"`
			Channel   Channel `json:"channel"`
		} `json:"channels"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, tbl := range []string{"jc_eventarc_triggers", "jc_eventarc_channels"} {
		if _, err := tx.Exec(ctx, `DELETE FROM `+tbl); err != nil {
			return err
		}
	}
	for _, r := range snap.Triggers {
		labels, _ := json.Marshal(r.Trigger.Labels)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_eventarc_triggers
				(project_id, location, trigger_id, config, labels, uid, etag, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`, r.ProjectID, r.Trigger.Location, r.Trigger.Name, nullableJSONRaw(r.Trigger.Config, "{}"), nullableJSONRaw(labels, "{}"),
			r.Trigger.UID, r.Trigger.Etag, r.Trigger.CreateTime, r.Trigger.UpdateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.Channels {
		labels, _ := json.Marshal(r.Channel.Labels)
		if _, err := tx.Exec(ctx, `
			INSERT INTO jc_eventarc_channels
				(project_id, location, channel_id, config, labels, uid, etag, activation_token, create_time, update_time)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		`, r.ProjectID, r.Channel.Location, r.Channel.Name, nullableJSONRaw(r.Channel.Config, "{}"), nullableJSONRaw(labels, "{}"),
			r.Channel.UID, r.Channel.Etag, r.Channel.ActivationToken, r.Channel.CreateTime, r.Channel.UpdateTime); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
