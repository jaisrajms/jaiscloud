package secretmanager

import (
	"context"
	"encoding/json"
	"io"
	"time"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.secrets) == 0 && len(s.versions) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"secrets":  s.secrets,
		"versions": s.versions,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Secrets  map[string]map[string]Secret  `json:"secrets"`
		Versions map[string]map[string]Version `json:"versions"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Secrets == nil {
		snap.Secrets = map[string]map[string]Secret{}
	}
	if snap.Versions == nil {
		snap.Versions = map[string]map[string]Version{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.secrets = snap.Secrets
	s.versions = snap.Versions
	return nil
}

// --- Postgres store ---

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM jc_sm_secrets) + (SELECT count(*) FROM jc_sm_versions)`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	type secretRow struct {
		ProjectID string `json:"projectId"`
		Secret    Secret `json:"secret"`
	}
	type versionRow struct {
		ProjectID string  `json:"projectId"`
		SecretID  string  `json:"secretId"`
		Version   Version `json:"version"`
	}
	secrets := make([]secretRow, 0)
	rows, err := s.pool.Query(ctx, `SELECT project_id, secret_id, labels, create_time, next_ver, rotation, version_aliases, kms_key_name, annotations FROM jc_sm_secrets ORDER BY project_id, secret_id`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r secretRow
		var labels, annotations, rotation, aliases []byte
		if err := rows.Scan(&r.ProjectID, &r.Secret.ID, &labels, &r.Secret.CreateTime, &r.Secret.NextVer, &rotation, &aliases, &r.Secret.KmsKeyName, &annotations); err != nil {
			rows.Close()
			return err
		}
		json.Unmarshal(labels, &r.Secret.Labels)
		json.Unmarshal(annotations, &r.Secret.Annotations)
		if len(rotation) > 0 {
			json.Unmarshal(rotation, &r.Secret.Rotation)
		}
		if len(aliases) > 0 {
			json.Unmarshal(aliases, &r.Secret.VersionAliases)
		}
		secrets = append(secrets, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	versions := make([]versionRow, 0)
	vrows, err := s.pool.Query(ctx, `SELECT project_id, secret_id, version_id, state, create_time, destroy_time, data FROM jc_sm_versions ORDER BY project_id, secret_id, version_id`)
	if err != nil {
		return err
	}
	for vrows.Next() {
		var r versionRow
		var destroyTime *time.Time
		if err := vrows.Scan(&r.ProjectID, &r.SecretID, &r.Version.VersionID, &r.Version.State, &r.Version.CreateTime, &destroyTime, &r.Version.Data); err != nil {
			vrows.Close()
			return err
		}
		if destroyTime != nil {
			r.Version.DestroyTime = *destroyTime
		}
		r.Version.SecretID = r.SecretID
		versions = append(versions, r)
	}
	vrows.Close()
	if err := vrows.Err(); err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{"secrets": secrets, "versions": versions})
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Secrets []struct {
			ProjectID string `json:"projectId"`
			Secret    Secret `json:"secret"`
		} `json:"secrets"`
		Versions []struct {
			ProjectID string  `json:"projectId"`
			SecretID  string  `json:"secretId"`
			Version   Version `json:"version"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_sm_versions`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_sm_secrets`); err != nil {
		return err
	}
	for _, r := range snap.Secrets {
		labels, _ := json.Marshal(r.Secret.Labels)
		var rotation, aliases, annotations []byte
		if r.Secret.Rotation != nil {
			rotation, _ = json.Marshal(r.Secret.Rotation)
		}
		if r.Secret.VersionAliases != nil {
			aliases, _ = json.Marshal(r.Secret.VersionAliases)
		}
		if r.Secret.Annotations != nil {
			annotations, _ = json.Marshal(r.Secret.Annotations)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO jc_sm_secrets (project_id, secret_id, labels, create_time, next_ver, rotation, version_aliases, kms_key_name, annotations) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			r.ProjectID, r.Secret.ID, json.RawMessage(labels), r.Secret.CreateTime, r.Secret.NextVer, nullableJSONBytes(rotation), nullableJSONBytes(aliases), r.Secret.KmsKeyName, nullableJSONBytes(annotations)); err != nil {
			return err
		}
	}
	for _, r := range snap.Versions {
		if _, err := tx.Exec(ctx, `INSERT INTO jc_sm_versions (project_id, secret_id, version_id, state, create_time, destroy_time, data) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			r.ProjectID, r.SecretID, r.Version.VersionID, r.Version.State, r.Version.CreateTime, nullableTime(r.Version.DestroyTime), r.Version.Data); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func nullableJSONBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}
