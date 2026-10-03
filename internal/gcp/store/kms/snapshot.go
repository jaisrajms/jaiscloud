package kms

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
)

// --- Memory store ---

func (s *MemoryStore) IsEmpty(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.keyrings) == 0 && len(s.cryptokeys) == 0 && len(s.versions) == 0, nil
}

func (s *MemoryStore) Snapshot(_ context.Context, w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.NewEncoder(w).Encode(map[string]any{
		"keyrings":   s.keyrings,
		"cryptokeys": s.cryptokeys,
		"versions":   s.versions,
		"dek":        s.serverDEK,
	})
}

func (s *MemoryStore) Restore(_ context.Context, r io.Reader) error {
	var snap struct {
		Keyrings   map[string]map[string]KeyRing   `json:"keyrings"`
		CryptoKeys map[string]map[string]CryptoKey `json:"cryptokeys"`
		Versions   map[string]map[string]Version   `json:"versions"`
		DEK        []byte                          `json:"dek"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	if snap.Keyrings == nil {
		snap.Keyrings = map[string]map[string]KeyRing{}
	}
	if snap.CryptoKeys == nil {
		snap.CryptoKeys = map[string]map[string]CryptoKey{}
	}
	if snap.Versions == nil {
		snap.Versions = map[string]map[string]Version{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keyrings = snap.Keyrings
	s.cryptokeys = snap.CryptoKeys
	s.versions = snap.Versions
	s.serverDEK = snap.DEK
	return nil
}

// --- Postgres store ---

type kmsKeyringRow struct {
	ProjectID  string    `json:"projectId"`
	Location   string    `json:"location"`
	ID         string    `json:"id"`
	CreateTime time.Time `json:"createTime"`
}

type kmsCryptokeyRow struct {
	ProjectID        string            `json:"projectId"`
	Location         string            `json:"location"`
	KeyRingID        string            `json:"keyRingId"`
	ID               string            `json:"id"`
	Purpose          string            `json:"purpose"`
	Algorithm        string            `json:"algorithm"`
	CreateTime       time.Time         `json:"createTime"`
	PrimaryVersion   string            `json:"primaryVersion"`
	Labels           map[string]string `json:"labels,omitempty"`
	RotationPeriod   time.Duration     `json:"rotationPeriod,omitempty"`
	NextRotationTime time.Time         `json:"nextRotationTime,omitempty"`
}

type kmsVersionRow struct {
	ProjectID        string    `json:"projectId"`
	Location         string    `json:"location"`
	KeyRingID        string    `json:"keyRingId"`
	KeyID            string    `json:"keyId"`
	Version          string    `json:"version"`
	State            string    `json:"state"`
	Algorithm        string    `json:"algorithm"`
	CreateTime       time.Time `json:"createTime"`
	KeyMaterial      []byte    `json:"keyMaterial,omitempty"`
	PrivateKey       []byte    `json:"privateKey,omitempty"`
	PublicKey        []byte    `json:"publicKey,omitempty"`
	DestroyTime      time.Time `json:"destroyTime,omitempty"`
	DestroyEventTime time.Time `json:"destroyEventTime,omitempty"`
}

func (s *PostgresStore) IsEmpty(ctx context.Context) (bool, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM jc_kms_keyrings) + (SELECT count(*) FROM jc_kms_cryptokeys) + (SELECT count(*) FROM jc_kms_cryptokey_versions)`).Scan(&n); err != nil {
		return false, err
	}
	return n == 0, nil
}

func (s *PostgresStore) Snapshot(ctx context.Context, w io.Writer) error {
	keyrings := make([]kmsKeyringRow, 0)
	rows, err := s.pool.Query(ctx, `SELECT project_id, location, keyring_id, create_time FROM jc_kms_keyrings ORDER BY project_id, location, keyring_id`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var r kmsKeyringRow
		if err := rows.Scan(&r.ProjectID, &r.Location, &r.ID, &r.CreateTime); err != nil {
			rows.Close()
			return err
		}
		keyrings = append(keyrings, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	cryptokeys := make([]kmsCryptokeyRow, 0)
	crows, err := s.pool.Query(ctx, `SELECT project_id, location, keyring_id, key_id, purpose, algorithm, create_time, primary_version, labels, rotation_period, next_rotation_time FROM jc_kms_cryptokeys ORDER BY project_id, location, keyring_id, key_id`)
	if err != nil {
		return err
	}
	for crows.Next() {
		var r kmsCryptokeyRow
		var labels []byte
		var rotationSecs *int64
		var next *time.Time
		if err := crows.Scan(&r.ProjectID, &r.Location, &r.KeyRingID, &r.ID, &r.Purpose, &r.Algorithm, &r.CreateTime, &r.PrimaryVersion, &labels, &rotationSecs, &next); err != nil {
			crows.Close()
			return err
		}
		if len(labels) > 0 {
			_ = json.Unmarshal(labels, &r.Labels)
		}
		if rotationSecs != nil {
			r.RotationPeriod = time.Duration(*rotationSecs) * time.Second
		}
		if next != nil {
			r.NextRotationTime = *next
		}
		cryptokeys = append(cryptokeys, r)
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return err
	}

	versions := make([]kmsVersionRow, 0)
	vrows, err := s.pool.Query(ctx, `SELECT project_id, location, keyring_id, key_id, version, state, algorithm, create_time, key_material, private_key, public_key, destroy_time, destroy_event_time FROM jc_kms_cryptokey_versions ORDER BY project_id, location, keyring_id, key_id, version`)
	if err != nil {
		return err
	}
	for vrows.Next() {
		var r kmsVersionRow
		var destroyTime, destroyEventTime *time.Time
		if err := vrows.Scan(&r.ProjectID, &r.Location, &r.KeyRingID, &r.KeyID, &r.Version, &r.State, &r.Algorithm, &r.CreateTime, &r.KeyMaterial, &r.PrivateKey, &r.PublicKey, &destroyTime, &destroyEventTime); err != nil {
			vrows.Close()
			return err
		}
		if destroyTime != nil {
			r.DestroyTime = *destroyTime
		}
		if destroyEventTime != nil {
			r.DestroyEventTime = *destroyEventTime
		}
		versions = append(versions, r)
	}
	vrows.Close()
	if err := vrows.Err(); err != nil {
		return err
	}

	rawDEK, err := s.rawDEKBlob(ctx)
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(map[string]any{
		"keyrings":   keyrings,
		"cryptokeys": cryptokeys,
		"versions":   versions,
		"dek":        rawDEK,
	})
}

// rawDEKBlob reads the DEK blob exactly as persisted in jc_kms_dek (the KEK-
// wrapped or plaintext bytes), without unwrapping it. Snapshotting the raw blob
// preserves the key-encryption invariant across export→import: Restore re-inserts
// it so key material remains decryptable with the same server DEK.
func (s *PostgresStore) rawDEKBlob(ctx context.Context) ([]byte, error) {
	var blob []byte
	err := s.pool.QueryRow(ctx, `SELECT dek FROM jc_kms_dek WHERE id = 1`).Scan(&blob)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return blob, nil
}

func (s *PostgresStore) Restore(ctx context.Context, r io.Reader) error {
	var snap struct {
		Keyrings   []kmsKeyringRow   `json:"keyrings"`
		CryptoKeys []kmsCryptokeyRow `json:"cryptokeys"`
		Versions   []kmsVersionRow   `json:"versions"`
		DEK        []byte            `json:"dek"`
	}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM jc_kms_dek`); err != nil {
		return err
	}
	if len(snap.DEK) > 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO jc_kms_dek (id, dek) VALUES (1, $1)`, snap.DEK); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_kms_cryptokey_versions`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_kms_cryptokeys`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM jc_kms_keyrings`); err != nil {
		return err
	}
	for _, r := range snap.Keyrings {
		if _, err := tx.Exec(ctx, `INSERT INTO jc_kms_keyrings (project_id, location, keyring_id, create_time) VALUES ($1,$2,$3,$4)`,
			r.ProjectID, r.Location, r.ID, r.CreateTime); err != nil {
			return err
		}
	}
	for _, r := range snap.CryptoKeys {
		if _, err := tx.Exec(ctx, `INSERT INTO jc_kms_cryptokeys (project_id, location, keyring_id, key_id, purpose, algorithm, create_time, primary_version, labels, rotation_period, next_rotation_time) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			r.ProjectID, r.Location, r.KeyRingID, r.ID, r.Purpose, r.Algorithm, r.CreateTime, r.PrimaryVersion, labelsJSON(r.Labels), rotationSeconds(r.RotationPeriod), nullableTime(r.NextRotationTime)); err != nil {
			return err
		}
	}
	for _, r := range snap.Versions {
		if _, err := tx.Exec(ctx, `INSERT INTO jc_kms_cryptokey_versions (project_id, location, keyring_id, key_id, version, state, algorithm, create_time, key_material, private_key, public_key, destroy_time, destroy_event_time) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			r.ProjectID, r.Location, r.KeyRingID, r.KeyID, r.Version, r.State, r.Algorithm, r.CreateTime, r.KeyMaterial, r.PrivateKey, r.PublicKey, nullableTime(r.DestroyTime), nullableTime(r.DestroyEventTime)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
