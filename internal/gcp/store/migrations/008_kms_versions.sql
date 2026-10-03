-- CryptoKeyVersion support: each crypto key has an ordered set of versions,
-- each with its own key material; the key tracks a primary version. Rotation
-- creates a new version and switches primary (mirrors AWS KeyEntry.Primary +
-- per-version material).
ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS primary_version TEXT NOT NULL DEFAULT '1';

CREATE TABLE IF NOT EXISTS jc_kms_cryptokey_versions (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    keyring_id   TEXT        NOT NULL,
    key_id       TEXT        NOT NULL,
    version      TEXT        NOT NULL,
    state        TEXT        NOT NULL DEFAULT 'ENABLED',
    algorithm    TEXT        NOT NULL DEFAULT 'GOOGLE_SYMMETRIC_ENCRYPTION',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    key_material BYTEA       NOT NULL,
    PRIMARY KEY (project_id, location, keyring_id, key_id, version)
);
