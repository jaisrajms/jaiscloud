-- KMS key rings and crypto keys (mirrors AWS jc_kms_keys, simplified to the
-- keyring/cryptokey hierarchy the GCP emulator models).
CREATE TABLE IF NOT EXISTS jc_kms_keyrings (
    project_id  TEXT        NOT NULL,
    location    TEXT        NOT NULL,
    keyring_id  TEXT        NOT NULL,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, keyring_id)
);

CREATE TABLE IF NOT EXISTS jc_kms_cryptokeys (
    project_id  TEXT        NOT NULL,
    location    TEXT        NOT NULL,
    keyring_id  TEXT        NOT NULL,
    key_id      TEXT        NOT NULL,
    purpose     TEXT        NOT NULL DEFAULT 'ENCRYPT_DECRYPT',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, keyring_id, key_id)
);
