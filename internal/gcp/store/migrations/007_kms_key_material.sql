-- KMS real encryption: per-cryptoKey AES-256 key material, wrapped by a
-- server DEK at rest (mirrors AWS internal/aws/key KeyMaterial + DEK).
ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS key_material BYTEA;

-- Server DEK (single row) protecting key material at rest.
CREATE TABLE IF NOT EXISTS jc_kms_dek (
    id  INT  PRIMARY KEY DEFAULT 1,
    dek BYTEA NOT NULL
);
