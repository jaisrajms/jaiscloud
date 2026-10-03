-- KMS asymmetric/HMAC key material: private + public key DER per version, and
-- a version-template algorithm on the crypto key (new versions inherit it).
ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS algorithm TEXT NOT NULL DEFAULT 'GOOGLE_SYMMETRIC_ENCRYPTION';
ALTER TABLE jc_kms_cryptokey_versions ADD COLUMN IF NOT EXISTS private_key BYTEA;
ALTER TABLE jc_kms_cryptokey_versions ADD COLUMN IF NOT EXISTS public_key BYTEA;
