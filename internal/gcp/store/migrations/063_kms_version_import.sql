-- KMS key import / HSM trusted wrapping (GA6). Versions gain their protection
-- level, import timestamp, and the trusted-wrapping / HSM-trusted flags that
-- ExportTrustedKeyWrappedCryptoKeyVersion and ImportTrustedKeyWrappedCryptoKeyVersion
-- depend on. Crypto keys gain their protection level and an import-only marker
-- (an import-only key's versions are created by import, not generation).
ALTER TABLE jc_kms_cryptokey_versions ADD COLUMN IF NOT EXISTS protection_level TEXT NOT NULL DEFAULT 'SOFTWARE';
ALTER TABLE jc_kms_cryptokey_versions ADD COLUMN IF NOT EXISTS import_time TIMESTAMPTZ;
ALTER TABLE jc_kms_cryptokey_versions ADD COLUMN IF NOT EXISTS trusted_wrapping_enabled BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE jc_kms_cryptokey_versions ADD COLUMN IF NOT EXISTS hsm_trusted BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS protection_level TEXT NOT NULL DEFAULT 'SOFTWARE';
ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS import_only BOOLEAN NOT NULL DEFAULT false;
