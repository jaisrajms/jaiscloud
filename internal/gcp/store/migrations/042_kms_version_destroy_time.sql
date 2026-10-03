-- KMS CryptoKeyVersion destruction timing. DestroyCryptoKeyVersion moves a
-- version to DESTROY_SCHEDULED with a destroy_time (default 30 days out); the
-- emulator lazily promotes it to DESTROYED once that instant passes, recording
-- destroy_event_time. Both columns are output-only and nullable (NULL unless the
-- version is in the corresponding state).
ALTER TABLE jc_kms_cryptokey_versions ADD COLUMN IF NOT EXISTS destroy_time TIMESTAMPTZ;
ALTER TABLE jc_kms_cryptokey_versions ADD COLUMN IF NOT EXISTS destroy_event_time TIMESTAMPTZ;
