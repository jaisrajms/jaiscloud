-- Atomic cryptoKeyVersion allocation: the crypto key carries a next_version
-- counter (mirrors Secret Manager jc_sm_secrets.next_ver) so concurrent
-- CreateVersion calls allocate distinct version numbers via UPDATE ... RETURNING
-- instead of a racy COALESCE(MAX(version)+1) SELECT. Version 1 is the primary,
-- so the counter starts at 2.
ALTER TABLE jc_kms_cryptokeys ADD COLUMN IF NOT EXISTS next_version INT NOT NULL DEFAULT 2;
