-- Secret Manager versions: per-replica wrapped DEKs (DC57). A user-managed
-- replication policy can name a different CMEK key per replica; real GCP wraps
-- the version's single DEK once per replica so each replica reports its own
-- CryptoKeyVersion. Rows written before this migration keep wrapped_dek (the
-- primary wrap) and read back with no per-replica entries.
ALTER TABLE jc_sm_versions ADD COLUMN IF NOT EXISTS replica_deks JSONB;
