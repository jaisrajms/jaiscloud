-- Secret Manager rotation + version aliases (GCP-native: the secret carries a
-- `rotation` schedule and a `versionAliases` map; no AWS version stages).
ALTER TABLE jc_sm_secrets ADD COLUMN IF NOT EXISTS rotation JSONB;
ALTER TABLE jc_sm_secrets ADD COLUMN IF NOT EXISTS version_aliases JSONB;
