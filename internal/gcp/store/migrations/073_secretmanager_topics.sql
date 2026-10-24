-- Secret Manager event notifications: the `topics` list every secret carries
-- (up to 10 Pub/Sub topics that receive SECRET_* control-plane messages),
-- stored as a JSON array of topic resource names.
ALTER TABLE jc_sm_secrets ADD COLUMN IF NOT EXISTS topics JSONB;
