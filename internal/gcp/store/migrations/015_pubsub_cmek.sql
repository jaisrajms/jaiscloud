ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS kms_key_name TEXT NOT NULL DEFAULT '';
ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS wrapped_dek BYTEA;
