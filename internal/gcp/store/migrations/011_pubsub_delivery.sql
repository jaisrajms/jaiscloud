-- Pub/Sub delivery depth: per-message visibility deadline (ackDeadlineSeconds) and
-- ordering key (mirrors SQS jc_sqs_messages visible_at / group_id).
ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS visible_at TIMESTAMPTZ;
ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS ordering_key TEXT NOT NULL DEFAULT '';
