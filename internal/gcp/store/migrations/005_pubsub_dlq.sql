-- Pub/Sub DLQ delivery-attempt tracking (mirrors SQS maxReceiveCount).
ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS delivery_attempt INT NOT NULL DEFAULT 0;
