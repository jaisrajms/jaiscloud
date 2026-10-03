-- Pub/Sub fan-out: messages are stored per subscription (a copy per pull
-- subscription) rather than once per topic, so every subscription receives
-- every message. Key rows by subscription; keep the logical topic for
-- display/snapshot fidelity.
ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS subscription TEXT NOT NULL DEFAULT '';

-- Backfill legacy topic-keyed rows so they keep a stable key.
UPDATE jc_pubsub_messages SET subscription = topic WHERE subscription = '';

ALTER TABLE jc_pubsub_messages DROP CONSTRAINT IF EXISTS jc_pubsub_messages_pkey;
ALTER TABLE jc_pubsub_messages ADD PRIMARY KEY (subscription, message_id);

CREATE INDEX IF NOT EXISTS idx_pubsub_messages_subscription
    ON jc_pubsub_messages (subscription, publish_time);
