-- Pub/Sub exactly-once ack-ID versioning (EOD2). A Seek that makes a message
-- visible again invalidates outstanding ack IDs, but bumping `delivery_attempt`
-- to do so would corrupt dead-letter accounting (a message would cross its
-- maxDeliveryAttempts threshold on a Seek). Track a per-message ack-ID version
-- separately: a claim advances both, a Seek advances only this one.
ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS delivery_version INT NOT NULL DEFAULT 0;
