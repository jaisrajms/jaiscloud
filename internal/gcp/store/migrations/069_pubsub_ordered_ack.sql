-- Pub/Sub ordered-delivery ack state. A keyed message is retained after ack so a
-- redelivery of an earlier message for the key can redeliver it too (real GCP
-- redelivers all subsequent messages for an ordering key, even acknowledged
-- ones). `ack_pending` records a later ack that arrived while an earlier message
-- for the key was still unacknowledged; it is applied once every earlier message
-- is acked. Unordered messages are deleted on ack and never set either column.
ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS acked BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE jc_pubsub_messages ADD COLUMN IF NOT EXISTS ack_pending BOOLEAN NOT NULL DEFAULT FALSE;
