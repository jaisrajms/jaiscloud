-- Pub/Sub message data plane (mirrors jc_sqs_messages for SQS).
CREATE TABLE IF NOT EXISTS jc_pubsub_messages (
    topic        TEXT        NOT NULL,
    message_id   TEXT        NOT NULL,
    data         TEXT        NOT NULL DEFAULT '',
    attributes   JSONB       NOT NULL DEFAULT '{}',
    publish_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, message_id)
);

CREATE INDEX IF NOT EXISTS idx_pubsub_messages_topic
    ON jc_pubsub_messages (topic, publish_time);
