-- Cloud Functions event dead-letter forwarding (FD9).
--
-- dead_letter_topic records the short id of the dead-letter topic an exhausted
-- delivery was forwarded to under the backing Pub/Sub subscription's
-- deadLetterPolicy (real GCP's CloudPubSubDeadLetterSource* republish). Empty
-- means no policy was configured, so the delivery just ends as dead_letter.
ALTER TABLE jc_functions_deliveries ADD COLUMN IF NOT EXISTS dead_letter_topic TEXT NOT NULL DEFAULT '';
