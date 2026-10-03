-- Eventarc (eventarc.googleapis.com/v1) metadata store. Eventarc is the GCP
-- analogue of AWS EventBridge in purpose, but a *different* model: it is
-- trigger-based (a Trigger routes events from a Pub/Sub topic or Channel to a
-- Cloud Run / Cloud Functions v2 / Workflows / GKE destination), not the
-- EventBridge "bus + rule + event-pattern + target" fan-out. This is
-- metadata-only CRUD — the emulator never stands up an event-delivery engine,
-- so a Trigger/Channel is a stored metadata record only.
--
-- Two logical tables:
--
--   jc_eventarc_triggers — (project_id, location, trigger_id) -> request body
--                          JSON verbatim (destination/transport/eventFilters/
--                          serviceAccount/channel/...) plus output-only uid/etag
--                          and create/update timestamps.
--   jc_eventarc_channels — (project_id, location, channel_id) -> request body
--                          JSON verbatim (provider/cryptoKeyName/...) plus
--                          output-only uid/activation_token and timestamps.
--
-- Providers are read-only discovery: a small static catalogue is served by the
-- provider layer, not stored in a table.

CREATE TABLE IF NOT EXISTS jc_eventarc_triggers (
    project_id  TEXT        NOT NULL,
    location    TEXT        NOT NULL,
    trigger_id  TEXT        NOT NULL,
    config      JSONB       NOT NULL DEFAULT '{}',
    labels      JSONB       NOT NULL DEFAULT '{}',
    uid         TEXT        NOT NULL DEFAULT '',
    etag        TEXT        NOT NULL DEFAULT '',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, trigger_id)
);

CREATE TABLE IF NOT EXISTS jc_eventarc_channels (
    project_id       TEXT        NOT NULL,
    location         TEXT        NOT NULL,
    channel_id       TEXT        NOT NULL,
    config           JSONB       NOT NULL DEFAULT '{}',
    labels           JSONB       NOT NULL DEFAULT '{}',
    uid              TEXT        NOT NULL DEFAULT '',
    activation_token TEXT        NOT NULL DEFAULT '',
    create_time      TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, channel_id)
);
