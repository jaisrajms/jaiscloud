-- Cloud Functions event-trigger delivery records. Real Cloud Functions does not
-- expose delivery records over the wire; the emulator persists one row per
-- event delivered (or attempted) to an event-triggered function so retry and
-- dead-letter outcomes are observable and survive a restart under --dsn.
-- Scoped by project + location and keyed by an opaque delivery id.

CREATE TABLE IF NOT EXISTS jc_functions_deliveries (
    project_id   TEXT        NOT NULL,
    location     TEXT        NOT NULL,
    delivery_id  TEXT        NOT NULL,
    function_id  TEXT        NOT NULL DEFAULT '',
    source       TEXT        NOT NULL DEFAULT '',
    event_type   TEXT        NOT NULL DEFAULT '',
    resource     TEXT        NOT NULL DEFAULT '',
    event_id     TEXT        NOT NULL DEFAULT '',
    data         TEXT        NOT NULL DEFAULT '',
    attributes   JSONB       NOT NULL DEFAULT '{}',
    attempts     INT         NOT NULL DEFAULT 0,
    status       TEXT        NOT NULL DEFAULT 'pending',
    error        TEXT        NOT NULL DEFAULT '',
    result       TEXT        NOT NULL DEFAULT '',
    create_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, location, delivery_id)
);
CREATE INDEX IF NOT EXISTS jc_functions_deliveries_function
    ON jc_functions_deliveries (project_id, location, function_id);
