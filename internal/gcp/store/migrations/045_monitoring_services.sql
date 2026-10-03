-- Cloud Monitoring (v3) Service Monitoring: services + service-level objectives.
--
--   jc_monitoring_services               Service resources, project-scoped
--   jc_monitoring_service_level_objectives  SLOs, scoped to a parent service
--
-- The Service identifier oneof and the SLO service_level_indicator oneof are
-- stored as opaque canonical JSON (JSONB): the transport-neutral store does not
-- depend on the proto package, and both REST (Discovery-shaped) and gRPC
-- (protojson) JSON round-trip faithfully. DeleteService cascades its SLOs in
-- the store (the emulator never evaluates SLOs, so there is no cross-state).

CREATE TABLE IF NOT EXISTS jc_monitoring_services (
    project_id    TEXT  NOT NULL,
    id            TEXT  NOT NULL,
    display_name  TEXT  NOT NULL DEFAULT '',
    identifier    JSONB,
    basic_service JSONB,
    telemetry     JSONB,
    user_labels   JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, id)
);

CREATE TABLE IF NOT EXISTS jc_monitoring_service_level_objectives (
    project_id              TEXT        NOT NULL,
    service_id              TEXT        NOT NULL,
    id                      TEXT        NOT NULL,
    display_name            TEXT        NOT NULL DEFAULT '',
    service_level_indicator JSONB,
    goal                    DOUBLE PRECISION NOT NULL DEFAULT 0,
    rolling_period_nanos    BIGINT      NOT NULL DEFAULT 0,
    calendar_period         INTEGER     NOT NULL DEFAULT 0,
    user_labels             JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, service_id, id)
);
