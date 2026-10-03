-- Cloud Tasks v2. Queues are project+location scoped with canonical names
-- projects/{project}/locations/{location}/queues/{queue}; tasks nest under a
-- queue. Each record is stored as a single JSONB document so read-back echoes
-- what the caller last observed; the name columns are the lookup key.

CREATE TABLE IF NOT EXISTS jc_tasks_queues (
    project_id  TEXT        NOT NULL,
    location    TEXT        NOT NULL,
    queue_name  TEXT        NOT NULL,
    data        JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, queue_name)
);

CREATE TABLE IF NOT EXISTS jc_tasks_tasks (
    project_id  TEXT        NOT NULL,
    location    TEXT        NOT NULL,
    queue_name  TEXT        NOT NULL,
    task_name   TEXT        NOT NULL,
    data        JSONB       NOT NULL DEFAULT '{}',
    PRIMARY KEY (project_id, location, queue_name, task_name)
);

CREATE INDEX IF NOT EXISTS idx_jc_tasks_tasks_queue
    ON jc_tasks_tasks (project_id, location, queue_name);
