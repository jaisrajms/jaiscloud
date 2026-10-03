-- Cloud Dataproc jobs: scheduling holds the job's restart policy
-- (dataproc.v1.Job.scheduling → JobScheduling{maxFailuresPerHour,
-- maxFailuresTotal}) verbatim as JSONB. long_running is the emulator's
-- long-running/streaming marker (a driver that never exits), used only by the
-- mock-mode state machine so such a job is not auto-settled to DONE. 025 and
-- 054 are checksum-frozen, hence this follow-up migration rather than an edit
-- there.
ALTER TABLE jc_dataproc_jobs
    ADD COLUMN IF NOT EXISTS scheduling JSONB NOT NULL DEFAULT '{}';

ALTER TABLE jc_dataproc_jobs
    ADD COLUMN IF NOT EXISTS long_running BOOL NOT NULL DEFAULT FALSE;
