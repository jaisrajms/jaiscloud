-- Cloud Dataproc jobs: placement_cluster_uuid holds the UUID of the cluster a
-- job was submitted to (dataproc.v1.JobPlacement.cluster_uuid, output-only).
-- It is captured at submit and persisted so the field survives the cluster
-- being deleted before the job reaches a terminal state. 025_dataproc.sql is
-- checksum-frozen, hence this follow-up migration rather than an edit there.
ALTER TABLE jc_dataproc_jobs
    ADD COLUMN IF NOT EXISTS placement_cluster_uuid TEXT NOT NULL DEFAULT '';
