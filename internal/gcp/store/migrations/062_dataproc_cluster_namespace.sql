-- Cloud Dataproc per-cluster workload namespace: the effective Kubernetes
-- namespace the cluster's jobs run in (the caller's
-- virtualClusterConfig.kubernetesClusterConfig.kubernetesNamespace when
-- supplied, else a derived per-cluster name) and whether the emulator created
-- it (so delete/reset only removes emulator-owned namespaces). 025_dataproc.sql
-- is checksum-frozen, hence this follow-up migration rather than an edit there.
ALTER TABLE jc_dataproc_clusters
    ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS namespace_owned BOOLEAN NOT NULL DEFAULT FALSE;
