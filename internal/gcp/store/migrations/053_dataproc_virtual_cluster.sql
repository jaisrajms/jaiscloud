-- Cloud Dataproc GKE-backed clusters: virtual_cluster_config holds the
-- dataproc.v1.VirtualClusterConfig wire object verbatim (kubernetesClusterConfig,
-- auxiliaryServicesConfig, stagingBucket, ...). It is mutually exclusive with
-- `config` (gceClusterConfig) in the API, so a GKE cluster renders
-- virtualClusterConfig and no config. 025_dataproc.sql is checksum-frozen, hence
-- this follow-up migration rather than an edit there.
ALTER TABLE jc_dataproc_clusters
    ADD COLUMN IF NOT EXISTS virtual_cluster_config JSONB NOT NULL DEFAULT '{}';
