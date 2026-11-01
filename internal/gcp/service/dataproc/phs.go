package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// Dataproc-on-GKE Persistent History Server (PHS) attachment.
//
// A virtual cluster may name an existing Dataproc cluster as its Spark History
// Server:
//
//	virtualClusterConfig.auxiliaryServicesConfig.sparkHistoryServerConfig.dataprocCluster
//
// (dataproc.v1.SparkHistoryServerConfig.dataprocCluster: "Resource name of an
// existing Dataproc Cluster to act as a Spark History Server for the workload.
// Example: projects/[project_id]/regions/[region]/clusters/[cluster_name]").
//
// The documented contract (Managed Service for Apache Spark on GKE quickstart,
// https://docs.cloud.google.com/managed-spark/docs/guides/dpgke/quickstarts/gke-quickstart-create-cluster,
// rev 2026-10-26) is:
//
//   - the PHS cluster "must be located in the same region as the Managed
//     Service for Apache Spark on GKE virtual cluster";
//   - the create response carries, under
//     virtualClusterConfig.kubernetesClusterConfig.kubernetesSoftwareConfig.properties,
//     "spark:spark.eventLog.dir" and "spark:spark.eventLog.enabled".
//
// The history-server page
// (https://docs.cloud.google.com/dataproc/docs/concepts/jobs/history-server)
// additionally requires the PHS cluster and job cluster image versions to match
// and sets spark.history.* on the PHS cluster (the reader), not on the
// workload. The workload side writes history with spark.eventLog.*; the
// emulator therefore injects exactly that documented pair, and never invents
// spark.history.* properties on the submitted job.
const (
	// phsEventLogEnabledKey / phsEventLogDirKey are the cluster-property keys
	// (component-prefixed) the documented create response contains.
	phsEventLogEnabledKey = "spark:spark.eventLog.enabled"
	phsEventLogDirKey     = "spark:spark.eventLog.dir"

	// phsEventLogEnabledConf / phsEventLogDirConf are the spark-submit --conf
	// keys (unprefixed) the same settings map to for a job driver.
	phsEventLogEnabledConf = "spark.eventLog.enabled"
	phsEventLogDirConf     = "spark.eventLog.dir"

	// phsHistoryDirName is the final segment of the documented
	// "gs://BUCKET/.../spark-job-history" log directory.
	phsHistoryDirName = "spark-job-history"
)

// phsClusterRefInVirtualClusterConfig reads
// virtualClusterConfig.auxiliaryServicesConfig.sparkHistoryServerConfig.dataprocCluster.
func phsClusterRefInVirtualClusterConfig(raw []byte) string {
	vcc := mustJSONMap(raw)
	aux, _ := vcc["auxiliaryServicesConfig"].(map[string]any)
	phs, _ := aux["sparkHistoryServerConfig"].(map[string]any)
	s, _ := phs["dataprocCluster"].(string)
	return strings.TrimSpace(s)
}

// parsePHSClusterRef parses the documented canonical
// projects/{project}/regions/{region}/clusters/{cluster} form. Dataproc
// documents no short form for this field (unlike a Metastore service
// reference), so anything else is InvalidArgument.
func parsePHSClusterRef(ref string) (project, region, name string, err error) {
	parts := strings.Split(strings.TrimSpace(ref), "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "regions" || parts[4] != "clusters" ||
		parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", "", invalidArgument(
			"sparkHistoryServerConfig.dataprocCluster must be a fully qualified Dataproc " +
				"cluster resource name (projects, regions and clusters segments)")
	}
	return parts[1], parts[3], parts[5], nil
}

// clusterImageVersion reads a stored Dataproc cluster's
// config.softwareConfig.imageVersion.
func clusterImageVersion(c dpstore.Cluster) string {
	cfg := mustJSONMap(c.Config)
	software, _ := cfg["softwareConfig"].(map[string]any)
	v, _ := software["imageVersion"].(string)
	return strings.TrimSpace(v)
}

// validatePHSClusterRef validates one Spark History Server reference against
// the Dataproc control plane. An empty reference is a no-op. The documented
// rules are: canonical name shape, same project, same region, the cluster must
// exist, and it must carry a Dataproc image version (a PHS cluster is a
// Dataproc cluster). The referenced cluster need not be RUNNING; it is settled
// lazily by advanceCluster, matching the cluster read path.
func (s *Service) validatePHSClusterRef(ctx context.Context, project, region, ref string) error {
	if ref == "" {
		return nil
	}
	refProject, refRegion, refName, err := parsePHSClusterRef(ref)
	if err != nil {
		return err
	}
	if refProject != project {
		return invalidArgument(
			"sparkHistoryServerConfig.dataprocCluster must be in the cluster project " +
				project + ", got " + refProject)
	}
	if refRegion != region {
		return invalidArgument(
			"sparkHistoryServerConfig.dataprocCluster must be in the cluster region " +
				region + ", got " + refRegion)
	}
	c, err := s.advanceCluster(ctx, refProject, refRegion, refName)
	if err != nil {
		if errors.Is(err, dpstore.ErrNoSuchCluster) {
			return model.NewProviderError("NotFound", "spark history server cluster not found: "+ref, 404)
		}
		return mapErr(err)
	}
	if clusterImageVersion(c) == "" {
		return invalidArgument(
			"sparkHistoryServerConfig.dataprocCluster must reference a Dataproc cluster " +
				"with a softwareConfig.imageVersion: " + ref)
	}
	return nil
}

// phsLogDirectory is the documented "gs://BUCKET/.../spark-job-history"
// location for a virtual cluster's history files. The docs elide the middle
// segment; the emulator uses the virtual cluster's name, which is stable and
// deterministic. The PHS reader uses a "*" wildcard for that segment, so any
// per-cluster directory is readable.
func phsLogDirectory(c dpstore.Cluster) string {
	return gsURI(stagingBucketForCluster(c), c.Name+"/"+phsHistoryDirName)
}

// preparePHSClusterConfig validates a caller-supplied virtualClusterConfig's
// PHS reference (if any) and injects the documented workload properties into
// its kubernetesSoftwareConfig. Client-supplied values win: a property the
// caller already set is left untouched. Every other field is preserved
// (encoding/json re-encodes the object, so only key order may differ).
func (s *Service) preparePHSClusterConfig(ctx context.Context, project, region, name string, raw json.RawMessage) (json.RawMessage, error) {
	ref := phsClusterRefInVirtualClusterConfig(raw)
	if ref == "" {
		return raw, nil
	}
	if err := s.validatePHSClusterRef(ctx, project, region, ref); err != nil {
		return nil, err
	}
	vcc := mustJSONMap(raw)
	if vcc == nil {
		return raw, nil
	}
	kcc, ok := vcc["kubernetesClusterConfig"].(map[string]any)
	if !ok {
		return raw, nil
	}
	software, ok := kcc["kubernetesSoftwareConfig"].(map[string]any)
	if !ok {
		software = map[string]any{}
		kcc["kubernetesSoftwareConfig"] = software
	}
	properties, ok := software["properties"].(map[string]any)
	if !ok {
		properties = map[string]any{}
		software["properties"] = properties
	}
	if _, set := properties[phsEventLogEnabledKey]; !set {
		properties[phsEventLogEnabledKey] = "true"
	}
	if _, set := properties[phsEventLogDirKey]; !set {
		properties[phsEventLogDirKey] = phsLogDirectory(dpstore.Cluster{
			ProjectID:            project,
			Region:               region,
			Name:                 name,
			VirtualClusterConfig: raw,
		})
	}
	data, err := json.Marshal(vcc)
	if err != nil {
		return nil, invalidArgument("virtualClusterConfig is not a valid object")
	}
	return data, nil
}

// phsSparkConfs returns the spark-submit --conf tokens that make a job on a
// PHS-attached virtual cluster write its Spark event log where the history
// server reads it. It returns nil when the stored virtualClusterConfig has no
// PHS reference. The stored cluster properties are the source of truth (they
// already carry the injected values, or the caller's overrides), with the
// documented defaults as a fallback for a cluster stored before this behaviour
// existed.
func phsSparkConfs(c dpstore.Cluster) []string {
	if phsClusterRefInVirtualClusterConfig(c.VirtualClusterConfig) == "" {
		return nil
	}
	properties := map[string]any{}
	if vcc := mustJSONMap(c.VirtualClusterConfig); vcc != nil {
		if kcc, ok := vcc["kubernetesClusterConfig"].(map[string]any); ok {
			if software, ok := kcc["kubernetesSoftwareConfig"].(map[string]any); ok {
				if p, ok := software["properties"].(map[string]any); ok {
					properties = p
				}
			}
		}
	}
	enabled, _ := properties[phsEventLogEnabledKey].(string)
	enabled = strings.TrimSpace(enabled)
	if enabled == "" {
		enabled = "true"
	}
	dir, _ := properties[phsEventLogDirKey].(string)
	dir = strings.TrimSpace(dir)
	if dir == "" {
		dir = phsLogDirectory(c)
	}
	return []string{
		"--conf", phsEventLogEnabledConf + "=" + enabled,
		"--conf", phsEventLogDirConf + "=" + dir,
	}
}
