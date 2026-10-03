package dataproc

import (
	"context"
	"encoding/json"
	"strings"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// MetastoreResolver validates a cluster's Dataproc Metastore attachment and
// derives the thrift endpoint Spark clients should use. It is implemented by the
// Metastore core (which owns the resource-name grammar and the endpoint format)
// and injected in main.go, so the Dataproc core never imports service/metastore.
type MetastoreResolver interface {
	// ValidateMetastoreService validates that ref names an existing Metastore
	// service, filling missing project/location segments from
	// defaultProject/defaultLocation. A malformed reference is InvalidArgument
	// and an unknown service is NotFound. It is called when a cluster is
	// created/updated, mirroring real Dataproc's provisioning-time check.
	ValidateMetastoreService(ctx context.Context, ref, defaultProject, defaultLocation string) error
	// MetastoreEndpoint formats the thrift endpoint for ref without a store
	// lookup. It is called at job submission so the attachment validated at
	// cluster creation is honored at execution.
	MetastoreEndpoint(ref, defaultProject, defaultLocation string) (string, error)
}

// WithMetastoreResolver wires the Metastore attachment resolver. Nil leaves a
// cluster's metastoreConfig stored and echoed but unvalidated and not injected
// into jobs (unit tests / deployments without the Metastore service).
func WithMetastoreResolver(r MetastoreResolver) Option {
	return func(s *Service) { s.metastoreResolver = r }
}

// WithHMSEndpointOverride overrides the thrift endpoint injected into job pods
// for every metastore-attached cluster. The synthesized per-service endpoint
// (thrift://<id>.<location>.metastore.jaiscloud.local:9083) is not resolvable
// from Spark pods, so a deployment sets JAISCLOUD_DATAPROC_HMS_ENDPOINT to the
// reachable Hive Metastore address. Accepts "host:port" or a "thrift://" URI.
func WithHMSEndpointOverride(uri string) Option {
	return func(s *Service) { s.hmsEndpointOverride = normalizeHMSEndpoint(uri) }
}

// normalizeHMSEndpoint trims an override and defaults a bare host:port to the
// thrift scheme.
func normalizeHMSEndpoint(uri string) string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return ""
	}
	if !strings.Contains(uri, "://") {
		return "thrift://" + uri
	}
	return uri
}

// metastoreServiceRefFromInput extracts the dataprocMetastoreService reference
// from a cluster create/update body: the GKE
// virtualClusterConfig.auxiliaryServicesConfig.metastoreConfig location first,
// then the GCE config.metastoreConfig location.
func metastoreServiceRefFromInput(in ClusterInput) string {
	if ref := metastoreRefInVirtualClusterConfig(in.VirtualClusterConfig); ref != "" {
		return ref
	}
	return metastoreRefInConfig(in.Config)
}

// metastoreServiceRefFromCluster is the stored-cluster counterpart of
// metastoreServiceRefFromInput.
func metastoreServiceRefFromCluster(c dpstore.Cluster) string {
	if ref := metastoreRefInVirtualClusterConfig(c.VirtualClusterConfig); ref != "" {
		return ref
	}
	return metastoreRefInConfig(c.Config)
}

// metastoreRefInConfig reads config.metastoreConfig.dataprocMetastoreService.
func metastoreRefInConfig(raw []byte) string {
	cfg := mustJSONMap(raw)
	mc, _ := cfg["metastoreConfig"].(map[string]any)
	s, _ := mc["dataprocMetastoreService"].(string)
	return strings.TrimSpace(s)
}

// metastoreRefInVirtualClusterConfig reads
// virtualClusterConfig.auxiliaryServicesConfig.metastoreConfig.dataprocMetastoreService.
func metastoreRefInVirtualClusterConfig(raw []byte) string {
	vcc := mustJSONMap(raw)
	aux, _ := vcc["auxiliaryServicesConfig"].(map[string]any)
	mc, _ := aux["metastoreConfig"].(map[string]any)
	s, _ := mc["dataprocMetastoreService"].(string)
	return strings.TrimSpace(s)
}

// validateClusterMetastore validates a cluster's metastore attachment, if any,
// against the wired resolver. A nil resolver is a no-op (metadata-only).
func (s *Service) validateClusterMetastore(ctx context.Context, project, region string, in ClusterInput) error {
	return s.validateMetastoreRef(ctx, project, region, metastoreServiceRefFromInput(in))
}

// validateMetastoreRef validates one metastore reference against the wired
// resolver (a nil resolver or empty reference is a no-op).
func (s *Service) validateMetastoreRef(ctx context.Context, project, region, ref string) error {
	if ref == "" || s.metastoreResolver == nil {
		return nil
	}
	return s.metastoreResolver.ValidateMetastoreService(ctx, ref, project, region)
}

// validateMetastoreConfigShape rejects a metastoreConfig that omits the
// required dataprocMetastoreService. It runs independently of the resolver so
// the shape is enforced even when no resolver is wired.
func validateMetastoreConfigShape(in ClusterInput) error {
	if err := checkMetastoreConfigShape(in.VirtualClusterConfig, true); err != nil {
		return err
	}
	return checkMetastoreConfigShape(in.Config, false)
}

// checkMetastoreConfigShape validates the metastoreConfig inside a config
// (inAux=false) or virtualClusterConfig.auxiliaryServicesConfig (inAux=true).
func checkMetastoreConfigShape(raw []byte, inAux bool) error {
	m := mustJSONMap(raw)
	if m == nil {
		return nil
	}
	if inAux {
		aux, _ := m["auxiliaryServicesConfig"].(map[string]any)
		if aux == nil {
			return nil
		}
		m = aux
	}
	v, present := m["metastoreConfig"]
	if !present || v == nil {
		return nil
	}
	mc, ok := v.(map[string]any)
	if !ok {
		return invalidArgument("metastoreConfig must be an object")
	}
	if ref, _ := mc["dataprocMetastoreService"].(string); strings.TrimSpace(ref) == "" {
		return invalidArgument("metastoreConfig requires dataprocMetastoreService")
	}
	return nil
}

// validateClusterMetastoreUpdate validates the metastore attachment of the
// effective cluster an update would produce: the incoming config is merged onto
// the stored cluster under the update mask (mirroring UpdateClusterAtomic) and
// the incoming virtualClusterConfig replaces the stored one only when the mask
// applies it. Validating the merged result avoids rejecting a reference the
// update will not actually apply.
func (s *Service) validateClusterMetastoreUpdate(ctx context.Context, project, region string, current dpstore.Cluster, in ClusterInput, mask []string) error {
	if s.metastoreResolver == nil {
		return nil
	}
	merged := current
	if in.Config != nil {
		stored := map[string]any{}
		if len(current.Config) > 0 {
			_ = json.Unmarshal(current.Config, &stored)
		}
		applyConfigMask(stored, mustJSONMap(in.Config), mask)
		if data, err := json.Marshal(stored); err == nil {
			merged.Config = data
		}
	}
	if in.VirtualClusterConfig != nil && (len(mask) == 0 || containsMaskField(mask, "virtualClusterConfig")) {
		merged.VirtualClusterConfig = in.VirtualClusterConfig
	}
	return s.validateMetastoreRef(ctx, project, region, metastoreServiceRefFromCluster(merged))
}

// clusterMetastoreEndpoint derives the thrift endpoint for a stored cluster's
// metastore attachment, honoring the deployment override. It returns "" when
// the cluster has no attachment or no resolver is wired. It does not re-check
// the control plane (see the MetastoreResolver contract).
func (s *Service) clusterMetastoreEndpoint(project, region string, c dpstore.Cluster) (string, error) {
	ref := metastoreServiceRefFromCluster(c)
	if ref == "" || s.metastoreResolver == nil {
		return "", nil
	}
	ep, err := s.metastoreResolver.MetastoreEndpoint(ref, project, region)
	if err != nil {
		return "", err
	}
	if s.hmsEndpointOverride != "" {
		return s.hmsEndpointOverride, nil
	}
	return ep, nil
}

// metastoreSparkConfs returns the spark-submit --conf tokens that attach a job
// to its cluster's Hive Metastore. The tokens are prepended before caller
// SparkSubmitArgs (ExtraSparkConfs), so a caller-supplied hive.metastore.uris /
// spark.sql.catalogImplementation wins via Spark's last-value-wins semantics.
// spark.hadoop.* confs propagate to executors, so the driver and executor pods
// share the attachment.
func metastoreSparkConfs(endpoint string) []string {
	if endpoint == "" {
		return nil
	}
	return []string{
		"--conf", "spark.hadoop.hive.metastore.uris=" + endpoint,
		"--conf", "spark.sql.catalogImplementation=hive",
	}
}
