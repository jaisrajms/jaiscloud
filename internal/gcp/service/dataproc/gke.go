package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/gcp/resource"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// GKENodePoolProvisioner resolves a Dataproc-on-GKE target cluster and reconciles
// the node pools a virtual cluster names. It is implemented by an adapter over
// the GKE core (internal/gcp/adapter/dataprocgke) and injected in main.go, so the
// Dataproc core never imports service/container — the same seam style as
// MetastoreResolver.
type GKENodePoolProvisioner interface {
	// ResolveGKETarget returns the target GKE cluster's reconcilable state. An
	// unknown cluster is NotFound (mapped by the adapter).
	ResolveGKETarget(ctx context.Context, project, location, cluster string) (GKECluster, error)
	// EnsureGKENodePool creates spec when the pool is absent and reports
	// created=true. When the pool exists and spec.Verify is set, it checks the
	// pool field-by-field and returns a *GKENodePoolMismatchError naming the
	// first difference; with Verify unset it adopts the existing pool unchanged.
	// It never deletes a pool, so pools outlive the virtual cluster (real GCP).
	EnsureGKENodePool(ctx context.Context, cluster GKECluster, spec GKENodePoolSpec) (created bool, err error)
}

// GKECluster is the reconcilable state of a target GKE cluster.
type GKECluster struct {
	Project  string
	Location string
	Name     string
	// Autopilot reports an Autopilot cluster. Dataproc on GKE requires a
	// standard cluster, so a create against an Autopilot cluster is rejected.
	Autopilot bool
	// WorkloadIdentityPool is the cluster's
	// workloadIdentityConfig.workloadPool; empty means Workload Identity is
	// disabled, which Dataproc also requires.
	WorkloadIdentityPool string
}

// GKENodePoolSpec is the pool shape the Dataproc core asks the provisioner for.
// It carries only what GkeNodePoolConfig expresses: the input-only
// accelerators/bootDiskKmsKey/gpuPartitionSize fields are accepted on the wire
// and never reflected (DPG25).
type GKENodePoolSpec struct {
	Name           string
	MachineType    string
	LocalSsdCount  int32
	Preemptible    bool
	Spot           bool
	MinCpuPlatform string
	ServiceAccount string
	Autoscaling    *GKENodePoolAutoscaling
	Locations      []string
	// Labels are the auto labels Dataproc applies to a pool it creates
	// (goog-dataproc-cluster-name/-cluster-uuid/-location/-environment).
	Labels map[string]string
	// Verify asks the provisioner to check an existing pool against this shape.
	// It is false when the caller sent no nodePoolConfig: the documented
	// behaviour is then "any node pool with the specified name is used".
	Verify bool
	// CreateDefaults fill the fields a *service-created* pool leaves empty
	// (the documented machine type and the 1/10 autoscaler defaults). They are
	// never compared against an existing pool: the defaults apply only to pools
	// the service creates.
	CreateDefaults GKENodePoolDefaults
}

// GKENodePoolDefaults are the values a service-created pool gets for fields the
// caller's nodePoolConfig left empty.
type GKENodePoolDefaults struct {
	MachineType string
	Autoscaling *GKENodePoolAutoscaling
}

// GKENodePoolAutoscaling is GkeNodePoolAutoscalingConfig.
type GKENodePoolAutoscaling struct {
	MinNodeCount int32
	MaxNodeCount int32
}

// GKENodePoolMismatchError reports that an existing pool disagrees with the
// requested nodePoolConfig. Field is the wire name that differed.
type GKENodePoolMismatchError struct {
	Pool  string
	Field string
	Want  string
	Got   string
}

func (e *GKENodePoolMismatchError) Error() string {
	return fmt.Sprintf("node pool %s: %s differs (want %q, got %q)", e.Pool, e.Field, e.Want, e.Got)
}

// WithGKENodePoolProvisioner wires the Dataproc-on-GKE reconciler. Nil leaves a
// virtual cluster's gkeClusterTarget/nodePoolTarget stored and echoed but
// unreconciled (unit tests, and deployments without the GKE service).
func WithGKENodePoolProvisioner(p GKENodePoolProvisioner) Option {
	return func(s *Service) { s.gkePools = p }
}

// gkeRoles is the GkeNodePoolTarget.Role enum (roles are wire names, not
// numbers, in both transports). ROLE_UNSPECIFIED is the enum default and is
// never a legal role on a target.
var gkeRoles = map[string]bool{
	"DEFAULT":        true,
	"CONTROLLER":     true,
	"SPARK_DRIVER":   true,
	"SPARK_EXECUTOR": true,
}

// defaultGKEPoolName is the pool the emulator synthesises when the caller names
// none (Dataproc constructs a DEFAULT target in that case). Real GCP picks an
// internal name; default-pool is the deterministic choice here, and it adopts
// the GKE mock's own default pool when the target cluster has one.
const defaultGKEPoolName = "default-pool"

// defaultGKENodePoolMachineType is the machine type a pool the service creates
// gets when nodePoolConfig omits one.
const defaultGKENodePoolMachineType = "e2-medium"

// gkeVirtualCluster is the parsed, validated subset of a
// dataproc.v1.VirtualClusterConfig that the Dataproc-on-GKE path acts on.
type gkeVirtualCluster struct {
	// target is the short cluster id of gkeClusterConfig.gkeClusterTarget.
	target string
	// targetProject/targetLocation are the project and location segments of the
	// canonical target, checked against the virtual cluster's own.
	targetProject  string
	targetLocation string
	// namespace is the effective kubernetesNamespace: the caller's value, or the
	// cluster name when it was not set (the API's documented default).
	namespace string
	// software is the parsed KubernetesSoftwareConfig (nil when the caller sent
	// none), with every component version already resolved.
	software    *gkeSoftwareConfig
	poolTargets []gkePoolTarget
}

// gkePoolTarget is one GkeNodePoolTarget after name normalisation.
type gkePoolTarget struct {
	name  string   // short node pool id
	roles []string // validated enum values
	// project/location/cluster are the pool's own resource-name segments. They
	// equal the target cluster's when gkeClusterTarget was given, and are the
	// only statement of the target when it was not (the pool names carry it).
	project  string
	location string
	cluster  string
	// config is nil when the caller sent no nodePoolConfig: the pool is adopted
	// as-is (or created with defaults) and not verified.
	config *gkeNodePoolConfig
}

// gkeNodePoolConfig is the parsed GkeNodePoolConfig.
type gkeNodePoolConfig struct {
	machineType    string
	localSsdCount  int32
	preemptible    bool
	spot           bool
	minCpuPlatform string
	serviceAccount string
	autoscaling    *GKENodePoolAutoscaling
	locations      []string
}

// validateVirtualClusterConfig validates the required shape of a caller-supplied
// dataproc.v1.VirtualClusterConfig and returns the parsed GKE placement (nil for
// a cluster with no virtualClusterConfig). It checks the API's structure, the
// effective Kubernetes namespace, the software config and the staging-bucket
// form, plus the GkeNodePoolTarget rules that need no store lookup: pool roles,
// the one-DEFAULT/one-pool-per-role constraints, the single-zone rule, the
// Spot/preemptible restrictions and the autoscaler bounds. Unknown sub-fields are
// accepted and preserved verbatim. clusterName supplies the namespace default.
func validateVirtualClusterConfig(raw json.RawMessage, clusterName string) (*gkeVirtualCluster, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var vcc map[string]any
	if err := json.Unmarshal(raw, &vcc); err != nil {
		return nil, invalidArgument("virtualClusterConfig must be an object")
	}
	kcc, ok := vcc["kubernetesClusterConfig"].(map[string]any)
	if !ok {
		return nil, invalidArgument("virtualClusterConfig requires kubernetesClusterConfig")
	}
	gke, ok := kcc["gkeClusterConfig"].(map[string]any)
	if !ok {
		return nil, invalidArgument("kubernetesClusterConfig requires gkeClusterConfig")
	}
	target, _ := gke["gkeClusterTarget"].(string)
	pools, _ := gke["nodePoolTarget"].([]any)
	if target == "" && len(pools) == 0 {
		return nil, invalidArgument("gkeClusterConfig requires gkeClusterTarget or nodePoolTarget")
	}
	out := &gkeVirtualCluster{namespace: bodyString(kcc, "kubernetesNamespace")}
	if out.namespace == "" {
		// "If not specified, the name of the Dataproc Cluster is used."
		out.namespace = clusterName
	}
	if software, ok := kcc["kubernetesSoftwareConfig"].(map[string]any); ok {
		parsed, err := parseKubernetesSoftwareConfig(software)
		if err != nil {
			return nil, err
		}
		out.software = parsed
	}
	if err := validateStagingBucketForm(bodyString(vcc, "stagingBucket")); err != nil {
		return nil, err
	}
	if target != "" {
		project, location, id, ok := splitGKEClusterName(target)
		if !ok {
			return nil, invalidArgument("gkeClusterTarget must be projects/{project}/locations/{location}/clusters/{cluster}: " + target)
		}
		out.target = id
		out.targetProject = project
		out.targetLocation = location
	}
	for i, rawTarget := range pools {
		m, ok := rawTarget.(map[string]any)
		if !ok {
			return nil, invalidArgument(fmt.Sprintf("nodePoolTarget[%d] must be an object", i))
		}
		pt, err := parseGKEPoolTarget(m, target, i)
		if err != nil {
			return nil, err
		}
		out.poolTargets = append(out.poolTargets, pt)
	}
	if len(out.poolTargets) == 0 {
		// "If a GkeNodePoolTarget is not specified, Dataproc constructs a
		// DEFAULT GkeNodePoolTarget."
		out.poolTargets = []gkePoolTarget{{name: defaultGKEPoolName, roles: []string{"DEFAULT"}}}
	}
	// A virtual cluster that names pools but no gkeClusterTarget still states
	// its target through the pool resource names, so use them rather than
	// accepting the placement and then silently reconciling nothing. All pools
	// must name one cluster, or there is nothing to resolve.
	if target == "" {
		out.target = out.poolTargets[0].cluster
		out.targetProject = out.poolTargets[0].project
		out.targetLocation = out.poolTargets[0].location
		for _, p := range out.poolTargets[1:] {
			if p.project != out.targetProject || p.location != out.targetLocation || p.cluster != out.target {
				return nil, invalidArgument(fmt.Sprintf(
					"nodePoolTarget entries must all name pools in one GKE cluster: %s and %s differ",
					poolResourceName(out.poolTargets[0]), poolResourceName(p)))
			}
		}
	}
	if err := validateGKEPoolTargets(out.poolTargets); err != nil {
		return nil, err
	}
	return out, nil
}

// poolResourceName renders a pool target's canonical resource name (for errors).
func poolResourceName(p gkePoolTarget) string {
	return resource.ResourceID(p.project)("container-node-pool", p.location+"/"+p.cluster+"/"+p.name)
}

// gkeNodePoolNamePattern is the pattern real GCP quotes when
// GkeNodePoolTarget.nodePool is not a full node-pool resource name. The
// surrounding message is verbatim from a real-GCP capture
// (docs/gcp/dataproc-gke-version-aliases.md, DPG44).
const gkeNodePoolNamePattern = "projects/([^/]+)/(?:locations|zones)/([^/]+)/clusters/([^/]+)/nodePools/([^/]+)"

// gkeNodePoolNameErr is the InvalidArgument real GCP returns for a nodePool that
// is not a full resource name.
func gkeNodePoolNameErr(raw string) error {
	return invalidArgument(fmt.Sprintf("GKE Node Pool name '%s', must conform to pattern '%s'", raw, gkeNodePoolNamePattern))
}

// parseGKEPoolTarget normalises one GkeNodePoolTarget and attaches its parsed
// nodePoolConfig (nil when the caller sent none).
func parseGKEPoolTarget(m map[string]any, target string, i int) (gkePoolTarget, error) {
	var pt gkePoolTarget
	rawName, _ := m["nodePool"].(string)
	if rawName == "" {
		return pt, invalidArgument(fmt.Sprintf("nodePoolTarget[%d] requires nodePool", i))
	}
	// Discovery requires the full resource name ("Format:
	// 'projects/{project}/locations/{location}/clusters/{cluster}/nodePools/{node_pool}'")
	// and real GCP rejects a bare pool id, so a short name is InvalidArgument
	// (DPG44).
	project, location, cluster, name, ok := splitGKENodePoolName(rawName)
	if !ok {
		return pt, gkeNodePoolNameErr(rawName)
	}
	// A pool outside the target cluster cannot be provisioned or verified.
	if target != "" {
		tp, tl, tc, _ := splitGKEClusterName(target)
		if project != tp || location != tl || cluster != tc {
			return pt, invalidArgument(fmt.Sprintf("nodePool %s is not in gkeClusterTarget %s", rawName, target))
		}
	}
	pt.name = name
	pt.project, pt.location, pt.cluster = project, location, cluster
	rawRoles, _ := m["roles"].([]any)
	if len(rawRoles) == 0 {
		return pt, invalidArgument(fmt.Sprintf("nodePoolTarget %s requires roles", name))
	}
	for _, r := range rawRoles {
		role, _ := r.(string)
		if !gkeRoles[role] {
			return pt, invalidArgument(fmt.Sprintf("nodePoolTarget %s has an invalid role %q", name, role))
		}
		pt.roles = append(pt.roles, role)
	}
	if cfg, ok := m["nodePoolConfig"].(map[string]any); ok {
		parsed, err := parseGKENodePoolConfig(cfg, name)
		if err != nil {
			return pt, err
		}
		pt.config = parsed
	}
	return pt, nil
}

// parseGKENodePoolConfig parses GkeNodePoolConfig and enforces the scalar rules
// (autoscaler bounds). The accelerators/bootDiskKmsKey/gpuPartitionSize fields
// are accepted and dropped: the emulated pool has no GPU or CMEK model (DPG25).
func parseGKENodePoolConfig(m map[string]any, pool string) (*gkeNodePoolConfig, error) {
	cfg := &gkeNodePoolConfig{locations: bodyStringSlice(m, "locations")}
	if inner, ok := m["config"].(map[string]any); ok {
		cfg.machineType = bodyString(inner, "machineType")
		cfg.localSsdCount = int32(bodyInt(inner, "localSsdCount"))
		cfg.preemptible = bodyBool(inner, "preemptible")
		cfg.spot = bodyBool(inner, "spot")
		cfg.minCpuPlatform = bodyString(inner, "minCpuPlatform")
		cfg.serviceAccount = bodyString(inner, "serviceAccount")
	}
	if as, ok := m["autoscaling"].(map[string]any); ok {
		a := &GKENodePoolAutoscaling{
			MinNodeCount: int32(bodyInt(as, "minNodeCount")),
			MaxNodeCount: int32(bodyInt(as, "maxNodeCount")),
		}
		// The API documents max > 0 and 0 <= min <= max.
		if a.MaxNodeCount <= 0 {
			return nil, invalidArgument(fmt.Sprintf("nodePoolTarget %s: autoscaling.maxNodeCount must be > 0", pool))
		}
		if a.MinNodeCount < 0 || a.MinNodeCount > a.MaxNodeCount {
			return nil, invalidArgument(fmt.Sprintf("nodePoolTarget %s: autoscaling must satisfy 0 <= minNodeCount <= maxNodeCount", pool))
		}
		cfg.autoscaling = a
	}
	return cfg, nil
}

// validateGKEPoolTargets enforces the cross-pool role rules: at least one
// DEFAULT, each role on at most one pool, and no Spot/preemptible control plane.
func validateGKEPoolTargets(pools []gkePoolTarget) error {
	roleOwner := map[string]string{}
	names := map[string]bool{}
	hasDefault := false
	hasController := false
	for _, p := range pools {
		if names[p.name] {
			return invalidArgument("duplicate nodePoolTarget " + p.name)
		}
		names[p.name] = true
		for _, role := range p.roles {
			if owner, ok := roleOwner[role]; ok {
				return invalidArgument(fmt.Sprintf("role %s is assigned to both %s and %s: each role can be given to only one nodePoolTarget", role, owner, p.name))
			}
			roleOwner[role] = p.name
			switch role {
			case "DEFAULT":
				hasDefault = true
			case "CONTROLLER":
				hasController = true
			}
		}
	}
	if !hasDefault {
		return invalidArgument("at least one nodePoolTarget must have the DEFAULT role")
	}
	for _, p := range pools {
		if p.config == nil || (!p.config.spot && !p.config.preemptible) {
			continue
		}
		// Spot/preemptible nodes cannot run the control plane: never on a
		// CONTROLLER pool, and not on DEFAULT when it assumes CONTROLLER because
		// no CONTROLLER pool exists.
		if roleOf(p.roles, "CONTROLLER") || (roleOf(p.roles, "DEFAULT") && !hasController) {
			return invalidArgument(fmt.Sprintf("nodePoolTarget %s: spot/preemptible nodes cannot be used in a CONTROLLER node pool or in the DEFAULT node pool when no CONTROLLER role is assigned", p.name))
		}
	}
	return nil
}

// validateGKEPlacement enforces the rules that depend on the virtual cluster's
// own project and region: the target must live in that project and region, and
// every specified pool zone must be the same zone inside the region.
func validateGKEPlacement(v *gkeVirtualCluster, project, region string) error {
	if v == nil || v.target == "" {
		return nil
	}
	if v.targetProject != project {
		return invalidArgument(fmt.Sprintf("gkeClusterTarget %s is in project %s, but the Dataproc cluster is in %s: they must be in the same project", v.target, v.targetProject, project))
	}
	// The GKE cluster may be regional or zonal, but must be in the Dataproc
	// cluster's region.
	if v.targetLocation != region && !zoneInRegion(v.targetLocation, region) {
		return invalidArgument(fmt.Sprintf("gkeClusterTarget %s is in %s, but the Dataproc cluster is in %s: they must be in the same region", v.target, v.targetLocation, region))
	}
	var zone string
	for _, p := range v.poolTargets {
		if p.config == nil {
			continue
		}
		for _, z := range p.config.locations {
			if !zoneInRegion(z, region) {
				return invalidArgument(fmt.Sprintf("nodePoolTarget %s: location %s is not a zone in region %s", p.name, z, region))
			}
			if zone == "" {
				zone = z
				continue
			}
			if z != zone {
				return invalidArgument(fmt.Sprintf("nodePoolTarget %s: all node pools must be located in the same zone, got %s and %s", p.name, zone, z))
			}
		}
	}
	return nil
}

// zoneInRegion reports whether zone is a zone of region (us-central1-a →
// us-central1).
func zoneInRegion(zone, region string) bool {
	if !strings.HasPrefix(zone, region+"-") {
		return false
	}
	suffix := strings.TrimPrefix(zone, region+"-")
	return suffix != "" && !strings.Contains(suffix, "-")
}

// splitGKEClusterName parses a canonical GKE cluster name. The location segment
// is "locations" today for both regional and zonal clusters; "zones" is the older
// spelling the real-GCP node-pool pattern still accepts, and accepting it is what
// lets a zonal target and its pools agree on one spelling.
func splitGKEClusterName(name string) (project, location, cluster string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 || segs[0] != "projects" || (segs[2] != "locations" && segs[2] != "zones") || segs[4] != "clusters" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// splitGKENodePoolName parses a canonical GKE node pool name. The location
// segment is "locations" for the clusters the emulator models; "zones" is the
// older spelling the real-GCP pattern still accepts.
func splitGKENodePoolName(name string) (project, location, cluster, pool string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 8 || segs[0] != "projects" || (segs[2] != "locations" && segs[2] != "zones") ||
		segs[4] != "clusters" || segs[6] != "nodePools" {
		return "", "", "", "", false
	}
	return segs[1], segs[3], segs[5], segs[7], true
}

// roleOf reports whether roles contains role.
func roleOf(roles []string, role string) bool {
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}

// gkeReconcileFailure marks a provisioning/verification failure (as opposed to a
// rejected request): the create operation fails and the cluster is persisted in
// ERROR. A resolution problem (unknown target, Autopilot, no Workload Identity)
// is returned as an ordinary error, because nothing was created.
type gkeReconcileFailure struct{ err error }

func (e *gkeReconcileFailure) Error() string { return e.err.Error() }
func (e *gkeReconcileFailure) Unwrap() error { return e.err }

// reconcileGKEVirtualCluster resolves the target GKE cluster and reconciles the
// pools through the provisioner. A nil provisioner (no GKE service, unit tests)
// leaves the placement metadata-only. A *gkeReconcileFailure means the request
// was accepted but provisioning failed.
func (s *Service) reconcileGKEVirtualCluster(ctx context.Context, project, region, cluster, clusterUUID string, v *gkeVirtualCluster, labels map[string]string) error {
	if v == nil || v.target == "" || s.gkePools == nil {
		return nil
	}
	gke, err := s.gkePools.ResolveGKETarget(ctx, project, region, v.target)
	if err != nil {
		return err
	}
	if gke.Autopilot {
		return invalidArgument("gkeClusterTarget " + v.target + " is an Autopilot cluster: Dataproc on GKE requires a standard cluster")
	}
	if gke.WorkloadIdentityPool == "" {
		return invalidArgument("gkeClusterTarget " + v.target + " does not have Workload Identity enabled, which Dataproc on GKE requires")
	}
	for _, p := range v.poolTargets {
		spec := gkeNodePoolSpec(p, cluster, clusterUUID, region, labels)
		if _, err := s.gkePools.EnsureGKENodePool(ctx, gke, spec); err != nil {
			return &gkeReconcileFailure{err: err}
		}
	}
	return nil
}

// gkeNodePoolSpec builds the provisioner request for one pool target. Pools the
// service creates get the documented autoscaler defaults (min 1, max 10) and a
// default machine type; a pool the caller shaped is verified field-by-field.
func gkeNodePoolSpec(p gkePoolTarget, cluster, clusterUUID, region string, labels map[string]string) GKENodePoolSpec {
	spec := GKENodePoolSpec{
		Name:   p.name,
		Labels: gkePoolLabels(cluster, clusterUUID, region, labels),
		CreateDefaults: GKENodePoolDefaults{
			MachineType: defaultGKENodePoolMachineType,
			Autoscaling: &GKENodePoolAutoscaling{MinNodeCount: 1, MaxNodeCount: 10},
		},
	}
	if p.config == nil {
		return spec
	}
	spec.Verify = true
	spec.MachineType = p.config.machineType
	spec.LocalSsdCount = p.config.localSsdCount
	spec.Preemptible = p.config.preemptible
	spec.Spot = p.config.spot
	spec.MinCpuPlatform = p.config.minCpuPlatform
	spec.ServiceAccount = p.config.serviceAccount
	spec.Autoscaling = p.config.autoscaling
	spec.Locations = p.config.locations
	return spec
}

// gkePoolLabels are the Dataproc auto labels a reconciled pool carries.
func gkePoolLabels(cluster, clusterUUID, region string, labels map[string]string) map[string]string {
	out := map[string]string{
		"goog-dataproc-cluster-name": cluster,
		"goog-dataproc-cluster-uuid": clusterUUID,
		"goog-dataproc-location":     region,
	}
	env := "default"
	if v := labels["environment"]; v != "" {
		env = v
	}
	out["goog-dataproc-environment"] = env
	return out
}

// gkeReconcileDetail formats a reconciliation failure as the cluster's
// status.detail, naming the pool and field when the provisioner reported one.
func gkeReconcileDetail(err error) string {
	var mismatch *GKENodePoolMismatchError
	if errors.As(err, &mismatch) {
		return fmt.Sprintf("node pool %s reconciliation failed: %s differs (want %q, got %q)", mismatch.Pool, mismatch.Field, mismatch.Want, mismatch.Got)
	}
	return "GKE node pool reconciliation failed: " + err.Error()
}

// gkeErrorState is the status a failed reconciliation persists: real Dataproc
// fails the create operation and leaves the cluster in ERROR.
func gkeErrorState(err error, now time.Time) dpstore.ClusterStatus {
	return dpstore.ClusterStatus{State: "ERROR", Detail: gkeReconcileDetail(err), StateStartTime: now}
}
