package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// ─── test fixtures ───────────────────────────────────────────────────────────

// gkeClusterPath is the canonical target used across these tests.
const gkeClusterPath = "projects/proj/locations/us-central1/clusters/gke-1"

// vccWith builds a virtualClusterConfig around one gkeClusterConfig.
func vccWith(gke map[string]any) map[string]any {
	return map[string]any{
		"kubernetesClusterConfig": map[string]any{
			"kubernetesNamespace": "dataproc",
			"gkeClusterConfig":    gke,
		},
	}
}

// poolTarget builds one GkeNodePoolTarget. A bare pool id is expanded to the
// full resource name Discovery requires; an already-qualified name is used
// as-is so the name-shape cases can stay explicit.
func poolTarget(name string, roles []string, config map[string]any) map[string]any {
	if !strings.Contains(name, "/") {
		name = gkeClusterPath + "/nodePools/" + name
	}
	t := map[string]any{"nodePool": name, "roles": roles}
	if config != nil {
		t["nodePoolConfig"] = config
	}
	return t
}

// fakeGKENodePoolProvisioner records the reconciliation it was asked for.
type fakeGKENodePoolProvisioner struct {
	gke        GKECluster
	resolveErr error
	ensureErr  error
	specs      []GKENodePoolSpec
	existing   map[string]bool // pool name → adopt instead of create
}

func (f *fakeGKENodePoolProvisioner) ResolveGKETarget(_ context.Context, project, location, cluster string) (GKECluster, error) {
	if f.resolveErr != nil {
		return GKECluster{}, f.resolveErr
	}
	out := f.gke
	out.Project, out.Location, out.Name = project, location, cluster
	return out, nil
}

func (f *fakeGKENodePoolProvisioner) EnsureGKENodePool(_ context.Context, _ GKECluster, spec GKENodePoolSpec) (bool, error) {
	f.specs = append(f.specs, spec)
	if f.ensureErr != nil {
		return false, f.ensureErr
	}
	return true, nil
}

// ─── pure validation matrix ──────────────────────────────────────────────────

func TestValidateVirtualClusterConfig_PoolRules(t *testing.T) {
	cases := []struct {
		name   string
		gke    map[string]any
		region string
		want   string // substring of the error, "" = accepted
	}{
		{
			name: "single DEFAULT",
			gke:  map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{poolTarget("default-pool", []string{"DEFAULT"}, nil)}},
		},
		{
			name: "no nodePoolTarget synthesises a DEFAULT target",
			gke:  map[string]any{"gkeClusterTarget": gkeClusterPath},
		},
		{
			name: "all roles",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("ctrl", []string{"CONTROLLER"}, nil),
				poolTarget("drv", []string{"SPARK_DRIVER"}, nil),
				poolTarget("exec", []string{"SPARK_EXECUTOR"}, nil),
				poolTarget("def", []string{"DEFAULT"}, nil),
			}},
		},
		{
			name: "roles required",
			gke:  map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{poolTarget("p", nil, nil)}},
			want: "requires roles",
		},
		{
			name: "unknown role",
			gke:  map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{poolTarget("p", []string{"SPARK_MASTER"}, nil)}},
			want: "invalid role",
		},
		{
			name: "ROLE_UNSPECIFIED is not a role",
			gke:  map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{poolTarget("p", []string{"ROLE_UNSPECIFIED"}, nil)}},
			want: "invalid role",
		},
		{
			name: "missing DEFAULT",
			gke:  map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{poolTarget("p", []string{"SPARK_EXECUTOR"}, nil)}},
			want: "DEFAULT role",
		},
		{
			name: "role on two pools",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("a", []string{"DEFAULT"}, nil),
				poolTarget("b", []string{"DEFAULT"}, nil),
			}},
			want: "only one nodePoolTarget",
		},
		{
			name: "duplicate pool",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("a", []string{"DEFAULT"}, nil),
				poolTarget("a", []string{"SPARK_DRIVER"}, nil),
			}},
			want: "duplicate nodePoolTarget",
		},
		{
			name: "spot on CONTROLLER",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("ctrl", []string{"CONTROLLER"}, map[string]any{"config": map[string]any{"spot": true}}),
				poolTarget("def", []string{"DEFAULT"}, nil),
			}},
			want: "spot/preemptible",
		},
		{
			name: "preemptible on DEFAULT with no CONTROLLER",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("def", []string{"DEFAULT"}, map[string]any{"config": map[string]any{"preemptible": true}}),
			}},
			want: "spot/preemptible",
		},
		{
			name: "spot on DEFAULT is allowed when a CONTROLLER pool exists",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("ctrl", []string{"CONTROLLER"}, nil),
				poolTarget("def", []string{"DEFAULT"}, map[string]any{"config": map[string]any{"spot": true}}),
			}},
		},
		{
			name: "spot on a SPARK_EXECUTOR pool",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("def", []string{"DEFAULT"}, nil),
				poolTarget("exec", []string{"SPARK_EXECUTOR"}, map[string]any{"config": map[string]any{"spot": true}}),
			}},
		},
		{
			name: "autoscaling max must be positive",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("p", []string{"DEFAULT"}, map[string]any{"autoscaling": map[string]any{"minNodeCount": 0, "maxNodeCount": 0}}),
			}},
			want: "maxNodeCount must be > 0",
		},
		{
			name: "autoscaling min above max",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("p", []string{"DEFAULT"}, map[string]any{"autoscaling": map[string]any{"minNodeCount": 5, "maxNodeCount": 2}}),
			}},
			want: "0 <= minNodeCount <= maxNodeCount",
		},
		{
			name: "autoscaling bounds accepted",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("p", []string{"DEFAULT"}, map[string]any{"autoscaling": map[string]any{"minNodeCount": 0, "maxNodeCount": 3}}),
			}},
		},
		{
			name: "bare pool name is rejected",
			gke:  map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{map[string]any{"nodePool": "bare-pool", "roles": []any{"DEFAULT"}}}},
			want: "GKE Node Pool name 'bare-pool', must conform to pattern",
		},
		{
			name: "canonical pool name is accepted",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget(gkeClusterPath+"/nodePools/canonical-pool", []string{"DEFAULT"}, nil),
			}},
		},
		{
			// The zonal spelling names a different cluster from a regional
			// target, so it is still a containment error (DPG50).
			name: "zonal pool name for another location is rejected",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("projects/proj/zones/us-central1-a/clusters/gke-1/nodePools/zonal-pool", []string{"DEFAULT"}, nil),
			}},
			want: "is not in gkeClusterTarget",
		},
		{
			name: "zonal target and zonal pool name are accepted",
			gke: map[string]any{
				"gkeClusterTarget": "projects/proj/zones/us-central1-a/clusters/gke-1",
				"nodePoolTarget": []any{
					poolTarget("projects/proj/zones/us-central1-a/clusters/gke-1/nodePools/zonal-pool", []string{"DEFAULT"}, nil),
				},
			},
		},

		{
			// Pools without gkeClusterTarget state the target themselves; the
			// reconciliation and placement rules then apply instead of silently
			// doing nothing (DPG52).
			name: "pools without a target derive it",
			gke: map[string]any{"nodePoolTarget": []any{
				poolTarget("projects/proj/locations/us-central1/clusters/gke-9/nodePools/a", []string{"DEFAULT"}, nil),
			}},
		},
		{
			name: "pools in two clusters without a target",
			gke: map[string]any{"nodePoolTarget": []any{
				poolTarget("projects/proj/locations/us-central1/clusters/gke-9/nodePools/a", []string{"DEFAULT"}, nil),
				poolTarget("projects/proj/locations/us-central1/clusters/gke-8/nodePools/b", []string{"SPARK_DRIVER"}, nil),
			}},
			want: "must all name pools in one GKE cluster",
		},
		{
			name: "pool outside the target cluster",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("projects/proj/locations/us-central1/clusters/other/nodePools/p", []string{"DEFAULT"}, nil),
			}},
			want: "is not in gkeClusterTarget",
		},
		{
			name: "malformed pool path",
			gke:  map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{poolTarget("projects/proj/nodePools/p", []string{"DEFAULT"}, nil)}},
			want: "must conform to pattern",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vcc := vccWith(tc.gke)
			raw, err := json.Marshal(vcc)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			v, err := validateVirtualClusterConfig(raw, "demo")
			if err != nil {
				if tc.want == "" {
					t.Fatalf("unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error %q does not mention %q", err, tc.want)
				}
				return
			}
			if tc.want != "" {
				t.Fatalf("expected an error mentioning %q, got none", tc.want)
			}
			if len(v.poolTargets) == 0 {
				t.Fatal("no pool targets parsed")
			}
			if err := validateGKEPlacement(v, "proj", "us-central1"); err != nil {
				t.Fatalf("placement: %v", err)
			}
		})
	}
}

func TestValidateVirtualClusterConfig_DerivesTargetFromPools(t *testing.T) {
	raw, err := json.Marshal(vccWith(map[string]any{
		"nodePoolTarget": []any{
			poolTarget("projects/proj/locations/us-central1/clusters/gke-9/nodePools/a", []string{"DEFAULT"}, nil),
		},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	v, err := validateVirtualClusterConfig(raw, "demo")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if v.target != "gke-9" || v.targetProject != "proj" || v.targetLocation != "us-central1" {
		t.Fatalf("target not derived from the pool name: %+v", v)
	}
	// The derived target is checkable: a virtual cluster in another project is
	// rejected even though it named no gkeClusterTarget.
	if err := validateGKEPlacement(v, "other", "us-central1"); err == nil {
		t.Fatal("a derived target in another project must be rejected")
	}
	if err := validateGKEPlacement(v, "proj", "us-central1"); err != nil {
		t.Fatalf("placement: %v", err)
	}
}

func TestValidateGKEPlacement(t *testing.T) {
	cases := []struct {
		name    string
		gke     map[string]any
		project string
		region  string
		want    string
	}{
		{
			name:    "same project and region",
			gke:     map[string]any{"gkeClusterTarget": gkeClusterPath},
			project: "proj", region: "us-central1",
		},
		{
			name:    "zonal GKE cluster inside the region",
			gke:     map[string]any{"gkeClusterTarget": "projects/proj/locations/us-central1-a/clusters/gke-1"},
			project: "proj", region: "us-central1",
		},
		{
			name:    "different project",
			gke:     map[string]any{"gkeClusterTarget": gkeClusterPath},
			project: "other", region: "us-central1",
			want: "same project",
		},
		{
			name:    "different region",
			gke:     map[string]any{"gkeClusterTarget": gkeClusterPath},
			project: "proj", region: "europe-west1",
			want: "same region",
		},
		{
			name: "pool zone outside the region",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("p", []string{"DEFAULT"}, map[string]any{"locations": []any{"europe-west1-b"}}),
			}},
			project: "proj", region: "us-central1",
			want: "not a zone in region",
		},
		{
			name: "two pools in different zones",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("a", []string{"DEFAULT"}, map[string]any{"locations": []any{"us-central1-a"}}),
				poolTarget("b", []string{"SPARK_EXECUTOR"}, map[string]any{"locations": []any{"us-central1-b"}}),
			}},
			project: "proj", region: "us-central1",
			want: "same zone",
		},
		{
			name: "two pools in the same zone",
			gke: map[string]any{"gkeClusterTarget": gkeClusterPath, "nodePoolTarget": []any{
				poolTarget("a", []string{"DEFAULT"}, map[string]any{"locations": []any{"us-central1-a"}}),
				poolTarget("b", []string{"SPARK_EXECUTOR"}, map[string]any{"locations": []any{"us-central1-a"}}),
			}},
			project: "proj", region: "us-central1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(vccWith(tc.gke))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			v, err := validateVirtualClusterConfig(raw, "demo")
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			err = validateGKEPlacement(v, tc.project, tc.region)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v does not mention %q", err, tc.want)
			}
		})
	}
}

// ─── reconciliation through the seam ────────────────────────────────────────

func TestCreateCluster_ReconcilesNodePools(t *testing.T) {
	stub := &fakeGKENodePoolProvisioner{gke: GKECluster{WorkloadIdentityPool: "proj.svc.id.goog"}}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithGKENodePoolProvisioner(stub))
	ctx := context.Background()
	raw, err := json.Marshal(vccWith(map[string]any{
		"gkeClusterTarget": gkeClusterPath,
		"nodePoolTarget": []any{
			poolTarget("default-pool", []string{"DEFAULT"}, map[string]any{
				"config":      map[string]any{"machineType": "n1-standard-4", "localSsdCount": 2},
				"autoscaling": map[string]any{"minNodeCount": 1, "maxNodeCount": 5},
				"locations":   []any{"us-central1-a"},
			}),
			// No nodePoolConfig: adopted, not verified.
			poolTarget("spark-pool", []string{"SPARK_DRIVER", "SPARK_EXECUTOR"}, nil),
		},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "dp-gke", ClusterInput{
		Labels:               map[string]string{"environment": "prod"},
		VirtualClusterConfig: raw,
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if len(stub.specs) != 2 {
		t.Fatalf("expected 2 pools reconciled, got %d", len(stub.specs))
	}
	shaped := stub.specs[0]
	if shaped.Name != "default-pool" || !shaped.Verify {
		t.Fatalf("shaped pool spec wrong: %+v", shaped)
	}
	if shaped.MachineType != "n1-standard-4" || shaped.LocalSsdCount != 2 {
		t.Fatalf("node config not carried: %+v", shaped)
	}
	if shaped.Autoscaling == nil || shaped.Autoscaling.MinNodeCount != 1 || shaped.Autoscaling.MaxNodeCount != 5 {
		t.Fatalf("autoscaling not carried: %+v", shaped.Autoscaling)
	}
	if len(shaped.Locations) != 1 || shaped.Locations[0] != "us-central1-a" {
		t.Fatalf("locations not carried: %v", shaped.Locations)
	}
	if shaped.Labels["goog-dataproc-cluster-name"] != "dp-gke" ||
		shaped.Labels["goog-dataproc-cluster-uuid"] != c.ClusterUUID ||
		shaped.Labels["goog-dataproc-location"] != "us-central1" ||
		shaped.Labels["goog-dataproc-environment"] != "prod" {
		t.Fatalf("auto labels wrong: %v", shaped.Labels)
	}
	adopted := stub.specs[1]
	if adopted.Verify {
		t.Fatal("a pool with no nodePoolConfig must be adopted, not verified")
	}
	if adopted.CreateDefaults.Autoscaling == nil || adopted.CreateDefaults.Autoscaling.MinNodeCount != 1 || adopted.CreateDefaults.Autoscaling.MaxNodeCount != 10 {
		t.Fatalf("a service-created pool must default the autoscaler to 1/10: %+v", adopted.CreateDefaults.Autoscaling)
	}
	if adopted.CreateDefaults.MachineType == "" {
		t.Fatal("a service-created pool must default the machine type")
	}
	if c.Status.State != "CREATING" {
		t.Fatalf("cluster should still be creating, got %s", c.Status.State)
	}
}

// TestCreateCluster_DuplicateNameHasNoSideEffects pins DPG51: real Dataproc
// rejects a duplicate cluster name before provisioning anything, so a rejected
// create must not create or verify node pools through the GKE API. Pools are
// never deleted, so anything reconciled here would be orphaned.
func TestCreateCluster_DuplicateNameHasNoSideEffects(t *testing.T) {
	stub := &fakeGKENodePoolProvisioner{gke: GKECluster{WorkloadIdentityPool: "proj.svc.id.goog"}}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithGKENodePoolProvisioner(stub))
	ctx := context.Background()
	raw, err := json.Marshal(vccWith(map[string]any{
		"gkeClusterTarget": gkeClusterPath,
		"nodePoolTarget":   []any{poolTarget("default-pool", []string{"DEFAULT"}, nil)},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "dp-gke", ClusterInput{VirtualClusterConfig: raw}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	stub.specs = nil
	_, _, err = p.CreateCluster(ctx, "proj", "us-central1", "dp-gke", ClusterInput{VirtualClusterConfig: raw})
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 409 {
		t.Fatalf("duplicate create: got %v, want 409", err)
	}
	if len(stub.specs) != 0 {
		t.Fatalf("a rejected duplicate create reconciled %d pools", len(stub.specs))
	}
}

// TestCreateCluster_ResolutionFailureRollsBackNamespace pins DPG53: a target that
// cannot be resolved rejects the create after the workload namespace was
// provisioned, and that namespace must not leak.
func TestCreateCluster_ResolutionFailureRollsBackNamespace(t *testing.T) {
	client := k8sfake.NewSimpleClientset()
	stub := &fakeGKENodePoolProvisioner{resolveErr: model.NewProviderError("NotFound", "cluster not found", 404)}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil),
		WithSparkImage("spark:test"),
		WithGKENodePoolProvisioner(stub))
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx := context.Background()
	raw, err := json.Marshal(map[string]any{
		"kubernetesClusterConfig": map[string]any{
			"kubernetesNamespace": "dp-ns-rollback",
			"gkeClusterConfig": map[string]any{
				"gkeClusterTarget": "projects/proj/locations/us-central1/clusters/missing",
				"nodePoolTarget": []any{map[string]any{
					"nodePool": "projects/proj/locations/us-central1/clusters/missing/nodePools/p",
					"roles":    []any{"DEFAULT"},
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "dp-gke-ns", ClusterInput{VirtualClusterConfig: raw}); err == nil {
		t.Fatal("an unresolvable target must reject the create")
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, "dp-ns-rollback", metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("a rejected create leaked its namespace: %v", err)
	}
}

func TestCreateCluster_SynthesisesDefaultPool(t *testing.T) {
	stub := &fakeGKENodePoolProvisioner{gke: GKECluster{WorkloadIdentityPool: "proj.svc.id.goog"}}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithGKENodePoolProvisioner(stub))
	raw, err := json.Marshal(vccWith(map[string]any{"gkeClusterTarget": gkeClusterPath}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := p.CreateCluster(context.Background(), "proj", "us-central1", "dp-gke", ClusterInput{VirtualClusterConfig: raw}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if len(stub.specs) != 1 || stub.specs[0].Name != "default-pool" || stub.specs[0].Verify {
		t.Fatalf("expected one synthesised DEFAULT pool, got %+v", stub.specs)
	}
}

func TestCreateCluster_ReconcileFailurePersistsErrorCluster(t *testing.T) {
	stub := &fakeGKENodePoolProvisioner{
		gke:       GKECluster{WorkloadIdentityPool: "proj.svc.id.goog"},
		ensureErr: &GKENodePoolMismatchError{Pool: "default-pool", Field: "machineType", Want: "n1-standard-4", Got: "e2-medium"},
	}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithGKENodePoolProvisioner(stub))
	ctx := context.Background()
	raw, err := json.Marshal(vccWith(map[string]any{
		"gkeClusterTarget": gkeClusterPath,
		"nodePoolTarget": []any{
			poolTarget("default-pool", []string{"DEFAULT"}, map[string]any{"config": map[string]any{"machineType": "n1-standard-4"}}),
		},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "dp-gke", ClusterInput{VirtualClusterConfig: raw})
	if err != nil {
		t.Fatalf("a reconciliation failure must not be a transport error: %v", err)
	}
	if c.Status.State != "ERROR" {
		t.Fatalf("expected ERROR, got %s", c.Status.State)
	}
	if !strings.Contains(c.Status.Detail, "default-pool") || !strings.Contains(c.Status.Detail, "machineType") {
		t.Fatalf("status.detail must name the pool and field: %q", c.Status.Detail)
	}
	// The failed cluster is readable and stays ERROR (a terminal state).
	got, err := p.GetCluster(ctx, "proj", "us-central1", "dp-gke")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if got.Status.State != "ERROR" || !strings.Contains(got.Status.Detail, "machineType") {
		t.Fatalf("stored cluster lost its ERROR status: %+v", got.Status)
	}
	rendered := ClusterJSON(got)
	status, _ := rendered["status"].(map[string]any)
	if status["detail"] == nil {
		t.Fatalf("rendered status lost the detail: %v", status)
	}
}

func TestCreateCluster_ReconcileResolutionFailures(t *testing.T) {
	cases := []struct {
		name string
		gke  GKECluster
		err  error
		code string
	}{
		{name: "missing GKE cluster", err: model.NewProviderError("NotFound", "cluster not found", 404), code: "NotFound"},
		{name: "autopilot target", gke: GKECluster{Autopilot: true, WorkloadIdentityPool: "proj.svc.id.goog"}, code: "InvalidArgument"},
		{name: "no workload identity", gke: GKECluster{}, code: "InvalidArgument"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &fakeGKENodePoolProvisioner{gke: tc.gke, resolveErr: tc.err}
			p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithGKENodePoolProvisioner(stub))
			raw, err := json.Marshal(vccWith(map[string]any{"gkeClusterTarget": gkeClusterPath}))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			_, _, err = p.CreateCluster(context.Background(), "proj", "us-central1", "dp-gke", ClusterInput{VirtualClusterConfig: raw})
			var pe *model.ProviderError
			if !errors.As(err, &pe) || pe.Code != tc.code {
				t.Fatalf("expected %s, got %v", tc.code, err)
			}
			// Nothing was created: a resolution failure rejects the request.
			if _, gerr := p.GetCluster(context.Background(), "proj", "us-central1", "dp-gke"); gerr == nil {
				t.Fatal("cluster must not be persisted when the target cannot be resolved")
			}
		})
	}
}

// TestClusterJSON_NodePoolConfigIsInputOnly verifies the input-only field is
// never echoed, on get and on list, while the stored object keeps it.
func TestClusterJSON_NodePoolConfigIsInputOnly(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	raw, err := json.Marshal(vccWith(map[string]any{
		"gkeClusterTarget": gkeClusterPath,
		"nodePoolTarget": []any{
			poolTarget("default-pool", []string{"DEFAULT"}, map[string]any{"config": map[string]any{"machineType": "n1-standard-4"}}),
		},
	}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "dp-gke", ClusterInput{VirtualClusterConfig: raw}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	stored, err := p.GetCluster(ctx, "proj", "us-central1", "dp-gke")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if !strings.Contains(string(stored.VirtualClusterConfig), "nodePoolConfig") {
		t.Fatalf("stored virtualClusterConfig lost the caller's nodePoolConfig: %s", stored.VirtualClusterConfig)
	}
	vcc, _ := ClusterJSON(stored)["virtualClusterConfig"].(map[string]any)
	kcc, _ := vcc["kubernetesClusterConfig"].(map[string]any)
	gke, _ := kcc["gkeClusterConfig"].(map[string]any)
	targets, _ := gke["nodePoolTarget"].([]any)
	if len(targets) != 1 {
		t.Fatalf("nodePoolTarget lost: %v", gke)
	}
	if _, ok := targets[0].(map[string]any)["nodePoolConfig"]; ok {
		t.Fatal("nodePoolConfig must not be rendered")
	}
}

// TestGKENodePoolSpec_DefaultsOnlyForCreatedPools pins ambiguity #2: the 1/10
// autoscaler and the default machine type are create-time defaults, never
// verification fields, so a caller-shaped pool without an autoscaling block does
// not mismatch an existing pool that has none.
func TestGKENodePoolSpec_DefaultsOnlyForCreatedPools(t *testing.T) {
	shaped := gkeNodePoolSpec(gkePoolTarget{
		name:   "shaped",
		roles:  []string{"DEFAULT"},
		config: &gkeNodePoolConfig{machineType: "n1-standard-4"},
	}, "dp-gke", "uuid", "us-central1", nil)
	if !shaped.Verify {
		t.Fatal("a caller-shaped pool must be verified")
	}
	if shaped.Autoscaling != nil {
		t.Fatalf("autoscaling must stay nil when the caller sent none: %+v", shaped.Autoscaling)
	}
	if shaped.CreateDefaults.Autoscaling == nil || shaped.CreateDefaults.Autoscaling.MaxNodeCount != 10 {
		t.Fatalf("a created pool must default the autoscaler to 1/10: %+v", shaped.CreateDefaults.Autoscaling)
	}
	if shaped.CreateDefaults.MachineType != defaultGKENodePoolMachineType {
		t.Fatalf("default machine type wrong: %q", shaped.CreateDefaults.MachineType)
	}

	// No config at all: adopted, not verified, and still created with defaults.
	adopted := gkeNodePoolSpec(gkePoolTarget{name: "plain", roles: []string{"DEFAULT"}}, "dp-gke", "uuid", "us-central1", nil)
	if adopted.Verify {
		t.Fatal("a pool with no nodePoolConfig must be adopted, not verified")
	}
	if adopted.MachineType != "" || adopted.Autoscaling != nil {
		t.Fatalf("adoption must not assert a shape: %+v", adopted)
	}
}
