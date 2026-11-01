package dataproc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

// gkeVirtualClusterConfig is a full dataproc.v1.VirtualClusterConfig covering
// every sub-object this session parses (kubernetesClusterConfig with a GKE
// target, node pools and software config, plus an auxiliary metastore).
const gkeVirtualClusterConfig = `{"stagingBucket":"dataproc-staging-proj","kubernetesClusterConfig":{"kubernetesNamespace":"dataproc","gkeClusterConfig":{"gkeClusterTarget":"projects/proj/locations/us-central1/clusters/gke-1","nodePoolTarget":[{"nodePool":"projects/proj/locations/us-central1/clusters/gke-1/nodePools/default","roles":["DEFAULT"]}]},"kubernetesSoftwareConfig":{"componentVersion":{"SPARK":"3.5"}}},"auxiliaryServicesConfig":{"metastoreConfig":{"dataprocMetastoreService":"projects/proj/locations/us-central1/services/hms"}}}`

// gceInput builds a minimal Compute-Engine ClusterInput. A create must carry
// exactly one of config or virtualClusterConfig, so a test that is not about
// the placement sends the GCE shape rather than an empty request.
func gceInput() ClusterInput {
	return ClusterInput{Config: []byte(`{"gceClusterConfig":{"zoneUri":"us-central1-a"}}`)}
}

// gkeInput builds a ClusterInput from a GKE virtualClusterConfig map.
func gkeInput(t *testing.T, vcc map[string]any) ClusterInput {
	t.Helper()
	data, err := json.Marshal(vcc)
	if err != nil {
		t.Fatalf("marshal vcc: %v", err)
	}
	return ClusterInput{VirtualClusterConfig: data}
}

// TestCreateCluster_GKEVirtualClusterConfigRoundTrip verifies a GKE-backed
// cluster keeps its defining virtualClusterConfig verbatim on get and list and
// carries no GCE config.
func TestCreateCluster_GKEVirtualClusterConfigRoundTrip(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-1", ClusterInput{
		Labels:               map[string]string{"env": "dev"},
		VirtualClusterConfig: []byte(gkeVirtualClusterConfig),
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	c, err := p.GetCluster(ctx, "proj", "us-central1", "gke-1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if !c.IsGKEBacked() {
		t.Fatal("expected IsGKEBacked() true")
	}
	if len(c.Config) != 0 {
		t.Fatalf("GKE cluster must have no GCE config, got %s", c.Config)
	}

	rendered := ClusterJSON(c)
	if _, ok := rendered["config"]; ok {
		t.Fatalf("config must be absent for a GKE cluster: %v", rendered)
	}
	gotVCC, _ := rendered["virtualClusterConfig"].(map[string]any)
	if gotVCC == nil {
		t.Fatalf("virtualClusterConfig missing from render: %v", rendered)
	}
	if gotVCC["stagingBucket"] != "dataproc-staging-proj" {
		t.Fatalf("stagingBucket lost: %v", gotVCC["stagingBucket"])
	}
	kcc, _ := gotVCC["kubernetesClusterConfig"].(map[string]any)
	gke, _ := kcc["gkeClusterConfig"].(map[string]any)
	if gke["gkeClusterTarget"] != "projects/proj/locations/us-central1/clusters/gke-1" {
		t.Fatalf("gkeClusterTarget lost: %v", gke)
	}
	if pools, _ := gke["nodePoolTarget"].([]any); len(pools) != 1 {
		t.Fatalf("nodePoolTarget lost: %v", gke["nodePoolTarget"])
	}
	aux, _ := gotVCC["auxiliaryServicesConfig"].(map[string]any)
	meta, _ := aux["metastoreConfig"].(map[string]any)
	if meta["dataprocMetastoreService"] != "projects/proj/locations/us-central1/services/hms" {
		t.Fatalf("auxiliaryServicesConfig lost: %v", gotVCC["auxiliaryServicesConfig"])
	}

	list, _, err := p.ListClusters(ctx, "proj", "us-central1", "", 0, "")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListClusters: %v %d", err, len(list))
	}
	if !list[0].IsGKEBacked() {
		t.Fatal("listed cluster lost virtualClusterConfig")
	}
}

// TestCreateCluster_VirtualClusterConfigValidation verifies malformed
// virtualClusterConfig values fail loud with InvalidArgument.
func TestCreateCluster_VirtualClusterConfigValidation(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	cases := map[string]map[string]any{
		"missing kubernetesClusterConfig": {"stagingBucket": "b"},
		"missing gkeClusterConfig": {
			"kubernetesClusterConfig": map[string]any{
				"kubernetesSoftwareConfig": map[string]any{"componentVersion": map[string]any{"SPARK": "3.5"}},
			},
		},
		"no target": {
			"kubernetesClusterConfig": map[string]any{"gkeClusterConfig": map[string]any{}},
		},
		"empty": {},
	}
	for name, vcc := range cases {
		_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "bad-"+name, gkeInput(t, vcc))
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
			t.Fatalf("%s: expected 400 InvalidArgument, got %v", name, err)
		}
	}
}

// TestCreateCluster_GKEAgainstNodePools verifies a cluster that only names
// nodePoolTarget (no existing gkeClusterTarget) is accepted.
func TestCreateCluster_GKEAgainstNodePools(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-pools", gkeInput(t, map[string]any{
		"kubernetesClusterConfig": map[string]any{
			"gkeClusterConfig": map[string]any{
				"nodePoolTarget": []any{map[string]any{
					"nodePool": "projects/proj/locations/us-central1/clusters/gke-1/nodePools/default",
					"roles":    []any{"DEFAULT"},
				}},
			},
		},
	})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := p.GetCluster(ctx, "proj", "us-central1", "gke-pools")
	if err != nil || !c.IsGKEBacked() {
		t.Fatalf("GetCluster: %v %+v", err, c)
	}
}

// TestUpdateCluster_VirtualClusterConfigRejected verifies the recreate-only
// lifecycle: real GCP disallows update operations on a Dataproc-on-GKE virtual
// cluster, so a patch whose mask applies virtualClusterConfig or config is
// InvalidArgument, while a labels-only mask is honoured even when the body
// carries a full resource. The stored placement survives every rejected patch.
func TestUpdateCluster_VirtualClusterConfigRejected(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-1", gkeInput(t, mustJSONMap([]byte(gkeVirtualClusterConfig)))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	full := ClusterInput{
		Labels:               map[string]string{"patched": "true"},
		VirtualClusterConfig: []byte(`{"kubernetesClusterConfig":{"gkeClusterConfig":{"gkeClusterTarget":"projects/proj/locations/us-central1/clusters/gke-2"}}}`),
		Config:               []byte(`{"gceClusterConfig":{"zoneUri":"z"}}`),
	}

	rejected := map[string]struct {
		in   ClusterInput
		mask []string
	}{
		// The mask applies the placement: rejected, whichever field it names.
		"virtualClusterConfig mask": {ClusterInput{VirtualClusterConfig: full.VirtualClusterConfig}, []string{"virtualClusterConfig"}},
		"config mask":               {ClusterInput{Config: full.Config}, []string{"config"}},
		"snake_case mask":           {ClusterInput{VirtualClusterConfig: full.VirtualClusterConfig}, []string{"virtual_cluster_config"}},
		// An empty mask means "everything supplied applies".
		"unmasked virtualClusterConfig": {ClusterInput{VirtualClusterConfig: full.VirtualClusterConfig}, nil},
		"unmasked gce config swap":      {ClusterInput{Config: full.Config}, nil},
	}
	for name, tc := range rejected {
		_, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "gke-1", tc.in, tc.mask)
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
			t.Fatalf("%s: expected 400 InvalidArgument, got %v", name, err)
		}
		if !strings.Contains(pe.Message, "recreate-only") || !strings.Contains(pe.Message, "Only labels are updatable") {
			t.Fatalf("%s: error must explain the recreate-only rule and the labels-only allowance, got %q", name, pe.Message)
		}
		if strings.Contains(pe.Message, "config.workerConfig.numInstances") {
			t.Fatalf("%s: error must not offer GCE config subfields as updatable on a virtual cluster, got %q", name, pe.Message)
		}
	}
	// A malformed virtualClusterConfig is a shape error, reported as such.
	bad := ClusterInput{VirtualClusterConfig: []byte(`{"kubernetesClusterConfig":{"gkeClusterConfig":{}}}`)}
	if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "gke-1", bad, []string{"virtualClusterConfig"}); err == nil {
		t.Fatal("expected malformed virtualClusterConfig update to fail")
	}
	c, err := p.GetCluster(ctx, "proj", "us-central1", "gke-1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if gkeOf(t, c.VirtualClusterConfig)["gkeClusterTarget"] == "" {
		t.Fatalf("stored virtualClusterConfig clobbered by rejected patches: %s", c.VirtualClusterConfig)
	}
	if len(c.Config) != 0 {
		t.Fatalf("rejected patch stored a GCE config: %s", c.Config)
	}

	// A labels-only mask is honoured when the body still carries the full
	// resource: the label lands, the placement is left alone.
	if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "gke-1", full, []string{"labels"}); err != nil {
		t.Fatalf("labels-only mask with a full body must be applied, got %v", err)
	}
	c, err = p.GetCluster(ctx, "proj", "us-central1", "gke-1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.Labels["patched"] != "true" {
		t.Fatalf("labels-only patch did not apply the label: %v", c.Labels)
	}
	if got, _ := gkeOf(t, c.VirtualClusterConfig)["gkeClusterTarget"].(string); strings.Contains(got, "gke-2") {
		t.Fatalf("labels-only patch applied the masked-out virtualClusterConfig: %s", c.VirtualClusterConfig)
	}
	if len(c.Config) != 0 {
		t.Fatalf("labels-only patch applied the masked-out config: %s", c.Config)
	}
}

// TestCreateCluster_BothConfigAndVirtualClusterConfigRejected verifies the API's
// "exactly one of config or virtualClusterConfig" rule is enforced.
func TestCreateCluster_BothConfigAndVirtualClusterConfigRejected(t *testing.T) {
	p := newProvider(t)
	_, _, err := p.CreateCluster(context.Background(), "proj", "us-central1", "both", ClusterInput{
		Config:               []byte(`{"gceClusterConfig":{"zoneUri":"z"}}`),
		VirtualClusterConfig: []byte(gkeVirtualClusterConfig),
	})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("expected 400 InvalidArgument, got %v", err)
	}
}

// TestCreateCluster_NeitherConfigRejected verifies the same "exactly one" rule
// rejects a create that carries neither: real Dataproc cannot place such a
// cluster, and accepting it would make every read invent a GCE config.
func TestCreateCluster_NeitherConfigRejected(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "neither", ClusterInput{})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("expected 400 InvalidArgument, got %v", err)
	}
	if _, err := p.GetCluster(ctx, "proj", "us-central1", "neither"); err == nil {
		t.Fatal("a rejected create must not persist a cluster")
	}
}

// TestUpdateCluster_LabelsOnlyPreservesVirtualClusterConfig verifies a masked
// labels update on a GKE cluster leaves its virtualClusterConfig untouched and
// does not invent a GCE config.
func TestUpdateCluster_LabelsOnlyPreservesVirtualClusterConfig(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "gke-1", gkeInput(t, mustJSONMap([]byte(gkeVirtualClusterConfig)))); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, _, err := p.UpdateCluster(ctx, "proj", "us-central1", "gke-1",
		ClusterInput{Labels: map[string]string{"env": "prod"}}, []string{"labels"}); err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	c, err := p.GetCluster(ctx, "proj", "us-central1", "gke-1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if len(c.Config) != 0 {
		t.Fatalf("labels update invented a GCE config: %s", c.Config)
	}
	if !c.IsGKEBacked() || gkeOf(t, c.VirtualClusterConfig)["gkeClusterTarget"] == "" {
		t.Fatalf("virtualClusterConfig lost on labels update: %s", c.VirtualClusterConfig)
	}
	if c.Labels["env"] != "prod" {
		t.Fatalf("labels not updated: %v", c.Labels)
	}
}

// gkeOf navigates a virtualClusterConfig raw JSON to its gkeClusterConfig map.
func gkeOf(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	vcc := mustJSONMap(raw)
	kcc, _ := vcc["kubernetesClusterConfig"].(map[string]any)
	gke, _ := kcc["gkeClusterConfig"].(map[string]any)
	return gke
}
