package container

import (
	"context"
	"errors"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/gcperr"
	core "jaiscloud/internal/gcp/service/container"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

func newProvider(t *testing.T) *Provider {
	t.Helper()
	return NewProvider(core.NewService(containerstore.NewMemoryStore()))
}

func req(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{Service: ServiceName, Params: params}
}

func clusterBody(name string) map[string]any {
	return map[string]any{"cluster": map[string]any{"name": name, "initialNodeCount": float64(1)}}
}

func TestProviderClusterCRUD(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	resp, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": clusterBody("c1")}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if resp.Data["operationType"] != containerstore.OperationCreateCluster || resp.Data["status"] != containerstore.OperationStatusDone {
		t.Fatalf("create operation = %v", resp.Data)
	}
	if _, ok := resp.Data["name"].(string); !ok {
		t.Fatalf("operation name = %v", resp.Data["name"])
	}

	getResp, err := p.GetCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if getResp.Data["name"] != "c1" || getResp.Data["status"] != containerstore.StatusRunning {
		t.Fatalf("cluster = %v", getResp.Data)
	}
	if _, ok := getResp.Data["masterAuth"].(map[string]any); !ok {
		t.Fatalf("cluster missing masterAuth: %v", getResp.Data)
	}

	listResp, err := p.ListClusters(ctx, req(map[string]any{"project": "p", "location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if got := listResp.Data["clusters"].([]any); len(got) != 1 {
		t.Fatalf("clusters = %v", listResp.Data["clusters"])
	}

	delResp, err := p.DeleteCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1"}))
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if delResp.Data["operationType"] != containerstore.OperationDeleteCluster {
		t.Fatalf("delete operation = %v", delResp.Data)
	}
	if _, err := p.GetCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1"})); err == nil {
		t.Fatal("cluster still present after delete")
	}
}

func TestProviderCreateClusterWithInlineNodePools(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	body := map[string]any{"cluster": map[string]any{
		"name": "c1",
		"nodePools": []any{
			map[string]any{"name": "pool-a", "initialNodeCount": float64(2),
				"config": map[string]any{"machineType": "e2-medium"}},
			map[string]any{"name": "pool-b", "initialNodeCount": float64(1)},
		},
	}}
	if _, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": body})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	get, err := p.GetCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	pools, ok := get.Data["nodePools"].([]any)
	if !ok || len(pools) != 2 {
		t.Fatalf("nodePools = %v", get.Data["nodePools"])
	}
	a := pools[0].(map[string]any)
	if a["name"] != "pool-a" || a["status"] != containerstore.StatusRunning {
		t.Fatalf("pool-a = %v", a)
	}
	if a["selfLink"] != "projects/p/locations/us-central1/clusters/c1/nodePools/pool-a" {
		t.Fatalf("pool-a selfLink = %v", a["selfLink"])
	}
	cfg, _ := a["config"].(map[string]any)
	if cfg["machineType"] != "e2-medium" {
		t.Fatalf("pool-a config = %v", a["config"])
	}
	if name := pools[1].(map[string]any)["name"]; name != "pool-b" {
		t.Fatalf("pool-b = %v", pools[1])
	}

	list, err := p.ListNodePools(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1"}))
	if err != nil {
		t.Fatalf("ListNodePools: %v", err)
	}
	if got := list.Data["nodePools"].([]any); len(got) != 2 {
		t.Fatalf("nodePools.list = %v", got)
	}
}

func TestProviderOperations(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	create, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "l", "body": clusterBody("c1")}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	name := create.Data["name"].(string)

	get, err := p.GetOperation(ctx, req(map[string]any{"project": "p", "location": "l", "operation": name}))
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if get.Data["operationType"] != containerstore.OperationCreateCluster {
		t.Fatalf("operation = %v", get.Data)
	}
	list, err := p.ListOperations(ctx, req(map[string]any{"project": "p", "location": "l"}))
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if got := list.Data["operations"].([]any); len(got) != 1 {
		t.Fatalf("operations = %v", list.Data["operations"])
	}
}

func TestProviderCreateRejectsMissingCluster(t *testing.T) {
	p := newProvider(t)
	if _, err := p.CreateCluster(context.Background(), req(map[string]any{"project": "p", "location": "l"})); err == nil {
		t.Fatal("expected InvalidArgument for missing cluster object")
	}
}

// TestProviderCreateRejectsDefaultPoolFieldsWithNodePools covers the wire
// path: inline nodePools combined with the legacy default-pool fields is the
// 400 real GKE returns (docs/gcp/gke-create-pool-conflict.md), and nothing is
// persisted.
func TestProviderCreateRejectsDefaultPoolFieldsWithNodePools(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	pools := []any{map[string]any{"name": "pool-a", "initialNodeCount": float64(1)}}

	cases := []struct {
		name    string
		cluster map[string]any
		want    string
	}{
		{
			name:    "initialNodeCount",
			cluster: map[string]any{"name": "c1", "initialNodeCount": float64(1), "nodePools": pools},
			want:    "It's invalid to specify both cluster.initial_node_count and a node pool. Please only provide a node pool.",
		},
		{
			name:    "nodeConfig",
			cluster: map[string]any{"name": "c1", "nodeConfig": map[string]any{"machineType": "e2-micro"}, "nodePools": pools},
			want:    "It's invalid to specify both cluster.node_config and a node pool. Please only provide a node pool.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": map[string]any{"cluster": tc.cluster}}))
			if err == nil {
				t.Fatal("want InvalidArgument, got nil")
			}
			var pe *model.ProviderError
			if !errors.As(err, &pe) || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
				t.Fatalf("error = %v, want 400 InvalidArgument", err)
			}
			if pe.Message != tc.want {
				t.Fatalf("message = %q, want %q", pe.Message, tc.want)
			}
			if _, err := p.GetCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1"})); err == nil {
				t.Fatal("rejected create must persist no cluster")
			}
		})
	}
}

// TestProviderCreateAppliesNodeConfigToDefaultPool covers the wire path for
// DPG42: a create that sends the legacy nodeConfig + initialNodeCount shape
// reads the config back on both the generated pool and the cluster-level echo
// (Discovery: the response's Cluster.nodeConfig comes from the first pool).
func TestProviderCreateAppliesNodeConfigToDefaultPool(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	body := map[string]any{"cluster": map[string]any{
		"name":             "c1",
		"initialNodeCount": float64(1),
		"nodeConfig":       map[string]any{"machineType": "e2-micro", "diskSizeGb": float64(50)},
	}}
	if _, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": body})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	resp, err := p.GetCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	cfg, _ := resp.Data["nodeConfig"].(map[string]any)
	if cfg["machineType"] != "e2-micro" || cfg["diskSizeGb"] != int32(50) {
		t.Fatalf("cluster nodeConfig = %v", resp.Data["nodeConfig"])
	}
	pools, _ := resp.Data["nodePools"].([]any)
	if len(pools) != 1 {
		t.Fatalf("nodePools = %v", resp.Data["nodePools"])
	}
	pool, _ := pools[0].(map[string]any)
	if pool["selfLink"] != "projects/p/locations/us-central1/clusters/c1/nodePools/default-pool" {
		t.Fatalf("default-pool selfLink = %v", pool["selfLink"])
	}
	if pool["version"] != core.DefaultMasterVersion {
		t.Fatalf("default-pool version = %v", pool["version"])
	}
	poolCfg, _ := pool["config"].(map[string]any)
	if poolCfg["machineType"] != "e2-micro" || poolCfg["diskSizeGb"] != int32(50) {
		t.Fatalf("default-pool config = %v", pool["config"])
	}
}

func TestProviderListPagination(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	for _, name := range []string{"a", "b", "c"} {
		if _, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "l", "body": clusterBody(name)})); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	first, err := p.ListClusters(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": "2"}))
	if err != nil {
		t.Fatalf("ListClusters page 1: %v", err)
	}
	if got := first.Data["clusters"].([]any); len(got) != 2 {
		t.Fatalf("page 1 = %v", first.Data["clusters"])
	}
	token, _ := first.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("missing nextPageToken")
	}
	second, err := p.ListClusters(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": "2", "pageToken": token}))
	if err != nil {
		t.Fatalf("ListClusters page 2: %v", err)
	}
	if got := second.Data["clusters"].([]any); len(got) != 1 {
		t.Fatalf("page 2 = %v", second.Data["clusters"])
	}
	if _, ok := second.Data["nextPageToken"]; ok {
		t.Fatalf("unexpected nextPageToken on last page: %v", second.Data["nextPageToken"])
	}
}

func TestProviderNodePoolsAndSetters(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": clusterBody("c1")})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	base := map[string]any{"project": "p", "location": "us-central1", "cluster": "c1"}
	createReq := map[string]any{}
	for k, v := range base {
		createReq[k] = v
	}
	createReq["body"] = map[string]any{"nodePool": map[string]any{
		"name":             "np1",
		"initialNodeCount": float64(1),
		"config":           map[string]any{"machineType": "e2-medium"},
		"autoscaling":      map[string]any{"enabled": true, "minNodeCount": float64(1), "maxNodeCount": float64(3)},
	}}
	created, err := p.CreateNodePool(ctx, req(createReq))
	if err != nil {
		t.Fatalf("CreateNodePool: %v", err)
	}
	if created.Data["operationType"] != containerstore.OperationCreateNodePool || created.Data["status"] != containerstore.OperationStatusDone {
		t.Fatalf("create op = %v", created.Data)
	}

	got, err := p.GetNodePool(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1", "nodepool": "np1"}))
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if got.Data["name"] != "np1" || got.Data["status"] != containerstore.StatusRunning {
		t.Fatalf("node pool = %v", got.Data)
	}
	if cfg, ok := got.Data["config"].(map[string]any); !ok || cfg["machineType"] != "e2-medium" {
		t.Fatalf("node config = %v", got.Data["config"])
	}
	if a, ok := got.Data["autoscaling"].(map[string]any); !ok || a["maxNodeCount"] != int32(3) {
		t.Fatalf("autoscaling = %v", got.Data["autoscaling"])
	}

	list, err := p.ListNodePools(ctx, req(base))
	if err != nil {
		t.Fatalf("ListNodePools: %v", err)
	}
	if pools := list.Data["nodePools"].([]any); len(pools) != 2 {
		t.Fatalf("nodePools = %v", pools)
	}

	size, err := p.SetNodePoolSize(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1", "nodepool": "np1", "body": map[string]any{"nodeCount": float64(3)}}))
	if err != nil {
		t.Fatalf("SetNodePoolSize: %v", err)
	}
	if size.Data["operationType"] != containerstore.OperationSetNodePoolSize {
		t.Fatalf("setSize op = %v", size.Data)
	}

	addons, err := p.SetAddonsConfig(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1", "body": map[string]any{"addonsConfig": map[string]any{"httpLoadBalancing": map[string]any{"disabled": true}}}}))
	if err != nil {
		t.Fatalf("SetAddonsConfig: %v", err)
	}
	if addons.Data["operationType"] != containerstore.OperationUpdateCluster {
		t.Fatalf("setAddons op = %v", addons.Data)
	}
	cluster, err := p.GetCluster(ctx, req(base))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if a, ok := cluster.Data["addonsConfig"].(map[string]any); !ok {
		t.Fatalf("addonsConfig = %v", cluster.Data["addonsConfig"])
	} else if h, ok := a["httpLoadBalancing"].(map[string]any); !ok || h["disabled"] != true {
		t.Fatalf("httpLoadBalancing = %v", a["httpLoadBalancing"])
	}

	if _, err := p.UpdateMaster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1", "body": map[string]any{"masterVersion": "1.31.0-gke.100"}})); err != nil {
		t.Fatalf("UpdateMaster: %v", err)
	}
	cluster, _ = p.GetCluster(ctx, req(base))
	if cluster.Data["currentMasterVersion"] != "1.31.0-gke.100" {
		t.Fatalf("currentMasterVersion = %v", cluster.Data["currentMasterVersion"])
	}

	if _, err := p.CancelOperation(ctx, req(map[string]any{"project": "p", "location": "us-central1", "operation": created.Data["name"]})); err != nil {
		t.Fatalf("CancelOperation: %v", err)
	}

	del, err := p.DeleteNodePool(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1", "nodepool": "np1"}))
	if err != nil {
		t.Fatalf("DeleteNodePool: %v", err)
	}
	if del.Data["operationType"] != containerstore.OperationDeleteNodePool {
		t.Fatalf("delete op = %v", del.Data)
	}
	if _, err := p.GetNodePool(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1", "nodepool": "np1"})); err == nil {
		t.Fatal("node pool still present after delete")
	}
}

func TestProviderReset(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "l", "body": clusterBody("c1")})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	p.Reset(ctx)
	resp, err := p.ListClusters(ctx, req(map[string]any{"project": "p", "location": "l"}))
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if got := resp.Data["clusters"].([]any); len(got) != 0 {
		t.Fatalf("Reset left clusters: %v", got)
	}
}

// TestProviderInvalidNameMessages pins the messages the REST surface answers for
// a malformed canonical cluster or node-pool name. The codec parses with the
// core's parsers (the ones the gRPC codec uses), so the message — including the
// offending value — is identical on both transports; the gRPC service_test.go
// case pins the same strings from the other side.
func TestProviderInvalidNameMessages(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	const badCluster = "projects/p/locations/us-central1/clusters/bad-name/extra"
	_, err := p.CreateCluster(ctx, req(map[string]any{
		"project": "p", "location": "us-central1", "body": clusterBody(badCluster),
	}))
	assertProviderMessage(t, err, "invalid cluster name: "+badCluster)

	if _, err := p.CreateCluster(ctx, req(map[string]any{
		"project": "p", "location": "us-central1", "body": clusterBody("c1"),
	})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	const badPool = "projects/p/locations/us-central1/clusters/c1/nodePools/bad/extra"
	_, err = p.CreateNodePool(ctx, req(map[string]any{
		"project": "p", "location": "us-central1", "cluster": "c1",
		"body": map[string]any{"nodePool": map[string]any{"name": badPool, "initialNodeCount": float64(1)}},
	}))
	assertProviderMessage(t, err, "invalid node pool name: "+badPool)
}

func assertProviderMessage(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error %q, got nil", want)
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.Code != "InvalidArgument" || pe.Message != want {
		t.Fatalf("error = %q / %q, want InvalidArgument / %q", pe.Code, pe.Message, want)
	}
}

// TestProviderOperationConflictAndQuota proves the REST surface resolves the
// simulated operation conflict and quota rejection to the google.rpc statuses
// real GKE uses: 400 FAILED_PRECONDITION for an overlapping mutation, 429
// RESOURCE_EXHAUSTED for an exhausted concurrent-operations quota.
func TestProviderOperationConflictAndQuota(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	p := NewProvider(core.NewService(containerstore.NewMemoryStore(),
		core.WithOperationWindow(time.Hour), core.WithConcurrentOperationQuota(1)))
	resp, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": clusterBody("c1")}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if resp.Data["status"] != containerstore.OperationStatusRunning {
		t.Fatalf("create operation = %v, want RUNNING", resp.Data)
	}

	_, err = p.SetLabels(ctx, req(map[string]any{
		"project": "p", "location": "us-central1", "cluster": "c1",
		"body": map[string]any{"resourceLabels": map[string]any{"k": "v"}},
	}))
	assertGCPError(t, err, "FAILED_PRECONDITION", 400, "Cluster is running incompatible operation "+resp.Data["name"].(string))

	_, err = p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": clusterBody("c2")}))
	assertGCPError(t, err, "RESOURCE_EXHAUSTED", 429, "")
}

func assertGCPError(t *testing.T, err error, wantStatus string, wantHTTP int, wantMessage string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %s, got nil", wantStatus)
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *model.ProviderError, got %T (%v)", err, err)
	}
	statusName, httpStatus := gcperr.Resolve(pe)
	if statusName != wantStatus || httpStatus != wantHTTP {
		t.Fatalf("resolved = %s/%d, want %s/%d (%v)", statusName, httpStatus, wantStatus, wantHTTP, err)
	}
	if wantMessage != "" && pe.Message != wantMessage {
		t.Fatalf("message = %q, want %q", pe.Message, wantMessage)
	}
}
