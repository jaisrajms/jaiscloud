package container

import (
	"context"
	"testing"

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
