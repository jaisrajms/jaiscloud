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
