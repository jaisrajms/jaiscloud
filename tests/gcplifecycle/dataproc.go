package gcplifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	dpcore "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	grpcdataproc "jaiscloud/internal/gcp/transport/grpc/dataproc"
	restdataproc "jaiscloud/internal/gcp/transport/rest/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const dpRegion = "us-central1"

// dataprocManifest is the FC5 ownership contract for a Dataproc cluster:
// jobs are project/region history and are retained when their placement
// cluster is deleted (SubmitJob to the deleted cluster is NOT_FOUND, but the
// job record stays readable and listable). The cluster's own workloads and the
// namespace it created are torn down by the executor (cascade), which the mock
// executor cannot express; the storage/queue surfaces are out of this pilot.
var dataprocManifest = manifest{
	parent: "dataproc cluster",
	policy: Cascade,
	children: []childSpec{
		{"jobs", Retain},
	},
	note: "the cluster record is a lazy tombstone: delete sets DELETING and the next read removes it",
}

type dataprocAdapter struct {
	seq    int
	store  dpstore.Store
	core   *dpcore.Service
	rest   *restdataproc.Provider
	client dataprocpb.ClusterControllerClient
	proj   string
}

func newDataprocAdapter(t *testing.T) adapter {
	t.Helper()
	dp := dpstore.NewMemoryStore()
	res := store.NewMemoryResourceStore()
	core := dpcore.NewService(dp, res)
	conn := serveGRPC(t, func(srv *grpc.Server) {
		dataprocpb.RegisterClusterControllerServer(srv, grpcdataproc.NewService(core, "proj"))
	})
	return &dataprocAdapter{
		store:  dp,
		core:   core,
		rest:   restdataproc.NewProvider(core, "proj"),
		client: dataprocpb.NewClusterControllerClient(conn),
		proj:   "proj",
	}
}

func (a *dataprocAdapter) name() string { return "dataproc cluster" }

func (a *dataprocAdapter) create(ctx context.Context, tr transport, id string) (string, error) {
	if tr == grpcT {
		op, err := a.client.CreateCluster(ctx, &dataprocpb.CreateClusterRequest{
			ProjectId: a.proj,
			Region:    dpRegion,
			Cluster: &dataprocpb.Cluster{
				ClusterName: id,
				Config: &dataprocpb.ClusterConfig{
					GceClusterConfig: &dataprocpb.GceClusterConfig{ZoneUri: dpRegion + "-a"},
				},
			},
		})
		if err != nil {
			return "", err
		}
		return op.GetName(), nil
	}
	_, err := a.rest.CreateCluster(ctx, a.nr(id, map[string]any{
		"clusterName": id,
		"config": map[string]any{
			"gceClusterConfig": map[string]any{"zoneUri": dpRegion + "-a"},
		},
	}))
	if err != nil {
		return "", err
	}
	return id, nil
}

func (a *dataprocAdapter) get(ctx context.Context, tr transport, id string) (bool, error) {
	if tr == grpcT {
		_, err := a.client.GetCluster(ctx, &dataprocpb.GetClusterRequest{ProjectId: a.proj, Region: dpRegion, ClusterName: id})
		return err == nil, ignoreNotFound(err)
	}
	_, err := a.rest.GetCluster(ctx, a.nr(id, nil))
	return err == nil, ignoreNotFound(err)
}

func (a *dataprocAdapter) list(ctx context.Context, tr transport) ([]string, error) {
	if tr == grpcT {
		resp, err := a.client.ListClusters(ctx, &dataprocpb.ListClustersRequest{ProjectId: a.proj, Region: dpRegion})
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(resp.GetClusters()))
		for _, c := range resp.GetClusters() {
			out = append(out, c.GetClusterName())
		}
		return out, nil
	}
	resp, err := a.rest.ListClusters(ctx, a.nr("", map[string]any{}))
	if err != nil {
		return nil, err
	}
	items, _ := resp.Data["clusters"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		m, _ := it.(map[string]any)
		if name, _ := m["clusterName"].(string); name != "" {
			out = append(out, name)
		}
	}
	return out, nil
}

func (a *dataprocAdapter) update(ctx context.Context, tr transport, id string) error {
	if tr == grpcT {
		op, err := a.client.UpdateCluster(ctx, &dataprocpb.UpdateClusterRequest{
			ProjectId:   a.proj,
			Region:      dpRegion,
			ClusterName: id,
			Cluster:     &dataprocpb.Cluster{Labels: map[string]string{"lifecycle": "update"}},
			UpdateMask:  &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		})
		_ = op
		return err
	}
	_, err := a.rest.UpdateCluster(ctx, a.nr(id, map[string]any{
		"labels":     map[string]any{"lifecycle": "update"},
		"updateMask": "labels",
	}))
	return err
}

func (a *dataprocAdapter) remove(ctx context.Context, tr transport, id string) error {
	if tr == grpcT {
		if _, err := a.client.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{ProjectId: a.proj, Region: dpRegion, ClusterName: id}); err != nil {
			return err
		}
	} else if _, err := a.rest.DeleteCluster(ctx, a.nr(id, nil)); err != nil {
		return err
	}
	// The record is a lazy tombstone: the next read applies the state machine.
	if _, err := a.get(ctx, tr, id); err != nil {
		return err
	}
	return nil
}

func (a *dataprocAdapter) blockedDelete(ctx context.Context, tr transport, id string) []blockedResult {
	// Dataproc defines no delete precondition. The negative that matters is a
	// delete of a cluster that is already gone, which must answer NOT_FOUND.
	return []blockedResult{{
		name:     "delete-missing-cluster",
		err:      a.remove(ctx, tr, id+"-missing"),
		wantCode: "NotFound",
	}}
}

func (a *dataprocAdapter) populate(ctx context.Context, id string) error {
	a.seq++
	return a.store.CreateJob(ctx, a.proj, dpRegion, dpstore.Job{
		JobID:                fmt.Sprintf("job%d", a.seq),
		PlacementClusterName: id,
		Type:                 "sparkJob",
		TypeJob:              json.RawMessage(`{}`),
		Status:               dpstore.JobStatus{State: "DONE"},
	})
}

func (a *dataprocAdapter) counts(ctx context.Context, id string) (map[string]childCount, error) {
	jobs, err := a.store.ListJobs(ctx, a.proj, dpRegion)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, job := range jobs {
		if job.PlacementClusterName == id {
			total++
		}
	}
	return map[string]childCount{"jobs": {total: total, attached: total}}, nil
}

func (a *dataprocAdapter) clearRejecting(context.Context, string) error { return nil }

// extra pins the create-while-DELETING edge: Dataproc deletes lazily, so a
// Terraform replace (delete -> create with the same name) arrives while the old
// record still says DELETING. The create must settle the state machine and
// succeed instead of answering ALREADY_EXISTS.
func (a *dataprocAdapter) extra(t *testing.T, ctx context.Context, tr transport, id string) {
	t.Helper()
	raceID := id + "-replace"
	if _, err := a.create(ctx, tr, raceID); err != nil {
		t.Fatalf("create before replace: %v", err)
	}
	if tr == grpcT {
		if _, err := a.client.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{ProjectId: a.proj, Region: dpRegion, ClusterName: raceID}); err != nil {
			t.Fatalf("delete before replace: %v", err)
		}
	} else if _, err := a.rest.DeleteCluster(ctx, a.nr(raceID, nil)); err != nil {
		t.Fatalf("delete before replace: %v", err)
	}
	if _, err := a.create(ctx, tr, raceID); err != nil {
		t.Fatalf("create while the previous incarnation is DELETING = %v, want success (a legal recreate must never answer ALREADY_EXISTS)", err)
	}
	if err := a.remove(ctx, tr, raceID); err != nil {
		t.Fatalf("cleanup replace cluster: %v", err)
	}
}

// nr builds a Dataproc NormalizedRequest for the REST provider.
func (a *dataprocAdapter) nr(name string, body map[string]any) *model.NormalizedRequest {
	params := map[string]any{"project": a.proj, "region": dpRegion}
	if name != "" {
		params["clusterName"] = name
	}
	if body != nil {
		params["body"] = body
	}
	return &model.NormalizedRequest{AccountID: a.proj, Params: params}
}
