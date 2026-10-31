package ui

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	restdataproc "jaiscloud/internal/gcp/transport/rest/dataproc"
	dataprocui "jaiscloud/internal/gcp/ui/dataproc"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

func newDataproc() (*dataproccore.Service, dataprocui.ProviderInterface, map[string]provider.HandlerFunc) {
	core := dataproccore.NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore())
	return core, dataprocui.NewProvider(core), restdataproc.NewProvider(core, "proj").Routes()
}

// canonicalJSON re-encodes raw JSON with sorted keys, so two encodings of the same
// object compare equal.
func canonicalJSON(raw json.RawMessage) string {
	var v any
	_ = json.Unmarshal(raw, &v)
	b, _ := json.Marshal(v)
	return string(b)
}

func TestDataprocConsolePassthroughMatchesDirect(t *testing.T) {
	core, direct, builtins := newDataproc()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeDataproc(typedFixture(builtins, wrap), direct)
	ctx := context.Background()

	cfg := json.RawMessage(`{"gceClusterConfig":{"zoneUri":"us-central1-a"},"masterConfig":{"numInstances":1}}`)
	c, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", dataproccore.ClusterInput{Labels: map[string]string{"a": "b"}, Config: cfg})
	if err != nil || c.Name != "c1" || c.Region != "us-central1" {
		t.Fatalf("CreateCluster = %+v, %v", c, err)
	}
	want, err := core.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil || got.Name != want.Name || got.Region != want.Region || got.ClusterUUID != want.ClusterUUID ||
		got.Status.State != want.Status.State || !reflect.DeepEqual(got.Labels, want.Labels) ||
		canonicalJSON(got.Config) != canonicalJSON(want.Config) {
		t.Fatalf("cluster through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, want, err)
	}
	if list, err := p.ListAllClusters(ctx, "proj"); err != nil || len(list) != 1 || list[0].Region != "us-central1" || list[0].Name != "c1" {
		t.Fatalf("ListAllClusters = %+v, %v", list, err)
	}
	if _, err := p.StopCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if _, err := p.StartCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	if jobs, err := p.ListAllJobs(ctx, "proj"); err != nil || len(jobs) != 0 {
		t.Fatalf("ListAllJobs = %+v, %v", jobs, err)
	}
	if ts, err := p.ListAllWorkflowTemplates(ctx, "proj"); err != nil || len(ts) != 0 {
		t.Fatalf("ListAllWorkflowTemplates = %+v, %v", ts, err)
	}
	if err := p.DeleteCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if len(seen) < 8 {
		t.Fatalf("the plugin saw only %d calls (%v)", len(seen), seen)
	}
}

func TestDataprocConsoleShowsWhatThePluginServes(t *testing.T) {
	_, direct, builtins := newDataproc()
	p := routeDataproc(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Dataproc.ListJobs": replaceWith(map[string]any{"jobs": []any{map[string]any{
			"reference": map[string]any{"projectId": "proj", "jobId": "from-plugin"}, "region": "europe-west1",
			"placement": map[string]any{"clusterName": "c9"}, "status": map[string]any{"state": "RUNNING"},
			"sparkJob": map[string]any{"mainClass": "Main"}}}}),
	}), direct)
	jobs, err := p.ListAllJobs(context.Background(), "proj")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListAllJobs = %+v, %v", jobs, err)
	}
	if j := jobs[0]; j.JobID != "from-plugin" || j.Region != "europe-west1" || j.PlacementClusterName != "c9" ||
		j.Status.State != "RUNNING" || j.Type != "sparkJob" || canonicalJSON(j.TypeJob) != `{"mainClass":"Main"}` {
		t.Fatalf("the console must show the plugin's job, got %+v", j)
	}
}
