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

func TestDataprocConsoleShowsAJobAndATemplateThePluginServes(t *testing.T) {
	_, direct, builtins := newDataproc()
	job := map[string]any{
		"reference": map[string]any{"projectId": "proj", "jobId": "j7"}, "placement": map[string]any{"clusterName": "c1"},
		"status": map[string]any{"state": "DONE"}, "hiveJob": map[string]any{"queryFileUri": "gs://b/q.sql"}}
	p := routeDataproc(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Dataproc.GetJob":    replaceWith(job),
		"Dataproc.CancelJob": replaceWith(job),
		"Dataproc.GetWorkflowTemplate": replaceWith(map[string]any{
			"id": "wt1", "name": "projects/proj/regions/europe-west1/workflowTemplates/wt1", "version": float64(3),
			"createTime": "2026-01-02T03:04:05Z", "placement": map[string]any{"managedCluster": map[string]any{"clusterName": "tmp"}}}),
		"Dataproc.ListWorkflowTemplates": replaceWith(map[string]any{"templates": []any{map[string]any{
			"id": "wt2", "name": "projects/proj/regions/us-east1/workflowTemplates/wt2"}}}),
	}), direct)
	ctx := context.Background()
	for _, get := range []func() (dpstore.Job, error){
		func() (dpstore.Job, error) { return p.GetJob(ctx, "proj", "us-central1", "j7") },
		func() (dpstore.Job, error) { return p.CancelJob(ctx, "proj", "us-central1", "j7") },
	} {
		j, err := get()
		if err != nil || j.JobID != "j7" || j.Region != "us-central1" || j.PlacementClusterName != "c1" || j.Type != "hiveJob" || j.Status.State != "DONE" {
			t.Fatalf("job = %+v, %v", j, err)
		}
	}
	tpl, err := p.GetWorkflowTemplate(ctx, "proj", "us-central1", "wt1")
	if err != nil || tpl.TemplateID != "wt1" || tpl.Region != "europe-west1" || tpl.Version != 3 || tpl.ProjectID != "proj" || tpl.CreateTime.IsZero() ||
		canonicalJSON(tpl.Definition) != `{"placement":{"managedCluster":{"clusterName":"tmp"}}}` {
		t.Fatalf("template = %+v, %v", tpl, err)
	}
	if all, err := p.ListAllWorkflowTemplates(ctx, "proj"); err != nil || len(all) != 1 || all[0].TemplateID != "wt2" || all[0].Region != "us-east1" {
		t.Fatalf("ListAllWorkflowTemplates = %+v, %v", all, err)
	}
}

// An unreachable direct job or template is the built-in's answer when no plugin covers the key.
func TestDataprocConsoleJobAndTemplateCallsStayDirectWithoutAPlugin(t *testing.T) {
	_, direct, builtins := newDataproc()
	p := routeDataproc(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{}), direct)
	ctx := context.Background()
	_, wantJob := direct.GetJob(ctx, "proj", "us-central1", "nope")
	if _, err := p.GetJob(ctx, "proj", "us-central1", "nope"); (err == nil) != (wantJob == nil) {
		t.Fatalf("GetJob err = %v, built-in %v", err, wantJob)
	}
	_, wantCancel := direct.CancelJob(ctx, "proj", "us-central1", "nope")
	if _, err := p.CancelJob(ctx, "proj", "us-central1", "nope"); (err == nil) != (wantCancel == nil) {
		t.Fatalf("CancelJob err = %v, built-in %v", err, wantCancel)
	}
	_, wantTpl := direct.GetWorkflowTemplate(ctx, "proj", "us-central1", "nope")
	if _, err := p.GetWorkflowTemplate(ctx, "proj", "us-central1", "nope"); (err == nil) != (wantTpl == nil) {
		t.Fatalf("GetWorkflowTemplate err = %v, built-in %v", err, wantTpl)
	}
}
