package scheduler

import (
	"context"
	"testing"

	core "jaiscloud/internal/gcp/service/scheduler"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
	"jaiscloud/internal/model"
)

func newProvider(t *testing.T) (*Provider, *core.Service) {
	t.Helper()
	mem := schedstore.NewMemoryStore()
	c := core.NewService(mem)
	p := NewProvider(c)
	return p, c
}

func req(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{Service: ServiceName, Params: params}
}

func jobBody() map[string]any {
	return map[string]any{
		"name":     "projects/p/locations/l/jobs/j1",
		"schedule": "* * * * *",
		"timeZone": "UTC",
		"httpTarget": map[string]any{
			"uri":        "http://example.test/hook",
			"httpMethod": "POST",
		},
	}
}

func TestProviderCRUD(t *testing.T) {
	ctx := context.Background()
	p, _ := newProvider(t)

	if _, err := p.CreateJob(ctx, req(map[string]any{"project": "p", "location": "l", "body": jobBody()})); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := p.GetJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"})); err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	resp, err := p.ListJobs(ctx, req(map[string]any{"project": "p", "location": "l"}))
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if got := resp.Data["jobs"].([]any); len(got) != 1 {
		t.Fatalf("jobs = %v", resp.Data["jobs"])
	}
	if _, err := p.UpdateJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1", "body": map[string]any{"description": "x"}, "updateMask": "description"})); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if _, err := p.PauseJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"})); err != nil {
		t.Fatalf("PauseJob: %v", err)
	}
	if _, err := p.ResumeJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"})); err != nil {
		t.Fatalf("ResumeJob: %v", err)
	}
	if _, err := p.RunJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"})); err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if _, err := p.DeleteJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"})); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := p.GetJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"})); err == nil {
		t.Fatalf("GetJob after delete: expected NotFound")
	}
	p.Reset(ctx)
	if resp, _ := p.ListJobs(ctx, req(map[string]any{"project": "p", "location": "l"})); len(resp.Data["jobs"].([]any)) != 0 {
		t.Fatalf("Reset left jobs")
	}
}

func TestProviderListPagination(t *testing.T) {
	ctx := context.Background()
	p, c := newProvider(t)
	for _, name := range []string{"a", "b", "c"} {
		if _, err := c.CreateJob(ctx, "p", "l", schedstore.Job{Name: name, Schedule: "* * * * *", Target: schedstore.TargetHTTP, HTTP: &schedstore.HttpTarget{URI: "http://x"}}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	resp, err := p.ListJobs(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": float64(2)}))
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(resp.Data["jobs"].([]any)) != 2 || resp.Data["nextPageToken"] != "2" {
		t.Fatalf("page1 = %v", resp.Data)
	}
	resp, err = p.ListJobs(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": float64(2), "pageToken": "2"}))
	if err != nil {
		t.Fatalf("ListJobs page2: %v", err)
	}
	if len(resp.Data["jobs"].([]any)) != 1 {
		t.Fatalf("page2 = %v", resp.Data)
	}
	if _, ok := resp.Data["nextPageToken"]; ok {
		t.Fatalf("unexpected nextPageToken on last page")
	}
}

func TestProviderEncodeError(t *testing.T) {
	codec := NewCodec()
	perr := model.NewProviderError("NotFound", "job not found", 404)
	st, hdr, out := codec.EncodeError(req(nil), perr)
	if st != 404 || hdr.Get("Content-Type") == "" || len(out) == 0 {
		t.Fatalf("EncodeError = %d %v %s", st, hdr, out)
	}
	if _, _, out := codec.Encode(req(nil), &model.ProviderResponse{Data: map[string]any{"a": 1}}); len(out) == 0 {
		t.Fatalf("Encode returned empty")
	}
}
