package scheduler

import (
	"context"
	"testing"
	"time"

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

// TestProviderRetryConfigDefaults pins the REST read of the RetryConfig a job
// answers when its create body carried none — the shape the Terraform provider
// produces for `retry_config = { retry_count = 0 }`, whose all-zero block it
// drops from the create body. The documented defaults (retry_count 0,
// min_backoff_duration 5s, max_backoff_duration 1h, max_doublings 5,
// max_retry_duration 0 = unlimited) must come back so the provider's read fills
// retry_config instead of leaving the block empty (TF3-4-2).
func TestProviderRetryConfigDefaults(t *testing.T) {
	ctx := context.Background()
	p, _ := newProvider(t)

	// No retryConfig in the create body.
	if _, err := p.CreateJob(ctx, req(map[string]any{"project": "p", "location": "l", "body": jobBody()})); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	resp, err := p.GetJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"}))
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	got, ok := resp.Data["retryConfig"].(map[string]any)
	if !ok {
		t.Fatalf("retryConfig = %#v, want the documented defaults", resp.Data["retryConfig"])
	}
	want := map[string]any{
		"retryCount":         int32(0),
		"maxRetryDuration":   "0s",
		"minBackoffDuration": "5s",
		"maxBackoffDuration": "3600s",
		"maxDoublings":       int32(5),
	}
	if len(got) != len(want) {
		t.Fatalf("retryConfig = %#v, want %#v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("retryConfig.%s = %#v, want %#v", k, got[k], v)
		}
	}

	// An all-zero retryConfig (the provider's dropped block, if a client sends
	// it explicitly) reads back the same defaults.
	body := jobBody()
	body["name"] = "projects/p/locations/l/jobs/j2"
	body["retryConfig"] = map[string]any{}
	created, err := p.CreateJob(ctx, req(map[string]any{"project": "p", "location": "l", "body": body}))
	if err != nil {
		t.Fatalf("CreateJob(all-zero retryConfig): %v", err)
	}
	if rc, ok := created.Data["retryConfig"].(map[string]any); !ok || rc["minBackoffDuration"] != "5s" {
		t.Fatalf("all-zero retryConfig read = %#v, want the documented defaults", created.Data["retryConfig"])
	}

	// A non-default config is echoed verbatim.
	body["name"] = "projects/p/locations/l/jobs/j3"
	body["retryConfig"] = map[string]any{
		"retryCount":         float64(3),
		"maxDoublings":       float64(2),
		"minBackoffDuration": "2s",
		"maxBackoffDuration": "20s",
		"maxRetryDuration":   "1m",
	}
	created, err = p.CreateJob(ctx, req(map[string]any{"project": "p", "location": "l", "body": body}))
	if err != nil {
		t.Fatalf("CreateJob(custom retryConfig): %v", err)
	}
	rc, ok := created.Data["retryConfig"].(map[string]any)
	if !ok || rc["retryCount"] != int32(3) || rc["maxDoublings"] != int32(2) ||
		rc["minBackoffDuration"] != "2s" || rc["maxBackoffDuration"] != "20s" || rc["maxRetryDuration"] != "60s" {
		t.Fatalf("custom retryConfig read = %#v", created.Data["retryConfig"])
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

func TestProviderListJobsNegativePageSize(t *testing.T) {
	ctx := context.Background()
	p, _ := newProvider(t)
	// AIP-158: projects.locations.jobs.list documents no non-positive exception,
	// so a negative pageSize is INVALID_ARGUMENT instead of a silent first page.
	_, err := p.ListJobs(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": float64(-1)}))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("ListJobs(pageSize=-1) err = %v, want InvalidArgument/400", err)
	}
	// Zero still selects the documented maximum (500).
	if _, err := p.ListJobs(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": float64(0)})); err != nil {
		t.Fatalf("ListJobs(pageSize=0): %v", err)
	}
	// A non-integer value fails the int32 query binding instead of being folded
	// into zero (an absent parameter).
	for _, v := range []any{"abc", "-0.5", "1.5"} {
		_, err := p.ListJobs(ctx, req(map[string]any{"project": "p", "location": "l", "pageSize": v}))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Fatalf("ListJobs(pageSize=%#v) err = %v, want InvalidArgument/400", v, err)
		}
	}
}

func TestProviderPauseClearsRunState(t *testing.T) {
	ctx := context.Background()
	mem := schedstore.NewMemoryStore()
	p := NewProvider(core.NewService(mem))
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Seed an ENABLED job that has already fired with a failed status; the REST
	// read reports all three run-state fields.
	if err := mem.CreateJob(ctx, "p", "l", schedstore.Job{
		Name:            "j1",
		State:           schedstore.StateEnabled,
		Schedule:        "* * * * *",
		TimeZone:        "UTC",
		Target:          schedstore.TargetHTTP,
		HTTP:            &schedstore.HttpTarget{URI: "http://example.test/hook"},
		ScheduleTime:    at.Add(time.Minute),
		LastAttemptTime: at,
		Status:          &schedstore.Status{Code: 13, Message: "boom"},
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	resp, err := p.GetJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"}))
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	for _, field := range []string{"scheduleTime", "lastAttemptTime", "status"} {
		if _, ok := resp.Data[field]; !ok {
			t.Fatalf("an ENABLED job that fired did not render %s", field)
		}
	}

	// Real Cloud Scheduler answers a paused job with no next run and no last
	// attempt: :pause removes them from the job and both the response and a
	// subsequent read leave scheduleTime/lastAttemptTime/status absent
	// (TF2-12-1).
	paused, err := p.PauseJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"}))
	if err != nil {
		t.Fatalf("PauseJob: %v", err)
	}
	if got := paused.Data["state"]; got != "PAUSED" {
		t.Fatalf("PauseJob state = %v, want PAUSED", got)
	}
	for _, field := range []string{"scheduleTime", "lastAttemptTime", "status"} {
		if v, ok := paused.Data[field]; ok {
			t.Fatalf("pause response rendered %s = %v", field, v)
		}
	}
	read, err := p.GetJob(ctx, req(map[string]any{"project": "p", "location": "l", "job": "j1"}))
	if err != nil {
		t.Fatalf("GetJob after pause: %v", err)
	}
	for _, field := range []string{"scheduleTime", "lastAttemptTime", "status"} {
		if v, ok := read.Data[field]; ok {
			t.Fatalf("a paused job read back %s = %v", field, v)
		}
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
