package ui

import (
	"context"
	"reflect"
	"testing"

	"jaiscloud/internal/gcp/service/scheduler"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
	restscheduler "jaiscloud/internal/gcp/transport/rest/scheduler"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

func newScheduler(t *testing.T) (*scheduler.Service, map[string]provider.HandlerFunc) {
	t.Helper()
	core := scheduler.NewService(schedstore.NewMemoryStore())
	return core, restscheduler.NewProvider(core).Routes()
}

func httpJob(name string) schedstore.Job {
	return schedstore.Job{
		Name: name, Schedule: "* * * * *", TimeZone: "UTC", Target: schedstore.TargetHTTP,
		HTTP: &schedstore.HttpTarget{URI: "https://example.com/hook", HTTPMethod: "POST"},
	}
}

// With a plugin that only watches, the console gets the same jobs it would get
// directly, and the plugin saw each call as origin ui under its REST key.
func TestSchedulerConsolePassthroughMatchesDirect(t *testing.T) {
	core, builtins := newScheduler(t)
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for _, k := range []string{"Scheduler.JobsList", "Scheduler.JobsCreate", "Scheduler.JobsGet", "Scheduler.JobsPatch",
		"Scheduler.JobsPause", "Scheduler.JobsResume", "Scheduler.JobsRun", "Scheduler.JobsDelete"} {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeScheduler(typedFixture(builtins, wrap), core)
	ctx := context.Background()

	created, err := p.CreateJob(ctx, "proj", "us-central1", httpJob("j1"))
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	direct, err := core.GetJob(ctx, "proj", "us-central1", "j1")
	if err != nil {
		t.Fatal(err)
	}
	if created.Name != "j1" || created.ProjectID != "proj" || created.Location != "us-central1" || created.Schedule != direct.Schedule {
		t.Fatalf("created = %+v, stored = %+v", created, direct)
	}

	got, err := p.GetJob(ctx, "proj", "us-central1", "j1")
	if err != nil || !reflect.DeepEqual(got, direct) {
		t.Fatalf("through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, direct, err)
	}
	list, err := p.ListJobsByProject(ctx, "proj")
	if err != nil || len(list) != 1 || list[0].Name != "j1" || list[0].Location != "us-central1" {
		t.Fatalf("ListJobsByProject = %+v, %v", list, err)
	}

	upd := httpJob("j1")
	upd.Description = "patched"
	if j, err := p.UpdateJob(ctx, "proj", "us-central1", "j1", upd, []string{"description"}); err != nil || j.Description != "patched" {
		t.Fatalf("UpdateJob = %+v, %v", j, err)
	}
	if j, err := p.PauseJob(ctx, "proj", "us-central1", "j1"); err != nil || j.State != schedstore.StatePaused {
		t.Fatalf("PauseJob = %+v, %v", j, err)
	}
	if j, err := p.ResumeJob(ctx, "proj", "us-central1", "j1"); err != nil || j.State != schedstore.StateEnabled {
		t.Fatalf("ResumeJob = %+v, %v", j, err)
	}
	if _, err := p.RunJob(ctx, "proj", "us-central1", "j1"); err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if err := p.DeleteJob(ctx, "proj", "us-central1", "j1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := core.GetJob(ctx, "proj", "us-central1", "j1"); err == nil {
		t.Fatal("the job must be gone after DeleteJob")
	}

	want := []string{"Scheduler.JobsCreate", "Scheduler.JobsGet", "Scheduler.JobsList", "Scheduler.JobsPatch",
		"Scheduler.JobsPause", "Scheduler.JobsResume", "Scheduler.JobsRun", "Scheduler.JobsDelete"}
	var got2 []string
	for _, s := range seen {
		got2 = append(got2, s.key)
		if s.origin != model.OriginUI {
			t.Errorf("%s: plugin saw origin %q, want ui", s.key, s.origin)
		}
	}
	if !reflect.DeepEqual(got2, want) {
		t.Fatalf("plugin saw %v, want %v", got2, want)
	}
}

// A key no plugin covers is not touched: the console calls the core directly.
func TestSchedulerConsoleUncoveredKeyIsDirect(t *testing.T) {
	core, builtins := newScheduler(t)
	var seen []seenCall
	p := routeScheduler(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Scheduler.JobsGet": passthrough(&seen, "Scheduler.JobsGet"),
	}), core)
	ctx := context.Background()
	if _, err := p.CreateJob(ctx, "proj", "us-central1", httpJob("j2")); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 0 {
		t.Fatalf("CreateJob is not covered, yet the plugin saw %v", seen)
	}
	if _, err := p.GetJob(ctx, "proj", "us-central1", "j2"); err != nil || len(seen) != 1 {
		t.Fatalf("GetJob is covered: err=%v seen=%v", err, seen)
	}
}

// A plugin that replaces a key is what the console shows.
func TestSchedulerConsoleShowsWhatThePluginServes(t *testing.T) {
	core, builtins := newScheduler(t)
	p := routeScheduler(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Scheduler.JobsList": replaceWith(map[string]any{"jobs": []any{map[string]any{
			"name": "projects/proj/locations/europe-west1/jobs/from-plugin", "schedule": "0 * * * *", "state": "ENABLED",
			"httpTarget": map[string]any{"uri": "https://plugin.example/x", "httpMethod": "GET"},
		}}}),
	}), core)
	jobs, err := p.ListJobsByProject(context.Background(), "proj")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobsByProject = %+v, %v", jobs, err)
	}
	if j := jobs[0]; j.Name != "from-plugin" || j.Location != "europe-west1" || j.ProjectID != "proj" || j.Schedule != "0 * * * *" {
		t.Fatalf("the console must show the plugin's job, got %+v", j)
	}
}

// A before hook's patch of the request reaches the built-in.
func TestSchedulerConsoleBeforePatchReachesTheBuiltIn(t *testing.T) {
	core, builtins := newScheduler(t)
	patch := func(next provider.HandlerFunc) provider.HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			body := map[string]any{}
			for k, v := range nr.Params["body"].(map[string]any) {
				body[k] = v
			}
			body["description"] = "stamped by plugin"
			nr.Params["body"] = body
			return next(ctx, nr)
		}
	}
	p := routeScheduler(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{"Scheduler.JobsCreate": patch}), core)
	if _, err := p.CreateJob(context.Background(), "proj", "us-central1", httpJob("j3")); err != nil {
		t.Fatal(err)
	}
	stored, err := core.GetJob(context.Background(), "proj", "us-central1", "j3")
	if err != nil || stored.Description != "stamped by plugin" {
		t.Fatalf("stored = %+v, %v", stored, err)
	}
}

// A plugin error reaches the console as the same kind of error.
func TestSchedulerConsolePluginErrorReachesTheConsole(t *testing.T) {
	core, builtins := newScheduler(t)
	deny := func(provider.HandlerFunc) provider.HandlerFunc {
		return func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return nil, model.NewProviderError("PermissionDenied", "no jobs today", 403)
		}
	}
	p := routeScheduler(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{"Scheduler.JobsCreate": deny}), core)
	_, err := p.CreateJob(context.Background(), "proj", "us-central1", httpJob("j4"))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 403 {
		t.Fatalf("err = %v, want the plugin's 403", err)
	}
}
