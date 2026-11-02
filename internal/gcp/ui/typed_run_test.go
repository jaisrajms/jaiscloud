package ui

import (
	"context"
	"reflect"
	"testing"

	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	restrun "jaiscloud/internal/gcp/transport/rest/run"
	runui "jaiscloud/internal/gcp/ui/run"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

func TestRunConsolePassthroughMatchesDirect(t *testing.T) {
	core := runcore.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	builtins := restrun.NewProvider(core).Routes()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeRun(typedFixture(builtins, wrap), runui.NewProvider(core))
	ctx := context.Background()
	if _, err := core.CreateService(ctx, "proj", "us-central1", "svc", map[string]any{"template": map[string]any{
		"containers": []any{map[string]any{"image": "nginx:latest"}}}}, false); err != nil {
		t.Fatal(err)
	}

	want, err := core.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.GetService(ctx, "proj", "us-central1", "svc"); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("service through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, want, err)
	}
	if list, err := p.ListAllServices(ctx, "proj"); err != nil || len(list) != 1 || !reflect.DeepEqual(list[0], want) {
		t.Fatalf("ListAllServices = %+v, %v", list, err)
	}
	revs, err := core.ListRevisions(ctx, "proj", "us-central1", "svc")
	if err != nil || len(revs) == 0 {
		t.Fatalf("direct revisions = %+v, %v", revs, err)
	}
	if got, err := p.ListRevisions(ctx, "proj", "us-central1", "svc"); err != nil || !reflect.DeepEqual(got, revs) {
		t.Fatalf("revisions through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, revs, err)
	}
	if got, err := p.GetRevision(ctx, "proj", "us-central1", "svc", revs[0].ID); err != nil || !reflect.DeepEqual(got, revs[0]) {
		t.Fatalf("revision through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, revs[0], err)
	}
	created, err := p.CreateService(ctx, "proj", "us-central1", "svc2", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": "nginx:latest"}}},
	}, false)
	if err != nil || created.ID != "svc2" {
		t.Fatalf("CreateService = %+v, %v", created, err)
	}
	updated, err := p.UpdateService(ctx, "proj", "us-central1", "svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": "nginx:2"}}},
	}, "template", false, false)
	if err != nil || updated.ID != "svc" {
		t.Fatalf("UpdateService = %+v, %v", updated, err)
	}
	if pol, err := p.ServiceGetIamPolicy(ctx, "proj", "us-central1", "svc"); err != nil || pol == nil {
		t.Fatalf("ServiceGetIamPolicy = %+v, %v", pol, err)
	}
	if _, err := p.ServiceSetIamPolicy(ctx, "proj", "us-central1", "svc", map[string]any{"policy": map[string]any{}}); err != nil {
		t.Fatalf("ServiceSetIamPolicy: %v", err)
	}
	if perms, err := p.ServiceTestIamPermissions(ctx, "proj", "us-central1", "svc", []string{"run.services.get"}); err != nil || len(perms) != 1 {
		t.Fatalf("ServiceTestIamPermissions = %+v, %v", perms, err)
	}

	op, err := p.DeleteService(ctx, "proj", "us-central1", "svc", false, "")
	if err != nil || op.ID == "" || !op.Done {
		t.Fatalf("DeleteService = %+v, %v", op, err)
	}
	if _, err := core.GetService(ctx, "proj", "us-central1", "svc"); err == nil {
		t.Fatal("the service must be gone")
	}
	if len(seen) != 10 {
		t.Fatalf("the plugin saw %d calls, want 10", len(seen))
	}
}

func TestRunConsoleShowsWhatThePluginServes(t *testing.T) {
	core := runcore.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	p := routeRun(typedFixture(restrun.NewProvider(core).Routes(), map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Run.ListServices": replaceWith(map[string]any{"services": []any{map[string]any{
			"name": "projects/proj/locations/europe-west1/services/from-plugin", "uri": "https://from-plugin.example", "generation": "3"}}}),
	}), runui.NewProvider(core))
	svcs, err := p.ListAllServices(context.Background(), "proj")
	if err != nil || len(svcs) != 1 {
		t.Fatalf("ListAllServices = %+v, %v", svcs, err)
	}
	if s := svcs[0]; s.ID != "from-plugin" || s.Location != "europe-west1" || s.Uri != "https://from-plugin.example" || s.Generation != 3 {
		t.Fatalf("the console must show the plugin's service, got %+v", s)
	}
}

// Without a plugin the new run methods call the typed core directly.
func TestRunConsoleDeployAndIamStayDirectWithoutAPlugin(t *testing.T) {
	core := runcore.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	p := routeRun(typedFixture(restrun.NewProvider(core).Routes(), map[string]func(provider.HandlerFunc) provider.HandlerFunc{}), runui.NewProvider(core))
	ctx := context.Background()

	svc, err := p.CreateService(ctx, "proj", "us-central1", "svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": "nginx"}}},
	}, false)
	if err != nil || svc.ID != "svc" {
		t.Fatalf("CreateService = %+v, %v", svc, err)
	}
	if _, err := p.UpdateService(ctx, "proj", "us-central1", "svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": "nginx:2"}}},
	}, "template", false, false); err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	if _, err := p.ServiceGetIamPolicy(ctx, "proj", "us-central1", "svc"); err != nil {
		t.Fatalf("ServiceGetIamPolicy: %v", err)
	}
	if _, err := p.ServiceSetIamPolicy(ctx, "proj", "us-central1", "svc", map[string]any{"policy": map[string]any{}}); err != nil {
		t.Fatalf("ServiceSetIamPolicy: %v", err)
	}
	if _, err := p.ServiceTestIamPermissions(ctx, "proj", "us-central1", "svc", []string{"run.services.get"}); err != nil {
		t.Fatalf("ServiceTestIamPermissions: %v", err)
	}
}
