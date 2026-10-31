package ui

import (
	"context"
	"reflect"
	"testing"

	"jaiscloud/internal/blobfs"

	functionscore "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	restfunctions "jaiscloud/internal/gcp/transport/rest/functions"
	functionsui "jaiscloud/internal/gcp/ui/functions"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

type noBuckets struct{}

func (noBuckets) EnsureBucket(context.Context, string, string, string) error { return nil }

func newFunctions() (*functionscore.Service, functionsui.ProviderInterface, map[string]provider.HandlerFunc) {
	core := functionscore.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(), functionscore.WithBlobs(blobfs.NewMemoryBlobStore()), functionscore.WithSourceBuckets(noBuckets{}))
	return core, functionsui.NewProvider(core, nil), restfunctions.NewProvider(core, "proj").Routes()
}

func TestFunctionsConsolePassthroughMatchesDirect(t *testing.T) {
	_, direct, builtins := newFunctions()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeFunctions(typedFixture(builtins, wrap), direct)
	ctx := context.Background()

	f, err := p.CreateFunction(ctx, "proj", "us-central1", functionsui.CreateInput{
		ID: "fn1", Runtime: "nodejs20", EntryPoint: "handler", Description: "hello", AvailableMemoryMB: 256, Timeout: "60s",
		MaxInstanceCount: 3, Labels: map[string]string{"team": "a"}})
	if err != nil || f.ID != "fn1" || f.Location != "us-central1" {
		t.Fatalf("CreateFunction = %+v, %v", f, err)
	}
	want, err := direct.GetFunction(ctx, "proj", "us-central1", "fn1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.GetFunction(ctx, "proj", "us-central1", "fn1")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("function through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, want, err)
	}
	if list, err := p.ListFunctions(ctx, "proj"); err != nil || len(list) != 1 || !reflect.DeepEqual(list[0], want) {
		t.Fatalf("ListFunctions = %+v, %v", list, err)
	}
	if target, err := p.GenerateUploadURL(ctx, "proj", "us-central1"); err != nil || target.Bucket == "" || target.Object == "" {
		t.Fatalf("GenerateUploadURL = %+v, %v", target, err)
	}
	if pol, err := p.GetIamPolicy(ctx, "proj", "us-central1", "fn1"); err != nil {
		t.Fatalf("GetIamPolicy = %+v, %v", pol, err)
	}
	if err := p.DeleteFunction(ctx, "proj", "us-central1", "fn1"); err != nil {
		t.Fatalf("DeleteFunction: %v", err)
	}
	if len(seen) != 6 {
		t.Fatalf("the plugin saw %d calls (%v), want 6", len(seen), seen)
	}
}

func TestFunctionsConsoleShowsWhatThePluginServes(t *testing.T) {
	_, direct, builtins := newFunctions()
	p := routeFunctions(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Function.ListFunctions": replaceWith(map[string]any{"functions": []any{map[string]any{
			"name": "projects/proj/locations/europe-west1/functions/from-plugin", "state": "ACTIVE",
			"buildConfig":   map[string]any{"runtime": "go122", "entryPoint": "Run"},
			"serviceConfig": map[string]any{"availableMemory": "512M", "timeoutSeconds": float64(30)}}}}),
	}), direct)
	fns, err := p.ListFunctions(context.Background(), "proj")
	if err != nil || len(fns) != 1 {
		t.Fatalf("ListFunctions = %+v, %v", fns, err)
	}
	if f := fns[0]; f.ID != "from-plugin" || f.Location != "europe-west1" || f.Runtime != "go122" || f.AvailableMemoryMB != 512 || f.Timeout != "30s" {
		t.Fatalf("the console must show the plugin's function, got %+v", f)
	}
}
