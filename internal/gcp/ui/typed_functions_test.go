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
	"jaiscloud/internal/model"
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

func TestFunctionsConsoleCallsAndPolicyThePluginServes(t *testing.T) {
	_, direct, builtins := newFunctions()
	pol := map[string]any{"version": float64(1), "etag": "e1", "bindings": []any{map[string]any{"role": "roles/viewer", "members": []any{"user:a@b.c"}}}}
	p := routeFunctions(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Function.CallFunction":         replaceWith(map[string]any{"executionId": "ex1", "result": "ok", "error": "boom"}),
		"Function.FunctionGetIamPolicy": replaceWith(pol),
		"Function.FunctionSetIamPolicy": replaceWith(pol),
	}), direct)
	ctx := context.Background()
	if id, result, errText, err := p.CallFunction(ctx, "proj", "us-central1", "f1", `{"a":1}`); err != nil || id != "ex1" || result != "ok" || errText != "boom" {
		t.Fatalf("CallFunction = %q %q %q, %v", id, result, errText, err)
	}
	for name, call := range map[string]func() (int, error){
		"GetIamPolicy": func() (int, error) {
			got, err := p.GetIamPolicy(ctx, "proj", "us-central1", "f1")
			return len(got.Bindings), err
		},
		"SetIamPolicy": func() (int, error) {
			got, err := p.SetIamPolicy(ctx, "proj", "us-central1", "f1", map[string]any{"policy": pol})
			return len(got.Bindings), err
		},
	} {
		if n, err := call(); err != nil || n != 1 {
			t.Fatalf("%s: %d bindings, %v", name, n, err)
		}
	}
}

func TestFunctionsConsoleCreateThroughThePlugin(t *testing.T) {
	_, direct, builtins := newFunctions()
	fn := map[string]any{"name": "projects/proj/locations/us-central1/functions/f1", "state": "ACTIVE",
		"buildConfig": map[string]any{"runtime": "go122", "entryPoint": "Run"}}
	in := functionsui.CreateInput{ID: "f1", Location: "us-central1", Runtime: "go122", EntryPoint: "Run", SourceBucket: "b", SourceObject: "o.zip"}

	// A finished operation carries the function in "response".
	p := routeFunctions(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Function.CreateFunction": replaceWith(map[string]any{"done": true, "response": fn}),
	}), direct)
	if got, err := p.CreateFunction(context.Background(), "proj", "us-central1", in); err != nil || got.ID != "f1" || got.Runtime != "go122" {
		t.Fatalf("CreateFunction (operation response) = %+v, %v", got, err)
	}

	// The plugin may also answer with the function itself.
	p = routeFunctions(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Function.CreateFunction": replaceWith(fn),
	}), direct)
	if got, err := p.CreateFunction(context.Background(), "proj", "us-central1", in); err != nil || got.ID != "f1" {
		t.Fatalf("CreateFunction (bare function) = %+v, %v", got, err)
	}

	// A bucket without an object is refused before the plugin is asked.
	asked := false
	p = routeFunctions(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Function.CreateFunction": func(next provider.HandlerFunc) provider.HandlerFunc {
			return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
				asked = true
				return next(ctx, nr)
			}
		},
	}), direct)
	bad := in
	bad.SourceObject = ""
	if _, err := p.CreateFunction(context.Background(), "proj", "us-central1", bad); err == nil {
		t.Fatal("a source bucket without an object must be refused")
	}
	if asked {
		t.Fatal("the plugin must not be asked about a request that is refused first")
	}
}
