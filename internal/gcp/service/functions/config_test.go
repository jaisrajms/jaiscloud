package functions

import (
	"context"
	"errors"
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// invalidArg asserts err is an InvalidArgument provider error.
func invalidArg(t *testing.T, err error) {
	t.Helper()
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if perr.Code != "InvalidArgument" {
		t.Fatalf("code = %q, want InvalidArgument (%v)", perr.Code, err)
	}
}

func TestInstanceConfigRoundTrip(t *testing.T) {
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	ctx := context.Background()

	in := FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{"runtime": "nodejs22", "entryPoint": "h"},
		"serviceConfig": map[string]any{
			"minInstanceCount":              float64(2),
			"maxInstanceCount":              float64(10),
			"maxInstanceRequestConcurrency": float64(80),
			"availableCpu":                  "1",
		},
	}, V2)
	f, _, err := s.CreateFunction(ctx, "p", "us-central1", "f1", in, V2)
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if f.MinInstanceCount != 2 || f.MaxInstanceCount != 10 || f.MaxInstanceRequestConcurrency != 80 || f.AvailableCPU != "1" {
		t.Fatalf("stored config = %+v", f)
	}

	// Re-read through the store and the shared renderer.
	got, err := s.GetFunction(ctx, "p", "us-central1", "f1")
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	sc, _ := FunctionJSON(V2, "p", got)["serviceConfig"].(map[string]any)
	if sc["minInstanceCount"] != 2 || sc["maxInstanceCount"] != 10 ||
		sc["maxInstanceRequestConcurrency"] != 80 || sc["availableCpu"] != "1" {
		t.Fatalf("v2 serviceConfig = %+v", sc)
	}
	// v1 has no instance/concurrency fields, so the v1 shape must not leak them.
	v1 := FunctionJSON(V1, "p", got)
	for _, k := range []string{"minInstanceCount", "maxInstanceCount", "maxInstanceRequestConcurrency", "availableCpu"} {
		if _, ok := v1[k]; ok {
			t.Fatalf("v1 shape must not carry %q: %+v", k, v1)
		}
	}
}

func TestInstanceConfigCreateValidation(t *testing.T) {
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	ctx := context.Background()

	v2 := func(sc map[string]any) FunctionInput {
		return FunctionInputFromMap(map[string]any{
			"buildConfig":   map[string]any{"runtime": "nodejs22"},
			"serviceConfig": sc,
		}, V2)
	}

	bad := []struct {
		name string
		in   FunctionInput
	}{
		{"maxInstanceCount too high", v2(map[string]any{"maxInstanceCount": float64(1001)})},
		{"minInstanceCount negative", v2(map[string]any{"minInstanceCount": float64(-1)})},
		{"minInstanceCount exceeds max", v2(map[string]any{"minInstanceCount": float64(5), "maxInstanceCount": float64(2)})},
		{"concurrency too high", v2(map[string]any{"maxInstanceRequestConcurrency": float64(1001)})},
		{"cpu below range", v2(map[string]any{"availableCpu": "0.01"})},
		{"cpu above range", v2(map[string]any{"availableCpu": "9"})},
		{"cpu not a number", v2(map[string]any{"availableCpu": "fast"})},
		{"cpu not a Cloud Run value", v2(map[string]any{"availableCpu": "3"})},
		{"cpu not a Cloud Run value", v2(map[string]any{"availableCpu": "1.5"})},
		{"cpu not in 0.01 increments", v2(map[string]any{"availableCpu": "0.085"})},
		{"cpu NaN", v2(map[string]any{"availableCpu": "NaN"})},
		{"sub-1 vCPU with concurrency > 1", v2(map[string]any{"availableCpu": "0.5", "maxInstanceRequestConcurrency": float64(80)})},
		{"memory not a valid v1 size", FunctionInputFromMap(map[string]any{
			"runtime": "nodejs20", "availableMemoryMb": float64(300),
		}, V1)},
	}
	for _, tc := range bad {
		if _, _, err := s.CreateFunction(ctx, "p", "us-central1", "f1", tc.in, versionOf(tc.in)); err == nil {
			t.Fatalf("%s: expected InvalidArgument", tc.name)
		} else {
			invalidArg(t, err)
		}
	}

	// Valid values are accepted.
	good := []FunctionInput{
		v2(map[string]any{"minInstanceCount": float64(1), "maxInstanceCount": float64(1000)}),
		v2(map[string]any{"maxInstanceRequestConcurrency": float64(1)}),
		v2(map[string]any{"availableCpu": "0.08"}),
		v2(map[string]any{"availableCpu": "0.75"}),
		v2(map[string]any{"availableCpu": "0.99"}),
		v2(map[string]any{"availableCpu": "2"}),
		v2(map[string]any{"availableCpu": "8"}),
		v2(map[string]any{"availableCpu": "500m"}),
		v2(map[string]any{"availableCpu": "1", "maxInstanceRequestConcurrency": float64(80)}),
		v2(map[string]any{"availableCpu": "0.5", "maxInstanceRequestConcurrency": float64(1)}),
		v2(map[string]any{"minInstanceCount": float64(3)}), // provisioned without a cap
		FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "availableMemoryMb": float64(128)}, V1),
		FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "availableMemoryMb": float64(8192)}, V1),
	}
	for i, in := range good {
		id := "g" + string(rune('a'+i))
		if _, _, err := s.CreateFunction(ctx, "p", "us-central1", id, in, versionOf(in)); err != nil {
			t.Fatalf("good[%d]: unexpected error: %v", i, err)
		}
	}
}

func TestInstanceConfigUpdate(t *testing.T) {
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	ctx := context.Background()

	create := func() {
		t.Helper()
		in := FunctionInputFromMap(map[string]any{
			"buildConfig": map[string]any{"runtime": "nodejs22"},
			"serviceConfig": map[string]any{
				"minInstanceCount": float64(3), "maxInstanceCount": float64(10), "availableCpu": "1",
			},
		}, V2)
		if _, _, err := s.CreateFunction(ctx, "p", "us-central1", "f1", in, V2); err != nil {
			t.Fatalf("CreateFunction: %v", err)
		}
	}
	create()

	// An empty-mask update whose body omits the config must not clear it.
	if _, _, err := s.UpdateFunction(ctx, "p", "us-central1", "f1",
		FunctionInputFromMap(map[string]any{"buildConfig": map[string]any{"runtime": "nodejs22"}}, V2), nil, V2); err != nil {
		t.Fatalf("UpdateFunction: %v", err)
	}
	got, _ := s.GetFunction(ctx, "p", "us-central1", "f1")
	if got.MinInstanceCount != 3 || got.MaxInstanceCount != 10 || got.AvailableCPU != "1" {
		t.Fatalf("empty-mask update cleared config: %+v", got)
	}

	// An empty-mask update whose body carries a value applies it.
	if _, _, err := s.UpdateFunction(ctx, "p", "us-central1", "f1",
		FunctionInputFromMap(map[string]any{"serviceConfig": map[string]any{"maxInstanceCount": float64(20)}}, V2), nil, V2); err != nil {
		t.Fatalf("UpdateFunction: %v", err)
	}
	got, _ = s.GetFunction(ctx, "p", "us-central1", "f1")
	if got.MaxInstanceCount != 20 {
		t.Fatalf("maxInstanceCount = %d, want 20", got.MaxInstanceCount)
	}

	// An explicit mask selecting the path applies an explicit 0 (clear).
	if _, _, err := s.UpdateFunction(ctx, "p", "us-central1", "f1",
		FunctionInputFromMap(map[string]any{"serviceConfig": map[string]any{"availableCpu": ""}}, V2),
		[]string{"serviceConfig.availableCpu"}, V2); err != nil {
		t.Fatalf("UpdateFunction clear: %v", err)
	}
	got, _ = s.GetFunction(ctx, "p", "us-central1", "f1")
	if got.AvailableCPU != "" {
		t.Fatalf("availableCpu = %q, want cleared", got.AvailableCPU)
	}

	// maxInstanceCount below minInstanceCount is rejected and leaves the record
	// unchanged.
	if _, _, err := s.UpdateFunction(ctx, "p", "us-central1", "f1",
		FunctionInputFromMap(map[string]any{"serviceConfig": map[string]any{"maxInstanceCount": float64(1)}}, V2),
		[]string{"serviceConfig.maxInstanceCount"}, V2); err == nil {
		t.Fatal("expected InvalidArgument for min > max")
	} else {
		invalidArg(t, err)
	}
	got, _ = s.GetFunction(ctx, "p", "us-central1", "f1")
	if got.MaxInstanceCount != 20 {
		t.Fatalf("rejected update was persisted: maxInstanceCount = %d", got.MaxInstanceCount)
	}

	// An explicit mask selecting a field applies an explicit 0 (clear).
	if _, _, err := s.UpdateFunction(ctx, "p", "us-central1", "f1",
		FunctionInputFromMap(map[string]any{"serviceConfig": map[string]any{"minInstanceCount": float64(0)}}, V2),
		[]string{"serviceConfig.minInstanceCount"}, V2); err != nil {
		t.Fatalf("UpdateFunction clear min: %v", err)
	}
	got, _ = s.GetFunction(ctx, "p", "us-central1", "f1")
	if got.MinInstanceCount != 0 || got.MaxInstanceCount != 20 {
		t.Fatalf("after clearing minInstanceCount: %+v", got)
	}
}

// versionOf infers the wire version an input was built for. The v2-only config
// fields are the discriminator; an input carrying none is treated as v1 (which
// is what the validation branches on for memory).
func versionOf(in FunctionInput) Version {
	if in.HasMinInstanceCount || in.HasMaxInstanceCount || in.HasMaxInstanceRequestConcurrency || in.HasAvailableCPU {
		return V2
	}
	return V1
}
