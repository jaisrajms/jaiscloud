package functions

import (
	"context"
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

func TestParseFunctionName(t *testing.T) {
	project, location, id, err := ParseFunctionName("projects/p/locations/us-central1/functions/f1")
	if err != nil {
		t.Fatalf("ParseFunctionName: %v", err)
	}
	if project != "p" || location != "us-central1" || id != "f1" {
		t.Fatalf("got %q %q %q", project, location, id)
	}
	// Relative form is accepted.
	if _, loc, id, err := ParseFunctionName("locations/europe-west1/functions/f2"); err != nil || loc != "europe-west1" || id != "f2" {
		t.Fatalf("relative: loc=%q id=%q err=%v", loc, id, err)
	}
	if _, _, _, err := ParseFunctionName("bogus"); err == nil {
		t.Fatal("expected InvalidArgument for malformed name")
	}
}

func TestParseLocationParent(t *testing.T) {
	project, location, err := ParseLocationParent("projects/p/locations/us-central1")
	if err != nil || project != "p" || location != "us-central1" {
		t.Fatalf("got %q %q %v", project, location, err)
	}
	if _, _, err := ParseLocationParent("projects/p"); err == nil {
		t.Fatal("expected error for parent without location")
	}
}

func TestVersionFromAPI(t *testing.T) {
	if VersionFromAPI("v2") != V2 {
		t.Fatal("v2 not detected")
	}
	if VersionFromAPI("v1") != V1 || VersionFromAPI("") != V1 {
		t.Fatal("v1 should be the default")
	}
}

func TestFunctionInputFromMapV1V2(t *testing.T) {
	v1 := FunctionInputFromMap(map[string]any{
		"runtime":              "nodejs20",
		"entryPoint":           "h",
		"availableMemoryMb":    float64(512),
		"timeout":              "120s",
		"environmentVariables": map[string]any{"K": "V"},
		"eventTrigger":         map[string]any{"eventType": "google.pubsub.topic.publish", "resource": "t"},
	}, V1)
	if v1.Runtime != "nodejs20" || v1.EntryPoint != "h" || v1.AvailableMemoryMB != 512 || v1.Timeout != "120s" {
		t.Fatalf("v1 input = %+v", v1)
	}
	if v1.EventTrigger == nil || v1.EventTrigger.Resource != "t" {
		t.Fatalf("v1 event trigger = %+v", v1.EventTrigger)
	}

	v2 := FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{
			"runtime":              "nodejs22",
			"entryPoint":           "h2",
			"environmentVariables": map[string]any{"A": "B"},
			"source":               map[string]any{"storageSource": map[string]any{"bucket": "b", "object": "o.zip"}},
		},
		"serviceConfig": map[string]any{"availableMemory": "1G", "timeoutSeconds": float64(300)},
		"eventTrigger":  map[string]any{"eventType": "google.pubsub.topic.publish", "pubsubTopic": "t2"},
	}, V2)
	if v2.Runtime != "nodejs22" || v2.EntryPoint != "h2" || v2.AvailableMemoryMB != 1024 || v2.Timeout != "300s" {
		t.Fatalf("v2 input = %+v", v2)
	}
	if v2.SourceArchiveURL != "gs://b/o.zip" || v2.SourceBucket != "b" || v2.SourceObject != "o.zip" {
		t.Fatalf("v2 source = %+v", v2)
	}
	if v2.EventTrigger == nil || v2.EventTrigger.Resource != "t2" {
		t.Fatalf("v2 event trigger = %+v", v2.EventTrigger)
	}
}

func TestApplyFunctionUpdateMask(t *testing.T) {
	f := functionsstore.Function{Runtime: "nodejs20", EntryPoint: "h", Description: "d0"}
	if err := ApplyFunctionUpdate(&f, FunctionInput{Runtime: "nodejs22", Description: "ignored"}, []string{"runtime"}); err != nil {
		t.Fatalf("ApplyFunctionUpdate: %v", err)
	}
	if f.Runtime != "nodejs22" || f.Description != "d0" {
		t.Fatalf("masked update = %+v", f)
	}
	if err := ApplyFunctionUpdate(&f, FunctionInput{}, []string{"bogus"}); err == nil {
		t.Fatal("expected Unimplemented for unknown mask path")
	}
	// v2 nested mask paths are accepted in both the proto's snake_case and the
	// JSON shape's camelCase.
	f2 := functionsstore.Function{Runtime: "nodejs20"}
	if err := ApplyFunctionUpdate(&f2, FunctionInput{Runtime: "nodejs23"}, []string{"build_config.runtime"}); err != nil {
		t.Fatalf("snake_case v2 mask: %v", err)
	}
	if f2.Runtime != "nodejs23" {
		t.Fatalf("snake_case v2 mask did not apply: %+v", f2)
	}
	if err := ApplyFunctionUpdate(&f2, FunctionInput{Timeout: "90s"}, []string{"service_config.timeout_seconds"}); err != nil {
		t.Fatalf("snake_case v2 timeout mask: %v", err)
	}
	if f2.Timeout != "90s" {
		t.Fatalf("snake_case v2 timeout mask did not apply: %+v", f2)
	}
}

func TestFunctionJSONVersions(t *testing.T) {
	f := functionsstore.Function{
		ID: "f1", Location: "us-central1", Runtime: "nodejs20", Status: "ACTIVE", HttpsTriggerURL: "https://x",
	}
	v1 := FunctionJSON(V1, "p", f)
	if v1["status"] != "ACTIVE" || v1["name"] != "projects/p/locations/us-central1/functions/f1" {
		t.Fatalf("v1 = %+v", v1)
	}
	v2 := FunctionJSON(V2, "p", f)
	if v2["state"] != "ACTIVE" {
		t.Fatalf("v2 = %+v", v2)
	}
	if v2["url"] != "https://x" {
		t.Fatalf("v2 url = %v, want https://x", v2["url"])
	}
	if v2["environment"] != "GEN_2" {
		t.Fatalf("v2 environment = %v, want GEN_2", v2["environment"])
	}
	if sc, _ := v2["serviceConfig"].(map[string]any); sc["service"] != "projects/p/locations/us-central1/services/f1" {
		t.Fatalf("v2 serviceConfig.service = %v", sc["service"])
	}
	if _, ok := v2["status"]; ok {
		t.Fatalf("v2 must not carry v1 status: %+v", v2)
	}
}

func TestFunctionInputV2SourceUploadURL(t *testing.T) {
	in := FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{
			"runtime": "nodejs20",
			"source":  map[string]any{"storageSource": map[string]any{"sourceUploadUrl": "https://upload.example/x.zip"}},
		},
	}, V2)
	if in.SourceUploadURL != "https://upload.example/x.zip" {
		t.Fatalf("sourceUploadUrl = %q", in.SourceUploadURL)
	}
	// It round-trips back through the v2 renderer as a sourceUploadUrl.
	out := FunctionJSON(V2, "p", functionsstore.Function{ID: "f1", Location: "l", SourceUploadURL: in.SourceUploadURL, Status: "ACTIVE"})
	bc, _ := out["buildConfig"].(map[string]any)
	src, _ := bc["source"].(map[string]any)
	if src["sourceUploadUrl"] != in.SourceUploadURL {
		t.Fatalf("rendered source = %+v", src)
	}
}

func TestServiceReset(t *testing.T) {
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	ctx := context.Background()
	if _, _, err := s.CreateFunction(ctx, "p", "us-central1", "f1", FunctionInput{Runtime: "nodejs20"}, V1); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	s.Reset(ctx)
	fns, _, err := s.ListFunctions(ctx, "p", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("ListFunctions: %v", err)
	}
	if len(fns) != 0 {
		t.Fatalf("expected no functions after reset, got %d", len(fns))
	}
}
