package gcf

import (
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/executor/container"
)

func envMap(env []container.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, kv := range env {
		m[kv.Name] = kv.Value
	}
	return m
}

func TestProfile_EssentialContract(t *testing.T) {
	p := FunctionsFrameworkProfile{}
	if p.Name() != "functions-framework" {
		t.Fatalf("Name = %q", p.Name())
	}
	if p.InvocationPort() != 8080 {
		t.Fatalf("InvocationPort = %d, want 8080", p.InvocationPort())
	}
	if p.CodeMountDir() != "/workspace" {
		t.Fatalf("CodeMountDir = %q, want /workspace", p.CodeMountDir())
	}
	if p.LayerMountDir() != "" {
		t.Fatalf("LayerMountDir = %q, want empty (GCF has no layers)", p.LayerMountDir())
	}
	if p.WorkloadLabel() != "jaiscloud-functions" {
		t.Fatalf("WorkloadLabel = %q", p.WorkloadLabel())
	}
	if got := p.WorkloadNamePrefix("0123456789abcdef"); got != "jc-functions-01234567-" {
		t.Fatalf("WorkloadNamePrefix = %q", got)
	}
	if got := p.WorkloadNamePrefix(""); got != "jc-functions-" {
		t.Fatalf("WorkloadNamePrefix empty = %q", got)
	}
	if got := p.ContainerArgs(container.Request{Handler: "x"}); got != nil {
		t.Fatalf("ContainerArgs = %v, want nil (image entrypoint)", got)
	}
}

func TestProfile_RuntimeEnv(t *testing.T) {
	p := FunctionsFrameworkProfile{}
	req := container.Request{
		FunctionName:  "hello",
		Runtime:       "python312",
		Handler:       "myhandler",
		AccountID:     "proj",
		ExecutionID:   "exec-1",
		SignatureType: "cloudevent",
		EnvVars:       map[string]string{"CUSTOM": "v"},
	}
	cfg := container.Config{Region: "us-central1", JaisCloudEndpoint: "http://jc:8080"}
	got := envMap(p.RuntimeEnv(cfg, req))

	want := map[string]string{
		"FUNCTION_TARGET":                "myhandler",
		"FUNCTION_SIGNATURE_TYPE":        "cloudevent",
		"GOOGLE_FUNCTION_TARGET":         "myhandler",
		"GOOGLE_FUNCTION_SIGNATURE_TYPE": "cloudevent",
		"PORT":                           "8080",
		"GCP_PROJECT":                    "proj",
		"GOOGLE_CLOUD_PROJECT":           "proj",
		"GOOGLE_CLOUD_LOCATION":          "us-central1",
		"LOG_EXECUTION_ID":               "exec-1",
		"JAISCLOUD_ENDPOINT":             "http://jc:8080",
		"CUSTOM":                         "v",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("env[%s] = %q, want %q", k, got[k], v)
		}
	}
}

func TestProfile_TargetAndSignatureDefaults(t *testing.T) {
	p := FunctionsFrameworkProfile{}
	cases := map[string]string{
		"python312": "main",
		"nodejs20":  "helloWorld",
		"go121":     "HelloWorld",
		"java21":    "Hello",
		"":          "",
	}
	for runtime, want := range cases {
		env := envMap(p.RuntimeEnv(container.Config{}, container.Request{Runtime: runtime}))
		if env["FUNCTION_TARGET"] != want {
			t.Errorf("runtime %q FUNCTION_TARGET = %q, want %q", runtime, env["FUNCTION_TARGET"], want)
		}
		if env["FUNCTION_SIGNATURE_TYPE"] != "http" {
			t.Errorf("runtime %q default signature = %q, want http", runtime, env["FUNCTION_SIGNATURE_TYPE"])
		}
	}

	// An explicit entry point wins over the per-runtime default.
	env := envMap(p.RuntimeEnv(container.Config{}, container.Request{Runtime: "python312", Handler: "custom"}))
	if env["FUNCTION_TARGET"] != "custom" {
		t.Errorf("explicit target = %q, want custom", env["FUNCTION_TARGET"])
	}
}

func TestProfile_HTTPInvocation(t *testing.T) {
	p := FunctionsFrameworkProfile{}
	inv := p.Invocation(container.Request{Payload: []byte(`{"name":"x"}`), SignatureType: "http"})
	if inv.Method != "POST" || inv.Path != "/" {
		t.Fatalf("invocation = %s %s, want POST /", inv.Method, inv.Path)
	}
	if inv.Header["Content-Type"] != "application/json" {
		t.Fatalf("Content-Type = %q", inv.Header["Content-Type"])
	}
	if string(inv.Body) != `{"name":"x"}` {
		t.Fatalf("body = %q", inv.Body)
	}
	if _, ok := inv.Header["ce-type"]; ok {
		t.Fatal("http invocation must not carry ce-* headers")
	}
}

func TestProfile_CloudEventInvocation(t *testing.T) {
	p := FunctionsFrameworkProfile{}
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	req := container.Request{
		Payload:       []byte(`{"raw":"payload"}`),
		SignatureType: "cloudevent",
		Event: &container.CloudEvent{
			SpecVersion:     "1.0",
			ID:              "evt-1",
			Source:          "//pubsub.googleapis.com/projects/p/topics/t",
			Type:            "google.cloud.pubsub.topic.v1.messagePublished",
			Subject:         "sub",
			Time:            when,
			DataContentType: "application/json",
			Data:            []byte(`{"message":{"data":"aGk="},"subscription":"projects/p/subscriptions/s"}`),
		},
	}
	inv := p.Invocation(req)
	if inv.Header["ce-specversion"] != "1.0" || inv.Header["ce-id"] != "evt-1" ||
		inv.Header["ce-source"] != "//pubsub.googleapis.com/projects/p/topics/t" ||
		inv.Header["ce-type"] != "google.cloud.pubsub.topic.v1.messagePublished" {
		t.Fatalf("ce headers = %v", inv.Header)
	}
	if inv.Header["ce-subject"] != "sub" {
		t.Fatalf("ce-subject = %q", inv.Header["ce-subject"])
	}
	if !strings.HasPrefix(inv.Header["ce-time"], "2026-01-02T03:04:05") {
		t.Fatalf("ce-time = %q", inv.Header["ce-time"])
	}
	// In binary content mode the event data is the body, not the request payload.
	if string(inv.Body) != string(req.Event.Data) {
		t.Fatalf("body = %q, want the CloudEvent data %q", inv.Body, req.Event.Data)
	}
	if inv.Header["Content-Type"] != "application/json" {
		t.Fatalf("Content-Type = %q", inv.Header["Content-Type"])
	}
}

// TestProfile_CloudEventBodyFallsBackToPayload verifies a CloudEvent invocation
// with no event data still sends the request payload as the body.
func TestProfile_CloudEventBodyFallsBackToPayload(t *testing.T) {
	p := FunctionsFrameworkProfile{}
	req := container.Request{
		Payload:       []byte(`{"msg":"hi"}`),
		SignatureType: "cloudevent",
		Event:         &container.CloudEvent{Type: "example.event", Source: "src"},
	}
	inv := p.Invocation(req)
	if string(inv.Body) != `{"msg":"hi"}` {
		t.Fatalf("body = %q, want the request payload", inv.Body)
	}
}

func TestProfile_DecodeResponse(t *testing.T) {
	p := FunctionsFrameworkProfile{}
	if b, err := p.DecodeResponse(200, []byte("ok")); err != nil || string(b) != "ok" {
		t.Fatalf("200: %q %v", b, err)
	}
	if _, err := p.DecodeResponse(500, []byte("boom")); err == nil {
		t.Fatal("500 must error")
	}
}

func TestProfile_ImageForRuntime(t *testing.T) {
	p := FunctionsFrameworkProfile{}
	t.Setenv("JAISCLOUD_FUNCTIONS_IMAGE", "")
	if got := p.ImageForRuntime(container.Config{}, container.Request{Runtime: "python312"}); got != "jaiscloud-functions-framework-python:latest" {
		t.Fatalf("python312 image = %q", got)
	}
	if got := p.ImageForRuntime(container.Config{}, container.Request{Runtime: "cobol1"}); got != defaultFunctionsFrameworkImage {
		t.Fatalf("unknown runtime image = %q", got)
	}
	t.Setenv("JAISCLOUD_FUNCTIONS_IMAGE", "override:1")
	if got := p.ImageForRuntime(container.Config{}, container.Request{Runtime: "python312"}); got != "override:1" {
		t.Fatalf("override image = %q", got)
	}
	// A per-function image still wins over the env override.
	if got := p.ImageForRuntime(container.Config{}, container.Request{Runtime: "python312", Image: "custom:2"}); got != "custom:2" {
		t.Fatalf("per-function image = %q", got)
	}
}
