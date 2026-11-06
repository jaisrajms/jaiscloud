package functions

import (
	"context"
	"errors"
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// v1ModuleBody is the shape the terraform-google-event-function module (pinned
// 9f2e26ae3b73) sends for a google_cloudfunctions_function (TGM3): ingress is
// always set, the event trigger always carries a failure_policy block, and the
// optional vpc/secret/docker fields are present when configured.
func v1ModuleBody() map[string]any {
	return map[string]any{
		"name":                       "projects/p/locations/us-central1/functions/f1",
		"runtime":                    "nodejs20",
		"entryPoint":                 "hello",
		"timeout":                    "60s",
		"ingressSettings":            "ALLOW_ALL",
		"maxInstances":               float64(7),
		"vpcConnector":               "projects/p/locations/us-central1/connectors/c1",
		"vpcConnectorEgressSettings": "ALL_TRAFFIC",
		"secretEnvironmentVariables": []any{
			map[string]any{"key": "TOKEN", "projectId": "12345", "secret": "token", "version": "3"},
		},
		"secretVolumes": []any{
			map[string]any{
				"mountPath": "/etc/secrets",
				"projectId": "12345",
				"secret":    "certs",
				"versions": []any{
					map[string]any{"path": "/tls.crt", "version": "latest"},
				},
			},
		},
		"dockerRegistry":   "ARTIFACT_REGISTRY",
		"dockerRepository": "projects/p/locations/us-central1/repositories/r1",
		"buildWorkerPool":  "projects/p/locations/us-central1/workerPools/w1",
		"eventTrigger": map[string]any{
			"eventType": "google.pubsub.topic.publish",
			"resource":  "projects/p/topics/t1",
			"failurePolicy": map[string]any{
				"retry": map[string]any{},
			},
		},
	}
}

func TestV1ModuleFieldsCreateGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())

	in := FunctionInputFromMap(v1ModuleBody(), V1)
	if _, _, err := s.CreateFunction(ctx, "p", "us-central1", "f1", in, V1); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	got, err := s.GetFunction(ctx, "p", "us-central1", "f1")
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	if got.IngressSettings != "ALLOW_ALL" {
		t.Fatalf("ingressSettings = %q, want ALLOW_ALL", got.IngressSettings)
	}
	if got.MaxInstances != 7 {
		t.Fatalf("maxInstances = %d, want 7", got.MaxInstances)
	}
	if got.VPCConnector != "projects/p/locations/us-central1/connectors/c1" || got.VPCConnectorEgressSettings != "ALL_TRAFFIC" {
		t.Fatalf("vpc = %q/%q", got.VPCConnector, got.VPCConnectorEgressSettings)
	}
	if len(got.SecretEnvironmentVariables) != 1 || got.SecretEnvironmentVariables[0].Secret != "token" ||
		got.SecretEnvironmentVariables[0].Version != "3" || got.SecretEnvironmentVariables[0].ProjectID != "12345" {
		t.Fatalf("secretEnvironmentVariables = %+v", got.SecretEnvironmentVariables)
	}
	if len(got.SecretVolumes) != 1 || got.SecretVolumes[0].MountPath != "/etc/secrets" ||
		len(got.SecretVolumes[0].Versions) != 1 || got.SecretVolumes[0].Versions[0].Path != "/tls.crt" {
		t.Fatalf("secretVolumes = %+v", got.SecretVolumes)
	}
	if got.DockerRegistry != "ARTIFACT_REGISTRY" || got.DockerRepository != "projects/p/locations/us-central1/repositories/r1" ||
		got.BuildWorkerPool != "projects/p/locations/us-central1/workerPools/w1" {
		t.Fatalf("docker/build = %+v", got)
	}
	if got.EventTrigger == nil || !got.EventTrigger.Retry {
		t.Fatalf("event trigger retry = %+v", got.EventTrigger)
	}

	// Every stored field renders in the v1 shape.
	v1 := FunctionJSON(V1, "p", got)
	if v1["ingressSettings"] != "ALLOW_ALL" || v1["maxInstances"] != 7 {
		t.Fatalf("v1 ingress/maxInstances = %+v", v1)
	}
	if v1["vpcConnector"] != got.VPCConnector || v1["vpcConnectorEgressSettings"] != "ALL_TRAFFIC" {
		t.Fatalf("v1 vpc = %+v", v1)
	}
	secretEnv, _ := v1["secretEnvironmentVariables"].([]any)
	if len(secretEnv) != 1 || secretEnv[0].(map[string]any)["version"] != "3" {
		t.Fatalf("v1 secretEnvironmentVariables = %+v", v1["secretEnvironmentVariables"])
	}
	secretVols, _ := v1["secretVolumes"].([]any)
	if len(secretVols) != 1 || secretVols[0].(map[string]any)["mountPath"] != "/etc/secrets" {
		t.Fatalf("v1 secretVolumes = %+v", v1["secretVolumes"])
	}
	et, _ := v1["eventTrigger"].(map[string]any)
	fp, _ := et["failurePolicy"].(map[string]any)
	if _, ok := fp["retry"]; !ok {
		t.Fatalf("v1 failurePolicy.retry missing: %+v", et)
	}

	// The v2 shape must stay unchanged: the v1-only fields never leak into it.
	v2 := FunctionJSON(V2, "p", got)
	for _, key := range []string{
		"ingressSettings", "maxInstances", "vpcConnector", "vpcConnectorEgressSettings",
		"secretEnvironmentVariables", "secretVolumes", "dockerRegistry", "dockerRepository", "buildWorkerPool",
	} {
		if _, ok := v2[key]; ok {
			t.Fatalf("v2 render leaked v1 field %q: %+v", key, v2)
		}
	}
}

// TestV1FailurePolicyDefaultRendered covers the module's default
// event_trigger_failure_policy_retry=false: the API answers with the empty
// failurePolicy message (its documented "ignore failures" default), which is
// what lets the provider's flatten produce retry=false instead of permadiffing.
func TestV1FailurePolicyDefaultRendered(t *testing.T) {
	f := functionsstore.Function{
		ID: "f1", Location: "us-central1", Status: "ACTIVE",
		EventTrigger: &functionsstore.EventTrigger{EventType: "google.pubsub.topic.publish", Resource: "t"},
	}
	v1 := FunctionJSON(V1, "p", f)
	et, _ := v1["eventTrigger"].(map[string]any)
	fp, ok := et["failurePolicy"].(map[string]any)
	if !ok || len(fp) != 0 {
		t.Fatalf("default failurePolicy = %+v, want empty object", et)
	}
	f.EventTrigger.Retry = true
	et, _ = FunctionJSON(V1, "p", f)["eventTrigger"].(map[string]any)
	fp, _ = et["failurePolicy"].(map[string]any)
	if _, ok := fp["retry"]; !ok {
		t.Fatalf("retry failurePolicy = %+v, want retry{}", fp)
	}
}

func TestV1ModuleFieldsUpdate(t *testing.T) {
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	in := FunctionInputFromMap(v1ModuleBody(), V1)
	if _, _, err := s.CreateFunction(ctx, "p", "us-central1", "f1", in, V1); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}

	// ingressSettings (the module's ForceNew-documented field; the provider
	// PATCHes it with updateMask=ingressSettings).
	body := map[string]any{"ingressSettings": "ALLOW_INTERNAL_ONLY"}
	f, _, err := s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1), []string{"ingressSettings"}, V1)
	if err != nil {
		t.Fatalf("update ingress: %v", err)
	}
	if f.IngressSettings != "ALLOW_INTERNAL_ONLY" {
		t.Fatalf("ingress after update = %q", f.IngressSettings)
	}

	// environmentVariables: set, then clear with the authoritative mask (the
	// provider sends no map when every entry is removed).
	body = map[string]any{"environmentVariables": map[string]any{"GREETING": "hi"}}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1), []string{"environmentVariables"}, V1)
	if err != nil || f.EnvironmentVariables["GREETING"] != "hi" {
		t.Fatalf("set env: %+v err=%v", f.EnvironmentVariables, err)
	}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(map[string]any{}, V1), []string{"environmentVariables"}, V1)
	if err != nil || len(f.EnvironmentVariables) != 0 {
		t.Fatalf("clear env: %+v err=%v", f.EnvironmentVariables, err)
	}

	// vpcConnector + egress set, then cleared by the mask with an absent body.
	body = map[string]any{
		"vpcConnector":               "projects/p/locations/us-central1/connectors/c2",
		"vpcConnectorEgressSettings": "PRIVATE_RANGES_ONLY",
	}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1),
		[]string{"vpcConnector", "vpcConnectorEgressSettings"}, V1)
	if err != nil || f.VPCConnector != "projects/p/locations/us-central1/connectors/c2" || f.VPCConnectorEgressSettings != "PRIVATE_RANGES_ONLY" {
		t.Fatalf("set vpc: %+v err=%v", f, err)
	}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(map[string]any{}, V1),
		[]string{"vpcConnector", "vpcConnectorEgressSettings"}, V1)
	if err != nil || f.VPCConnector != "" || f.VPCConnectorEgressSettings != "" {
		t.Fatalf("clear vpc: %q/%q err=%v", f.VPCConnector, f.VPCConnectorEgressSettings, err)
	}

	// secretEnvironmentVariables / secretVolumes: replace with a new list, then
	// clear with an explicit empty list (the provider's expand of an empty block
	// sends []).
	body = map[string]any{
		"secretEnvironmentVariables": []any{map[string]any{"key": "K", "secret": "s", "version": "1"}},
		"secretVolumes": []any{map[string]any{
			"mountPath": "/mnt", "secret": "v",
			"versions": []any{map[string]any{"path": "/a", "version": "2"}},
		}},
	}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1),
		[]string{"secretEnvironmentVariables", "secretVolumes"}, V1)
	if err != nil || len(f.SecretEnvironmentVariables) != 1 || len(f.SecretVolumes) != 1 ||
		f.SecretVolumes[0].Versions[0].Version != "2" {
		t.Fatalf("set secrets: %+v err=%v", f, err)
	}
	body = map[string]any{"secretEnvironmentVariables": []any{}, "secretVolumes": []any{}}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1),
		[]string{"secretEnvironmentVariables", "secretVolumes"}, V1)
	if err != nil || len(f.SecretEnvironmentVariables) != 0 || len(f.SecretVolumes) != 0 {
		t.Fatalf("clear secrets: %+v err=%v", f, err)
	}

	// maxInstances (v1) set and explicitly cleared to 0 by the mask.
	body = map[string]any{"maxInstances": float64(12)}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1), []string{"maxInstances"}, V1)
	if err != nil || f.MaxInstances != 12 {
		t.Fatalf("set maxInstances: %d err=%v", f.MaxInstances, err)
	}
	body = map[string]any{"maxInstances": float64(0)}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1), []string{"maxInstances"}, V1)
	if err != nil || f.MaxInstances != 0 {
		t.Fatalf("clear maxInstances: %d err=%v", f.MaxInstances, err)
	}

	// eventTrigger.failurePolicy.retry: the provider sends the full trigger plus
	// the nested mask path. Only the retry flag may change.
	body = map[string]any{"eventTrigger": map[string]any{
		"eventType":     "google.pubsub.topic.publish",
		"resource":      "projects/p/topics/t1",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	}}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1),
		[]string{"eventTrigger", "eventTrigger.failurePolicy.retry"}, V1)
	if err != nil || f.EventTrigger == nil || !f.EventTrigger.Retry ||
		f.EventTrigger.EventType != "google.pubsub.topic.publish" || f.EventTrigger.Resource != "projects/p/topics/t1" {
		t.Fatalf("retry update: %+v err=%v", f.EventTrigger, err)
	}
	// A mask naming only the nested path must not wipe the trigger.
	body = map[string]any{"eventTrigger": map[string]any{"failurePolicy": map[string]any{}}}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "f1", FunctionInputFromMap(body, V1),
		[]string{"eventTrigger.failurePolicy.retry"}, V1)
	if err != nil || f.EventTrigger == nil || f.EventTrigger.Retry ||
		f.EventTrigger.EventType != "google.pubsub.topic.publish" {
		t.Fatalf("retry clear: %+v err=%v", f.EventTrigger, err)
	}
}

func TestV1HTTPSecurityLevelUpdate(t *testing.T) {
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	in := FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "hello"}, V1)
	f, _, err := s.CreateFunction(ctx, "p", "us-central1", "http1", in, V1)
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if f.HttpsTriggerSecurityLevel != "" {
		t.Fatalf("unset security level = %q, want empty (renderer supplies SECURE_ALWAYS)", f.HttpsTriggerSecurityLevel)
	}
	v1 := FunctionJSON(V1, "p", f)
	ht, _ := v1["httpsTrigger"].(map[string]any)
	if ht["securityLevel"] != "SECURE_ALWAYS" {
		t.Fatalf("default rendered securityLevel = %+v", ht)
	}
	body := map[string]any{"httpsTrigger": map[string]any{"securityLevel": "SECURE_OPTIONAL"}}
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "http1", FunctionInputFromMap(body, V1),
		[]string{"httpsTrigger", "httpsTrigger.securityLevel"}, V1)
	if err != nil || f.HttpsTriggerSecurityLevel != "SECURE_OPTIONAL" {
		t.Fatalf("security level = %q err=%v", f.HttpsTriggerSecurityLevel, err)
	}
	v1 = FunctionJSON(V1, "p", f)
	ht, _ = v1["httpsTrigger"].(map[string]any)
	if ht["securityLevel"] != "SECURE_OPTIONAL" {
		t.Fatalf("rendered securityLevel = %+v", ht)
	}
	// The proto/snake_case spelling is accepted too.
	f, _, err = s.UpdateFunction(ctx, "p", "us-central1", "http1",
		FunctionInputFromMap(map[string]any{"httpsTrigger": map[string]any{"securityLevel": "SECURE_ALWAYS"}}, V1),
		[]string{"https_trigger.security_level"}, V1)
	if err != nil || f.HttpsTriggerSecurityLevel != "SECURE_ALWAYS" {
		t.Fatalf("snake_case security level = %q err=%v", f.HttpsTriggerSecurityLevel, err)
	}
}

func TestV1UnknownMaskRejectedInvalidArgument(t *testing.T) {
	f := functionsstore.Function{Runtime: "nodejs20"}
	err := ApplyFunctionUpdate(&f, FunctionInput{}, []string{"madeUpField"})
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.HTTPStatus != 400 || perr.Code != "InvalidArgument" {
		t.Fatalf("unknown mask error = %v, want InvalidArgument/400", err)
	}
}

// TestV2ServiceConfigIngressSettings pins the v2 ServiceConfig.ingressSettings
// round-trip. google_cloudfunctions2_function defaults the field to ALLOW_ALL
// and always sends it under serviceConfig; the emulator dropped it, so every
// plan proposed the same in-place update (`+ ingress_settings = "ALLOW_ALL"`)
// and never converged.
func TestV2ServiceConfigIngressSettings(t *testing.T) {
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())

	body := map[string]any{
		"name": "projects/p/locations/us-central1/functions/f2",
		"buildConfig": map[string]any{
			"runtime":    "python312",
			"entryPoint": "handler",
			"source": map[string]any{
				"storageSource": map[string]any{"bucket": "b", "object": "o"},
			},
		},
		"serviceConfig": map[string]any{
			"ingressSettings":  "ALLOW_ALL",
			"availableMemory":  "256M",
			"maxInstanceCount": float64(1),
		},
	}
	if _, _, err := s.CreateFunction(ctx, "p", "us-central1", "f2", FunctionInputFromMap(body, V2), V2); err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	got, err := s.GetFunction(ctx, "p", "us-central1", "f2")
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	if got.IngressSettings != "ALLOW_ALL" {
		t.Fatalf("stored ingressSettings = %q, want ALLOW_ALL", got.IngressSettings)
	}

	v2 := FunctionJSON(V2, "p", got)
	sc, _ := v2["serviceConfig"].(map[string]any)
	if sc["ingressSettings"] != "ALLOW_ALL" {
		t.Fatalf("v2 serviceConfig.ingressSettings = %v, want ALLOW_ALL", sc["ingressSettings"])
	}
	if _, has := v2["ingressSettings"]; has {
		t.Errorf("v2 body carries the v1 top-level ingressSettings: %v", v2["ingressSettings"])
	}
	// The v1 view of the same record still carries it, so a cross-version read
	// is not silently empty.
	if v1 := FunctionJSON(V1, "p", got); v1["ingressSettings"] != "ALLOW_ALL" {
		t.Errorf("v1 ingressSettings = %v, want ALLOW_ALL", v1["ingressSettings"])
	}

	// A v2 PATCH names the nested path; the proto snake_case and the JSON
	// camelCase forms both resolve to the same field.
	for _, mask := range []string{"serviceConfig.ingressSettings", "service_config.ingress_settings"} {
		upd := FunctionInputFromMap(map[string]any{
			"name":          "projects/p/locations/us-central1/functions/f2",
			"serviceConfig": map[string]any{"ingressSettings": "ALLOW_INTERNAL_ONLY"},
		}, V2)
		f, _, err := s.UpdateFunction(ctx, "p", "us-central1", "f2", upd, []string{mask}, V2)
		if err != nil {
			t.Fatalf("UpdateFunction(%s): %v", mask, err)
		}
		if f.IngressSettings != "ALLOW_INTERNAL_ONLY" {
			t.Fatalf("ingress after %s = %q, want ALLOW_INTERNAL_ONLY", mask, f.IngressSettings)
		}
		svc, _ := FunctionJSON(V2, "p", f)["serviceConfig"].(map[string]any)
		if svc["ingressSettings"] != "ALLOW_INTERNAL_ONLY" {
			t.Fatalf("rendered ingress after %s = %v, want ALLOW_INTERNAL_ONLY", mask, svc["ingressSettings"])
		}
	}
}
