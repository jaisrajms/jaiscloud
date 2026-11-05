package functions

import (
	"bytes"
	"context"
	"testing"
)

// v1FieldsFunction is a stored Function carrying every field TGM3 added, so the
// memory, Postgres and snapshot backends can be compared field-for-field.
func v1FieldsFunction() Function {
	return Function{
		ID: "v1", Location: "us-central1", Runtime: "nodejs20", EntryPoint: "hello",
		Status:                     "ACTIVE",
		IngressSettings:            "ALLOW_INTERNAL_ONLY",
		MaxInstances:               9,
		VPCConnector:               "projects/p/locations/us-central1/connectors/c1",
		VPCConnectorEgressSettings: "PRIVATE_RANGES_ONLY",
		SecretEnvironmentVariables: []SecretEnvVar{
			{Key: "TOKEN", ProjectID: "12345", Secret: "token", Version: "3"},
		},
		SecretVolumes: []SecretVolume{
			{MountPath: "/etc/secrets", ProjectID: "12345", Secret: "certs",
				Versions: []SecretVolumeVersion{{Path: "/tls.crt", Version: "latest"}}},
		},
		DockerRegistry:            "ARTIFACT_REGISTRY",
		DockerRepository:          "projects/p/locations/us-central1/repositories/r1",
		BuildWorkerPool:           "projects/p/locations/us-central1/workerPools/w1",
		HttpsTriggerSecurityLevel: "SECURE_OPTIONAL",
	}
}

// assertV1Fields fails when a read-back does not carry every TGM3 field.
func assertV1Fields(t *testing.T, f Function) {
	t.Helper()
	if f.IngressSettings != "ALLOW_INTERNAL_ONLY" || f.MaxInstances != 9 {
		t.Fatalf("ingress/maxInstances = %q/%d", f.IngressSettings, f.MaxInstances)
	}
	if f.VPCConnector != "projects/p/locations/us-central1/connectors/c1" ||
		f.VPCConnectorEgressSettings != "PRIVATE_RANGES_ONLY" {
		t.Fatalf("vpc = %q/%q", f.VPCConnector, f.VPCConnectorEgressSettings)
	}
	if len(f.SecretEnvironmentVariables) != 1 ||
		f.SecretEnvironmentVariables[0] != (SecretEnvVar{Key: "TOKEN", ProjectID: "12345", Secret: "token", Version: "3"}) {
		t.Fatalf("secretEnvironmentVariables = %+v", f.SecretEnvironmentVariables)
	}
	if len(f.SecretVolumes) != 1 || f.SecretVolumes[0].MountPath != "/etc/secrets" ||
		len(f.SecretVolumes[0].Versions) != 1 ||
		f.SecretVolumes[0].Versions[0] != (SecretVolumeVersion{Path: "/tls.crt", Version: "latest"}) {
		t.Fatalf("secretVolumes = %+v", f.SecretVolumes)
	}
	if f.DockerRegistry != "ARTIFACT_REGISTRY" ||
		f.DockerRepository != "projects/p/locations/us-central1/repositories/r1" ||
		f.BuildWorkerPool != "projects/p/locations/us-central1/workerPools/w1" {
		t.Fatalf("docker/build = %q/%q/%q", f.DockerRegistry, f.DockerRepository, f.BuildWorkerPool)
	}
	if f.HttpsTriggerSecurityLevel != "SECURE_OPTIONAL" {
		t.Fatalf("https security level = %q", f.HttpsTriggerSecurityLevel)
	}
}

func TestMemoryStoreV1ModuleFields(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	f := v1FieldsFunction()
	if err := s.CreateFunction(ctx, "p", "us-central1", "v1", f); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetFunction(ctx, "p", "us-central1", "v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertV1Fields(t, got)

	// An update persists the cleared values too (the API's mask semantics).
	got.IngressSettings = "ALLOW_ALL"
	got.MaxInstances = 0
	got.VPCConnector = ""
	got.VPCConnectorEgressSettings = ""
	got.SecretEnvironmentVariables = nil
	got.SecretVolumes = nil
	got.DockerRegistry = ""
	got.DockerRepository = ""
	got.BuildWorkerPool = ""
	got.HttpsTriggerSecurityLevel = "SECURE_ALWAYS"
	if err := s.UpdateFunction(ctx, "p", "us-central1", "v1", got); err != nil {
		t.Fatalf("update: %v", err)
	}
	read, err := s.GetFunction(ctx, "p", "us-central1", "v1")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if read.IngressSettings != "ALLOW_ALL" || read.MaxInstances != 0 || read.VPCConnector != "" ||
		len(read.SecretEnvironmentVariables) != 0 || len(read.SecretVolumes) != 0 ||
		read.DockerRegistry != "" || read.BuildWorkerPool != "" ||
		read.HttpsTriggerSecurityLevel != "SECURE_ALWAYS" {
		t.Fatalf("cleared function = %+v", read)
	}
}

func TestMemoryStoreV1ModuleFieldsSnapshot(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateFunction(ctx, "p", "us-central1", "v1", v1FieldsFunction()); err != nil {
		t.Fatalf("create: %v", err)
	}
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := NewMemoryStore()
	if err := restored.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := restored.GetFunction(ctx, "p", "us-central1", "v1")
	if err != nil {
		t.Fatalf("get restored: %v", err)
	}
	assertV1Fields(t, got)
}
