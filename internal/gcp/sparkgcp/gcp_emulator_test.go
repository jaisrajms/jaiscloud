package sparkgcp

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestDriverEnv_NilConfigReturnsEmpty(t *testing.T) {
	if got := DriverEnv(nil); got != nil {
		t.Fatalf("DriverEnv(nil) = %v, want nil", got)
	}
}

func TestDriverEnv_ProjectAndRegion(t *testing.T) {
	env := DriverEnv(&GCPEmulatorConfig{ProjectID: "my-proj", Region: "us-central1"})
	m := envMap(env)
	if m["GOOGLE_CLOUD_PROJECT"] != "my-proj" || m["GOOGLE_CLOUD_PROJECT_ID"] != "my-proj" {
		t.Fatalf("project env missing: %v", m)
	}
	if m["GOOGLE_CLOUD_LOCATION"] != "us-central1" {
		t.Fatalf("location env missing: %v", m)
	}
	if _, ok := m["STORAGE_EMULATOR_HOST"]; ok {
		t.Fatalf("STORAGE_EMULATOR_HOST should be unset when GCSEndpoint empty: %v", m)
	}
}

func TestDriverEnv_StorageEndpointAndCredentials(t *testing.T) {
	env := DriverEnv(&GCPEmulatorConfig{ProjectID: "p", GCSEndpoint: "http://emu:8080", Credentials: "/keys/sa.json"})
	m := envMap(env)
	if m["STORAGE_EMULATOR_HOST"] != "http://emu:8080" {
		t.Fatalf("STORAGE_EMULATOR_HOST not set: %v", m)
	}
	if m["GOOGLE_APPLICATION_CREDENTIALS"] != "/keys/sa.json" {
		t.Fatalf("GOOGLE_APPLICATION_CREDENTIALS not set: %v", m)
	}
}

func TestDriverSparkConfs_NilConfigReturnsEmpty(t *testing.T) {
	if got := DriverSparkConfs(nil); got != nil {
		t.Fatalf("DriverSparkConfs(nil) = %v, want nil", got)
	}
	if got := DriverSparkConfsFromEnv(nil, nil); got != nil {
		t.Fatalf("DriverSparkConfsFromEnv(nil, nil) = %v, want nil", got)
	}
}

func TestDriverSparkConfs_RequiredKeysPresent(t *testing.T) {
	confs := confMap(DriverSparkConfs(&GCPEmulatorConfig{
		ProjectID:   "my-proj",
		Region:      "us-central1",
		GCSEndpoint: "http://emu:8080",
	}))
	for key, want := range map[string]string{
		"spark.hadoop.fs.gs.impl":                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		"spark.hadoop.fs.gs.project.id":                  "my-proj",
		"spark.hadoop.fs.gs.auth.service.account.enable": "false",
		"spark.hadoop.fs.AbstractFileSystem.gs.impl":     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
		"spark.executorEnv.GOOGLE_CLOUD_PROJECT":         "my-proj",
		"spark.executorEnv.GOOGLE_CLOUD_PROJECT_ID":      "my-proj",
		"spark.executorEnv.STORAGE_EMULATOR_HOST":        "http://emu:8080",
		"spark.executorEnv.GOOGLE_CLOUD_LOCATION":        "us-central1",
	} {
		if got := confs[key]; got != want {
			t.Errorf("conf %q = %q, want %q", key, got, want)
		}
	}
}

func TestDriverSparkConfs_DefaultProject(t *testing.T) {
	confs := confMap(DriverSparkConfs(&GCPEmulatorConfig{}))
	if got := confs["spark.hadoop.fs.gs.project.id"]; got != "test-project" {
		t.Fatalf("default project not applied, got %q", got)
	}
}

func TestProtectedSparkConf(t *testing.T) {
	protected := []string{
		"spark.hadoop.fs.gs.impl",
		"spark.hadoop.fs.gs.project.id",
		"spark.hadoop.fs.gs.storage.root.url",
		"spark.hadoop.fs.AbstractFileSystem.gs.impl",
		"spark.executorEnv.GOOGLE_CLOUD_PROJECT",
		"spark.executorEnv.STORAGE_EMULATOR_HOST",
		"spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS",
	}
	for _, k := range protected {
		if !ProtectedSparkConf(k) {
			t.Errorf("ProtectedSparkConf(%q) = false, want true", k)
		}
	}
	for _, k := range []string{
		"spark.hadoop.hive.metastore.uris",
		"spark.executorEnv.MY_VAR",
		"spark.some.other",
		"fs.gs.impl",
	} {
		if ProtectedSparkConf(k) {
			t.Errorf("ProtectedSparkConf(%q) = true, want false", k)
		}
	}
}

// every key DriverSparkConfsFromEnv emits must be reported protected, so a
// caller can never strip the emulator's own connector wiring.
func TestProtectedSparkConfCoversEmittedConfs(t *testing.T) {
	cfg := &GCPEmulatorConfig{ProjectID: "p", Region: "r", GCSEndpoint: "http://emu", Credentials: "/k.json"}
	for _, tok := range DriverSparkConfs(cfg) {
		k, _, ok := strings.Cut(tok, "=")
		if !ok {
			continue // the "--conf" flag token
		}
		if !ProtectedSparkConf(k) {
			t.Errorf("emitted conf %q is not protected", k)
		}
	}
}

func TestDriverSparkConfs_ExecutorEnvMirrorsDriverEnv(t *testing.T) {
	cfg := &GCPEmulatorConfig{ProjectID: "p", Region: "r", GCSEndpoint: "http://emu"}
	driver := envMap(DriverEnv(cfg))
	confs := confMap(DriverSparkConfs(cfg))
	for k, v := range driver {
		key := "spark.executorEnv." + k
		if got, ok := confs[key]; !ok || got != v {
			t.Errorf("executorEnv mismatch for %s: got=%q want=%q", k, got, v)
		}
	}
}

func TestDriverSparkConfsFromEnv_PrecomputedEnvUsed(t *testing.T) {
	cfg := &GCPEmulatorConfig{ProjectID: "p", Region: "r", GCSEndpoint: "http://emu"}
	confsA := confMap(DriverSparkConfs(cfg))
	confsB := confMap(DriverSparkConfsFromEnv(cfg, DriverEnv(cfg)))
	for k, v := range confsA {
		if confsB[k] != v {
			t.Errorf("key %q: DriverSparkConfs=%q DriverSparkConfsFromEnv=%q", k, v, confsB[k])
		}
	}
}

func envMap(env []corev1.EnvVar) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		m[e.Name] = e.Value
	}
	return m
}

// confMap pairs adjacent tokens and splits each "key=value" on "=".
func confMap(tokens []string) map[string]string {
	out := make(map[string]string)
	for i := 0; i+1 < len(tokens); i += 2 {
		if tokens[i] != "--conf" {
			continue
		}
		parts := strings.SplitN(tokens[i+1], "=", 2)
		if len(parts) == 2 {
			out[parts[0]] = parts[1]
		}
	}
	return out
}
