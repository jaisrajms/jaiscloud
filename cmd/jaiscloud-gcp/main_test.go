package main

import (
	"context"
	"errors"
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"jaiscloud/internal/executor/container"
	"jaiscloud/internal/platform"
)

// TestLambdaCodeURL locks the K8s code-mount URL derivation: the admin base is
// built from a cluster-reachable emulator endpoint (with /_jaiscloud appended),
// and stays empty when none is configured so no unreachable localhost URL is
// guessed.
func TestLambdaCodeURL(t *testing.T) {
	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "")
	if got := lambdaCodeURL(); got != "" {
		t.Errorf("unset endpoint: got %q, want empty (code mount disabled)", got)
	}

	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "jaiscloud-gcp.jaiscloud.svc.cluster.local:8080")
	want := "http://jaiscloud-gcp.jaiscloud.svc.cluster.local:8080/_jaiscloud"
	if got := lambdaCodeURL(); got != want {
		t.Errorf("scheme-less endpoint: got %q, want %q", got, want)
	}

	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "https://emulator.example.com/")
	if got := lambdaCodeURL(); got != "https://emulator.example.com/_jaiscloud" {
		t.Errorf("trailing slash: got %q, want https://emulator.example.com/_jaiscloud", got)
	}
}

// TestResolveKafkaBrokerMode locks the Managed Kafka broker mode precedence:
// JAISCLOUD_KAFKA_BROKER_MODE → JAISCLOUD_EXECUTOR_MODE → mock, so a global
// executor mode covers Kafka while Kafka's own variable keeps precedence.
func TestResolveKafkaBrokerMode(t *testing.T) {
	t.Setenv("JAISCLOUD_GCS_EMULATOR_ENDPOINT", "") // unrelated; keep env hermetic
	cases := []struct {
		name         string
		kafkaMode    string
		executorMode string
		wantMode     string
		wantSource   string
	}{
		{"default is mock", "", "", "mock", "default"},
		{"global executor mode is the fallback", "", "docker", "docker", "JAISCLOUD_EXECUTOR_MODE"},
		{"kafka variable wins over the global", "native", "docker", "native", "JAISCLOUD_KAFKA_BROKER_MODE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("JAISCLOUD_KAFKA_BROKER_MODE", tc.kafkaMode)
			t.Setenv("JAISCLOUD_EXECUTOR_MODE", tc.executorMode)
			mode, source := resolveKafkaBrokerMode()
			if mode != tc.wantMode || source != tc.wantSource {
				t.Fatalf("resolveKafkaBrokerMode() = (%q, %q), want (%q, %q)", mode, source, tc.wantMode, tc.wantSource)
			}
		})
	}
}

// TestEffectiveFunctionsMode locks the executor-mode probe: a requested docker
// mode with no reachable daemon, or a k8s mode whose client cannot be built,
// degrades to mock (as GCP Cloud Run/Dataproc and AWS Lambda do), while mock
// and unknown modes pass through untouched and never probe.
func TestEffectiveFunctionsMode(t *testing.T) {
	reachable := func(context.Context) error { return nil }
	unreachable := func(context.Context) error {
		return errors.New("dial unix /var/run/docker.sock: connect: no such file or directory")
	}
	k8sFail := func() (kubernetes.Interface, error) {
		return nil, errors.New("no in-cluster config and no JAISCLOUD_K8S_APISERVER")
	}

	origClient := buildFunctionsK8sClient
	t.Cleanup(func() { buildFunctionsK8sClient = origClient })
	client := fake.NewSimpleClientset()

	cases := []struct {
		name       string
		mode       string
		dockerPing func(context.Context) error
		k8sClient  func() (kubernetes.Interface, error)
		want       string
		wantClient bool
	}{
		{"mock passes through", "mock", unreachable, k8sFail, "mock", false},
		{"empty passes through", "", unreachable, k8sFail, "", false},
		{"docker with a reachable daemon stays docker", "docker", reachable, k8sFail, "docker", false},
		{"docker with no daemon degrades to mock", "docker", unreachable, k8sFail, "mock", false},
		{"k8s with a buildable client stays k8s", "k8s", unreachable, func() (kubernetes.Interface, error) { return client, nil }, "k8s", true},
		{"k8s with no client degrades to mock", "k8s", reachable, k8sFail, "mock", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buildFunctionsK8sClient = tc.k8sClient
			got, gotClient := effectiveFunctionsMode(tc.mode, tc.dockerPing)
			if got != tc.want {
				t.Fatalf("effectiveFunctionsMode(%q) = %q, want %q", tc.mode, got, tc.want)
			}
			if (gotClient != nil) != tc.wantClient {
				t.Fatalf("effectiveFunctionsMode(%q) client nil=%v, want client=%v", tc.mode, gotClient == nil, tc.wantClient)
			}
		})
	}

	// Only docker mode probes the daemon; only k8s builds a client.
	var dockerProbes, k8sBuilds int
	buildFunctionsK8sClient = func() (kubernetes.Interface, error) { k8sBuilds++; return client, nil }
	if got, _ := effectiveFunctionsMode("mock", func(context.Context) error { dockerProbes++; return nil }); got != "mock" || dockerProbes != 0 {
		t.Fatalf("mock mode probed (%d) or changed mode (%q)", dockerProbes, got)
	}
	if got, _ := effectiveFunctionsMode("docker", func(context.Context) error { dockerProbes++; return nil }); got != "docker" || dockerProbes != 1 {
		t.Fatalf("docker mode did not probe exactly once (probes=%d, mode=%q)", dockerProbes, got)
	}
	if k8sBuilds != 0 {
		t.Fatalf("non-k8s modes built a k8s client (%d)", k8sBuilds)
	}
	var probes int
	if got, _ := effectiveFunctionsMode("k8s", func(context.Context) error { probes++; return nil }); got != "k8s" || probes != 0 {
		t.Fatalf("k8s mode probed the daemon (%d) or changed mode (%q)", probes, got)
	}
	if k8sBuilds != 1 {
		t.Fatalf("k8s mode built %d clients, want 1", k8sBuilds)
	}
}

// TestFunctionsPlatformConfig locks the platform-overlay wiring (FDF2): the
// overlay is loaded only for docker/k8s (the backends whose containers/pods it
// reaches) and never for mock, and a malformed platform config degrades to nil
// (non-fatal) rather than failing startup.
func TestFunctionsPlatformConfig(t *testing.T) {
	clearPlatformEnv(t)

	cases := []struct {
		name    string
		mode    string
		wantNil bool
	}{
		{"mock mode skips the overlay", "mock", true},
		{"empty mode skips the overlay", "", true},
		{"docker mode loads the overlay", "docker", false},
		{"k8s mode loads the overlay", "k8s", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := functionsPlatformConfig(tc.mode)
			if (got == nil) != tc.wantNil {
				t.Fatalf("functionsPlatformConfig(%q) nil=%v, want nil=%v", tc.mode, got == nil, tc.wantNil)
			}
		})
	}

	// A bad platform config is non-fatal: the docker executor runs without it.
	t.Setenv("JAISCLOUD_PLATFORM_VOLUMES", "not-json")
	if got := functionsPlatformConfig("docker"); got != nil {
		t.Fatalf("functionsPlatformConfig(docker) with a malformed config = %+v, want nil", got)
	}
}

// TestNewFunctionsExecutorWiring locks the mode → platform → constructor path
// (FDF2/FDF3): docker and k8s receive a loaded platform overlay, a k8s executor
// reuses the probed client instead of building a second one, and every other
// mode stays mock and never reaches a platform-aware constructor.
func TestNewFunctionsExecutorWiring(t *testing.T) {
	clearPlatformEnv(t)

	origDocker, origK8s := newFunctionsDockerExecutor, newFunctionsK8sExecutor
	t.Cleanup(func() {
		newFunctionsDockerExecutor, newFunctionsK8sExecutor = origDocker, origK8s
	})

	var dockerPlat, k8sPlat *platform.PlatformConfig
	var dockerCalls, k8sCalls, k8sOpts int
	newFunctionsDockerExecutor = func(_ container.Config, _ container.Profile, p *platform.PlatformConfig) container.Executor {
		dockerCalls++
		dockerPlat = p
		return &container.MockExecutor{}
	}
	newFunctionsK8sExecutor = func(_ container.Config, _ container.Profile, p *platform.PlatformConfig, opts ...container.K8sExecutorOption) container.Executor {
		k8sCalls++
		k8sPlat = p
		k8sOpts = len(opts)
		return &container.MockExecutor{}
	}

	// docker: the docker constructor receives a loaded platform overlay.
	newFunctionsExecutor(container.Config{Mode: "docker"}, nil, nil)
	if dockerCalls != 1 || k8sCalls != 0 {
		t.Fatalf("docker mode calls: docker=%d k8s=%d, want 1/0", dockerCalls, k8sCalls)
	}
	if dockerPlat == nil {
		t.Fatal("docker mode passed a nil platform overlay")
	}

	// k8s with a resolved client: the constructor receives the overlay and the
	// client option (so it never builds a second one).
	newFunctionsExecutor(container.Config{Mode: "k8s"}, nil, fake.NewSimpleClientset())
	if k8sCalls != 1 || k8sPlat == nil || k8sOpts != 1 {
		t.Fatalf("k8s mode calls=%d plat=%v opts=%d, want 1/non-nil/1", k8sCalls, k8sPlat, k8sOpts)
	}

	// k8s with no resolved client (effectiveFunctionsMode should have already
	// fallen back to mock) still builds the executor, passing no client option.
	newFunctionsExecutor(container.Config{Mode: "k8s"}, nil, nil)
	if k8sOpts != 0 {
		t.Fatalf("k8s mode with a nil client passed %d options, want 0", k8sOpts)
	}

	// mock/unknown: stay mock and never reach a platform-aware constructor.
	for _, mode := range []string{"mock", "", "native"} {
		exec := newFunctionsExecutor(container.Config{Mode: mode}, nil, nil)
		if _, ok := exec.(*container.MockExecutor); !ok {
			t.Fatalf("mode %q returned %T, want *container.MockExecutor", mode, exec)
		}
	}
	if dockerCalls != 1 || k8sCalls != 2 {
		t.Fatalf("non-platform modes called a constructor: docker=%d k8s=%d", dockerCalls, k8sCalls)
	}
}

// clearPlatformEnv isolates tests from an ambient platform configuration so the
// only overlay in play is the one the test sets.
func clearPlatformEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"JAISCLOUD_PLATFORM_TLS_ENABLED",
		"JAISCLOUD_PLATFORM_TLS_CA_SOURCES",
		"JAISCLOUD_PLATFORM_TLS_CLIENT_CERT",
		"JAISCLOUD_PLATFORM_VOLUMES",
		"JAISCLOUD_PLATFORM_ENV",
		"JAISCLOUD_PLATFORM_HOSTPATH_ALLOWLIST",
	} {
		t.Setenv(k, "")
	}
	t.Setenv("JAISCLOUD_PLATFORM_TLS_ENABLED", "false")
}
