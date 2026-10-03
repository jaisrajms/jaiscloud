package kafka

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestNewModeSelection(t *testing.T) {
	// Native resolution must not accidentally find a PATH binary.
	t.Setenv("PATH", "")
	t.Setenv("JAISCLOUD_KAFKA_BROKER_BIN", "")

	cases := []struct {
		name     string
		cfg      Config
		wantMove Mode
	}{
		{"empty defaults to mock", Config{}, ModeMock},
		{"explicit mock", Config{Mode: "mock"}, ModeMock},
		{"unknown degrades to mock", Config{Mode: "docker"}, ModeMock},
		{"k8s without client degrades to mock", Config{Mode: "k8s"}, ModeMock},
		{"k8s with client", Config{Mode: "k8s", Client: fake.NewSimpleClientset()}, ModeK8s},
		{"native without binary degrades to mock", Config{Mode: "native"}, ModeMock},
		{"native with binary", Config{Mode: "native", BinaryPath: "/bin/true"}, ModeNative},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := New(tc.cfg).Mode(); got != tc.wantMove {
				t.Fatalf("Mode() = %q, want %q", got, tc.wantMove)
			}
		})
	}
}

func TestK8sBrokerEnsureEndpointAndReap(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	ctx := context.Background()
	key := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "My_Cluster"}
	name := brokerResourceName(key)

	// Endpoint before ensure is empty and does not start anything.
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != "" {
		t.Fatalf("Endpoint before ensure = %q, want empty", ep)
	}

	got, err := b.EnsureCluster(ctx, key.Project, key.Location, key.Cluster)
	if err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	want := name + ".jaiscloud.svc.cluster.local:9092"
	if got != want {
		t.Fatalf("endpoint = %q, want %q", got, want)
	}
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != want {
		t.Fatalf("Endpoint after ensure = %q, want %q", ep, want)
	}

	// Service and Pod exist with the same name as the DNS label.
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("service not created: %v", err)
	}
	pod, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod not created: %v", err)
	}
	if pod.Spec.Containers[0].Image != "redpanda:test" {
		t.Errorf("pod image = %q, want redpanda:test", pod.Spec.Containers[0].Image)
	}
	if !hasArg(pod, "--advertise-kafka-addr", "PLAINTEXT://"+want) {
		t.Errorf("pod does not advertise the service DNS: %v", pod.Spec.Containers[0].Args)
	}

	// A second ensure reuses the running broker (no new objects, same address).
	again, err := b.EnsureCluster(ctx, key.Project, key.Location, key.Cluster)
	if err != nil || again != want {
		t.Fatalf("second EnsureCluster = %q, %v; want %q, nil", again, err, want)
	}

	if err := b.StopCluster(ctx, key.Project, key.Location, key.Cluster); err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != "" {
		t.Fatalf("Endpoint after stop = %q, want empty", ep)
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Error("pod still present after StopCluster")
	}
	// Reaping an unknown cluster is a no-op.
	if err := b.StopCluster(ctx, "p", "l", "absent"); err != nil {
		t.Errorf("StopCluster(absent): %v", err)
	}
}

func TestK8sBrokerForbiddenNamespaceGetIsNotFatal(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "jaiscloud", errors.New("forbidden"))
	})
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	if _, err := b.EnsureCluster(context.Background(), "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster with a forbidden namespace Get: %v", err)
	}
}

func TestK8sBrokerShutdownReapsAll(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	ctx := context.Background()
	for _, c := range []string{"c1", "c2"} {
		if _, err := b.EnsureCluster(ctx, "p", "l", c); err != nil {
			t.Fatalf("EnsureCluster(%s): %v", c, err)
		}
	}
	if err := b.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	for _, c := range []string{"c1", "c2"} {
		if ep := b.Endpoint("p", "l", c); ep != "" {
			t.Errorf("Endpoint(%s) after shutdown = %q, want empty", c, ep)
		}
	}
}

// TestK8sBrokerResetReapsAndSweeps proves /_jaiscloud/reset teardown: Reset
// clears tracked endpoints and also sweeps broker resources the process never
// tracked (a failed start or a previous instance's leftovers).
func TestK8sBrokerResetReapsAndSweeps(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	ctx := context.Background()
	if _, err := b.EnsureCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	name := brokerResourceName(ClusterKey{Project: "p", Location: "l", Cluster: "c1"})

	if _, err := client.CoreV1().Pods("jaiscloud").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "mkbroker-orphan", Namespace: "jaiscloud", Labels: map[string]string{brokerLabelKey: brokerLabelValue}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed orphan pod: %v", err)
	}

	if err := b.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if ep := b.Endpoint("p", "l", "c1"); ep != "" {
		t.Errorf("Endpoint after Reset = %q, want empty", ep)
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Error("tracked broker pod survived Reset")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Error("tracked broker service survived Reset")
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, "mkbroker-orphan", metav1.GetOptions{}); err == nil {
		t.Error("untracked orphan pod survived Reset")
	}
}

// TestK8sBrokerSweepOrphans proves the startup sweep deletes only
// broker-labeled Pods/Services, leaving unrelated resources alone.
func TestK8sBrokerSweepOrphans(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mkbroker-orphan", Namespace: "jaiscloud", Labels: map[string]string{brokerLabelKey: brokerLabelValue}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other-pod", Namespace: "jaiscloud"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "mkbroker-orphan", Namespace: "jaiscloud", Labels: map[string]string{brokerLabelKey: brokerLabelValue}}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "other-svc", Namespace: "jaiscloud"}},
	)
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.sweepOrphans()

	ctx := context.Background()
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, "mkbroker-orphan", metav1.GetOptions{}); err == nil {
		t.Error("labeled orphan pod was not swept")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, "mkbroker-orphan", metav1.GetOptions{}); err == nil {
		t.Error("labeled orphan service was not swept")
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, "other-pod", metav1.GetOptions{}); err != nil {
		t.Error("unlabeled pod must not be swept")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, "other-svc", metav1.GetOptions{}); err != nil {
		t.Error("unlabeled service must not be swept")
	}
}

// TestK8sBrokerEnsureFailureReapsResources proves a broker that never becomes
// ready does not leak the Pod/Service it created.
func TestK8sBrokerEnsureFailureReapsResources(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return false } // never ready

	key := ClusterKey{Project: "p", Location: "l", Cluster: "c1"}
	name := brokerResourceName(key)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := b.EnsureCluster(ctx, key.Project, key.Location, key.Cluster); err == nil {
		t.Fatal("EnsureCluster succeeded with a never-ready broker; want error")
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(context.Background(), name, metav1.GetOptions{}); err == nil {
		t.Error("pod leaked after a failed broker start")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(context.Background(), name, metav1.GetOptions{}); err == nil {
		t.Error("service leaked after a failed broker start")
	}
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != "" {
		t.Errorf("Endpoint after failed start = %q, want empty", ep)
	}
}

func TestBrokerResourceName(t *testing.T) {
	key := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "My_Cluster!!"}
	name := brokerResourceName(key)
	if name != strings.ToLower(name) {
		t.Errorf("name %q is not lowercase", name)
	}
	if strings.ContainsAny(name, "_!") {
		t.Errorf("name %q contains characters unsafe for a DNS label", name)
	}
	if len(name) > 63 {
		t.Errorf("name %q exceeds 63 chars", name)
	}
	// Stable for the same key and distinct across keys.
	if brokerResourceName(key) != name {
		t.Error("name is not stable")
	}
	other := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "other"}
	if brokerResourceName(other) == name {
		t.Error("distinct clusters produced the same name")
	}
}

func TestNativeBrokerLifecycle(t *testing.T) {
	// A fake "redpanda" that stays alive until reaped. EnsureCluster's readiness
	// probe is injected, so no real listener is needed.
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-redpanda")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}

	b := newNativeBroker(bin, dir, discardLogger())
	b.probe = func(string) bool { return true }

	ctx := context.Background()
	got, err := b.EnsureCluster(ctx, "p", "l", "c1")
	if err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	if !strings.HasPrefix(got, "127.0.0.1:") {
		t.Fatalf("endpoint = %q, want loopback", got)
	}
	if b.Endpoint("p", "l", "c1") != got {
		t.Errorf("Endpoint = %q, want %q", b.Endpoint("p", "l", "c1"), got)
	}
	// The broker data dir is created under the configured root.
	if _, err := os.Stat(filepath.Join(dir, "managedkafka", brokerResourceName(ClusterKey{Project: "p", Location: "l", Cluster: "c1"}))); err != nil {
		t.Errorf("data dir not created: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- b.StopCluster(ctx, "p", "l", "c1") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StopCluster: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("StopCluster did not reap the subprocess in time")
	}
	if b.Endpoint("p", "l", "c1") != "" {
		t.Error("Endpoint not cleared after StopCluster")
	}
	// Broker data is ephemeral: the per-cluster scratch dir is removed on stop.
	if _, err := os.Stat(filepath.Join(dir, "managedkafka", brokerResourceName(ClusterKey{Project: "p", Location: "l", Cluster: "c1"}))); !os.IsNotExist(err) {
		t.Errorf("data dir not removed after StopCluster: %v", err)
	}
}

// TestNativeBrokerResetRemovesData proves /_jaiscloud/reset teardown: Reset
// stops the broker, clears its endpoint, and removes the data root.
func TestNativeBrokerResetRemovesData(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-redpanda")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	b := newNativeBroker(bin, dir, discardLogger())
	b.probe = func(string) bool { return true }

	ctx := context.Background()
	if _, err := b.EnsureCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if ep := b.Endpoint("p", "l", "c1"); ep != "" {
		t.Errorf("Endpoint after Reset = %q, want empty", ep)
	}
	if _, err := os.Stat(filepath.Join(dir, "managedkafka")); !os.IsNotExist(err) {
		t.Errorf("data root not removed after Reset: %v", err)
	}
}

// TestNativeBrokerSweepsStaleDataOnStart proves a restarted emulator does not
// leak a previous instance's data dirs.
func TestNativeBrokerSweepsStaleDataOnStart(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "managedkafka", "mkbroker-stale")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatalf("seed stale data dir: %v", err)
	}
	bin := filepath.Join(dir, "fake-redpanda")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	b := newNativeBroker(bin, dir, discardLogger())
	b.probe = func(string) bool { return true }
	t.Cleanup(func() { _ = b.Shutdown(context.Background()) })

	if _, err := b.EnsureCluster(context.Background(), "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("stale data dir not swept on first start: %v", err)
	}
}

func hasArg(pod *corev1.Pod, name, value string) bool {
	args := pod.Spec.Containers[0].Args
	for i, a := range args {
		if a == name && i+1 < len(args) && args[i+1] == value {
			return true
		}
	}
	return false
}

// --- native broker reaping (MK10) ---

// requireLinux skips tests that depend on Linux /proc start-time fingerprints
// and Pdeathsig, both of which are Linux-only.
func requireLinux(t *testing.T) {
	t.Helper()
	if goruntime.GOOS != "linux" {
		t.Skip("native broker reaping uses Linux /proc and Pdeathsig")
	}
}

// fakeRedpandaBinary writes a stand-in broker that stays alive until killed.
func fakeRedpandaBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "fake-redpanda")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexec sleep 60\n"), 0o755); err != nil {
		t.Fatalf("write fake binary: %v", err)
	}
	return bin
}

// processRunning reports whether pid exists and is not a zombie. A killed but
// unreaped child is still visible in /proc but is effectively dead.
func processRunning(pid int) bool {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	end := bytes.LastIndexByte(data, ')')
	if end < 0 || end+2 >= len(data) {
		return false
	}
	return data[end+2] != 'Z'
}

func waitProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := clock.RealNow().Add(5 * time.Second)
	for clock.RealNow().Before(deadline) {
		if !processRunning(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("process %d still running", pid)
}

// TestNativeBrokerPidfileLifecycle proves the pidfile is written on start and
// removed on a graceful stop, so a reaped broker can never be killed again.
func TestNativeBrokerPidfileLifecycle(t *testing.T) {
	requireLinux(t)
	dir := t.TempDir()
	key := ClusterKey{Project: "p", Location: "l", Cluster: "c1"}
	clusterDir := filepath.Join(dir, "managedkafka", brokerResourceName(key))
	pidPath := filepath.Join(clusterDir, nativePidFile)

	b := newNativeBroker(fakeRedpandaBinary(t), dir, discardLogger())
	b.probe = func(string) bool { return true }

	if _, err := b.EnsureCluster(context.Background(), "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	rec, ok := readPidRecord(clusterDir)
	if !ok {
		t.Fatalf("pidfile not written at %s", pidPath)
	}
	if rec.PID <= 0 || rec.StartTime == "" {
		t.Fatalf("pid record = %+v, want a pid and start-time", rec)
	}
	if _, ok := procStartTime(rec.PID); !ok {
		t.Fatalf("recorded pid %d is not live", rec.PID)
	}

	if err := b.StopCluster(context.Background(), "p", "l", "c1"); err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if _, err := os.Stat(pidPath); !os.IsNotExist(err) {
		t.Errorf("pidfile survived StopCluster: %v", err)
	}
}

// TestNativeBrokerSweepReapsMatchingPID proves the startup sweep kills the
// recorded orphan when its start-time fingerprint still matches, then removes
// the data root.
func TestNativeBrokerSweepReapsMatchingPID(t *testing.T) {
	requireLinux(t)
	root := t.TempDir()
	key := ClusterKey{Project: "p", Location: "l", Cluster: "c1"}
	clusterDir := filepath.Join(root, "managedkafka", brokerResourceName(key))
	if err := os.MkdirAll(clusterDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	orphan := exec.Command("sleep", "60")
	if err := orphan.Start(); err != nil {
		t.Fatalf("start orphan: %v", err)
	}
	t.Cleanup(func() { _ = orphan.Process.Kill(); _, _ = orphan.Process.Wait() })
	if err := writePidRecord(clusterDir, orphan.Process.Pid); err != nil {
		t.Fatalf("write pid record: %v", err)
	}

	newNativeBroker("/nonexistent", root, discardLogger()).sweepOrphans()

	waitProcessGone(t, orphan.Process.Pid)
	if _, err := os.Stat(filepath.Join(root, "managedkafka")); !os.IsNotExist(err) {
		t.Errorf("data root not removed after sweep: %v", err)
	}
}

// TestNativeBrokerFailedStartRemovesPidfile proves the failed-readiness reap
// does not leave a pidfile behind that a later sweep might rerun.
func TestNativeBrokerFailedStartRemovesPidfile(t *testing.T) {
	requireLinux(t)
	dir := t.TempDir()
	key := ClusterKey{Project: "p", Location: "l", Cluster: "c1"}
	clusterDir := filepath.Join(dir, "managedkafka", brokerResourceName(key))

	b := newNativeBroker(fakeRedpandaBinary(t), dir, discardLogger())
	b.probe = func(string) bool { return false } // never ready

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := b.EnsureCluster(ctx, key.Project, key.Location, key.Cluster); err == nil {
		t.Fatal("EnsureCluster succeeded with a never-ready broker; want error")
	}
	if _, ok := readPidRecord(clusterDir); ok {
		t.Error("pidfile leaked after a failed broker start")
	}
}

// TestNativeBrokerSweepLeavesMismatchedPID proves a recycled/mismatched PID is
// never killed: the recorded start-time does not match the live process.
func TestNativeBrokerSweepLeavesMismatchedPID(t *testing.T) {
	requireLinux(t)
	root := t.TempDir()
	key := ClusterKey{Project: "p", Location: "l", Cluster: "c1"}
	clusterDir := filepath.Join(root, "managedkafka", brokerResourceName(key))
	if err := os.MkdirAll(clusterDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	orphan := exec.Command("sleep", "60")
	if err := orphan.Start(); err != nil {
		t.Fatalf("start orphan: %v", err)
	}
	t.Cleanup(func() { _ = orphan.Process.Kill(); _, _ = orphan.Process.Wait() })

	// Correct PID, wrong fingerprint: a reused PID would look like this.
	rec := fmt.Sprintf("%d 0\n", orphan.Process.Pid)
	if err := os.WriteFile(filepath.Join(clusterDir, nativePidFile), []byte(rec), 0o644); err != nil {
		t.Fatalf("write pid record: %v", err)
	}

	newNativeBroker("/nonexistent", root, discardLogger()).sweepOrphans()

	if !processRunning(orphan.Process.Pid) {
		t.Fatal("sweep killed a process whose fingerprint did not match")
	}
}

// TestNativeBrokerSweepIgnoresMalformedPidfile proves a corrupt pidfile cannot
// cause a kill, and the stale dir is still cleared.
func TestNativeBrokerSweepIgnoresMalformedPidfile(t *testing.T) {
	root := t.TempDir()
	clusterDir := filepath.Join(root, "managedkafka", "mkbroker-stale")
	if err := os.MkdirAll(clusterDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(clusterDir, nativePidFile), []byte("not a pid\n"), 0o644); err != nil {
		t.Fatalf("write pidfile: %v", err)
	}

	newNativeBroker("/nonexistent", root, discardLogger()).sweepOrphans()

	if _, err := os.Stat(clusterDir); !os.IsNotExist(err) {
		t.Errorf("stale dir survived sweep: %v", err)
	}
}

// TestNativeBrokerPdeathsigReapsChild proves a SIGKILLed emulator leaves no
// broker child behind. The test re-execs itself as a helper that starts a
// native broker, reports the child PID, then blocks; the parent SIGKILLs the
// helper (no Go cleanup runs) and asserts the broker is gone via Pdeathsig.
func TestNativeBrokerPdeathsigReapsChild(t *testing.T) {
	requireLinux(t)

	if os.Getenv("KAFKA_PDEATHSIG_HELPER") == "1" {
		runPdeathsigHelper()
		return
	}

	helper := exec.Command(os.Args[0], "-test.run=TestNativeBrokerPdeathsigReapsChild", "-test.v")
	helper.Env = append(os.Environ(),
		"KAFKA_PDEATHSIG_HELPER=1",
		"KAFKA_PDEATHSIG_DIR="+t.TempDir(),
		"KAFKA_PDEATHSIG_BIN="+fakeRedpandaBinary(t),
	)
	stdout, err := helper.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	helper.Stderr = os.Stderr
	if err := helper.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}

	pid := readBrokerPID(t, stdout)
	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	_, _ = helper.Process.Wait()

	waitProcessGone(t, pid)
}

// runPdeathsigHelper runs inside the re-exec'd test binary: it starts a broker
// and blocks forever (the parent SIGKILLs it). LockOSThread keeps the spawning
// OS thread alive so Pdeathsig's thread-death semantics match process death.
func runPdeathsigHelper() {
	goruntime.LockOSThread()
	defer goruntime.UnlockOSThread()

	dir := os.Getenv("KAFKA_PDEATHSIG_DIR")
	b := newNativeBroker(os.Getenv("KAFKA_PDEATHSIG_BIN"), dir, discardLogger())
	b.probe = func(string) bool { return true }
	if _, err := b.EnsureCluster(context.Background(), "p", "l", "c1"); err != nil {
		fmt.Fprintf(os.Stdout, "HELPER_ERROR=%v\n", err)
		os.Exit(1)
	}
	key := ClusterKey{Project: "p", Location: "l", Cluster: "c1"}
	rec, ok := readPidRecord(filepath.Join(dir, "managedkafka", brokerResourceName(key)))
	if !ok {
		fmt.Fprintln(os.Stdout, "HELPER_ERROR=no pidfile written")
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "BROKER_PID=%d\n", rec.PID)
	_ = os.Stdout.Sync()
	select {}
}

func readBrokerPID(t *testing.T, stdout io.Reader) int {
	t.Helper()
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if v, ok := strings.CutPrefix(line, "BROKER_PID="); ok {
			pid, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				t.Fatalf("bad broker pid in %q: %v", line, err)
			}
			return pid
		}
		if strings.HasPrefix(line, "HELPER_ERROR=") {
			t.Fatalf("helper failed: %s", line)
		}
	}
	t.Fatalf("helper exited without reporting a broker pid: %v", scanner.Err())
	return 0
}
