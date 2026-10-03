package kafka

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/managedkafka"
)

// nativeBroker runs a Redpanda subprocess per Managed Kafka cluster, bound to
// loopback ports. It mirrors the AWS kinesis-mock lifecycle: locate a binary,
// pick free ports, poll for readiness, and reap on stop.
type nativeBroker struct {
	binary  string
	dataDir string
	logger  *slog.Logger

	mu        sync.Mutex
	instances map[ClusterKey]*nativeInstance
	admins    *adminPool
	// sweepOnce removes stale data dirs from a previous instance exactly once,
	// before the first broker starts.
	sweepOnce sync.Once

	// probe reports whether addr is accepting TCP connections. Overridable in
	// tests; defaults to a one-second TCP dial.
	probe func(addr string) bool
}

type nativeInstance struct {
	cmd  *exec.Cmd
	addr string
	dir  string
}

func newNativeBroker(binary, dataDir string, logger *slog.Logger) *nativeBroker {
	return &nativeBroker{
		binary:    binary,
		dataDir:   dataDir,
		logger:    logger,
		instances: make(map[ClusterKey]*nativeInstance),
		admins:    newAdminPool(nil),
		probe:     tcpProbe,
	}
}

func (b *nativeBroker) Mode() Mode { return ModeNative }

func (b *nativeBroker) Endpoint(project, location, cluster string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if inst, ok := b.instances[ClusterKey{Project: project, Location: location, Cluster: cluster}]; ok {
		return inst.addr
	}
	return ""
}

// EnsureTopic provisions the topic on the live broker, or no-ops when no broker
// is running for the cluster (metadata-only topology).
func (b *nativeBroker) EnsureTopic(ctx context.Context, project, location, cluster, topic string, partitions int, configs map[string]string) error {
	return ensureTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic, partitions, configs)
}

// AlterTopicConfigs applies an incremental topic-config change on the live
// broker, or no-ops when no broker is running for the cluster.
func (b *nativeBroker) AlterTopicConfigs(ctx context.Context, project, location, cluster, topic string, set map[string]string, remove []string) error {
	return alterTopicConfigs(ctx, b.admins, b.Endpoint(project, location, cluster), topic, set, remove)
}

// AddTopicPartitions raises the topic's partition count on the live broker.
func (b *nativeBroker) AddTopicPartitions(ctx context.Context, project, location, cluster, topic string, totalPartitions int) error {
	return addPartitions(ctx, b.admins, b.Endpoint(project, location, cluster), topic, totalPartitions)
}

// DeleteBrokerTopic removes the topic from the live broker.
func (b *nativeBroker) DeleteBrokerTopic(ctx context.Context, project, location, cluster, topic string) error {
	return deleteBrokerTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic)
}

// ListConsumerGroups returns the live broker's consumer groups, or an empty set
// when no broker is running.
func (b *nativeBroker) ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]string, error) {
	return listGroups(ctx, b.admins, b.Endpoint(project, location, cluster))
}

// ConsumerGroupOffsets returns the group's committed offsets on the live broker.
func (b *nativeBroker) ConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string) ([]core.ConsumerGroupOffset, bool, error) {
	return groupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// ConsumerGroupMembers returns the group's active member count on the live broker.
func (b *nativeBroker) ConsumerGroupMembers(ctx context.Context, project, location, cluster, group string) (int, bool, error) {
	return groupMembers(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// DeleteConsumerGroup removes the group from the live broker.
func (b *nativeBroker) DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) (bool, error) {
	return deleteGroup(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// CommitConsumerGroupOffsets sets the group's committed offsets on the live broker.
func (b *nativeBroker) CommitConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string, offsets []core.ConsumerGroupOffset) error {
	return commitGroupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group, offsets)
}

// ReplaceAcl mirrors an ACL's entry set onto the live broker.
func (b *nativeBroker) ReplaceAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []core.AclBinding) error {
	return replaceACLs(ctx, b.admins, b.Endpoint(project, location, cluster), aclSpec{
		ResourceType: resourceType,
		ResourceName: resourceName,
		PatternType:  patternType,
		Entries:      entries,
	})
}

func (b *nativeBroker) EnsureCluster(ctx context.Context, project, location, cluster string) (string, error) {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}

	// Remove stale data dirs from a previous instance before creating one.
	b.sweepOnce.Do(b.sweepOrphans)

	b.mu.Lock()
	if inst, ok := b.instances[key]; ok {
		b.mu.Unlock()
		return inst.addr, nil
	}
	b.mu.Unlock()

	port, err := freePort()
	if err != nil {
		return "", fmt.Errorf("managedkafka broker: find free port: %w", err)
	}
	rpcPort, err := freePort()
	if err != nil {
		return "", fmt.Errorf("managedkafka broker: find free rpc port: %w", err)
	}

	dir := ""
	if b.dataDir != "" {
		dir = filepath.Join(b.dataDir, "managedkafka", brokerResourceName(key))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("managedkafka broker: data dir: %w", err)
		}
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// The resolved binary is the Redpanda CLI (`rpk`); `rpk redpanda start`
	// writes the node config and launches the broker.
	args := []string{
		"redpanda",
		"start",
		"--overprovisioned",
		"--smp", "1",
		"--memory", "1G",
		"--reserve-memory", "0M",
		"--node-id", "0",
		"--check=false",
		"--kafka-addr", "PLAINTEXT://" + addr,
		"--advertise-kafka-addr", "PLAINTEXT://" + addr,
		"--rpc-addr", fmt.Sprintf("127.0.0.1:%d", rpcPort),
	}
	if dir != "" {
		args = append(args, "--data-dir", dir)
	}

	// The subprocess lifetime is owned by the broker manager (StopCluster /
	// Shutdown), not by the request context, so it is deliberately not bound to
	// ctx: a request-scoped context would kill the broker when the create call
	// returns.
	cmd := exec.Command(b.binary, args...)
	applySysProcAttr(cmd)
	cmd.Stdout = &slogWriter{level: slog.LevelDebug, prefix: "redpanda"}
	cmd.Stderr = &slogWriter{level: slog.LevelWarn, prefix: "redpanda"}

	b.logger.Info("managedkafka broker: starting redpanda", "cluster", key.String(), "address", addr, "binary", b.binary)
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("managedkafka broker: start redpanda: %w", err)
	}

	// Record the child (PID + /proc start-time) under its data dir so a later
	// emulator instance can safely reap it if this one is SIGKILLed before
	// StopCluster/Shutdown runs. Pdeathsig covers the live case; the pidfile
	// fingerprint covers a crashed parent whose child outlived it.
	if dir != "" {
		if err := writePidRecord(dir, cmd.Process.Pid); err != nil {
			b.logger.Warn("managedkafka broker: pidfile write failed", "dir", dir, "err", err)
		}
	}

	if err := b.waitReady(ctx, addr); err != nil {
		// Reap the failed process and collect it so no zombie or orphaned
		// child is left behind.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if dir != "" {
			_ = os.Remove(filepath.Join(dir, nativePidFile))
		}
		return "", err
	}

	b.mu.Lock()
	b.instances[key] = &nativeInstance{cmd: cmd, addr: addr, dir: dir}
	b.mu.Unlock()
	b.logger.Info("managedkafka broker ready", "cluster", key.String(), "address", addr)
	return addr, nil
}

func (b *nativeBroker) StopCluster(_ context.Context, project, location, cluster string) error {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	b.mu.Lock()
	inst := b.instances[key]
	delete(b.instances, key)
	b.mu.Unlock()
	if inst == nil {
		return nil
	}
	// Drop the pooled admin client before the process goes away.
	b.admins.close(inst.addr)
	err := stopProcess(inst.cmd)
	// Broker data is ephemeral and non-portable (MK5): remove the per-cluster
	// scratch dir so a reused cluster id starts clean. The pidfile goes with
	// it, so a graceful stop can never be reaped again on a later startup.
	if inst.dir != "" {
		_ = os.Remove(filepath.Join(inst.dir, nativePidFile))
		if rmErr := os.RemoveAll(inst.dir); rmErr != nil {
			b.logger.Warn("managedkafka broker: data dir cleanup failed", "dir", inst.dir, "err", rmErr)
		}
	}
	return err
}

// Reset stops and reaps every broker, clears its in-memory bookkeeping, and
// removes the broker data root so a reset leaves no subprocess bookkeeping or
// on-disk state behind. The manager stays usable: a later EnsureCluster starts
// a fresh broker.
func (b *nativeBroker) Reset(ctx context.Context) error {
	b.mu.Lock()
	keys := make([]ClusterKey, 0, len(b.instances))
	for k := range b.instances {
		keys = append(keys, k)
	}
	b.mu.Unlock()

	var firstErr error
	for _, k := range keys {
		if err := b.StopCluster(ctx, k.Project, k.Location, k.Cluster); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	b.admins.closeAll()
	b.removeDataRoot()
	return firstErr
}

// Shutdown reaps every broker on emulator shutdown.
func (b *nativeBroker) Shutdown(ctx context.Context) error { return b.Reset(ctx) }

// sweepOrphans reaps brokers left by a previous emulator instance: for each
// per-cluster data dir it kills the recorded child — but only when the PID's
// /proc start-time still matches the recorded one, so a recycled PID is never
// killed — then removes the broker data root. A subprocess is reaped on a
// graceful StopCluster/Shutdown too; this covers the SIGKILLed-parent path.
func (b *nativeBroker) sweepOrphans() {
	if b.dataDir != "" {
		root := filepath.Join(b.dataDir, "managedkafka")
		if entries, err := os.ReadDir(root); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					b.reapRecordedPID(filepath.Join(root, e.Name()))
				}
			}
		}
	}
	b.removeDataRoot()
}

// reapRecordedPID kills the broker process recorded in dir's pidfile, but only
// when the recorded start-time equals the live process's — the PID-reuse-safe
// alternative to killing by name/arg. os.FindProcess + Process.Kill is used
// rather than syscall.Kill so the file still builds off Unix (the start-time
// fingerprint already guarantees the process is live).
func (b *nativeBroker) reapRecordedPID(dir string) {
	rec, ok := readPidRecord(dir)
	if !ok {
		return
	}
	if live, ok := procStartTime(rec.PID); !ok || live != rec.StartTime {
		return
	}
	proc, err := os.FindProcess(rec.PID)
	if err != nil {
		b.logger.Warn("managedkafka broker: find orphan failed", "pid", rec.PID, "err", err)
		return
	}
	if err := proc.Kill(); err != nil {
		b.logger.Warn("managedkafka broker: reap orphan failed", "pid", rec.PID, "err", err)
		return
	}
	b.logger.Info("managedkafka broker: reaped orphaned broker subprocess", "pid", rec.PID, "dir", dir)
}

// nativePidFile is the per-cluster pidfile name written under the broker data
// dir so a later emulator instance can safely reap an orphaned broker.
const nativePidFile = "broker.pid"

// pidRecord identifies a broker subprocess without relying on its name: the
// PID plus its /proc start-time, which is stable for the life of the process
// and changes when the kernel reuses the PID.
type pidRecord struct {
	PID       int
	StartTime string
}

// writePidRecord records pid (with its start-time fingerprint) in dir. It
// fails — leaving no pidfile — when the start-time cannot be read, because a
// record without a fingerprint could never be safely reaped.
func writePidRecord(dir string, pid int) error {
	start, ok := procStartTime(pid)
	if !ok {
		return fmt.Errorf("read start time for pid %d", pid)
	}
	rec := fmt.Sprintf("%d %s\n", pid, start)
	return os.WriteFile(filepath.Join(dir, nativePidFile), []byte(rec), 0o644)
}

// readPidRecord reads and validates dir's pidfile.
func readPidRecord(dir string) (pidRecord, bool) {
	data, err := os.ReadFile(filepath.Join(dir, nativePidFile))
	if err != nil {
		return pidRecord{}, false
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return pidRecord{}, false
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil || pid <= 0 {
		return pidRecord{}, false
	}
	return pidRecord{PID: pid, StartTime: fields[1]}, true
}

// procStartTime returns field 22 (start-time) of Linux /proc/<pid>/stat, a
// value that is stable for the life of the process and distinguishes a PID
// from a later process that reused it. ok is false when it cannot be read
// (the process is gone, or the platform has no /proc).
func procStartTime(pid int) (string, bool) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", false
	}
	// The comm field (2) is parenthesized and may itself contain spaces and
	// parentheses, so split only after the final ')'. Field 3 (state) then
	// starts the remainder, making start-time (field 22) index 19.
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return "", false
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return "", false
	}
	return fields[19], true
}

// removeDataRoot deletes the broker data root under the configured data dir.
// Broker data is ephemeral and non-portable, so nothing here is restored.
func (b *nativeBroker) removeDataRoot() {
	if b.dataDir == "" {
		return
	}
	root := filepath.Join(b.dataDir, "managedkafka")
	if err := os.RemoveAll(root); err != nil {
		b.logger.Warn("managedkafka broker: data root cleanup failed", "dir", root, "err", err)
	}
}

func (b *nativeBroker) waitReady(ctx context.Context, addr string) error {
	deadline := clock.RealNow().Add(brokerReadyTimeout)
	for clock.RealNow().Before(deadline) {
		if b.probe(addr) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("managedkafka broker: %s not ready within %s", addr, brokerReadyTimeout)
}

// stopProcess sends SIGTERM and waits up to 5 seconds, then SIGKILL.
func stopProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
	return nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// slogWriter adapts a broker subprocess's stdout/stderr to slog.
type slogWriter struct {
	level  slog.Level
	prefix string
}

func (w *slogWriter) Write(p []byte) (int, error) {
	slog.Log(context.Background(), w.level, string(p), "source", w.prefix)
	return len(p), nil
}
