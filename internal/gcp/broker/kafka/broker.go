// Package kafka manages the single-node Kafka-compatible broker (Redpanda)
// that backs a Google Cloud Managed Kafka cluster's bootstrapAddress. It owns
// the broker data-plane lifecycle:
//
//   - mock (default): no broker is started and the caller keeps rendering the
//     synthesized cloud.goog bootstrap name. This is the hermetic default so
//     unit/integration tests never depend on a running broker.
//   - k8s: a single-node Redpanda Pod plus a ClusterIP Service in the
//     emulator's namespace; the bootstrap address is the Service DNS name.
//   - native: a Redpanda subprocess bound to loopback ports, modelled on the
//     AWS kinesis-mock lifecycle (locate a binary, pick free ports, poll for
//     readiness, reap on stop).
//
// Every real-mode failure (missing binary, no cluster, image pull failure,
// startup timeout) is reported as an error so the caller can degrade to mock;
// the broker never panics the control plane. The manager holds endpoints in
// memory only — broker liveness is runtime state, not persisted metadata — so a
// restarted emulator never advertises a dead address. Broker data is ephemeral
// and non-portable (MK5): a k8s broker uses an emptyDir, a native broker a
// per-cluster scratch dir, and both are removed on stop/reset. Reset tears down
// every broker for /_jaiscloud/reset, and a startup sweep reaps resources left
// by a previous instance so restarted emulators do not leak broker Pods,
// Services, or data dirs.
package kafka

import (
	"context"
	"log/slog"
	"os"
	"os/exec"

	"k8s.io/client-go/kubernetes"

	core "jaiscloud/internal/gcp/service/managedkafka"
)

// Mode selects the broker backend.
type Mode string

const (
	// ModeMock starts no broker; EnsureCluster returns an empty endpoint.
	ModeMock Mode = "mock"
	// ModeK8s runs a Redpanda Pod + Service in the emulator's namespace.
	ModeK8s Mode = "k8s"
	// ModeNative runs a Redpanda subprocess on loopback ports.
	ModeNative Mode = "native"
)

// ClusterKey identifies the Managed Kafka cluster a broker serves.
type ClusterKey struct {
	Project  string
	Location string
	Cluster  string
}

// String returns the stable "project/location/cluster" identity used as a map
// key and to derive per-cluster resource names.
func (k ClusterKey) String() string {
	return k.Project + "/" + k.Location + "/" + k.Cluster
}

// Broker manages the broker backing one or more Managed Kafka clusters. The
// method set intentionally mirrors the transport-neutral core's Broker
// interface (plain-string arguments) so *this value can be injected directly
// via managedkafka.WithBroker without an adapter.
type Broker interface {
	// EnsureCluster starts (or reuses) the broker for the cluster and returns
	// its bootstrap endpoint. An empty endpoint with a nil error means the
	// broker is intentionally absent (mock mode); a non-nil error means the
	// caller should fall back to the synthesized address.
	EnsureCluster(ctx context.Context, project, location, cluster string) (string, error)
	// Endpoint returns the live bootstrap endpoint for the cluster, or "" when
	// no broker is currently running for it. It never starts a broker.
	Endpoint(project, location, cluster string) string
	// StopCluster stops and reaps the broker (called when the cluster itself is
	// deleted).
	StopCluster(ctx context.Context, project, location, cluster string) error
	// EnsureTopic provisions the topic on the cluster's live broker with the
	// given partition count and property overrides. With no running broker
	// (mock, or a cluster whose broker is not live) it is a metadata-only
	// no-op, so hermetic tests need nothing. The emulator broker is
	// single-node, so callers' replication factor is metadata only and the
	// broker replica count is always 1. An invalid config key/value is
	// reported as an error wrapping the core's ErrInvalidTopicConfig.
	EnsureTopic(ctx context.Context, project, location, cluster, topic string, partitions int, configs map[string]string) error
	// AlterTopicConfigs applies an incremental topic-config change: it writes
	// every key in set and clears every key in remove. With no running broker
	// it is a metadata-only no-op; an invalid key/value is reported as an
	// error wrapping the core's ErrInvalidTopicConfig.
	AlterTopicConfigs(ctx context.Context, project, location, cluster, topic string, set map[string]string, remove []string) error
	// AddTopicPartitions raises the topic's broker partition count to
	// totalPartitions. Lowering the count is rejected by the broker.
	AddTopicPartitions(ctx context.Context, project, location, cluster, topic string, totalPartitions int) error
	// DeleteBrokerTopic removes the topic from the broker. It is idempotent and
	// a no-op when no broker is running.
	DeleteBrokerTopic(ctx context.Context, project, location, cluster, topic string) error
	// ListConsumerGroups returns the group ids known to the cluster's group
	// coordinator. No live broker yields an empty set.
	ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]string, error)
	// ConsumerGroupOffsets returns the group's committed offsets (bare topic
	// ids). found is false when no broker is running or the group is unknown.
	ConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string) ([]core.ConsumerGroupOffset, bool, error)
	// ConsumerGroupMembers returns the number of active members in the group.
	// found is false when no broker is running or the group is unknown.
	ConsumerGroupMembers(ctx context.Context, project, location, cluster, group string) (int, bool, error)
	// DeleteConsumerGroup removes the group and its offsets. existed is false
	// when no broker is running or the group was already absent.
	DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) (bool, error)
	// CommitConsumerGroupOffsets sets the group's committed offsets. With no
	// live broker it is a no-op.
	CommitConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string, offsets []core.ConsumerGroupOffset) error
	// ReplaceAcl mirrors an ACL's entry set onto the cluster's live broker,
	// replacing every binding for the ACL's resource pattern; an empty entry
	// set removes them. With no live broker it is a no-op.
	ReplaceAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []core.AclBinding) error
	// Reset stops and reaps every broker this manager owns and clears all
	// tracked state, leaving the manager reusable: a later EnsureCluster starts
	// a fresh broker. It also sweeps broker resources this process never
	// tracked (a failed partial create, or a previous instance's leftovers).
	// The control plane calls it on /_jaiscloud/reset so a reset does not leak
	// broker Pods, Services, or subprocesses.
	Reset(ctx context.Context) error
	// Shutdown stops every broker this manager owns (called on emulator
	// shutdown). It has the same effect as Reset.
	Shutdown(ctx context.Context) error
	// Mode reports the configured backend.
	Mode() Mode
}

// Config configures New.
type Config struct {
	// Mode is "mock" (default), "k8s", or "native".
	Mode string
	// Namespace is the Kubernetes namespace for k8s mode (default "jaiscloud").
	Namespace string
	// Image is the Redpanda image for k8s mode.
	Image string
	// BinaryPath overrides the native Redpanda CLI (`rpk`) location.
	BinaryPath string
	// DataDir is the root data directory for native brokers.
	DataDir string
	// Client is the Kubernetes client used by k8s mode.
	Client kubernetes.Interface
	// Logger receives lifecycle logs (default slog.Default()).
	Logger *slog.Logger
}

// New returns the Broker for cfg. An unknown mode, or k8s mode without a
// Kubernetes client, degrades to mock with a warning. Native mode without a
// resolvable Redpanda binary also degrades to mock.
func New(cfg Config) Broker {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	switch Mode(cfg.Mode) {
	case ModeK8s:
		if cfg.Client == nil {
			logger.Warn("managedkafka broker: k8s mode requested without a kubernetes client; falling back to mock")
			return mockBroker{}
		}
		ns := cfg.Namespace
		if ns == "" {
			ns = "jaiscloud"
		}
		image := cfg.Image
		if image == "" {
			image = defaultRedpandaImage
		}
		return newK8sBroker(cfg.Client, ns, image, logger)
	case ModeNative:
		binary := resolveNativeBinary(cfg.BinaryPath)
		if binary == "" {
			logger.Warn("managedkafka broker: native mode requested but no rpk binary found; falling back to mock (set JAISCLOUD_KAFKA_BROKER_BIN)")
			return mockBroker{}
		}
		return newNativeBroker(binary, cfg.DataDir, logger)
	case "", ModeMock:
		return mockBroker{}
	default:
		logger.Warn("managedkafka broker: unknown mode; falling back to mock", "mode", cfg.Mode)
		return mockBroker{}
	}
}

// resolveNativeBinary returns the configured Redpanda CLI path, or the first
// `rpk` on PATH. `rpk redpanda start` is the invocation native mode uses (the
// bare `redpanda` binary no longer takes the --kafka-addr family). An empty
// return means native mode is unavailable.
func resolveNativeBinary(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if p := os.Getenv("JAISCLOUD_KAFKA_BROKER_BIN"); p != "" {
		return p
	}
	if p, err := exec.LookPath("rpk"); err == nil {
		return p
	}
	return ""
}

// mockBroker implements the no-broker backend: the caller keeps the
// synthesized bootstrap name.
type mockBroker struct{}

func (mockBroker) EnsureCluster(context.Context, string, string, string) (string, error) {
	return "", nil
}
func (mockBroker) Endpoint(string, string, string) string                    { return "" }
func (mockBroker) StopCluster(context.Context, string, string, string) error { return nil }
func (mockBroker) EnsureTopic(context.Context, string, string, string, string, int, map[string]string) error {
	return nil
}
func (mockBroker) AlterTopicConfigs(context.Context, string, string, string, string, map[string]string, []string) error {
	return nil
}
func (mockBroker) AddTopicPartitions(context.Context, string, string, string, string, int) error {
	return nil
}
func (mockBroker) DeleteBrokerTopic(context.Context, string, string, string, string) error {
	return nil
}
func (mockBroker) ListConsumerGroups(context.Context, string, string, string) ([]string, error) {
	return nil, nil
}
func (mockBroker) ConsumerGroupOffsets(context.Context, string, string, string, string) ([]core.ConsumerGroupOffset, bool, error) {
	return nil, false, nil
}
func (mockBroker) ConsumerGroupMembers(context.Context, string, string, string, string) (int, bool, error) {
	return 0, false, nil
}
func (mockBroker) DeleteConsumerGroup(context.Context, string, string, string, string) (bool, error) {
	return false, nil
}
func (mockBroker) CommitConsumerGroupOffsets(context.Context, string, string, string, string, []core.ConsumerGroupOffset) error {
	return nil
}
func (mockBroker) ReplaceAcl(context.Context, string, string, string, string, string, string, []core.AclBinding) error {
	return nil
}
func (mockBroker) Reset(context.Context) error    { return nil }
func (mockBroker) Shutdown(context.Context) error { return nil }
func (mockBroker) Mode() Mode                     { return ModeMock }

// defaultRedpandaImage is the single-node Redpanda image used by k8s mode.
const defaultRedpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"
