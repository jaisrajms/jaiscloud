package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	core "jaiscloud/internal/gcp/service/managedkafka"
	"jaiscloud/internal/k8shelpers"
)

const (
	// kafkaPort is the PLAINTEXT listener the broker serves and the Service
	// exposes. Real GCP omits the port from bootstrapAddress; the emulator
	// includes it so a raw client can dial the in-cluster Service directly.
	kafkaPort = 9092
	// brokerReadyTimeout bounds how long the native broker waits for its
	// listener to accept a TCP connection. The k8s broker uses the shared
	// k8shelpers workload readiness timeout.
	brokerReadyTimeout = 180 * time.Second
	// namespaceTeardownTimeout bounds a detached best-effort namespace deletion so
	// a teardown never hangs on a stuck API call.
	namespaceTeardownTimeout = 30 * time.Second
	// brokerLabelKey/Value tag every managedkafka broker resource so the K8s
	// e2e smoke can find the per-cluster Service the cluster advertises.
	brokerLabelKey   = "jaiscloud.io/broker"
	brokerLabelValue = "managedkafka"
	// brokerSelector matches every broker resource this package owns; the
	// startup orphan sweep and Reset use it to reap leftovers.
	brokerSelector = brokerLabelKey + "=" + brokerLabelValue

	// kafkaService is the ownership label value for Managed Kafka's per-cluster
	// namespaces (k8shelpers.NamespaceName / EnsureManagedNamespace). It keeps a
	// namespace sweep scoped to Kafka so it cannot touch another engine's
	// namespaces.
	kafkaService = "managedkafka"
)

// k8sBroker runs a single-node Redpanda Pod plus a ClusterIP Service per
// Managed Kafka cluster. It is safe for concurrent use.
type k8sBroker struct {
	client    kubernetes.Interface
	namespace string
	image     string
	logger    *slog.Logger

	mu        sync.Mutex
	endpoints map[ClusterKey]string // key → "<svc>.<ns>.svc.cluster.local:9092"
	// namespaces records the workload namespace actually used per cluster: the
	// derived per-cluster namespace, or the process-wide fallback when namespace
	// lifecycle RBAC is unavailable. It is runtime state (like endpoints) and is
	// never persisted, so a restart re-derives it.
	namespaces map[ClusterKey]string
	admins     *adminPool
	// sweepOnce runs the startup orphan sweep exactly once, before the first
	// broker starts.
	sweepOnce sync.Once

	// probe reports whether addr is accepting TCP connections. Overridable in
	// tests; defaults to a one-second TCP dial.
	probe func(addr string) bool
}

func newK8sBroker(client kubernetes.Interface, namespace, image string, logger *slog.Logger) *k8sBroker {
	return &k8sBroker{
		client:     client,
		namespace:  namespace,
		image:      image,
		logger:     logger,
		endpoints:  make(map[ClusterKey]string),
		namespaces: make(map[ClusterKey]string),
		admins:     newAdminPool(nil),
		probe:      tcpProbe,
	}
}

// ClusterNamespace returns the deterministic Kubernetes namespace that owns the
// broker for the given Managed Kafka cluster, derived from the cluster identity
// (project, location, cluster). It is exported so the k3d e2e and operators can
// derive the same name the broker manager provisions with the KNS1 namespace
// seam.
func ClusterNamespace(project, location, cluster string) string {
	return k8shelpers.NamespaceName(kafkaService, project, location+"/"+cluster)
}

func (b *k8sBroker) Mode() Mode { return ModeK8s }

func (b *k8sBroker) Endpoint(project, location, cluster string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.endpoints[ClusterKey{Project: project, Location: location, Cluster: cluster}]
}

// EnsureTopic provisions the topic on the live broker, or no-ops when no broker
// is running for the cluster (metadata-only topology).
func (b *k8sBroker) EnsureTopic(ctx context.Context, project, location, cluster, topic string, partitions int, configs map[string]string) error {
	return ensureTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic, partitions, configs)
}

// AlterTopicConfigs applies an incremental topic-config change on the live
// broker, or no-ops when no broker is running for the cluster.
func (b *k8sBroker) AlterTopicConfigs(ctx context.Context, project, location, cluster, topic string, set map[string]string, remove []string) error {
	return alterTopicConfigs(ctx, b.admins, b.Endpoint(project, location, cluster), topic, set, remove)
}

// AddTopicPartitions raises the topic's partition count on the live broker.
func (b *k8sBroker) AddTopicPartitions(ctx context.Context, project, location, cluster, topic string, totalPartitions int) error {
	return addPartitions(ctx, b.admins, b.Endpoint(project, location, cluster), topic, totalPartitions)
}

// DeleteBrokerTopic removes the topic from the live broker.
func (b *k8sBroker) DeleteBrokerTopic(ctx context.Context, project, location, cluster, topic string) error {
	return deleteBrokerTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic)
}

// ListConsumerGroups returns the live broker's consumer groups, or an empty set
// when no broker is running.
func (b *k8sBroker) ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]string, error) {
	return listGroups(ctx, b.admins, b.Endpoint(project, location, cluster))
}

// ConsumerGroupOffsets returns the group's committed offsets on the live broker.
func (b *k8sBroker) ConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string) ([]core.ConsumerGroupOffset, bool, error) {
	return groupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// ConsumerGroupMembers returns the group's active member count on the live broker.
func (b *k8sBroker) ConsumerGroupMembers(ctx context.Context, project, location, cluster, group string) (int, bool, error) {
	return groupMembers(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// DeleteConsumerGroup removes the group from the live broker.
func (b *k8sBroker) DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) (bool, error) {
	return deleteGroup(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// CommitConsumerGroupOffsets sets the group's committed offsets on the live broker.
func (b *k8sBroker) CommitConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string, offsets []core.ConsumerGroupOffset) error {
	return commitGroupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group, offsets)
}

// ReplaceAcl mirrors an ACL's entry set onto the live broker.
func (b *k8sBroker) ReplaceAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []core.AclBinding) error {
	return replaceACLs(ctx, b.admins, b.Endpoint(project, location, cluster), aclSpec{
		ResourceType: resourceType,
		ResourceName: resourceName,
		PatternType:  patternType,
		Entries:      entries,
	})
}

func (b *k8sBroker) EnsureCluster(ctx context.Context, project, location, cluster string) (string, error) {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	keyStr := key.String()

	// Reap broker resources left behind by a previous emulator instance (or a
	// failed start) before starting a new one.
	b.sweepOnce.Do(b.sweepOrphans)

	b.mu.Lock()
	if ep, ok := b.endpoints[key]; ok {
		b.mu.Unlock()
		return ep, nil
	}
	b.mu.Unlock()

	name := brokerResourceName(key)
	ns, owned := b.provisionNamespace(ctx, key)
	dns := k8shelpers.WorkloadEndpoint(name, ns, kafkaPort)

	b.logger.Info("managedkafka broker: waiting for redpanda pod", "cluster", keyStr, "namespace", ns, "address", dns)
	// The workload helper owns the Pod + ClusterIP Service lifecycle and reaps
	// both if the broker never becomes ready, so a failed start cannot leak.
	if _, err := k8shelpers.EnsureWorkload(ctx, b.client, k8shelpers.WorkloadSpec{
		Namespace:     ns,
		Pod:           *b.buildPod(name, ns, dns),
		ServiceLabels: map[string]string{"app": name, "managed-by": "jaiscloud", brokerLabelKey: brokerLabelValue},
		Selector:      map[string]string{"app": name},
		Ports:         []k8shelpers.ServicePort{{Name: "kafka", Port: kafkaPort, TargetPort: kafkaPort}},
		Probe:         b.probe,
	}); err != nil {
		// A broker that never became ready left no workload, but the namespace we
		// created for it is empty and orphaned; reap it so a failed start does not
		// leak a namespace. An adopted namespace is never deleted.
		if owned {
			b.deleteOwnedNamespace(ctx, ns, "failed broker start")
		}
		return "", fmt.Errorf("managedkafka broker: ensure workload: %w", err)
	}

	b.mu.Lock()
	b.endpoints[key] = dns
	b.namespaces[key] = ns
	b.mu.Unlock()
	b.logger.Info("managedkafka broker ready", "cluster", keyStr, "namespace", ns, "address", dns)
	return dns, nil
}

// provisionNamespace resolves and provisions the per-cluster namespace for key
// and returns the namespace the broker workload will use plus whether the
// emulator created it (so teardown only removes a namespace we own).
//
// A namespace lifecycle failure (the ServiceAccount lacks the cluster-scoped
// jaiscloud-namespace-admin RBAC, or the per-namespace executor RoleBinding
// cannot be created) falls back to the process-wide namespace with owned=false,
// so a broker never hard-fails on namespace plumbing — mirroring Dataproc's
// KNS2 fallback.
func (b *k8sBroker) provisionNamespace(ctx context.Context, key ClusterKey) (string, bool) {
	ns := ClusterNamespace(key.Project, key.Location, key.Cluster)
	created, err := k8shelpers.EnsureManagedNamespace(ctx, b.client, ns, kafkaService)
	if err != nil {
		b.logger.Warn("managedkafka broker: cannot provision per-cluster namespace; falling back",
			"cluster", key.String(), "namespace", ns, "fallback", b.namespace, "err", err)
		return b.namespace, false
	}
	// The emulator's own ServiceAccount creates the broker Pod/Service in the new
	// namespace, so the executor ClusterRole must be bound to it there.
	if rbacErr := k8shelpers.EnsureNamespaceRBAC(ctx, b.client, ns, "", ""); rbacErr != nil {
		b.logger.Warn("managedkafka broker: cannot bootstrap executor RBAC in per-cluster namespace; falling back",
			"cluster", key.String(), "namespace", ns, "fallback", b.namespace, "err", rbacErr)
		if created {
			b.deleteOwnedNamespace(ctx, ns, "unusable namespace")
		}
		return b.namespace, false
	}
	return ns, created
}

// deleteOwnedNamespace best-effort deletes a namespace the emulator owns,
// logging (never failing) a teardown error. The deletion runs on a context
// detached from the request (and bounded) so a cancelled/failed create or
// delete request still reaps the namespace — mirroring EnsureWorkload's own
// WithoutCancel cleanup and Dataproc's teardown.
func (b *k8sBroker) deleteOwnedNamespace(ctx context.Context, namespace, reason string) {
	if namespace == "" || namespace == b.namespace {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), namespaceTeardownTimeout)
	defer cancel()
	if _, err := k8shelpers.DeleteManagedNamespace(ctx, b.client, namespace, kafkaService); err != nil {
		b.logger.Warn("managedkafka broker: failed to delete namespace", "namespace", namespace, "reason", reason, "err", err)
	}
}

func (b *k8sBroker) StopCluster(ctx context.Context, project, location, cluster string) error {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	name := brokerResourceName(key)

	b.mu.Lock()
	ns, tracked := b.namespaces[key]
	b.mu.Unlock()
	if !tracked {
		// Untracked (a reaped/absent cluster): best-effort against the derived
		// namespace. Deleting an absent workload/namespace is a no-op.
		ns = ClusterNamespace(project, location, cluster)
	}

	reapErr := k8shelpers.DeleteWorkload(ctx, b.client, ns, name)
	if !tracked && ns != b.namespace {
		// A previous process may have started this broker in the process-wide
		// fallback namespace; reap it too (the resource name is deterministic).
		if perr := k8shelpers.DeleteWorkload(ctx, b.client, b.namespace, name); perr != nil && reapErr == nil {
			reapErr = perr
		}
	}
	// Delete the per-cluster namespace the emulator owns (label-guarded, so an
	// adopted/pre-existing namespace is left, and the process-wide namespace is
	// never touched).
	b.deleteOwnedNamespace(ctx, ns, "cluster deleted")

	// Always drop the endpoint and its pooled admin, even if a delete failed:
	// keeping a dead address advertised is worse than retrying a delete.
	b.mu.Lock()
	ep := b.endpoints[key]
	delete(b.endpoints, key)
	delete(b.namespaces, key)
	b.mu.Unlock()
	if ep != "" {
		b.admins.close(ep)
	}
	if reapErr != nil {
		return fmt.Errorf("managedkafka broker: %w", reapErr)
	}
	return nil
}

// Reset stops and reaps every tracked broker, then sweeps any broker resources
// left in the namespace that this process never tracked (a failed partial
// create, or a previous instance's leftovers). The manager stays usable: a
// later EnsureCluster starts a fresh broker.
func (b *k8sBroker) Reset(ctx context.Context) error {
	b.mu.Lock()
	keys := make([]ClusterKey, 0, len(b.endpoints))
	for k := range b.endpoints {
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
	b.sweepOrphans()
	return firstErr
}

// Shutdown reaps every broker on emulator shutdown.
func (b *k8sBroker) Shutdown(ctx context.Context) error { return b.Reset(ctx) }

// sweepOrphans deletes broker Pods and Services left by a previous emulator
// instance or a failed start, matched by the broker label, and reaps the
// per-cluster namespaces the emulator owns. Broker liveness is runtime state
// this process cannot adopt across the deterministic resource names, so every
// labeled resource is an orphan. Best-effort: a failure is logged, never fatal
// to cluster creation.
func (b *k8sBroker) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The process-wide namespace holds legacy leftovers and RBAC-fallback brokers.
	if n, err := k8shelpers.SweepWorkloads(ctx, b.client, b.namespace, brokerSelector); err != nil {
		b.logger.Warn("managedkafka broker: orphan sweep incomplete", "namespace", b.namespace, "deleted", n, "err", err)
	} else if n > 0 {
		b.logger.Info("managedkafka broker: reaped orphan workloads", "namespace", b.namespace, "count", n)
	}

	// Per-cluster namespaces the emulator owns for managedkafka. Reap the broker
	// workloads first so a namespace stuck on a finalizer cannot keep a broker
	// Pod alive, then delete the namespace itself. An adopted namespace carries
	// no ownership label and is left in place; its workloads are reaped when the
	// owning cluster is deleted or reset.
	namespaces, err := k8shelpers.ListManagedNamespaces(ctx, b.client, kafkaService)
	if err != nil {
		b.logger.Warn("managedkafka broker: cannot list owned namespaces; skipping namespace sweep", "err", err)
		return
	}
	for _, ns := range namespaces {
		if n, werr := k8shelpers.SweepWorkloads(ctx, b.client, ns, brokerSelector); werr != nil {
			b.logger.Warn("managedkafka broker: orphan sweep incomplete", "namespace", ns, "deleted", n, "err", werr)
		}
	}
	if n, derr := k8shelpers.SweepManagedNamespaces(ctx, b.client, kafkaService); derr != nil {
		b.logger.Warn("managedkafka broker: namespace sweep incomplete", "deleted", n, "err", derr)
	} else if n > 0 {
		b.logger.Info("managedkafka broker: reaped orphan namespaces", "count", n)
	}
}

// ─── K8s resource management ─────────────────────────────────────────────────

func (b *k8sBroker) buildPod(name, namespace, dns string) *corev1.Pod {
	labels := map[string]string{"app": name, "managed-by": "jaiscloud", brokerLabelKey: brokerLabelValue}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "redpanda",
				Image: b.image,
				// `rpk redpanda start` writes the node config and launches the
				// broker; the bare `redpanda` binary no longer parses a `start`
				// subcommand or the --kafka-addr family (v24+).
				Command: []string{"rpk", "redpanda"},
				Args: []string{
					"start",
					"--overprovisioned",
					"--smp", "1",
					"--memory", "1G",
					"--reserve-memory", "0M",
					"--node-id", "0",
					"--check=false",
					"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", kafkaPort),
					"--advertise-kafka-addr", "PLAINTEXT://" + dns,
					"--rpc-addr", "0.0.0.0:33145",
					"--pandaproxy-addr", "0.0.0.0:8082",
					"--schema-registry-addr", "0.0.0.0:8081",
				},
				Ports: []corev1.ContainerPort{{Name: "kafka", ContainerPort: kafkaPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(kafkaPort)},
					},
					InitialDelaySeconds: 5,
					PeriodSeconds:       3,
					FailureThreshold:    40,
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/redpanda/data"}},
			}},
			Volumes: []corev1.Volume{{
				Name: "data",
				// Broker data is ephemeral and non-portable (MK5): an emptyDir
				// is deleted with the Pod, so a reaped/reused cluster id starts
				// clean and no volume is left behind. Control-plane metadata is
				// the authoritative state; producer bytes never travel in a
				// --dsn snapshot.
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	}
}

// brokerResourceName returns a DNS-safe, per-cluster resource name derived from
// the cluster identity. The hash keeps it stable and collision-free while the
// sanitized cluster name keeps it debuggable.
func brokerResourceName(key ClusterKey) string {
	sum := sha256.Sum256([]byte(key.String()))
	hash := hex.EncodeToString(sum[:])[:10]
	sanitized := sanitizeDNSLabel(key.Cluster)
	if sanitized == "" {
		return "mkbroker-" + hash
	}
	if len(sanitized) > 40 {
		sanitized = sanitized[:40]
	}
	return "mkbroker-" + sanitized + "-" + hash
}

func sanitizeDNSLabel(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+('a'-'A'))
		}
	}
	return string(out)
}

// tcpProbe reports whether addr accepts a TCP connection.
func tcpProbe(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
