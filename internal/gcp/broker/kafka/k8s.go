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
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/managedkafka"
)

const (
	// kafkaPort is the PLAINTEXT listener the broker serves and the Service
	// exposes. Real GCP omits the port from bootstrapAddress; the emulator
	// includes it so a raw client can dial the in-cluster Service directly.
	kafkaPort = 9092
	// brokerReadyTimeout bounds how long EnsureCluster waits for the Pod's
	// Service to accept a TCP connection.
	brokerReadyTimeout = 180 * time.Second
	// brokerLabelKey/Value tag every managedkafka broker resource so the K8s
	// e2e smoke can find the per-cluster Service the cluster advertises.
	brokerLabelKey   = "jaiscloud.io/broker"
	brokerLabelValue = "managedkafka"
	// brokerSelector matches every broker resource this package owns; the
	// startup orphan sweep and Reset use it to reap leftovers.
	brokerSelector = brokerLabelKey + "=" + brokerLabelValue
	// brokerSweepTimeout bounds the startup orphan sweep.
	brokerSweepTimeout = 30 * time.Second
	// brokerSweepCap bounds a sweep so a mislabelled namespace cannot trigger
	// unbounded deletes.
	brokerSweepCap = 2000
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
	admins    *adminPool
	// sweepOnce runs the startup orphan sweep exactly once, before the first
	// broker starts.
	sweepOnce sync.Once

	// probe reports whether addr is accepting TCP connections. Overridable in
	// tests; defaults to a one-second TCP dial.
	probe func(addr string) bool
}

func newK8sBroker(client kubernetes.Interface, namespace, image string, logger *slog.Logger) *k8sBroker {
	return &k8sBroker{
		client:    client,
		namespace: namespace,
		image:     image,
		logger:    logger,
		endpoints: make(map[ClusterKey]string),
		admins:    newAdminPool(nil),
		probe:     tcpProbe,
	}
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
	dns := fmt.Sprintf("%s.%s.svc.cluster.local:%d", name, b.namespace, kafkaPort)

	if err := b.ensureNamespace(ctx); err != nil {
		return "", fmt.Errorf("managedkafka broker: ensure namespace: %w", err)
	}
	if err := b.ensureService(ctx, name); err != nil {
		return "", fmt.Errorf("managedkafka broker: ensure service: %w", err)
	}
	if err := b.ensurePod(ctx, name, dns); err != nil {
		b.reapResources(ctx, name)
		return "", fmt.Errorf("managedkafka broker: ensure pod: %w", err)
	}

	b.logger.Info("managedkafka broker: waiting for redpanda pod", "cluster", keyStr, "address", dns)
	if err := b.waitReady(ctx, dns); err != nil {
		// A broker that never became ready must not leak its Pod/Service; the
		// caller degrades to the metadata-only topology.
		b.reapResources(ctx, name)
		return "", err
	}

	b.mu.Lock()
	b.endpoints[key] = dns
	b.mu.Unlock()
	b.logger.Info("managedkafka broker ready", "cluster", keyStr, "address", dns)
	return dns, nil
}

func (b *k8sBroker) StopCluster(ctx context.Context, project, location, cluster string) error {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	name := brokerResourceName(key)

	podErr := b.client.CoreV1().Pods(b.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if podErr != nil && !k8serrors.IsNotFound(podErr) {
		podErr = fmt.Errorf("managedkafka broker: delete pod: %w", podErr)
	} else {
		podErr = nil
	}
	svcErr := b.client.CoreV1().Services(b.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if svcErr != nil && !k8serrors.IsNotFound(svcErr) {
		svcErr = fmt.Errorf("managedkafka broker: delete service: %w", svcErr)
	} else {
		svcErr = nil
	}

	// Always drop the endpoint and its pooled admin, even if a delete failed:
	// keeping a dead address advertised is worse than retrying a delete.
	b.mu.Lock()
	ep := b.endpoints[key]
	delete(b.endpoints, key)
	b.mu.Unlock()
	if ep != "" {
		b.admins.close(ep)
	}
	if podErr != nil {
		return podErr
	}
	return svcErr
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

// reapResources deletes a broker's Pod and Service, ignoring NotFound, so a
// failed start does not leak them.
func (b *k8sBroker) reapResources(ctx context.Context, name string) {
	if err := b.client.CoreV1().Pods(b.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		b.logger.Warn("managedkafka broker: reap pod failed", "pod", name, "err", err)
	}
	if err := b.client.CoreV1().Services(b.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		b.logger.Warn("managedkafka broker: reap service failed", "service", name, "err", err)
	}
}

// sweepOrphans deletes broker Pods and Services left by a previous emulator
// instance or a failed start, matched by the broker label. Broker liveness is
// runtime state this process cannot adopt across the deterministic resource
// names, so every labeled resource is an orphan. Best-effort: a failure is
// logged, never fatal to cluster creation.
func (b *k8sBroker) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), brokerSweepTimeout)
	defer cancel()
	b.sweepOrphanPods(ctx)
	b.sweepOrphanServices(ctx)
}

func (b *k8sBroker) sweepOrphanPods(ctx context.Context) {
	deleted := 0
	var cont string
	for {
		list, err := b.client.CoreV1().Pods(b.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: brokerSelector,
			Limit:         500,
			Continue:      cont,
		})
		if err != nil {
			b.logger.Warn("managedkafka broker: orphan pod sweep failed", "err", err)
			return
		}
		for i := range list.Items {
			name := list.Items[i].Name
			if err := b.client.CoreV1().Pods(b.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
				b.logger.Warn("managedkafka broker: orphan pod delete failed", "pod", name, "err", err)
				continue
			}
			deleted++
			b.logger.Info("managedkafka broker: reaped orphan pod", "pod", name)
		}
		cont = list.Continue
		if cont == "" || deleted >= brokerSweepCap {
			return
		}
	}
}

func (b *k8sBroker) sweepOrphanServices(ctx context.Context) {
	deleted := 0
	var cont string
	for {
		list, err := b.client.CoreV1().Services(b.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: brokerSelector,
			Limit:         500,
			Continue:      cont,
		})
		if err != nil {
			b.logger.Warn("managedkafka broker: orphan service sweep failed", "err", err)
			return
		}
		for i := range list.Items {
			name := list.Items[i].Name
			if err := b.client.CoreV1().Services(b.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
				b.logger.Warn("managedkafka broker: orphan service delete failed", "service", name, "err", err)
				continue
			}
			deleted++
			b.logger.Info("managedkafka broker: reaped orphan service", "service", name)
		}
		cont = list.Continue
		if cont == "" || deleted >= brokerSweepCap {
			return
		}
	}
}

// ─── K8s resource management ─────────────────────────────────────────────────

// ensureNamespace is best-effort: the emulator's ServiceAccount is granted only
// namespaced RBAC (namespaces are cluster-scoped), so a Forbidden Get is not an
// error — it means the namespace is managed externally (it is, in every
// deployment: the emulator runs in it). A genuinely missing namespace surfaces
// on the subsequent Pod/Service create.
func (b *k8sBroker) ensureNamespace(ctx context.Context) error {
	_, err := b.client.CoreV1().Namespaces().Get(ctx, b.namespace, metav1.GetOptions{})
	if err == nil || k8serrors.IsForbidden(err) {
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	_, err = b.client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: b.namespace, Labels: map[string]string{"managed-by": "jaiscloud"}},
	}, metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) || k8serrors.IsForbidden(err) {
		return nil
	}
	return err
}

func (b *k8sBroker) ensureService(ctx context.Context, name string) error {
	_, err := b.client.CoreV1().Services(b.namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil // already provisioned
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	_, err = b.client.CoreV1().Services(b.namespace).Create(ctx, b.buildService(name), metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (b *k8sBroker) ensurePod(ctx context.Context, name, dns string) error {
	_, err := b.client.CoreV1().Pods(b.namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil // already provisioned
	}
	if !k8serrors.IsNotFound(err) {
		return err
	}
	_, err = b.client.CoreV1().Pods(b.namespace).Create(ctx, b.buildPod(name, dns), metav1.CreateOptions{})
	if k8serrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (b *k8sBroker) buildPod(name, dns string) *corev1.Pod {
	labels := map[string]string{"app": name, "managed-by": "jaiscloud", brokerLabelKey: brokerLabelValue}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.namespace, Labels: labels},
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

func (b *k8sBroker) buildService(name string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.namespace, Labels: map[string]string{"app": name, "managed-by": "jaiscloud", brokerLabelKey: brokerLabelValue}},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app": name},
			Ports: []corev1.ServicePort{{
				Name:       "kafka",
				Port:       kafkaPort,
				TargetPort: intstr.FromInt(kafkaPort),
			}},
		},
	}
}

func (b *k8sBroker) waitReady(ctx context.Context, addr string) error {
	// Broker startup is real elapsed time, not the simulated clock.
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
