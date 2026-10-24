package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/docker"
	core "jaiscloud/internal/gcp/service/managedkafka"
	"jaiscloud/internal/platform"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// dockerServiceValue scopes Managed Kafka's broker containers to the
	// managedkafka service in the shared container label set.
	dockerServiceValue = "managedkafka"
	// dockerLabelCluster identifies the owning Managed Kafka cluster within the
	// managedkafka label namespace, so one cluster's container can be reaped
	// without touching another's.
	dockerLabelCluster = "jaiscloud.io/managedkafka-cluster"
	// dockerReapTimeout bounds a best-effort container teardown.
	dockerReapTimeout = 30 * time.Second
)

// dockerBroker runs a single-node Redpanda container per Managed Kafka cluster
// on the local Docker daemon — the host-toolchain-free sibling of the k8s and
// native backends. It reuses the shared internal/docker client (the same one
// Cloud Run's DockerManager and Dataproc's DockerDriver use) and the shared
// adminPool, so only container lifecycle is local. It is safe for concurrent
// use.
//
// Unlike Cloud Run/Dataproc, a Kafka broker must advertise a reachable
// listener, so the container publishes a chosen loopback host port and the
// broker is told to advertise 127.0.0.1:<hostPort> (the k8s broker advertises
// the in-cluster DNS, native the loopback port).
type dockerBroker struct {
	docker       *docker.Client
	platform     *platform.PlatformConfig
	instanceID   string
	image        string
	readyTimeout time.Duration
	addr         docker.Addressing
	logger       *slog.Logger

	mu        sync.Mutex
	instances map[ClusterKey]*dockerInstance
	admins    *adminPool
	// locks serializes the create/reap of one cluster's container while leaving
	// different clusters independent, so two concurrent EnsureCluster calls
	// cannot both create a container (the second's idempotent pre-Remove would
	// otherwise reap the first's).
	locks *keyLocks
	// sweepOnce reaps containers left by a previous emulator instance exactly
	// once, before the first broker starts.
	sweepOnce sync.Once

	// ready reports whether the broker's Kafka listener is actually serving at
	// addr. Overridable in tests; the default issues a Kafka metadata ping. A
	// bare TCP dial is not enough through Docker's published port: the
	// userland proxy completes the handshake before the container's listener is
	// up (Cloud Run's DockerManager hit the same issue and probes at the HTTP
	// layer instead).
	ready func(ctx context.Context, addr string) error
}

type dockerInstance struct {
	id   string
	addr string
}

func newDockerBroker(cfg Config, logger *slog.Logger) *dockerBroker {
	image := cfg.Image
	if image == "" {
		image = defaultRedpandaImage
	}
	ready := cfg.ReadyTimeout
	if ready == 0 {
		ready = brokerReadyTimeout
	}
	b := &dockerBroker{
		docker:       docker.New(docker.Config{Logger: logger, Socket: cfg.Socket, Client: cfg.DockerClient}),
		platform:     cfg.Platform,
		instanceID:   cfg.InstanceID,
		addr:         cfg.Addressing,
		image:        image,
		readyTimeout: ready,
		logger:       logger,
		instances:    make(map[ClusterKey]*dockerInstance),
		admins:       newAdminPool(nil),
		locks:        newKeyLocks(),
		ready:        kafkaReady,
	}
	return b
}

func (b *dockerBroker) Mode() Mode { return ModeDocker }

func (b *dockerBroker) Endpoint(project, location, cluster string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if inst, ok := b.instances[ClusterKey{Project: project, Location: location, Cluster: cluster}]; ok {
		return inst.addr
	}
	return ""
}

// EnsureTopic provisions the topic on the live broker, or no-ops when no broker
// is running for the cluster (metadata-only topology).
func (b *dockerBroker) EnsureTopic(ctx context.Context, project, location, cluster, topic string, partitions int, configs map[string]string) error {
	return ensureTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic, partitions, configs)
}

// AlterTopicConfigs applies an incremental topic-config change on the live
// broker, or no-ops when no broker is running for the cluster.
func (b *dockerBroker) AlterTopicConfigs(ctx context.Context, project, location, cluster, topic string, set map[string]string, remove []string) error {
	return alterTopicConfigs(ctx, b.admins, b.Endpoint(project, location, cluster), topic, set, remove)
}

// AddTopicPartitions raises the topic's partition count on the live broker.
func (b *dockerBroker) AddTopicPartitions(ctx context.Context, project, location, cluster, topic string, totalPartitions int) error {
	return addPartitions(ctx, b.admins, b.Endpoint(project, location, cluster), topic, totalPartitions)
}

// DeleteBrokerTopic removes the topic from the live broker.
func (b *dockerBroker) DeleteBrokerTopic(ctx context.Context, project, location, cluster, topic string) error {
	return deleteBrokerTopic(ctx, b.admins, b.Endpoint(project, location, cluster), topic)
}

// ListConsumerGroups returns the live broker's consumer groups, or an empty set
// when no broker is running.
func (b *dockerBroker) ListConsumerGroups(ctx context.Context, project, location, cluster string) ([]string, error) {
	return listGroups(ctx, b.admins, b.Endpoint(project, location, cluster))
}

// ConsumerGroupOffsets returns the group's committed offsets on the live broker.
func (b *dockerBroker) ConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string) ([]core.ConsumerGroupOffset, bool, error) {
	return groupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// ConsumerGroupMembers returns the group's active member count on the live broker.
func (b *dockerBroker) ConsumerGroupMembers(ctx context.Context, project, location, cluster, group string) (int, bool, error) {
	return groupMembers(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// DeleteConsumerGroup removes the group from the live broker.
func (b *dockerBroker) DeleteConsumerGroup(ctx context.Context, project, location, cluster, group string) (bool, error) {
	return deleteGroup(ctx, b.admins, b.Endpoint(project, location, cluster), group)
}

// CommitConsumerGroupOffsets sets the group's committed offsets on the live broker.
func (b *dockerBroker) CommitConsumerGroupOffsets(ctx context.Context, project, location, cluster, group string, offsets []core.ConsumerGroupOffset) error {
	return commitGroupOffsets(ctx, b.admins, b.Endpoint(project, location, cluster), group, offsets)
}

// ReplaceAcl mirrors an ACL's entry set onto the live broker.
func (b *dockerBroker) ReplaceAcl(ctx context.Context, project, location, cluster, resourceType, resourceName, patternType string, entries []core.AclBinding) error {
	return replaceACLs(ctx, b.admins, b.Endpoint(project, location, cluster), aclSpec{
		ResourceType: resourceType,
		ResourceName: resourceName,
		PatternType:  patternType,
		Entries:      entries,
	})
}

func (b *dockerBroker) EnsureCluster(ctx context.Context, project, location, cluster string) (string, error) {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}

	// Reap containers left behind by a previous emulator instance (or a failed
	// start) before starting a new one.
	b.sweepOnce.Do(b.sweepOrphans)

	// Serialize starts for this cluster: two concurrent callers must not both
	// create a container, because startContainer's idempotent pre-Remove would
	// otherwise reap the first caller's just-started broker.
	unlock := b.locks.lock(key)
	defer unlock()

	b.mu.Lock()
	if inst, ok := b.instances[key]; ok {
		b.mu.Unlock()
		return inst.addr, nil
	}
	b.mu.Unlock()

	// A Kafka broker must advertise a reachable listener, so the address is
	// chosen up front (Docker cannot assign a host port before we bake the
	// advertise address into the container command). In host mode it is the
	// published loopback port; in container mode the emulator reaches the broker
	// by container name on the shared network, so nothing is published.
	name := dockerBrokerName(b.instanceID, key)
	var hostPort int
	var addr string
	if b.addr.Containerized() {
		addr = b.addr.HostPort(name, kafkaPort, 0)
	} else {
		var err error
		hostPort, err = freePort()
		if err != nil {
			return "", fmt.Errorf("managedkafka broker: find free port: %w", err)
		}
		addr = b.addr.HostPort(name, kafkaPort, hostPort)
	}

	id, err := b.startContainer(ctx, name, key, hostPort, addr)
	if err != nil {
		return "", err
	}
	// From here the container exists: never leak it on a later failure.
	if err := b.waitReady(ctx, addr); err != nil {
		_ = b.docker.Remove(context.WithoutCancel(ctx), id)
		return "", err
	}

	b.mu.Lock()
	b.instances[key] = &dockerInstance{id: id, addr: addr}
	b.mu.Unlock()
	b.logger.Info("managedkafka broker ready", "cluster", key.String(), "address", addr, "container", docker.ShortID(id))
	return addr, nil
}

// startContainer creates and starts the cluster's Redpanda container.
func (b *dockerBroker) startContainer(ctx context.Context, name string, key ClusterKey, hostPort int, addr string) (string, error) {
	portKey := fmt.Sprintf("%d/tcp", kafkaPort)
	hostCfg := map[string]any{
		"AutoRemove": false,
	}
	// Host mode: bind the chosen loopback port so the broker can advertise it.
	// Container mode: publish nothing; the broker is advertised by container
	// name on the shared network instead.
	if bindings := b.addr.PortBindings(kafkaPort, hostPort); bindings != nil {
		hostCfg["PortBindings"] = bindings
	}
	if b.addr.Network != "" {
		hostCfg["NetworkMode"] = b.addr.Network
	}
	binds, platformEnv, pErr := docker.BindsAndEnv(b.platform)
	if pErr != nil {
		b.logger.Warn("managedkafka docker: platform apply failed", "err", pErr)
	}
	if len(binds) > 0 {
		hostCfg["Binds"] = binds
	}
	labels := map[string]string{
		docker.LabelService: dockerServiceValue,
		dockerLabelCluster:  key.String(),
	}
	if b.instanceID != "" {
		labels[docker.LabelInstance] = b.instanceID
	}

	// `rpk redpanda start` writes the node config and launches the broker; the
	// same arguments as the k8s broker, but the advertised listener is the
	// published loopback port the host client dials.
	args := []string{
		"start",
		"--overprovisioned",
		"--smp", "1",
		"--memory", "1G",
		"--reserve-memory", "0M",
		"--node-id", "0",
		"--check=false",
		"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", kafkaPort),
		"--advertise-kafka-addr", "PLAINTEXT://" + addr,
		"--rpc-addr", "0.0.0.0:33145",
	}
	createBody := map[string]any{
		"Image":        b.image,
		"Env":          platformEnv,
		"Labels":       labels,
		"ExposedPorts": map[string]any{portKey: map[string]any{}},
		"Entrypoint":   []string{"rpk", "redpanda"},
		"Cmd":          args,
		"HostConfig":   hostCfg,
	}
	// Container mode advertises the broker by its container name; register the
	// DNS-safe alias so a long name still resolves.
	if network := b.addr.Network; network != "" && b.addr.Containerized() {
		createBody["NetworkingConfig"] = b.addr.NetworkingConfig(name)
	}
	body, err := json.Marshal(createBody)
	if err != nil {
		return "", fmt.Errorf("managedkafka broker: marshal container: %w", err)
	}

	// Idempotent re-ensure: a prior attempt may have left the container behind.
	_ = b.docker.Remove(ctx, name)
	b.logger.Info("managedkafka broker: starting redpanda container", "cluster", key.String(), "address", addr, "image", b.image)
	id, err := b.docker.Create(ctx, name, body)
	if err != nil {
		return "", fmt.Errorf("managedkafka broker: %w", err)
	}
	if _, status, err := b.docker.Call(ctx, http.MethodPost, "/containers/"+id+"/start", nil); err != nil {
		_ = b.docker.Remove(context.WithoutCancel(ctx), id)
		return "", fmt.Errorf("managedkafka broker: start container: %w", err)
	} else if status >= 300 {
		_ = b.docker.Remove(context.WithoutCancel(ctx), id)
		return "", fmt.Errorf("managedkafka broker: start container: HTTP %d", status)
	}
	return id, nil
}

// waitReady polls the advertised address until the broker's Kafka listener is
// actually serving (a metadata ping), not merely until Docker's published-port
// proxy accepts a connection.
func (b *dockerBroker) waitReady(ctx context.Context, addr string) error {
	deadline := clock.RealNow().Add(b.readyTimeout)
	for clock.RealNow().Before(deadline) {
		if b.ready(ctx, addr) == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("managedkafka broker: %s not ready within %s", addr, b.readyTimeout)
}

func (b *dockerBroker) StopCluster(ctx context.Context, project, location, cluster string) error {
	key := ClusterKey{Project: project, Location: location, Cluster: cluster}
	unlock := b.locks.lock(key)
	defer unlock()

	b.mu.Lock()
	inst := b.instances[key]
	delete(b.instances, key)
	b.mu.Unlock()

	// Drop the pooled admin client before the container goes away.
	if inst != nil {
		b.admins.close(inst.addr)
	}
	// A teardown must survive a cancelled request context: a cluster delete
	// whose client disconnected still has to reap the container.
	reapCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dockerReapTimeout)
	defer cancel()
	name := dockerBrokerName(b.instanceID, key)
	if _, err := b.docker.RemoveByFilter(reapCtx, b.clusterFilter(key)); err != nil {
		b.logger.Warn("managedkafka broker: cluster reap failed", "cluster", key.String(), "err", err)
	}
	if err := b.docker.Remove(reapCtx, name); err != nil {
		return fmt.Errorf("managedkafka broker: %w", err)
	}
	return nil
}

// Reset stops and reaps every broker, clears its in-memory bookkeeping, and
// sweeps any container this process never tracked (a failed partial create, or
// a previous instance's leftovers). The manager stays usable: a later
// EnsureCluster starts a fresh broker.
func (b *dockerBroker) Reset(ctx context.Context) error {
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
	sweepCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dockerReapTimeout)
	defer cancel()
	if n, err := b.docker.RemoveByFilter(sweepCtx, b.instanceFilter()); err != nil {
		b.logger.Warn("managedkafka broker: reset sweep incomplete", "err", err)
	} else if n > 0 {
		b.logger.Info("managedkafka broker: reaped broker containers on reset", "count", n)
	}
	return firstErr
}

// Shutdown reaps every broker on emulator shutdown.
func (b *dockerBroker) Shutdown(ctx context.Context) error { return b.Reset(ctx) }

// sweepOrphans reaps containers left by a previous emulator instance. Best
// effort: a failure is logged, never fatal to cluster creation.
func (b *dockerBroker) sweepOrphans() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if n, err := b.docker.RemoveByFilter(ctx, b.instanceFilter()); err != nil {
		b.logger.Warn("managedkafka broker: orphan sweep incomplete", "err", err)
	} else if n > 0 {
		b.logger.Info("managedkafka broker: reaped orphan broker containers", "count", n)
	}
}

// instanceFilter selects this instance's broker containers.
func (b *dockerBroker) instanceFilter() map[string][]string {
	return docker.LabelFilter(dockerServiceValue, b.instanceID)
}

// clusterFilter selects this instance's containers for one cluster.
func (b *dockerBroker) clusterFilter(key ClusterKey) map[string][]string {
	return docker.LabelFilter(dockerServiceValue, b.instanceID, dockerLabelCluster+"="+key.String())
}

// dockerBrokerName is the deterministic, instance-scoped container name for a
// cluster. Instance scoping keeps two emulators on one daemon from colliding or
// cross-reaping.
func dockerBrokerName(instanceID string, key ClusterKey) string {
	return "jc-mkbroker-" + docker.ShortInstance(instanceID) + "-" + brokerResourceName(key)
}

// kafkaReady reports whether a Kafka-wire metadata request succeeds at addr.
// Unlike a bare TCP dial it verifies the broker's own listener is serving, not
// just Docker's published-port proxy.
func kafkaReady(ctx context.Context, addr string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ClientID("jaiscloud-managedkafka-ready"),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return cl.Ping(pingCtx)
}

// keyLocks is a per-key mutex set. It serializes the docker broker's
// create/reap for one cluster while leaving different clusters independent.
type keyLocks struct {
	mu    sync.Mutex
	locks map[ClusterKey]*sync.Mutex
}

func newKeyLocks() *keyLocks { return &keyLocks{locks: make(map[ClusterKey]*sync.Mutex)} }

// lock acquires key's mutex and returns its unlock function.
func (k *keyLocks) lock(key ClusterKey) func() {
	k.mu.Lock()
	m, ok := k.locks[key]
	if !ok {
		m = &sync.Mutex{}
		k.locks[key] = m
	}
	k.mu.Unlock()
	m.Lock()
	return m.Unlock
}
