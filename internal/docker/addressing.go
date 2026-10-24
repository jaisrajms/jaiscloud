package docker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// The docker executors have two deployments, and every one of them must pick
// the same one. The default is the emulator process sharing the Docker daemon's
// host; the other is the emulator itself running as a container on that daemon
// (the socket bind-mounted, as docker-compose.yml does).
const (
	// EnvDeployMode forces the deployment: "host" or "container". Unset
	// auto-detects from the emulator's own environment.
	EnvDeployMode = "JAISCLOUD_DOCKER_MODE"
	// EnvNetwork is the user-defined Docker network the emulator and its
	// workloads share, so a containerized emulator resolves a workload by its
	// container name (Docker's embedded DNS) instead of a published host port.
	EnvNetwork = "JAISCLOUD_DOCKER_NETWORK"
	// EnvBindRoot is a directory bind-mounted into the emulator at the same
	// absolute path it has on the daemon's host, so a bind source the emulator
	// creates there resolves to the same bytes the daemon mounts. It is where
	// code archives, extracted layers and similar mount inputs are staged in
	// container mode.
	EnvBindRoot = "JAISCLOUD_DOCKER_BIND_ROOT"
)

// DeployMode is how the emulator reaches the workloads it launches.
type DeployMode string

const (
	// DeployHost is the emulator process running on the Docker daemon's host:
	// workloads are reached on the published host loopback port, and a bind
	// source is an emulator-local path the daemon resolves on the same
	// filesystem.
	DeployHost DeployMode = "host"
	// DeployContainer is the emulator running in a container on the daemon (the
	// socket bind-mounted): its loopback is its own network namespace, and its
	// filesystem is not the daemon's, so workloads must be reached by container
	// name on a shared user-defined network and every bind source must live
	// under a bind-mounted shared root.
	DeployContainer DeployMode = "container"
)

// Addressing is the resolved way this emulator process reaches the containers
// it creates on the Docker daemon. The zero value is DeployHost with no network
// or bind root, so an executor that is not handed one keeps the host-process
// behaviour.
type Addressing struct {
	// Mode is the deployment the emulator is running in.
	Mode DeployMode
	// Network is the user-defined Docker network workloads join and are reached
	// on by container name. Empty leaves the daemon default and is only valid in
	// host mode.
	Network string
	// BindRoot is a directory visible at the same path inside the emulator and
	// on the daemon's host (a bind-mounted volume), where mount inputs are
	// staged in container mode. Unused in host mode.
	BindRoot string
}

// ResolveAddressing resolves the deployment addressing from the environment.
// network is the executor's already-resolved Docker network (flag/env/default);
// EnvNetwork overrides it. Only the startup wiring calls this — executors take
// an Addressing explicitly, so a package that is not wired keeps host mode.
func ResolveAddressing(network string) Addressing {
	envNetwork := strings.TrimSpace(os.Getenv(EnvNetwork))
	a := Addressing{
		Mode:     DeployHost,
		Network:  firstNonEmpty(envNetwork, network),
		BindRoot: strings.TrimSpace(os.Getenv(EnvBindRoot)),
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvDeployMode))) {
	case string(DeployHost):
		a.Mode = DeployHost
	case string(DeployContainer):
		a.Mode = DeployContainer
	default:
		if emulatorContainerized() {
			a.Mode = DeployContainer
		}
	}
	if a.Containerized() {
		// The shared network is a property of the container deployment, so it
		// must be configured explicitly: an executor's own default network
		// ("jaiscloud-net") is a host-mode value and not a network the
		// deployment created, so accepting it would defeat the probe.
		a.Network = envNetwork
	}
	return a
}

// Containerized reports whether the emulator runs in a container on the daemon,
// so workloads are addressed by name and binds must use BindRoot.
func (a Addressing) Containerized() bool { return a.Mode == DeployContainer }

// Validate reports whether this addressing can actually run a docker workload.
// It is the second half of the startup probe: a socket ping alone reports a
// containerized deployment "reachable" and then fails on the first invoke or
// bind mount.
func (a Addressing) Validate() error {
	if !a.Containerized() {
		return nil
	}
	if a.Network == "" {
		return fmt.Errorf("docker: the emulator runs in a container, so workloads must be reached by name on a shared network: set %s to the network the emulator and its workloads share (published loopback ports are not reachable from the emulator's own network namespace)", EnvNetwork)
	}
	return nil
}

// PublishHostPorts reports whether a workload's port is published on the daemon
// host. A containerized emulator cannot reach a host-published port, so it
// addresses the workload on its network instead and publishes nothing.
func (a Addressing) PublishHostPorts() bool { return !a.Containerized() }

// PortBindings returns the HostConfig.PortBindings entry that publishes
// containerPort on the daemon's host loopback, or nil when the emulator is
// containerized. A publishedPort of 0 asks Docker for an ephemeral port.
func (a Addressing) PortBindings(containerPort, publishedPort int) map[string]any {
	if !a.PublishHostPorts() {
		return nil
	}
	hostPort := ""
	if publishedPort > 0 {
		hostPort = strconv.Itoa(publishedPort)
	}
	return map[string]any{
		fmt.Sprintf("%d/tcp", containerPort): []map[string]any{{"HostIp": "127.0.0.1", "HostPort": hostPort}},
	}
}

// DNSLabelMax is the maximum length of a DNS label.
const DNSLabelMax = 63

// DNSName returns the DNS-safe name a workload is addressed by in container
// mode. Docker's embedded DNS cannot resolve a label longer than DNSLabelMax,
// and the descriptive container name (instance + project + location + service +
// revision) routinely exceeds it — a Cloud Run revision name reaches 70 bytes.
// A longer name is therefore replaced by a truncated prefix plus a hash of the
// full name; every create registers the result as a network alias so the alias
// and the address agree.
func DNSName(containerName string) string {
	name := strings.ToLower(containerName)
	if len(name) <= DNSLabelMax {
		return name
	}
	sum := sha256.Sum256([]byte(containerName))
	suffix := hex.EncodeToString(sum[:6]) // 12 hex chars
	return strings.Trim(name[:DNSLabelMax-len(suffix)-1], "-") + "-" + suffix
}

// NetworkingConfig registers the workload's DNSName as a network alias on the
// shared network, so a container name longer than a DNS label is still
// addressable. It is nil in host mode, where there is no shared network.
func (a Addressing) NetworkingConfig(containerName string) map[string]any {
	if !a.Containerized() || a.Network == "" {
		return nil
	}
	return map[string]any{
		"EndpointsConfig": map[string]any{
			a.Network: map[string]any{"Aliases": []string{DNSName(containerName)}},
		},
	}
}

// HostPort is the "host:port" a workload is reached on. containerName is the
// workload's container name (used in container mode); publishedPort is the
// daemon-assigned host port (used in host mode). containerPort is the port the
// workload listens on inside its container.
func (a Addressing) HostPort(containerName string, containerPort, publishedPort int) string {
	if a.Containerized() && containerName != "" {
		return net.JoinHostPort(DNSName(containerName), strconv.Itoa(containerPort))
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(publishedPort))
}

// URL is the http:// URL of a workload's listener, matching HostPort.
func (a Addressing) URL(containerName string, containerPort, publishedPort int) string {
	return "http://" + a.HostPort(containerName, containerPort, publishedPort)
}

// TempDir creates a directory to bind-mount into a workload. In container mode
// it is created under BindRoot, which the daemon resolves on its own host; an
// emulator-local temp dir would mount empty (or not at all). In host mode it is
// an ordinary os.MkdirTemp.
func (a Addressing) TempDir(pattern string) (string, error) {
	if !a.Containerized() {
		return os.MkdirTemp("", pattern)
	}
	if a.BindRoot == "" {
		return "", fmt.Errorf("docker: the emulator runs in a container, so a bind source must live on a directory shared with the daemon: set %s to a directory bind-mounted into the emulator at the same path (an emulator-local temp dir mounts empty for the daemon)", EnvBindRoot)
	}
	if err := os.MkdirAll(a.BindRoot, 0o777); err != nil {
		return "", fmt.Errorf("docker: create bind root %s: %w", a.BindRoot, err)
	}
	return os.MkdirTemp(a.BindRoot, pattern)
}

// Probe verifies the docker executor is usable: the daemon answers on socket,
// and the deployment addressing is configured for it. It exists because the
// socket ping alone cannot tell a containerized emulator from a host process —
// the ping succeeds while the loopback invokes and local bind sources fail.
// Callers degrade to mock (or k8s) when it returns an error, so the console
// reports the truth instead of a backend that fails on first use.
func Probe(ctx context.Context, socket string, a Addressing) error {
	if err := Ping(ctx, socket); err != nil {
		return err
	}
	return a.Validate()
}

// ResolveAndProbe resolves the deployment addressing from the environment and
// validates it against the daemon in one step. It is the startup wiring for
// every docker executor: the returned Addressing is handed to the executor, and
// an error means the caller should degrade to another backend (mock/k8s) rather
// than report a docker backend that fails on first use.
func ResolveAndProbe(ctx context.Context, socket, network string) (Addressing, error) {
	a := ResolveAddressing(network)
	if err := Probe(ctx, socket, a); err != nil {
		return a, err
	}
	return a, nil
}

// emulatorContainerized reports whether this process is running in a container.
// Docker/Kubernetes leave one of these markers; a normal host process leaves
// none, so the auto-detection is conservative and an operator can always
// override it with EnvDeployMode.
func emulatorContainerized() bool {
	for _, marker := range []string{"/.dockerenv", "/run/.containerenv"} {
		if _, err := os.Stat(marker); err == nil {
			return true
		}
	}
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return true
	}
	if b, err := os.ReadFile("/proc/1/cgroup"); err == nil {
		s := string(b)
		for _, marker := range []string{"docker", "kubepods", "containerd", "libpod"} {
			if strings.Contains(s, marker) {
				return true
			}
		}
	}
	return false
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
