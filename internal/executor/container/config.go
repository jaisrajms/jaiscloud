package container

import (
	"log/slog"
	"os"
	"strconv"
)

// Config holds executor-wide configuration shared by all functions of a cloud
// profile. It is cloud-neutral; the AWS Lambda and GCP Functions constructors
// layer their own env-var parsing and defaults on top.
type Config struct {
	// Mode is "mock", "docker", or "k8s".
	Mode string
	// DefaultImage overrides the profile's runtime→image lookup for all functions.
	DefaultImage string
	// Network is the Docker network for warm containers (docker mode only).
	Network string
	// KeepaliveSecs is the idle timeout before a warm container/pod is stopped.
	KeepaliveSecs int
	// Namespace is the K8s namespace for warm pods (k8s mode only).
	Namespace string
	// ServiceAccount is the K8s service account for warm pods.
	ServiceAccount string
	// JaisCloudEndpoint is passed to workloads so they can call back (optional).
	JaisCloudEndpoint string
	// Region is the cloud region exposed to the workload (AWS_DEFAULT_REGION, GCP
	// GOOGLE_CLOUD_LOCATION, ...).
	Region string
	// Cloud is used for metadata labels on managed pods/containers.
	Cloud string
	// InstanceID uniquely identifies this JaisCloud deployment. Stamped on all
	// managed workloads so two instances on one host never reap each other's.
	InstanceID string

	// CodeURL is the base URL of the JaisCloud admin endpoint reachable from K8s
	// init containers, including the /_jaiscloud prefix. Empty disables the
	// code-fetch init container.
	CodeURL string
	// InitImage is the container image used in the code-fetch init container.
	// Defaults to "alpine:latest" when empty.
	InitImage string

	// ConcurrencyLimit caps account-level concurrent invocations (0 = unlimited).
	ConcurrencyLimit int64
	// SyncPayloadMax is the max sync invocation payload in bytes (0 = unlimited).
	SyncPayloadMax int64
	// AsyncPayloadMax is the max async invocation payload in bytes (0 = unlimited).
	AsyncPayloadMax int64
	// ResponsePayloadMax is the max response payload in bytes (0 = unlimited).
	ResponsePayloadMax int64
}

// DefaultConfig returns a Config with the shared defaults, reading the
// deployment-wide JAISCLOUD_K8S_* variables.
func DefaultConfig() Config {
	cfg := Config{
		Mode:          "mock",
		Network:       "jaiscloud-net",
		KeepaliveSecs: 300,
		Namespace:     "jaiscloud",
		InitImage:     "alpine:latest",
	}
	if v := os.Getenv("JAISCLOUD_K8S_NAMESPACE"); v != "" {
		cfg.Namespace = v
	}
	if v := os.Getenv("JAISCLOUD_K8S_SA"); v != "" {
		cfg.ServiceAccount = v
	}
	return cfg
}

// Int64Env reads an env var as int64, returning def on empty or parse failure.
func Int64Env(name string, def int64) int64 {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		slog.Warn("executor config: invalid env var; using default", "name", name, "value", v, "default", def)
		return def
	}
	return n
}
