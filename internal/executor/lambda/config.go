package lambda

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// LambdaConfig holds executor-wide configuration shared by all functions.
type LambdaConfig struct {
	// Mode is "mock", "docker", or "k8s".
	Mode string
	// DefaultImage overrides the runtime→image lookup for all functions.
	DefaultImage string
	// Network is the Docker network for warm containers (docker mode only).
	Network string
	// KeepaliveSecs is the idle timeout before a warm container is stopped (docker mode).
	KeepaliveSecs int
	// Namespace is the K8s namespace for Lambda Jobs (k8s mode only).
	Namespace string
	// ServiceAccount is the K8s service account for Lambda Jobs.
	ServiceAccount string
	// APIServer is the K8s API server URL (k8s mode only).
	APIServer string
	// JaisCloudEndpoint is passed to containers so they can call back (optional).
	JaisCloudEndpoint string
	// Region is injected as AWS_DEFAULT_REGION into containers.
	Region string
	// Cloud is used for metadata labels on managed pods/containers.
	Cloud string
	// InstanceID uniquely identifies this JaisCloud deployment.
	// Stamped as a label on all managed Lambda pods and containers.
	// Populated by main.go from config.LoadOrCreateInstanceID.
	InstanceID string

	// CodeURL is the base URL of the JaisCloud admin endpoint reachable from K8s
	// init containers, including the /_jaiscloud prefix (e.g.
	// http://jaiscloud:8080/_jaiscloud). The executor appends
	// /lambda/code/{account}/{key}/$LATEST to it, which matches the admin route
	// registered under /_jaiscloud. Empty disables the code-fetch init container.
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

// regionOrDefault returns r if non-empty, else "us-east-1".
func regionOrDefault(r string) string {
	if r != "" {
		return r
	}
	return "us-east-1"
}

// DefaultLambdaConfig returns a LambdaConfig with sensible defaults.
func DefaultLambdaConfig() LambdaConfig {
	cfg := LambdaConfig{
		Mode:          "mock",
		Network:       "jaiscloud-net",
		KeepaliveSecs: 300,
		Namespace:     "jaiscloud",
		InitImage:     "alpine:latest",
	}
	if v := os.Getenv("JAISCLOUD_K8S_APISERVER"); v != "" {
		cfg.APIServer = v
	}
	if v := os.Getenv("JAISCLOUD_K8S_NAMESPACE"); v != "" {
		cfg.Namespace = v
	}
	if v := os.Getenv("JAISCLOUD_K8S_SA"); v != "" {
		cfg.ServiceAccount = v
	}
	if v := os.Getenv("JAISCLOUD_LAMBDA_CODE_URL"); v != "" {
		cfg.CodeURL = strings.TrimRight(v, "/")
	}
	return cfg
}

// LambdaConfigFrom populates a LambdaConfig with values from environment variables.
func LambdaConfigFrom(base LambdaConfig) LambdaConfig {
	base.ConcurrencyLimit = int64Env("JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT", 1000)
	base.SyncPayloadMax = int64Env("JAISCLOUD_LAMBDA_SYNC_PAYLOAD_MAX_BYTES", 6*1024*1024)
	base.AsyncPayloadMax = int64Env("JAISCLOUD_LAMBDA_ASYNC_PAYLOAD_MAX_BYTES", 256*1024)
	base.ResponsePayloadMax = int64Env("JAISCLOUD_LAMBDA_RESPONSE_PAYLOAD_MAX_BYTES", 6*1024*1024)
	return base
}

// int64Env reads an env var as int64, returning def on empty or parse failure.
func int64Env(name string, def int64) int64 {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		slog.Warn("lambda config: invalid env var; using default", "name", name, "value", v, "default", def)
		return def
	}
	return n
}

// runtimeImages maps Lambda runtime identifiers to their default public ECR images.
var runtimeImages = map[string]string{
	"python3.12":      "public.ecr.aws/lambda/python:3.12",
	"python3.11":      "public.ecr.aws/lambda/python:3.11",
	"python3.10":      "public.ecr.aws/lambda/python:3.10",
	"python3.9":       "public.ecr.aws/lambda/python:3.9",
	"nodejs20.x":      "public.ecr.aws/lambda/nodejs:20",
	"nodejs18.x":      "public.ecr.aws/lambda/nodejs:18",
	"java21":          "public.ecr.aws/lambda/java:21",
	"java17":          "public.ecr.aws/lambda/java:17",
	"java11":          "public.ecr.aws/lambda/java:11",
	"dotnet8":         "public.ecr.aws/lambda/dotnet:8",
	"dotnet6":         "public.ecr.aws/lambda/dotnet:6",
	"go1.x":           "public.ecr.aws/lambda/provided:al2",
	"provided.al2":    "public.ecr.aws/lambda/provided:al2",
	"provided.al2023": "public.ecr.aws/lambda/provided:al2023",
}

// ImageForRuntime returns the container image for the given Lambda runtime.
// cfg.DefaultImage overrides the lookup when non-empty.
// req.Image (per-function ImageUri) takes precedence over both.
func ImageForRuntime(req InvokeRequest, cfg LambdaConfig) string {
	if req.Image != "" {
		return req.Image
	}
	if cfg.DefaultImage != "" {
		return cfg.DefaultImage
	}
	if img, ok := runtimeImages[req.Runtime]; ok {
		return img
	}
	return "public.ecr.aws/lambda/provided:al2"
}

// NewExecutor constructs the appropriate LambdaExecutor for the given mode.
// For executors with a platform config use NewK8sExecutor / NewDockerExecutor directly.
func NewExecutor(cfg LambdaConfig) LambdaExecutor {
	switch cfg.Mode {
	case "docker":
		return NewDockerExecutor(cfg, nil)
	case "k8s":
		return NewK8sExecutor(cfg, nil)
	default:
		return &MockExecutor{}
	}
}
