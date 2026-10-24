package awslambda

import (
	"os"
	"strings"

	"k8s.io/client-go/kubernetes"

	"jaiscloud/internal/executor/container"
	"jaiscloud/internal/platform"
)

// Type aliases keep the historical Lambda API names source-compatible while the
// implementation lives in the cloud-neutral container package.
type (
	// LayerInfo carries the information needed to mount a Lambda layer.
	LayerInfo = container.Layer
	// InvokeRequest carries everything needed to invoke a Lambda function.
	InvokeRequest = container.Request
	// InvokeResult holds the function response payload and optional log tail.
	InvokeResult = container.Result
	// LambdaConfig holds executor-wide configuration shared by all functions.
	LambdaConfig = container.Config
	// LambdaExecutor is the interface satisfied by the executors.
	LambdaExecutor = container.Executor
	// CodeLoader fetches function zip bytes for mounting into a container.
	CodeLoader = container.CodeLoader
	// LayerBlobLoader fetches layer zip bytes by blob key.
	LayerBlobLoader = container.LayerBlobLoader
	// LogsIngestor writes CloudWatch log events.
	LogsIngestor = container.LogsIngestor
	// DockerExecutor manages warm Docker containers per Lambda function.
	DockerExecutor = container.DockerExecutor
	// K8sExecutor manages warm Pods per Lambda function.
	K8sExecutor = container.K8sExecutor
	// K8sExecutorOption customizes a K8sExecutor at construction.
	K8sExecutorOption = container.K8sExecutorOption
	// MockExecutor echoes the request payload as the response.
	MockExecutor = container.MockExecutor
)

// WithK8sClient injects the Kubernetes client.
func WithK8sClient(client kubernetes.Interface) container.K8sExecutorOption {
	return container.WithK8sClient(client)
}

// WithWorkloadProbe overrides the endpoint readiness probe (tests).
func WithWorkloadProbe(probe func(string) bool) container.K8sExecutorOption {
	return container.WithWorkloadProbe(probe)
}

// ExtractZip unzips zipBytes into dest.
func ExtractZip(zipBytes []byte, dest string) error { return container.ExtractZip(zipBytes, dest) }

// DefaultLambdaConfig returns a LambdaConfig with sensible defaults.
func DefaultLambdaConfig() LambdaConfig {
	cfg := container.DefaultConfig()
	if v := os.Getenv("JAISCLOUD_LAMBDA_CODE_URL"); v != "" {
		cfg.CodeURL = strings.TrimRight(v, "/")
	}
	return cfg
}

// LambdaConfigFrom populates a LambdaConfig with values from environment variables.
func LambdaConfigFrom(base LambdaConfig) LambdaConfig {
	base.ConcurrencyLimit = container.Int64Env("JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT", 1000)
	base.SyncPayloadMax = container.Int64Env("JAISCLOUD_LAMBDA_SYNC_PAYLOAD_MAX_BYTES", 6*1024*1024)
	base.AsyncPayloadMax = container.Int64Env("JAISCLOUD_LAMBDA_ASYNC_PAYLOAD_MAX_BYTES", 256*1024)
	base.ResponsePayloadMax = container.Int64Env("JAISCLOUD_LAMBDA_RESPONSE_PAYLOAD_MAX_BYTES", 6*1024*1024)
	return base
}

// NewExecutor constructs the appropriate LambdaExecutor for the given mode.
// For executors with a platform config use NewK8sExecutor / NewDockerExecutor.
func NewExecutor(cfg LambdaConfig) LambdaExecutor {
	switch cfg.Mode {
	case "docker":
		return container.NewDockerExecutor(cfg, LambdaProfile{}, nil)
	case "k8s":
		return container.NewK8sExecutor(cfg, LambdaProfile{}, nil)
	default:
		return &container.MockExecutor{}
	}
}

// NewDockerExecutor creates a DockerExecutor running the Lambda profile.
func NewDockerExecutor(cfg LambdaConfig, plat *platform.PlatformConfig) *DockerExecutor {
	return container.NewDockerExecutor(cfg, LambdaProfile{}, plat)
}

// NewK8sExecutor creates a K8sExecutor running the Lambda profile.
func NewK8sExecutor(cfg LambdaConfig, plat *platform.PlatformConfig, opts ...container.K8sExecutorOption) *K8sExecutor {
	return container.NewK8sExecutor(cfg, LambdaProfile{}, plat, opts...)
}
