package container

// Invocation is the HTTP request the executor sends to a warm workload. A
// profile builds it from the Request: AWS Lambda returns a fixed RIE path and
// forwards the payload as an application/json body; GCP Cloud Functions posts to
// the Functions Framework root with either the raw HTTP body or a CloudEvent
// (ce-* headers).
type Invocation struct {
	Method string
	Path   string
	Header map[string]string
	Body   []byte
}

// EnvVar is one environment variable for a runtime container/pod.
type EnvVar struct {
	Name  string
	Value string
}

// Profile is the per-cloud runtime contract a warm-pool executor runs. The
// executor owns the lifecycle (warm pool, Docker/K8s create/readiness/reap, code
// loading, GC); the profile owns everything cloud-specific about *what* runs and
// *how it is called*.
type Profile interface {
	// Name identifies the profile (logs/health).
	Name() string
	// InvocationPort is the container/pod port the runtime serves on.
	InvocationPort() int
	// Invocation builds the HTTP request the executor sends to the workload.
	Invocation(req Request) Invocation
	// DecodeResponse maps a workload HTTP status/body onto the invocation result
	// payload, returning an error for a workload that failed (e.g. HTTP >= 500).
	DecodeResponse(status int, body []byte) ([]byte, error)
	// RuntimeEnv returns the profile's base runtime environment for a workload.
	RuntimeEnv(cfg Config, req Request) []EnvVar
	// CodeMountDir is where the function archive is mounted in the workload.
	CodeMountDir() string
	// LayerMountDir is where layer archives are mounted, or "" when the cloud has
	// no layer concept (GCP).
	LayerMountDir() string
	// WorkloadLabel is the app label stamped on managed containers/pods.
	WorkloadLabel() string
	// WorkloadNamePrefix is the instance-scoped name prefix for managed
	// containers/pods.
	WorkloadNamePrefix(instanceID string) string
	// ContainerArgs is the entrypoint argument list for the runtime container
	// (e.g. a Lambda handler), or nil when the image's own entrypoint suffices.
	ContainerArgs(req Request) []string
	// ImageForRuntime resolves the container image for req.
	ImageForRuntime(cfg Config, req Request) string
}
