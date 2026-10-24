// Package container is the cloud-neutral warm-container / warm-pod executor
// shared by AWS Lambda and GCP Cloud Functions. It owns the per-function warm
// pool, the Docker and Kubernetes lifecycles, code mounting and the invocation
// transport; the per-cloud runtime contract (invocation path/env, code layout,
// image table, workload naming) is supplied by a Profile.
//
// It deliberately imports no cloud provider package: the Docker transport is the
// shared internal/docker engine client and the Kubernetes lifecycle goes through
// internal/k8shelpers, so both transports speak the same wire to the host.
package container

import (
	"context"
	"time"

	"jaiscloud/internal/logstream"
)

// LogsIngestor is the interface an executor uses to write workload log events.
// It is satisfied structurally by the CloudWatch Logs provider via the
// logstream adapter (AWS Lambda). It is optional and a profile that has no log
// sink simply never wires it.
type LogsIngestor = logstream.Ingestor

// Layer is a resolved function layer to mount into a container/pod.
type Layer struct {
	ARN     string // layer version ARN (or equivalent identifier)
	BlobKey string // blob store key for the layer archive
}

// Request carries everything needed to invoke a function.
type Request struct {
	FunctionName string
	Runtime      string
	Handler      string
	Image        string // override image; empty = derive from Runtime via the profile
	MemoryMB     int
	TimeoutSecs  int
	EnvVars      map[string]string
	Payload      []byte
	AccountID    string
	Layers       []Layer
	LogType      string // "Tail" → executor captures the last 4 KiB of stdout into LogTail
	// CodeKey is an optional code identifier passed to the CodeLoader in place
	// of FunctionName. Providers whose function identity is scoped beyond
	// (account, name) — e.g. GCP Cloud Functions' project+location+id — set it
	// so the loader can resolve the right archive. Empty falls back to
	// FunctionName.
	CodeKey string

	// SignatureType is the per-function invocation signature a profile may map
	// onto the workload (e.g. GCP "http" / "cloudevent"). Empty means the
	// profile's default.
	SignatureType string
	// ExecutionID is the invocation id, when the provider has one, so a profile
	// can expose it to the workload (e.g. GCP LOG_EXECUTION_ID).
	ExecutionID string
	// Event carries producer event metadata for an event-driven invocation. It is
	// nil for a plain payload invocation (AWS always, GCP HTTP).
	Event *CloudEvent
}

// CloudEvent is the per-invocation event metadata a profile may map onto an
// HTTP/CloudEvent request.
type CloudEvent struct {
	SpecVersion     string
	ID              string
	Source          string
	Type            string
	Subject         string
	Time            time.Time
	DataContentType string
	Attributes      map[string]string
	Data            []byte
}

// codeKey returns the identifier the CodeLoader should resolve for req, falling
// back to FunctionName when no explicit CodeKey is set.
func codeKey(req Request) string {
	if req.CodeKey != "" {
		return req.CodeKey
	}
	return req.FunctionName
}

// Result holds the function response payload and optional log tail.
type Result struct {
	Payload []byte
	LogTail []byte // populated when Request.LogType == "Tail"
}

// CodeLoader fetches function archive bytes for mounting into a container/pod.
type CodeLoader interface {
	LoadCode(ctx context.Context, account, funcName, version string) ([]byte, error)
}

// LayerBlobLoader fetches layer archive bytes by blob key.
type LayerBlobLoader interface {
	GetLayerBlob(ctx context.Context, blobKey string) ([]byte, error)
}

// Executor is the interface satisfied by MockExecutor, DockerExecutor, and
// K8sExecutor.
type Executor interface {
	// Invoke executes a function synchronously and returns the result.
	Invoke(ctx context.Context, req Request) (Result, error)
	// DeleteFunction tears down any warm container or pod for the named function.
	DeleteFunction(ctx context.Context, functionName string)
	// Reset tears down all warm containers or pods (called on /_jaiscloud/reset).
	Reset(ctx context.Context)
	// Close releases all resources held by the executor (containers, goroutines).
	Close() error
}
