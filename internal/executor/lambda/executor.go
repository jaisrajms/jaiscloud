// Package lambda provides pluggable Lambda executors.
// The LambdaExecutor interface decouples the FunctionProvider from the
// underlying invocation mechanism (mock echo, Docker warm container, K8s Job).
package lambda

import "context"

// LayerInfo carries the information needed to mount a Lambda layer.
type LayerInfo struct {
	ARN     string // layer version ARN
	BlobKey string // blob store key for the layer zip
}

// InvokeRequest carries everything needed to invoke a Lambda function.
type InvokeRequest struct {
	FunctionName string
	Runtime      string
	Handler      string
	Image        string // override image; empty = derive from Runtime
	MemoryMB     int
	TimeoutSecs  int
	EnvVars      map[string]string
	Payload      []byte
	AccountID    string
	Layers       []LayerInfo // resolved layer zip blobs to mount at /opt
	LogType      string      // "Tail" → executor captures last 4 KiB of stdout into LogTail
	// CodeKey is an optional code identifier passed to the CodeLoader in place
	// of FunctionName. Emulators whose function identity is scoped beyond
	// (account, name) — e.g. GCP Cloud Functions' project+location+id — set it
	// so the loader can resolve the right archive. Empty falls back to
	// FunctionName (AWS Lambda behavior).
	CodeKey string
}

// codeKey returns the identifier the CodeLoader should resolve for req, falling
// back to FunctionName when no explicit CodeKey is set.
func codeKey(req InvokeRequest) string {
	if req.CodeKey != "" {
		return req.CodeKey
	}
	return req.FunctionName
}

// InvokeResult holds the function response payload and optional log tail.
type InvokeResult struct {
	Payload []byte
	LogTail []byte // populated when InvokeRequest.LogType == "Tail"
}

// LambdaExecutor is the interface satisfied by MockExecutor, DockerExecutor,
// and K8sExecutor.
type LambdaExecutor interface {
	// Invoke executes a Lambda function synchronously and returns the result.
	Invoke(ctx context.Context, req InvokeRequest) (InvokeResult, error)
	// DeleteFunction tears down any warm container or pod for the named function.
	DeleteFunction(ctx context.Context, functionName string)
	// Reset tears down all warm containers or pods (called on /_jaiscloud/reset).
	Reset(ctx context.Context)
	// Close releases all resources held by the executor (containers, goroutines).
	Close() error
}
