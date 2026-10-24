// Package gcf is the GCP Cloud Functions (Functions Framework) profile for the
// shared container executor. The warm-pool lifecycle lives in
// internal/executor/container; this package supplies the GCP runtime contract:
// the Functions Framework HTTP server on $PORT (HTTP request / CloudEvent
// invocation), the FUNCTION_TARGET / FUNCTION_SIGNATURE_TYPE / GCP_PROJECT /
// LOG_EXECUTION_ID environment, the /workspace source layout, and GCP runtime
// images.
package gcf

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/executor/container"
)

const (
	// functionsPort is the Functions Framework's default listen port ($PORT).
	functionsPort = 8080
	// functionsPath is the Functions Framework root endpoint.
	functionsPath = "/"
	// workloadLabel is the app label stamped on managed function pods.
	workloadLabel = "jaiscloud-functions"
	// codeMountDir is the Functions Framework workspace where the source
	// archive is mounted.
	codeMountDir = "/workspace"
)

// SignatureType values accepted by the Functions Framework.
const (
	SignatureHTTP       = "http"
	SignatureCloudEvent = "cloudevent"
)

// FunctionsFrameworkProfile implements container.Profile for GCP Cloud Functions.
type FunctionsFrameworkProfile struct{}

// NewFunctionsFrameworkProfile returns the GCP runtime profile.
func NewFunctionsFrameworkProfile() FunctionsFrameworkProfile {
	return FunctionsFrameworkProfile{}
}

func (FunctionsFrameworkProfile) Name() string { return "functions-framework" }

func (FunctionsFrameworkProfile) InvocationPort() int { return functionsPort }

// Invocation builds the POST to the Functions Framework root. For a CloudEvent
// invocation the event metadata is carried in the ce-* headers (binary content
// mode) and the event data is the HTTP request body; otherwise the raw payload
// is the HTTP request body.
func (p FunctionsFrameworkProfile) Invocation(req container.Request) container.Invocation {
	header := map[string]string{"Content-Type": "application/json"}
	body := req.Payload
	if req.SignatureType == SignatureCloudEvent && req.Event != nil {
		ev := req.Event
		specVersion := ev.SpecVersion
		if specVersion == "" {
			specVersion = "1.0"
		}
		header["ce-specversion"] = specVersion
		if ev.ID != "" {
			header["ce-id"] = ev.ID
		}
		if ev.Source != "" {
			header["ce-source"] = ev.Source
		}
		if ev.Type != "" {
			header["ce-type"] = ev.Type
		}
		if ev.Subject != "" {
			header["ce-subject"] = ev.Subject
		}
		if !ev.Time.IsZero() {
			header["ce-time"] = ev.Time.UTC().Format(time.RFC3339Nano)
		}
		if ev.DataContentType != "" {
			header["Content-Type"] = ev.DataContentType
		}
		// In binary content mode the event data is the body. Fall back to the
		// request payload only when the caller supplied no event data (e.g. an
		// HTTP body routed through a CloudEvent-signed function).
		if len(ev.Data) > 0 {
			body = ev.Data
		}
	}
	return container.Invocation{Method: http.MethodPost, Path: functionsPath, Header: header, Body: body}
}

// DecodeResponse returns the framework's response body, mapping an HTTP >= 500
// to an error (an unhandled function exception).
func (FunctionsFrameworkProfile) DecodeResponse(status int, body []byte) ([]byte, error) {
	if status >= 500 {
		return nil, fmt.Errorf("functions framework returned HTTP %d", status)
	}
	return body, nil
}

// RuntimeEnv returns the Functions Framework environment.
func (p FunctionsFrameworkProfile) RuntimeEnv(cfg container.Config, req container.Request) []container.EnvVar {
	env := []container.EnvVar{
		{Name: "FUNCTION_TARGET", Value: functionTarget(req)},
		{Name: "FUNCTION_SIGNATURE_TYPE", Value: signatureType(req)},
		{Name: "GOOGLE_FUNCTION_TARGET", Value: functionTarget(req)},
		{Name: "GOOGLE_FUNCTION_SIGNATURE_TYPE", Value: signatureType(req)},
		{Name: "PORT", Value: strconv.Itoa(functionsPort)},
		{Name: "GCP_PROJECT", Value: req.AccountID},
		{Name: "GOOGLE_CLOUD_PROJECT", Value: req.AccountID},
		{Name: "GOOGLE_CLOUD_LOCATION", Value: cfg.Region},
		{Name: "LOG_EXECUTION_ID", Value: req.ExecutionID},
	}
	if cfg.JaisCloudEndpoint != "" {
		env = append(env, container.EnvVar{Name: "JAISCLOUD_ENDPOINT", Value: cfg.JaisCloudEndpoint})
	}
	for k, v := range req.EnvVars {
		env = append(env, container.EnvVar{Name: k, Value: v})
	}
	return env
}

func (FunctionsFrameworkProfile) CodeMountDir() string  { return codeMountDir }
func (FunctionsFrameworkProfile) LayerMountDir() string { return "" }

func (FunctionsFrameworkProfile) WorkloadLabel() string { return workloadLabel }

// WorkloadNamePrefix returns the instance-scoped workload name prefix.
func (FunctionsFrameworkProfile) WorkloadNamePrefix(instanceID string) string {
	if len(instanceID) >= 8 {
		return "jc-functions-" + instanceID[:8] + "-"
	}
	if instanceID != "" {
		return "jc-functions-" + instanceID + "-"
	}
	return "jc-functions-"
}

// ContainerArgs is nil: the Functions Framework image's own entrypoint starts
// the server (it reads FUNCTION_TARGET from the environment).
func (FunctionsFrameworkProfile) ContainerArgs(container.Request) []string { return nil }

// ImageForRuntime resolves the Functions Framework image. Google does not
// publish prebuilt Functions Framework images (Cloud Functions builds them from
// source with buildpacks), so the default is a locally buildable tag per
// language; JAISCLOUD_FUNCTIONS_IMAGE overrides every mapping.
func (FunctionsFrameworkProfile) ImageForRuntime(cfg container.Config, req container.Request) string {
	if req.Image != "" {
		return req.Image
	}
	if v := os.Getenv("JAISCLOUD_FUNCTIONS_IMAGE"); v != "" {
		return v
	}
	if cfg.DefaultImage != "" {
		return cfg.DefaultImage
	}
	if img, ok := functionsFrameworkImages[req.Runtime]; ok {
		return img
	}
	return defaultFunctionsFrameworkImage
}

// functionsFrameworkImages maps GCP runtime ids to locally buildable
// Functions-Framework image tags (see README-GCP.md). JAISCLOUD_FUNCTIONS_IMAGE
// overrides every mapping.
var functionsFrameworkImages = map[string]string{
	"python310": "jaiscloud-functions-framework-python:latest",
	"python311": "jaiscloud-functions-framework-python:latest",
	"python312": "jaiscloud-functions-framework-python:latest",
	"python313": "jaiscloud-functions-framework-python:latest",
	"nodejs18":  "jaiscloud-functions-framework-nodejs:latest",
	"nodejs20":  "jaiscloud-functions-framework-nodejs:latest",
	"nodejs22":  "jaiscloud-functions-framework-nodejs:latest",
	"nodejs24":  "jaiscloud-functions-framework-nodejs:latest",
	"go121":     "jaiscloud-functions-framework-go:latest",
	"go122":     "jaiscloud-functions-framework-go:latest",
	"go123":     "jaiscloud-functions-framework-go:latest",
	"go124":     "jaiscloud-functions-framework-go:latest",
	"java17":    "jaiscloud-functions-framework-java:latest",
	"java21":    "jaiscloud-functions-framework-java:latest",
	"ruby32":    "jaiscloud-functions-framework-ruby:latest",
	"ruby33":    "jaiscloud-functions-framework-ruby:latest",
	"php82":     "jaiscloud-functions-framework-php:latest",
	"php83":     "jaiscloud-functions-framework-php:latest",
	"php84":     "jaiscloud-functions-framework-php:latest",
	"dotnet6":   "jaiscloud-functions-framework-dotnet:latest",
	"dotnet8":   "jaiscloud-functions-framework-dotnet:latest",
}

// defaultFunctionsFrameworkImage is the fallback for an unknown runtime.
const defaultFunctionsFrameworkImage = "jaiscloud-functions-framework:latest"

// signatureType returns the function's invocation signature, defaulting to HTTP.
func signatureType(req container.Request) string {
	if req.SignatureType != "" {
		return req.SignatureType
	}
	return SignatureHTTP
}

// functionTarget returns FUNCTION_TARGET: the configured entry point, else the
// conventional default for the runtime's language.
func functionTarget(req container.Request) string {
	if req.Handler != "" {
		return req.Handler
	}
	return defaultFunctionTargets[runtimeLanguage(req.Runtime)]
}

// runtimeLanguage strips the version digits from a GCP runtime id
// (e.g. "python312" → "python").
func runtimeLanguage(runtime string) string {
	i := len(runtime)
	for i > 0 && runtime[i-1] >= '0' && runtime[i-1] <= '9' {
		i--
	}
	return runtime[:i]
}

// defaultFunctionTargets holds the conventional entry point per language, used
// when the function did not set an entryPoint.
var defaultFunctionTargets = map[string]string{
	"python": "main",
	"nodejs": "helloWorld",
	"go":     "HelloWorld",
	"java":   "Hello",
	"ruby":   "hello",
	"php":    "hello",
	"dotnet": "Function",
}

// FunctionsFrameworkImage returns the configured image for a runtime (for the
// console/health view), applying the JAISCLOUD_FUNCTIONS_IMAGE override.
func FunctionsFrameworkImage(runtime string) string {
	if v := os.Getenv("JAISCLOUD_FUNCTIONS_IMAGE"); v != "" {
		return v
	}
	if img, ok := functionsFrameworkImages[runtime]; ok {
		return img
	}
	return defaultFunctionsFrameworkImage
}

// DefaultConfig returns the executor config with the Cloud Functions defaults
// and JAISCLOUD_FUNCTIONS_* env applied.
func DefaultConfig() container.Config {
	cfg := container.DefaultConfig()
	cfg.Cloud = "gcp"
	if v := strings.TrimRight(os.Getenv("JAISCLOUD_FUNCTIONS_CODE_URL"), "/"); v != "" {
		cfg.CodeURL = v
	}
	cfg.ConcurrencyLimit = container.Int64Env("JAISCLOUD_FUNCTIONS_CONCURRENCY_LIMIT",
		container.Int64Env("JAISCLOUD_LAMBDA_CONCURRENCY_LIMIT", 1000))
	cfg.SyncPayloadMax = container.Int64Env("JAISCLOUD_FUNCTIONS_SYNC_PAYLOAD_MAX_BYTES", 6*1024*1024)
	cfg.AsyncPayloadMax = container.Int64Env("JAISCLOUD_FUNCTIONS_ASYNC_PAYLOAD_MAX_BYTES", 256*1024)
	cfg.ResponsePayloadMax = container.Int64Env("JAISCLOUD_FUNCTIONS_RESPONSE_PAYLOAD_MAX_BYTES", 6*1024*1024)
	return cfg
}
