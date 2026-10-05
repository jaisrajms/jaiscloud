// Package lambda is the AWS Lambda profile for the shared container executor.
// The warm-pool lifecycle lives in internal/executor/container; this package
// supplies the Lambda runtime contract (Runtime Interface Emulator invocation
// path, AWS_LAMBDA_* / LAMBDA_TASK_ROOT environment, /var/task code layout,
// layer mounting, Lambda public images, and Lambda workload naming) and thin
// constructors so existing AWS callers are unchanged.
package awslambda

import (
	"fmt"
	"net/http"

	"jaiscloud/internal/executor/container"
)

const (
	// riePort is the Lambda base image's Runtime Interface Emulator port.
	riePort = 8080
	// riePath is the RIE invocation endpoint.
	riePath = "/2015-03-31/functions/function/invocations"
	// labelApp is the app label stamped on managed Lambda pods.
	labelApp = "jaiscloud-lambda"
)

// LambdaProfile implements container.Profile for AWS Lambda.
type LambdaProfile struct{}

// NewLambdaProfile returns the AWS Lambda runtime profile.
func NewLambdaProfile() LambdaProfile { return LambdaProfile{} }

func (LambdaProfile) Name() string { return "lambda" }

func (LambdaProfile) InvocationPort() int { return riePort }

// Invocation returns the RIE POST the executor sends to a warm container/pod.
func (LambdaProfile) Invocation(req container.Request) container.Invocation {
	return container.Invocation{
		Method: http.MethodPost,
		Path:   riePath,
		Header: map[string]string{"Content-Type": "application/json"},
		Body:   req.Payload,
	}
}

// DecodeResponse returns the RIE body, mapping an HTTP >= 500 to an error (the
// runtime failed to handle the invocation).
func (LambdaProfile) DecodeResponse(status int, body []byte) ([]byte, error) {
	if status >= 500 {
		return nil, fmt.Errorf("RIE returned HTTP %d", status)
	}
	return body, nil
}

func (LambdaProfile) RuntimeEnv(cfg container.Config, req container.Request) []container.EnvVar {
	return runtimeEnvPairs(cfg, req)
}

func (LambdaProfile) CodeMountDir() string  { return "/var/task" }
func (LambdaProfile) LayerMountDir() string { return "/opt" }

func (LambdaProfile) WorkloadLabel() string { return labelApp }

func (LambdaProfile) WorkloadNamePrefix(instanceID string) string { return instancePrefix(instanceID) }

// ContainerArgs passes the handler as the Lambda base image's entrypoint
// argument (it starts the RIE and hands the handler to the runtime bootstrap).
func (LambdaProfile) ContainerArgs(req container.Request) []string {
	if req.Handler == "" {
		return nil
	}
	return []string{req.Handler}
}

func (LambdaProfile) ImageForRuntime(cfg container.Config, req container.Request) string {
	return ImageForRuntime(req, cfg)
}

// regionOrDefault returns r if non-empty, else "us-east-1".
func regionOrDefault(r string) string {
	if r != "" {
		return r
	}
	return "us-east-1"
}

// commonRuntimeEnv returns the base Lambda environment shared by the Docker and
// K8s executors: the function identity, emulator credentials, and the Lambda
// task layout.
//
// It deliberately does NOT set AWS_LAMBDA_RUNTIME_API. The public Lambda base
// image's /lambda-entrypoint.sh starts its bundled Runtime Interface Emulator
// (RIE) only when that variable is unset; the RIE then exports
// AWS_LAMBDA_RUNTIME_API=127.0.0.1:9001 for the runtime bootstrap and serves
// invocations on :8080. Setting it here makes the entrypoint skip the RIE, so
// the bootstrap has no runtime API to talk to and exits — the container never
// serves and the pod never becomes Ready.
func commonRuntimeEnv(cfg container.Config, req container.Request) []container.EnvVar {
	return []container.EnvVar{
		{Name: "AWS_LAMBDA_FUNCTION_NAME", Value: req.FunctionName},
		{Name: "AWS_DEFAULT_REGION", Value: regionOrDefault(cfg.Region)},
		{Name: "AWS_REGION", Value: regionOrDefault(cfg.Region)},
		{Name: "_HANDLER", Value: req.Handler},
		{Name: "AWS_ACCESS_KEY_ID", Value: req.AccountID},
		{Name: "AWS_SECRET_ACCESS_KEY", Value: "test"},
		{Name: "AWS_SESSION_TOKEN", Value: "test"},
		{Name: "LAMBDA_TASK_ROOT", Value: "/var/task"},
		{Name: "LAMBDA_RUNTIME_DIR", Value: "/var/runtime"},
	}
}

// runtimeEnvPairs returns the full Lambda runtime environment — the shared base
// plus the emulator endpoint and per-function env — as ordered name/value pairs.
func runtimeEnvPairs(cfg container.Config, req container.Request) []container.EnvVar {
	pairs := commonRuntimeEnv(cfg, req)
	if cfg.JaisCloudEndpoint != "" {
		pairs = append(pairs,
			container.EnvVar{Name: "AWS_ENDPOINT_URL", Value: cfg.JaisCloudEndpoint},
			container.EnvVar{Name: "JAISCLOUD_ENDPOINT", Value: cfg.JaisCloudEndpoint},
		)
	}
	for k, v := range req.EnvVars {
		pairs = append(pairs, container.EnvVar{Name: k, Value: v})
	}
	return pairs
}

// instancePrefix returns the instance-scoped pod/service prefix.
// Format: jc-lambda-<instanceID[:8]>-
// Falls back to "jc-lambda-" when InstanceID is empty (dev/test mode).
func instancePrefix(instanceID string) string {
	if len(instanceID) >= 8 {
		return "jc-lambda-" + instanceID[:8] + "-"
	}
	if instanceID != "" {
		return "jc-lambda-" + instanceID + "-"
	}
	return "jc-lambda-"
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
func ImageForRuntime(req container.Request, cfg container.Config) string {
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
