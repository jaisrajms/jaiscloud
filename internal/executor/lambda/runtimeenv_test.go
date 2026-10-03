package lambda

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommonRuntimeEnv_DoesNotSetLambdaRuntimeAPI is the FD14 regression guard:
// AWS_LAMBDA_RUNTIME_API must never be injected, because the Lambda base image's
// entrypoint starts its bundled RIE only when the variable is unset. Setting it
// suppresses the RIE, so the runtime bootstrap exits and the container/pod never
// serves. It must hold for both the Docker and K8s env builders.
func TestCommonRuntimeEnv_DoesNotSetLambdaRuntimeAPI(t *testing.T) {
	cfg := LambdaConfig{Region: "us-east-1", JaisCloudEndpoint: "http://jc:8080"}
	req := InvokeRequest{FunctionName: "fn", Handler: "app.handler", AccountID: "acct"}

	for _, env := range [][]runtimeEnvVar{commonRuntimeEnv(cfg, req), runtimeEnvPairs(cfg, req)} {
		for _, kv := range env {
			assert.NotEqual(t, "AWS_LAMBDA_RUNTIME_API", kv.Name,
				"AWS_LAMBDA_RUNTIME_API must not be set: it stops the base image entrypoint from starting the RIE")
		}
	}

	for _, kv := range dockerRuntimeEnv(cfg, req) {
		assert.False(t, strings.HasPrefix(kv, "AWS_LAMBDA_RUNTIME_API="),
			"docker env must not set AWS_LAMBDA_RUNTIME_API")
	}
	for _, kv := range k8sRuntimeEnv(cfg, req) {
		assert.NotEqual(t, "AWS_LAMBDA_RUNTIME_API", kv.Name,
			"k8s env must not set AWS_LAMBDA_RUNTIME_API")
	}
}

// TestRuntimeEnv_BaseContents asserts the base env still carries the fields the
// runtime needs (identity, handler, credentials, task layout) and that the
// emulator endpoint and per-function env are appended for both transports.
func TestRuntimeEnv_BaseContents(t *testing.T) {
	cfg := LambdaConfig{Region: "us-east-1", JaisCloudEndpoint: "http://jc:8080"}
	req := InvokeRequest{
		FunctionName: "fn",
		Handler:      "app.handler",
		AccountID:    "acct",
		EnvVars:      map[string]string{"MY_VAR": "my-value"},
	}

	pairs := runtimeEnvPairs(cfg, req)
	got := make(map[string]string, len(pairs))
	for _, kv := range pairs {
		got[kv.Name] = kv.Value
	}

	assert.Equal(t, "fn", got["AWS_LAMBDA_FUNCTION_NAME"])
	assert.Equal(t, "us-east-1", got["AWS_REGION"])
	assert.Equal(t, "us-east-1", got["AWS_DEFAULT_REGION"])
	assert.Equal(t, "app.handler", got["_HANDLER"])
	assert.Equal(t, "acct", got["AWS_ACCESS_KEY_ID"])
	assert.Equal(t, "/var/task", got["LAMBDA_TASK_ROOT"])
	assert.Equal(t, "/var/runtime", got["LAMBDA_RUNTIME_DIR"])
	assert.Equal(t, "http://jc:8080", got["AWS_ENDPOINT_URL"])
	assert.Equal(t, "http://jc:8080", got["JAISCLOUD_ENDPOINT"])
	assert.Equal(t, "my-value", got["MY_VAR"])

	// The Docker and K8s renderings carry the same names/values.
	dockerEnv := dockerRuntimeEnv(cfg, req)
	require.Len(t, dockerEnv, len(pairs))
	for i, kv := range pairs {
		assert.Equal(t, kv.Name+"="+kv.Value, dockerEnv[i])
	}
	k8sEnv := k8sRuntimeEnv(cfg, req)
	require.Len(t, k8sEnv, len(pairs))
	for i, kv := range pairs {
		assert.Equal(t, kv.Name, k8sEnv[i].Name)
		assert.Equal(t, kv.Value, k8sEnv[i].Value)
	}
}
