package lambda

import "jaiscloud/internal/k8stypes"

// runtimeEnvVar is one environment variable for a Lambda runtime container/pod.
type runtimeEnvVar struct {
	Name  string
	Value string
}

// commonRuntimeEnv returns the base environment shared by the Docker and K8s
// executors: the function identity, emulator credentials, and the Lambda task
// layout.
//
// It deliberately does NOT set AWS_LAMBDA_RUNTIME_API. The public Lambda base
// image's /lambda-entrypoint.sh starts its bundled Runtime Interface Emulator
// (RIE) only when that variable is unset; the RIE then exports
// AWS_LAMBDA_RUNTIME_API=127.0.0.1:9001 for the runtime bootstrap and serves
// invocations on :8080. Setting it here makes the entrypoint skip the RIE, so
// the bootstrap has no runtime API to talk to and exits — the container never
// serves and the pod never becomes Ready. The executor POSTs to the RIE path on
// port 8080 (invocationPort / riePort), so the RIE must be allowed to start.
//
// Keep Docker and K8s on this one list so the two transports cannot drift.
func commonRuntimeEnv(cfg LambdaConfig, req InvokeRequest) []runtimeEnvVar {
	return []runtimeEnvVar{
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

// runtimeEnvPairs returns the full runtime environment — the shared base plus
// the emulator endpoint and per-function env — as ordered name/value pairs.
func runtimeEnvPairs(cfg LambdaConfig, req InvokeRequest) []runtimeEnvVar {
	pairs := commonRuntimeEnv(cfg, req)
	if cfg.JaisCloudEndpoint != "" {
		pairs = append(pairs,
			runtimeEnvVar{Name: "AWS_ENDPOINT_URL", Value: cfg.JaisCloudEndpoint},
			runtimeEnvVar{Name: "JAISCLOUD_ENDPOINT", Value: cfg.JaisCloudEndpoint},
		)
	}
	for k, v := range req.EnvVars {
		pairs = append(pairs, runtimeEnvVar{Name: k, Value: v})
	}
	return pairs
}

// dockerRuntimeEnv renders the runtime environment as "KEY=VALUE" strings for
// the Docker create API.
func dockerRuntimeEnv(cfg LambdaConfig, req InvokeRequest) []string {
	pairs := runtimeEnvPairs(cfg, req)
	env := make([]string, 0, len(pairs))
	for _, kv := range pairs {
		env = append(env, kv.Name+"="+kv.Value)
	}
	return env
}

// k8sRuntimeEnv renders the runtime environment as pod env entries.
func k8sRuntimeEnv(cfg LambdaConfig, req InvokeRequest) []k8stypes.EnvVar {
	pairs := runtimeEnvPairs(cfg, req)
	env := make([]k8stypes.EnvVar, 0, len(pairs))
	for _, kv := range pairs {
		env = append(env, k8stypes.EnvVar{Name: kv.Name, Value: kv.Value})
	}
	return env
}
