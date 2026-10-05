package container

import "jaiscloud/internal/k8stypes"

// DockerEnv renders env pairs as "KEY=VALUE" strings for the Docker create API.
func DockerEnv(pairs []EnvVar) []string {
	env := make([]string, 0, len(pairs))
	for _, kv := range pairs {
		env = append(env, kv.Name+"="+kv.Value)
	}
	return env
}

// K8sEnv renders env pairs as pod env entries.
func K8sEnv(pairs []EnvVar) []k8stypes.EnvVar {
	env := make([]k8stypes.EnvVar, 0, len(pairs))
	for _, kv := range pairs {
		env = append(env, k8stypes.EnvVar{Name: kv.Name, Value: kv.Value})
	}
	return env
}
