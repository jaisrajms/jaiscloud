package runexec

import (
	"testing"

	runstore "jaiscloud/internal/gcp/store/run"
)

func revisionWithContainer(c map[string]any) (runstore.Service, runstore.Revision) {
	svc := runstore.Service{ProjectID: "p", Location: "l", ID: "svc"}
	rev := runstore.Revision{
		ProjectID: "p", Location: "l", Service: "svc", ID: "svc-00001",
		Data: map[string]any{"containers": []any{c}},
	}
	return svc, rev
}

func TestBuildPodInjectsPlatformEnvAndPort(t *testing.T) {
	svc, rev := revisionWithContainer(map[string]any{
		"image":   "nginx:latest",
		"command": []any{"/bin/sh"},
		"args":    []any{"-c", "serve"},
		"env": []any{
			map[string]any{"name": "FOO", "value": "bar"},
			map[string]any{"name": "PORT", "value": "9999"},
		},
		"ports":     []any{map[string]any{"containerPort": float64(80)}},
		"resources": map[string]any{"limits": map[string]any{"cpu": "250m", "memory": "128Mi"}},
	})

	pod, port, err := buildPod(svc, rev, "wl-svc", "jaiscloud")
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	if port != 80 {
		t.Errorf("port = %d, want 80", port)
	}
	if pod.Name != "wl-svc" || pod.Namespace != "jaiscloud" {
		t.Errorf("pod meta = %s/%s", pod.Namespace, pod.Name)
	}
	if pod.Spec.RestartPolicy != "Always" {
		t.Errorf("restart policy = %q, want Always", pod.Spec.RestartPolicy)
	}
	c := pod.Spec.Containers[0]
	if c.Image != "nginx:latest" {
		t.Errorf("image = %q", c.Image)
	}
	if len(c.Command) != 1 || c.Command[0] != "/bin/sh" || len(c.Args) != 2 {
		t.Errorf("command/args = %v / %v", c.Command, c.Args)
	}
	if len(c.Ports) != 1 || c.Ports[0].ContainerPort != 80 {
		t.Errorf("container ports = %v", c.Ports)
	}
	env := map[string]string{}
	for _, e := range c.Env {
		env[e.Name] = e.Value
	}
	if env["FOO"] != "bar" {
		t.Errorf("template env FOO = %q", env["FOO"])
	}
	if env["PORT"] != "80" {
		t.Errorf("PORT = %q, want injected 80 (platform overrides template)", env["PORT"])
	}
	if env["K_SERVICE"] != "svc" || env["K_CONFIGURATION"] != "svc" || env["K_REVISION"] != "svc-00001" {
		t.Errorf("platform env = %v", env)
	}
	if got := c.Resources.Limits.Cpu().String(); got != "250m" {
		t.Errorf("cpu limit = %q", got)
	}
	if got := c.Resources.Limits.Memory().String(); got != "128Mi" {
		t.Errorf("memory limit = %q", got)
	}
	if pod.Labels[labelApp] != labelAppValue || pod.Labels[labelService] != "svc" || pod.Labels[labelRevision] != "svc-00001" {
		t.Errorf("labels = %v", pod.Labels)
	}
}

func TestBuildPodDefaultsPortAndRejectsMissingImage(t *testing.T) {
	svc, rev := revisionWithContainer(map[string]any{"image": "nginx:latest"})
	_, port, err := buildPod(svc, rev, "wl", "ns")
	if err != nil {
		t.Fatalf("buildPod: %v", err)
	}
	if port != defaultContainerPort {
		t.Errorf("default port = %d, want %d", port, defaultContainerPort)
	}

	svc, rev = revisionWithContainer(map[string]any{})
	if _, _, err := buildPod(svc, rev, "wl", "ns"); err == nil {
		t.Error("missing image: want error")
	}

	rev = runstore.Revision{ProjectID: "p", Location: "l", Service: "svc", ID: "r"}
	if _, _, err := buildPod(svc, rev, "wl", "ns"); err == nil {
		t.Error("empty template: want error")
	}
}

func TestWorkloadNameIsStableAndDNS(t *testing.T) {
	svc := runstore.Service{ProjectID: "p", Location: "l", ID: "My_Service"}
	rev := runstore.Revision{ProjectID: "p", Location: "l", Service: "My_Service", ID: "My_Service-00001"}
	name := workloadName(svc, rev)
	if name != workloadName(svc, rev) {
		t.Error("workload name is not stable")
	}
	if len(name) > 63 {
		t.Errorf("workload name %q exceeds 63 chars", name)
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			t.Fatalf("workload name %q has an unsafe rune %q", name, r)
		}
	}
	other := runstore.Revision{ProjectID: "p", Location: "l", Service: "My_Service", ID: "My_Service-00002"}
	if workloadName(svc, other) == name {
		t.Error("distinct revisions produced the same workload name")
	}
}
