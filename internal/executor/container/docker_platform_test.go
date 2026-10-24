package container

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"jaiscloud/internal/docker"
	"jaiscloud/internal/platform"
)

// fakeDockerTransport stands in for the Docker Engine API: it records the
// container-create body and answers the start/list calls the executor makes.
type fakeDockerTransport struct {
	createBody map[string]any
}

func (f *fakeDockerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	switch {
	case strings.Contains(r.URL.Path, "/containers/create"):
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &f.createBody)
		return dockerResponse(http.StatusCreated, `{"Id":"deadbeef"}`), nil
	case strings.HasSuffix(r.URL.Path, "/start"):
		return dockerResponse(http.StatusNoContent, ""), nil
	case strings.Contains(r.URL.Path, "/containers/json"):
		return dockerResponse(http.StatusOK, "[]"), nil
	default:
		return dockerResponse(http.StatusOK, "{}"), nil
	}
}

func dockerResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

// TestDockerExecutor_AppliesPlatformOverlay is the FDF2 regression: the platform
// overlay handed to NewDockerExecutor (as the Functions wiring now does) reaches
// the container create body as extra env and a bind mount, exactly like the
// Cloud Run/Dataproc docker executors.
func TestDockerExecutor_AppliesPlatformOverlay(t *testing.T) {
	fake := &fakeDockerTransport{}
	client := docker.New(docker.Config{Client: &http.Client{Transport: fake}})

	plat := &platform.PlatformConfig{
		// TLS must be enabled for the overlay to apply; with no file-kind CA
		// sources the PEM bundle is a no-op, so only the extra volume/env land.
		TLS: platform.TLSConfig{Enabled: true},
		Volumes: []platform.VolumeSpec{{
			Name:   "extra",
			Source: platform.VolumeSource{Kind: "hostPath", HostPath: &platform.HostPathSource{Path: "/host/extra"}},
			Mounts: []platform.MountSpec{{MountPath: "/mnt/extra"}},
		}},
		Env: map[string]string{"FDF2_ENV": "on"},
	}

	e := NewDockerExecutor(
		Config{Mode: "docker", InstanceID: "fdf2", Network: "jaiscloud-net"},
		testProfile{}, plat, WithDockerClient(client),
	)
	defer e.Close()

	if _, _, _, _, err := e.startContainer(context.Background(), Request{FunctionName: "fn"}, "img:1", 9100); err != nil {
		t.Fatalf("startContainer: %v", err)
	}
	if fake.createBody == nil {
		t.Fatal("no container create body captured")
	}

	env, _ := fake.createBody["Env"].([]any)
	if !containsString(env, "FDF2_ENV=on") {
		t.Fatalf("platform env missing from create body Env: %v", env)
	}

	hostCfg, _ := fake.createBody["HostConfig"].(map[string]any)
	binds, _ := hostCfg["Binds"].([]any)
	if !containsString(binds, "/host/extra:/mnt/extra:ro") {
		t.Fatalf("platform bind missing from HostConfig.Binds: %v", binds)
	}
}

func containsString(vals []any, want string) bool {
	for _, v := range vals {
		if s, ok := v.(string); ok && s == want {
			return true
		}
	}
	return false
}
