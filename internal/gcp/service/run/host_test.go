package run

import (
	"testing"

	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/store"
)

func TestInvocationHostConfig(t *testing.T) {
	old := DefaultInvocationHost()
	defer ConfigureInvocationHost(old)

	ConfigureInvocationHost(HostConfig{Scheme: "http", Suffix: "run.floci-gcp", Port: "4588"})

	want := "http://svc-" + projectToken("proj") + ".us-central1.run.floci-gcp:4588"
	if got := InvocationURL("proj", "us-central1", "svc"); got != want {
		t.Errorf("InvocationURL = %q, want %q", got, want)
	}
	if !IsInvocationHost("svc-abcdef012345.us-central1.run.floci-gcp:4588") {
		t.Error("configured host not detected")
	}
	if IsInvocationHost("svc-abcdef012345.us-central1.run.app") {
		t.Error("default suffix detected after reconfigure")
	}

	// The per-core override drives uri synthesis without touching the global.
	s := NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithInvocationHost(HostConfig{Scheme: "http", Suffix: "run.localhost", Port: "8080"}))
	host := s.ServiceAuthority("p", "l", "svc")
	if want := "svc-" + projectToken("p") + ".l.run.localhost:8080"; host != want {
		t.Errorf("ServiceAuthority = %q, want %q", host, want)
	}
	if got := s.uri("p", "l", "svc"); got != "http://"+host {
		t.Errorf("uri = %q", got)
	}
}

func TestNormalizeHostConfigDefaults(t *testing.T) {
	if got := normalizeHostConfig(HostConfig{}); got.Scheme != "https" || got.Suffix != DefaultURLSuffix || got.Port != "" {
		t.Errorf("empty config = %+v", got)
	}
	if got := normalizeHostConfig(HostConfig{Suffix: ".run.local", Port: ":9090"}); got.Suffix != "run.local" || got.Port != "9090" {
		t.Errorf("normalized = %+v", got)
	}
}
