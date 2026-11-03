package container

import (
	"testing"
)

func TestGetServerConfigShape(t *testing.T) {
	cfg, err := newTestService().GetServerConfig("p", "us-central1")
	if err != nil {
		t.Fatalf("GetServerConfig: %v", err)
	}
	// Discovery ServerConfig: defaultClusterVersion, validMasterVersions,
	// validNodeVersions, defaultImageType, validImageTypes, channels. The
	// emulator supports exactly the version its clusters report.
	if cfg.DefaultClusterVersion != DefaultMasterVersion {
		t.Fatalf("defaultClusterVersion = %q, want %q", cfg.DefaultClusterVersion, DefaultMasterVersion)
	}
	if len(cfg.ValidMasterVersions) != 1 || cfg.ValidMasterVersions[0] != DefaultMasterVersion {
		t.Fatalf("validMasterVersions = %v", cfg.ValidMasterVersions)
	}
	if len(cfg.ValidNodeVersions) != 1 || cfg.ValidNodeVersions[0] != DefaultMasterVersion {
		t.Fatalf("validNodeVersions = %v", cfg.ValidNodeVersions)
	}
	if cfg.DefaultImageType != DefaultNodeImageType {
		t.Fatalf("defaultImageType = %q, want %q", cfg.DefaultImageType, DefaultNodeImageType)
	}
	// A pool created without nodeConfig carries DefaultNodeImageType, so the
	// image the server config advertises and the pool read must agree.
	if len(cfg.ValidImageTypes) == 0 || cfg.ValidImageTypes[0] != cfg.DefaultImageType {
		t.Fatalf("validImageTypes = %v, want the default image first", cfg.ValidImageTypes)
	}
	wantChannels := []string{"RAPID", "REGULAR", "STABLE", "EXTENDED"}
	if len(cfg.Channels) != len(wantChannels) {
		t.Fatalf("channels = %+v, want %v", cfg.Channels, wantChannels)
	}
	for i, want := range wantChannels {
		ch := cfg.Channels[i]
		if ch.Channel != want || ch.DefaultVersion != DefaultMasterVersion {
			t.Fatalf("channels[%d] = %+v, want channel %s default %s", i, ch, want, DefaultMasterVersion)
		}
		if len(ch.ValidVersions) != 1 || ch.ValidVersions[0] != DefaultMasterVersion {
			t.Fatalf("channels[%d].validVersions = %v", i, ch.ValidVersions)
		}
	}
}

// TestGetServerConfigReturnsIndependentCopies proves DefaultServerConfig hands
// each call its own slices, so a transport rendering a response can never mutate
// what the next caller sees.
func TestGetServerConfigReturnsIndependentCopies(t *testing.T) {
	svc := newTestService()
	first, err := svc.GetServerConfig("p", "us-central1")
	if err != nil {
		t.Fatalf("GetServerConfig: %v", err)
	}
	first.ValidImageTypes[0] = "MUTATED"
	first.Channels[0].Channel = "MUTATED"

	second, err := svc.GetServerConfig("p", "us-central1")
	if err != nil {
		t.Fatalf("GetServerConfig: %v", err)
	}
	if second.ValidImageTypes[0] != DefaultNodeImageType || second.Channels[0].Channel != "RAPID" {
		t.Fatalf("second response mutated: %+v", second)
	}
}

func TestGetServerConfigRequiresProjectAndLocation(t *testing.T) {
	svc := newTestService()
	_, err := svc.GetServerConfig("", "us-central1")
	providerErr(t, err, "InvalidArgument")
	_, err = svc.GetServerConfig("p", "")
	providerErr(t, err, "InvalidArgument")
}
