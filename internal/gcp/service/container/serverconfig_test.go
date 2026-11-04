package container

import (
	"context"
	"strings"
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
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

// A create must honour the version the caller pinned — resolving the documented
// aliases and rejecting an unsupported release — instead of rewriting it to the
// mock's default. Real GKE never silently substitutes a different release.
func TestResolveClusterVersion(t *testing.T) {
	accepted := []string{"", "latest", "1", "1.30", "1.30.5", DefaultMasterVersion}
	for _, in := range accepted {
		got, err := ResolveClusterVersion(in)
		if err != nil {
			t.Fatalf("ResolveClusterVersion(%q): %v", in, err)
		}
		if got != DefaultMasterVersion {
			t.Fatalf("ResolveClusterVersion(%q) = %q, want %q", in, got, DefaultMasterVersion)
		}
	}
	for _, in := range []string{"1.29", "1.31", "2"} {
		_, err := ResolveClusterVersion(in)
		if err == nil {
			t.Fatalf("ResolveClusterVersion(%q) must reject an unsupported prefix", in)
		}
		if !strings.Contains(err.Error(), `No valid versions with the prefix "`+in+`" found`) {
			t.Fatalf("ResolveClusterVersion(%q) error = %v, want the real-GCP prefix message", in, err)
		}
	}
	for _, in := range []string{"1.29.7-gke.1000", "1.30.5-gke.9999999"} {
		_, err := ResolveClusterVersion(in)
		if err == nil {
			t.Fatalf("ResolveClusterVersion(%q) must reject an unsupported release", in)
		}
		if !strings.Contains(err.Error(), `Master version "`+in+`" is unsupported`) {
			t.Fatalf("ResolveClusterVersion(%q) error = %v, want the real-GCP unsupported-release message", in, err)
		}
	}
}

// CreateCluster must persist and read back the caller's pinned version.
func TestCreateClusterHonoursInitialClusterVersion(t *testing.T) {
	svc := newTestService()
	ctx := context.Background()
	op, err := svc.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "pinned", InitialClusterVersion: "1.30"})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	_ = op
	c, err := svc.GetCluster(ctx, "p", "us-central1", "pinned")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.InitialClusterVersion != DefaultMasterVersion || c.CurrentMasterVersion != DefaultMasterVersion {
		t.Fatalf("pinned version not honoured: initial=%q current=%q", c.InitialClusterVersion, c.CurrentMasterVersion)
	}
	_, err = svc.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "bad", InitialClusterVersion: "1.29.7-gke.1000"})
	if err == nil {
		t.Fatal("a create pinned to an unsupported version must be rejected")
	}
}
