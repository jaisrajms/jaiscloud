//go:build gcp_differential

package gcpdifferential

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestGRPCGoldensAreClean applies the same credential/identifier guard as the
// REST goldens to the gRPC golden directory.
func TestGRPCGoldensAreClean(t *testing.T) {
	assertGoldensClean(t, grpcGoldenDir())
}

// TestGRPCGoldensAreMarked guards that every committed gRPC golden carries
// Transport="grpc", so a REST golden cannot be committed into the gRPC
// directory (which would silently replay HTTP against the gRPC listener).
func TestGRPCGoldensAreMarked(t *testing.T) {
	files := goldensOrSkip(t, grpcGoldenDir())
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(grpcGoldenDir(), f))
		if err != nil {
			t.Fatalf("read gRPC golden %s: %v", f, err)
		}
		var ex Exchange
		if err := json.Unmarshal(data, &ex); err != nil {
			t.Errorf("gRPC golden %s is not valid JSON: %v", f, err)
			continue
		}
		if ex.Transport != "grpc" {
			t.Errorf("gRPC golden %s transport = %q, want %q", f, ex.Transport, "grpc")
		}
	}
}

// TestGRPCGoldenManifest checks the gRPC manifest's op count and that it carries
// no project identifier.
func TestGRPCGoldenManifest(t *testing.T) {
	dir := grpcGoldenDir()
	files := goldensOrSkip(t, dir)
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("read gRPC manifest: %v", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse gRPC manifest: %v", err)
	}
	if m.Ops != len(files) {
		t.Errorf("gRPC manifest ops=%d but %d golden files present", m.Ops, len(files))
	}
	if m.Project != "<project>" {
		t.Errorf("gRPC manifest project %q must be the normalized placeholder <project>", m.Project)
	}
}
