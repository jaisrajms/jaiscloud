// Package grpcconformance drives the jaiscloud-gcp emulator's gRPC endpoint
// with the OFFICIAL Google client libraries and reports, per service/RPC,
// whether the call succeeded, failed, or returned codes.Unimplemented.
//
// This is the gRPC analogue of the REST wire-conformance harness in
// tests/gcpconformance/. It intentionally lives in its own Go module because
// the official clients are heavy dependencies that the repo keeps out of the
// main module (see tests/integration/gcp/sdk-*).
package grpcconformance

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	// DefaultEndpoint is the emulator's default gRPC listener.
	DefaultEndpoint = "localhost:8081"
	// DefaultRESTEndpoint is the emulator's default REST listener. The
	// Workflow Executions probes use it to deploy their prerequisite workflow
	// (the Workflows management API is REST-only).
	DefaultRESTEndpoint = "http://localhost:8080"
	// DefaultProject mirrors internal/config's default GCP project.
	DefaultProject = "jaiscloud-project"

	endpointEnv     = "GCP_EMULATOR_ENDPOINT_GRPC"
	restEndpointEnv = "GCP_EMULATOR_ENDPOINT_REST"
	projectEnv      = "GCP_EMULATOR_PROJECT"
)

// Config is the resolved per-run emulator connection + naming context.
type Config struct {
	// Endpoint is the gRPC host:port the emulator listens on (no scheme).
	Endpoint string
	// RESTEndpoint is the REST base URL (with scheme) of the emulator, used by
	// probes that must deploy a prerequisite over the management API.
	RESTEndpoint string
	// Project is the GCP project all resources are created under.
	Project string
	// Suffix is a per-run unique token appended to every resource name so
	// repeated runs against a long-lived emulator stay idempotent.
	Suffix string
}

// ConfigFromEnv builds a Config from the environment, falling back to the
// emulator defaults. A scheme on the endpoint (e.g. http://host:port) is
// stripped so the value is safe to hand to option.WithEndpoint.
func ConfigFromEnv() Config {
	endpoint := strings.TrimSpace(os.Getenv(endpointEnv))
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	restEndpoint := strings.TrimSpace(os.Getenv(restEndpointEnv))
	if restEndpoint == "" {
		restEndpoint = DefaultRESTEndpoint
	}
	project := strings.TrimSpace(os.Getenv(projectEnv))
	if project == "" {
		project = DefaultProject
	}
	return Config{
		Endpoint:     stripScheme(endpoint),
		RESTEndpoint: restEndpoint,
		Project:      project,
		Suffix:       runSuffix(),
	}
}

// GRPCAddr is the endpoint in host:port form accepted by option.WithEndpoint
// and the client-library emulator hooks.
func (c Config) GRPCAddr() string { return stripScheme(c.Endpoint) }

// ResourceName builds a run-unique resource name with the given prefix.
func (c Config) ResourceName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, c.Suffix)
}

// runSuffix returns a per-run unique suffix (mirrors runSuffix in
// tests/gcpconformance/scenarios.go) so resource names never collide across
// repeated runs and every run is self-cleaning.
func runSuffix() string {
	return fmt.Sprintf("%06x%06x", os.Getpid()&0xffffff, time.Now().UnixNano()&0xffffff)
}

func stripScheme(host string) string {
	if i := strings.Index(host, "://"); i >= 0 {
		return host[i+3:]
	}
	return host
}
