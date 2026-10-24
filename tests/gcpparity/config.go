//go:build gcp_parity

package gcpparity

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	// DefaultRESTEndpoint is the emulator's default REST listener.
	DefaultRESTEndpoint = "http://localhost:8080"
	// DefaultGRPCEndpoint is the emulator's default gRPC listener.
	DefaultGRPCEndpoint = "localhost:8081"
	// DefaultProject mirrors internal/config's default GCP project.
	DefaultProject = "jaiscloud-project"

	restEndpointEnv = "GCP_EMULATOR_ENDPOINT_REST"
	grpcEndpointEnv = "GCP_EMULATOR_ENDPOINT_GRPC"
	projectEnv      = "GCP_EMULATOR_PROJECT"
)

// Config is the resolved connection + naming context for one run. A per-run
// suffix keeps every created resource unique so repeated runs against a
// long-lived emulator stay idempotent and self-cleaning.
type Config struct {
	REST    string // REST base URL, with scheme and no trailing slash
	GRPC    string // gRPC host:port, no scheme
	Project string
	Suffix  string
}

// ConfigFromEnv builds a Config from the environment, falling back to the
// emulator defaults.
func ConfigFromEnv() Config {
	rest := strings.TrimRight(strings.TrimSpace(os.Getenv(restEndpointEnv)), "/")
	if rest == "" {
		rest = DefaultRESTEndpoint
	}
	grpc := stripScheme(strings.TrimSpace(os.Getenv(grpcEndpointEnv)))
	if grpc == "" {
		grpc = DefaultGRPCEndpoint
	}
	project := strings.TrimSpace(os.Getenv(projectEnv))
	if project == "" {
		project = DefaultProject
	}
	return Config{REST: rest, GRPC: grpc, Project: project, Suffix: runSuffix()}
}

// GRPCAddr is the endpoint in host:port form accepted by option.WithGRPCConn.
func (c Config) GRPCAddr() string { return stripScheme(c.GRPC) }

// ResourceName builds a run-unique resource id with the given prefix.
func (c Config) ResourceName(prefix string) string { return prefix + "-" + c.Suffix }

// ProjectPath is the emulator project as a canonical resource path.
func (c Config) ProjectPath() string { return "projects/" + c.Project }

func stripScheme(host string) string {
	if i := strings.Index(host, "://"); i >= 0 {
		return host[i+3:]
	}
	return host
}

// runSuffix returns a per-run unique, resource-name-safe suffix (mirrors the
// suffix generators in the REST and gRPC conformance harnesses).
func runSuffix() string {
	return fmt.Sprintf("%06x%06x", os.Getpid()&0xffffff, time.Now().UnixNano()&0xffffff)
}
