// Package grpc hosts the jaiscloud-gcp gRPC transport. The server is plaintext
// (h2c) for emulator-mode SDKs, which connect via FIRESTORE_EMULATOR_HOST with
// option.WithoutAuthentication(); it serves the health + reflection services and
// lets each GCP service register its own grpc.ServiceDesc.
package grpc

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// Server wraps a plaintext grpc.Server with the standard health and reflection
// services. Service handlers register themselves via Server.GRPC().
type Server struct {
	addr   string
	gsrv   *grpc.Server
	health *health.Server
}

// NewServer builds a plaintext gRPC server bound to addr (e.g. ":8081"). The
// empty health service is registered as SERVING by default.
func NewServer(addr string, opts ...grpc.ServerOption) *Server {
	h := health.NewServer()
	gsrv := grpc.NewServer(opts...)
	healthpb.RegisterHealthServer(gsrv, h)
	reflection.Register(gsrv)
	return &Server{addr: addr, gsrv: gsrv, health: h}
}

// GRPC returns the underlying *grpc.Server for service registration.
func (s *Server) GRPC() *grpc.Server { return s.gsrv }

// SetServingStatus sets the health status for a service (or "" for the server).
func (s *Server) SetServingStatus(service string, status healthpb.HealthCheckResponse_ServingStatus) {
	s.health.SetServingStatus(service, status)
}

// Serve listens on the configured address and blocks serving until Stop or the
// process exits.
func (s *Server) Serve() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc listen on %s: %w", s.addr, err)
	}
	return s.gsrv.Serve(ln)
}

// Stop gracefully stops the server.
func (s *Server) Stop() { s.gsrv.GracefulStop() }

// Reset clears per-server transport state. For M-grpc-0/1 there is no separate
// transport state (transactions live in the shared provider Service, reset via
// the provider's own Reset); the method exists so the admin handler can reset
// the transport uniformly as the streaming follow-ups add state.
func (s *Server) Reset(ctx context.Context) {}
