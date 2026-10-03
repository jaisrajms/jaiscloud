package grpcconformance

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Probe verifies the emulator's gRPC endpoint is reachable before running the
// suite. It dials with insecure credentials, forces a connection, and waits
// for the channel to reach READY. A non-nil error means the endpoint is not
// serving and the caller should skip rather than fail.
func Probe(ctx context.Context, cfg Config) error {
	conn, err := grpc.NewClient(cfg.GRPCAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", cfg.GRPCAddr(), err)
	}
	defer conn.Close()

	conn.Connect()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(waitCtx, state) {
			return fmt.Errorf("gRPC endpoint %s not ready (state %s)", cfg.GRPCAddr(), state)
		}
	}
}
