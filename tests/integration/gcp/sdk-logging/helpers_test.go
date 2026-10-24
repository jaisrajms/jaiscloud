package logging_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	logging "cloud.google.com/go/logging/apiv2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func ProjectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "test-project"
}

func LoggingClient(ctx context.Context) *logging.Client {
	host := os.Getenv("LOGGING_EMULATOR_HOST")
	if host == "" {
		host = "localhost:8081"
	}
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("failed to create gRPC connection: " + err.Error())
	}
	client, err := logging.NewClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		panic("failed to create logging client: " + err.Error())
	}
	return client
}

// grpcConn dials the emulator's gRPC endpoint (LOGGING_EMULATOR_HOST). The
// caller owns the returned connection and must Close it.
func grpcConn() *grpc.ClientConn {
	host := os.Getenv("LOGGING_EMULATOR_HOST")
	if host == "" {
		host = "localhost:8081"
	}
	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic("failed to create gRPC connection: " + err.Error())
	}
	return conn
}

// ConfigClient returns a ConfigServiceV2 client (sinks/exclusions) over the
// emulator's gRPC endpoint.
func ConfigClient(ctx context.Context) *logging.ConfigClient {
	client, err := logging.NewConfigClient(ctx, option.WithGRPCConn(grpcConn()))
	if err != nil {
		panic("failed to create config client: " + err.Error())
	}
	return client
}

// MetricsClient returns a MetricsServiceV2 client (logs-based metrics) over the
// emulator's gRPC endpoint.
func MetricsClient(ctx context.Context) *logging.MetricsClient {
	client, err := logging.NewMetricsClient(ctx, option.WithGRPCConn(grpcConn()))
	if err != nil {
		panic("failed to create metrics client: " + err.Error())
	}
	return client
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(randomBytes(4)))
}
