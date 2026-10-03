package gcs_grpc_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/storage"
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

func StorageGRPCClient(ctx context.Context) *storage.Client {
	host := os.Getenv("STORAGE_EMULATOR_HOST_GRPC")
	if host == "" {
		host = "localhost:8081"
	}
	host = stripScheme(host)
	client, err := storage.NewGRPCClient(ctx,
		option.WithEndpoint(host),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		storage.WithDisabledClientMetrics(),
	)
	if err != nil {
		panic("failed to create gRPC storage client: " + err.Error())
	}
	return client
}

func stripScheme(host string) string {
	if i := strings.Index(host, "://"); i >= 0 {
		return host[i+3:]
	}
	return host
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(randomBytes(4)))
}
