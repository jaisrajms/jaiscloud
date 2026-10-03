package datastore_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"cloud.google.com/go/datastore"
)

func ProjectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "test-project"
}

func DatastoreClient(ctx context.Context) *datastore.Client {
	client, err := datastore.NewClient(ctx, ProjectID())
	if err != nil {
		panic("failed to create datastore client: " + err.Error())
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
