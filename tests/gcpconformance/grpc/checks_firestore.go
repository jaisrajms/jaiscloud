package grpcconformance

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/firestore"
)

// firestoreChecks covers the Firestore document surface
// (google.firestore.v1.Firestore) via the official high-level client. Doc.Set
// and Doc.Delete exercise Commit; Doc.Get exercises the streaming
// BatchGetDocuments RPC.
func firestoreChecks() []Check {
	return []Check{
		{Service: "firestore", RPC: "Doc.Set (Commit)", Method: "Commit", KeyField: "success", Run: checkFirestoreSet},
		{Service: "firestore", RPC: "Doc.Get (BatchGetDocuments)", Method: "BatchGetDocuments", KeyField: "field name", Run: checkFirestoreGet},
		{Service: "firestore", RPC: "Doc.Delete (Commit)", Method: "Commit", KeyField: "success", Run: checkFirestoreDelete},
	}
}

const (
	firestoreDocValue = "grpc-conformance"
	firestoreDocID    = "doc1"
)

// newFirestoreClient points the official client at the emulator via its
// first-class FIRESTORE_EMULATOR_HOST hook, which installs the insecure
// transport and emulator credentials for us.
func newFirestoreClient(ctx context.Context, cfg Config) (*firestore.Client, error) {
	if err := os.Setenv("FIRESTORE_EMULATOR_HOST", cfg.GRPCAddr()); err != nil {
		return nil, fmt.Errorf("set FIRESTORE_EMULATOR_HOST: %w", err)
	}
	return firestore.NewClient(ctx, cfg.Project)
}

func firestoreDocRef(client *firestore.Client, cfg Config) *firestore.DocumentRef {
	return client.Collection("gcpc_grpc_" + cfg.Suffix).Doc(firestoreDocID)
}

func checkFirestoreSet(ctx context.Context, cfg Config) error {
	client, err := newFirestoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	_, err = firestoreDocRef(client, cfg).Set(ctx, map[string]any{
		"name": firestoreDocValue,
		"ok":   true,
	})
	return err
}

func checkFirestoreGet(ctx context.Context, cfg Config) error {
	client, err := newFirestoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	snap, err := firestoreDocRef(client, cfg).Get(ctx)
	if err != nil {
		return err
	}
	if !snap.Exists() {
		return fmt.Errorf("Doc.Get reported the document does not exist")
	}
	if got, _ := snap.Data()["name"].(string); got != firestoreDocValue {
		return fmt.Errorf("Doc.Get field name = %q, want %q", got, firestoreDocValue)
	}
	return nil
}

func checkFirestoreDelete(ctx context.Context, cfg Config) error {
	client, err := newFirestoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	_, err = firestoreDocRef(client, cfg).Delete(ctx)
	return err
}
