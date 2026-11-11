package firestoreadmin_test

import (
	"context"
	"testing"

	gcloud "cloud.google.com/go/firestore"
	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// newDatabaseClient returns the official high-level Firestore data client bound
// to one named database. The admin suite owns the database/index setup, so the
// data client reuses the emulator's gRPC listener.
func newDatabaseClient(t *testing.T, databaseID string) *gcloud.Client {
	t.Helper()
	conn, err := grpc.NewClient(emulatorAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client, err := gcloud.NewClientWithDatabase(context.Background(), project, databaseID,
		option.WithGRPCConn(conn), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

// TestSDKFirestoreAdminUniqueIndexEnforcement is the FE2 official-client check of
// the enforcement data path: on an Enterprise database a unique index rejects a
// duplicate unique-index entry on create, update and commit with ALREADY_EXISTS
// (the write-side behaviour of the index surface FE1 models), while a commit
// that deletes the holder and reuses the value in the same batch succeeds.
func TestSDKFirestoreAdminUniqueIndexEnforcement(t *testing.T) {
	ctx := context.Background()
	c := newAdmin(t)

	dbID := unique("uniqent")
	op, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     projectName(),
		DatabaseId: dbID,
		Database: &adminpb.Database{
			LocationId:      "us-central1",
			Type:            adminpb.Database_FIRESTORE_NATIVE,
			DatabaseEdition: adminpb.Database_ENTERPRISE,
		},
	})
	require.NoError(t, err)
	_, err = op.Wait(ctx)
	require.NoError(t, err)
	deleteDatabaseOnCleanup(t, c, dbID)

	parent := collectionGroupParent(dbID, "users")
	createOp, err := c.CreateIndex(ctx, &adminpb.CreateIndexRequest{
		Parent: parent,
		Index: &adminpb.Index{
			QueryScope: adminpb.Index_COLLECTION_GROUP,
			Density:    adminpb.Index_DENSE,
			Unique:     true,
			Fields: []*adminpb.Index_IndexField{{
				FieldPath: "email",
				ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING},
			}},
		},
	})
	require.NoError(t, err)
	created, err := createOp.Wait(ctx)
	require.NoError(t, err)
	require.True(t, created.GetUnique())
	deleteIndexOnCleanup(t, c, created.GetName())

	data := newDatabaseClient(t, dbID)
	users := data.Collection("users")

	_, err = users.Doc("alice").Create(ctx, map[string]any{"email": "alice@example.com"})
	require.NoError(t, err)
	_, err = users.Doc("bob").Create(ctx, map[string]any{"email": "alice@example.com"})
	require.Equal(t, codes.AlreadyExists, status.Code(err), "duplicate create = %v, want AlreadyExists", err)
	_, err = users.Doc("carol").Set(ctx, map[string]any{"email": "alice@example.com"})
	require.Equal(t, codes.AlreadyExists, status.Code(err), "duplicate set = %v, want AlreadyExists", err)

	// Freeing the value and reusing it in one commit is legal: the batch is
	// validated against its post-state, not document-by-document.
	batch := data.Batch()
	batch.Delete(users.Doc("alice"))
	batch.Create(users.Doc("dave"), map[string]any{"email": "alice@example.com"})
	_, err = batch.Commit(ctx)
	require.NoError(t, err)

	_, err = users.Doc("alice").Get(ctx)
	require.Equal(t, codes.NotFound, status.Code(err), "alice after the swap commit = %v, want NotFound", err)
	snap, err := users.Doc("dave").Get(ctx)
	require.NoError(t, err)
	require.Equal(t, "alice@example.com", snap.Data()["email"])
}
