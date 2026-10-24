package firestoreadmin_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	admin "cloud.google.com/go/firestore/apiv1/admin"
	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	testdepth "jaiscloud-gcp-testdepth"
)

// project is the project every Firestore Admin case drives. It matches the
// emulator's configured default (and the sibling sdk-rest/sdk-firestore
// suites); the Admin requests carry full "projects/{p}/..." names, so the
// handler resolves the project from the resource name rather than the gRPC
// metadata.
const project = "proj"

// emulatorAddr is the emulator's gRPC listener (the FirestoreAdmin wire service
// is served there, alongside the longrunning Operations used by the generated
// client).
func emulatorAddr() string {
	if h := os.Getenv("GCP_EMULATOR_ENDPOINT_GRPC"); h != "" {
		return h
	}
	return "localhost:8081"
}

// newAdmin dials the emulator and returns the official Firestore Admin gRPC
// client (google.firestore.admin.v1.FirestoreAdmin). A plaintext insecure
// connection with no credentials is the emulator convention (sdk-monitoring).
func newAdmin(t *testing.T) *admin.FirestoreAdminClient {
	t.Helper()
	conn, err := grpc.NewClient(emulatorAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	c, err := admin.NewFirestoreAdminClient(context.Background(),
		option.WithGRPCConn(conn), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

// unique returns a per-run lowercase suffix. Database ids are constrained to
// [a-z][a-z0-9-]{3,62}, so a dash-separated prefix-plus-nanos is always valid.
func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func projectName() string { return "projects/" + project }

func databaseName(id string) string { return projectName() + "/databases/" + id }

func collectionGroupParent(dbID, cg string) string {
	return databaseName(dbID) + "/collectionGroups/" + cg
}

func fieldName(dbID, cg, fieldPath string) string {
	return collectionGroupParent(dbID, cg) + "/fields/" + fieldPath
}

func backupParent(location string) string {
	return projectName() + "/locations/" + location
}

func backupName(location, id string) string {
	return backupParent(location) + "/backups/" + id
}

// compositeIndex builds a two-field composite index definition (the emulator,
// like real GCP, rejects a single-field composite index).
func compositeIndex(first, second string) *adminpb.Index {
	return &adminpb.Index{
		QueryScope: adminpb.Index_COLLECTION,
		Fields: []*adminpb.Index_IndexField{
			{FieldPath: first, ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING}},
			{FieldPath: second, ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_DESCENDING}},
		},
	}
}

// createDatabase creates a named database and returns the database the terminal
// operation carries. The create is registered for cleanup — a second delete of
// an already-deleted database reports NOT_FOUND and is ignored.
func createDatabase(t *testing.T, c *admin.FirestoreAdminClient, id string) *adminpb.Database {
	t.Helper()
	ctx := context.Background()
	op, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     projectName(),
		DatabaseId: id,
		Database: &adminpb.Database{
			LocationId: "us-central1",
			Type:       adminpb.Database_FIRESTORE_NATIVE,
		},
	})
	require.NoError(t, err)
	db, err := op.Wait(ctx)
	require.NoError(t, err)
	require.Equal(t, databaseName(id), db.GetName())
	deleteDatabaseOnCleanup(t, c, id)
	return db
}

// deleteDatabaseOnCleanup registers a best-effort delete of a database at the
// end of the test. The control plane is synchronous, so the operation is
// terminal; a record already deleted (or the implicit default) is ignored.
func deleteDatabaseOnCleanup(t *testing.T, c *admin.FirestoreAdminClient, id string) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = c.DeleteDatabase(context.Background(), &adminpb.DeleteDatabaseRequest{Name: databaseName(id)})
	})
}

// deleteIndexOnCleanup registers a best-effort delete of a composite index.
func deleteIndexOnCleanup(t *testing.T, c *admin.FirestoreAdminClient, name string) {
	t.Helper()
	t.Cleanup(func() {
		_ = c.DeleteIndex(context.Background(), &adminpb.DeleteIndexRequest{Name: name})
	})
}

// deleteUserCredsOnCleanup registers a best-effort delete of a user creds.
func deleteUserCredsOnCleanup(t *testing.T, c *admin.FirestoreAdminClient, name string) {
	t.Helper()
	t.Cleanup(func() {
		_ = c.DeleteUserCreds(context.Background(), &adminpb.DeleteUserCredsRequest{Name: name})
	})
}

// createIndex creates a composite index on a collection group and returns the
// stored index.
func createIndex(t *testing.T, c *admin.FirestoreAdminClient, parent, first, second string) *adminpb.Index {
	t.Helper()
	ctx := context.Background()
	op, err := c.CreateIndex(ctx, &adminpb.CreateIndexRequest{Parent: parent, Index: compositeIndex(first, second)})
	require.NoError(t, err)
	idx, err := op.Wait(ctx)
	require.NoError(t, err)
	return idx
}

// conflictStatus reports whether a losing writer observed the documented
// conflict (ALREADY_EXISTS over gRPC). It is the predicate the contention
// contract uses for a create-arbitered resource.
func conflictStatus(err error) bool {
	obs, ok := testdepth.StatusOf(err)
	return ok && obs.Kind == testdepth.KindGRPC && obs.Code == codes.AlreadyExists
}

// goneStatus reports whether a losing writer observed the documented
// NOT_FOUND: the delete of an already-gone resource is not idempotent in the
// Admin control plane.
func goneStatus(err error) bool {
	obs, ok := testdepth.StatusOf(err)
	return ok && obs.Kind == testdepth.KindGRPC && obs.Code == codes.NotFound
}
