// Package sdk_firestore_test exercises the jaiscloud-gcp Firestore emulator
// through the official high-level cloud.google.com/go/firestore client over
// gRPC. This is the acceptance client for the Phase B gRPC transport.
//
// Run with the binary started and FIRESTORE_EMULATOR_HOST set to the gRPC port:
//
//	./jaiscloud-gcp start --port 8090 --grpc-port 8081 --ephemeral &
//	FIRESTORE_EMULATOR_HOST=localhost:8081 go test -race ./...
//
// This is a separate Go module so the heavy cloud.google.com/go/firestore
// dependency does not bloat the main module.
//
// The high-level client routes document reads (DocRef.Get, transaction reads)
// and queries through the streaming RPCs (BatchGetDocuments / RunQuery), so
// this test exercises those as well as the unary surface (Commit, ListDocuments,
// ListCollectionIds, BeginTransaction). The remaining unary RPCs
// (GetDocument/CreateDocument/UpdateDocument/DeleteDocument/BatchWrite/Rollback)
// are covered directly in firestore_unary_test.go.
package sdk_firestore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const projectID = "proj"

func newClient(t *testing.T) *firestore.Client {
	t.Helper()
	ctx := context.Background()
	client, err := firestore.NewClient(ctx, projectID, option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// docRefIDs lists the document IDs in a collection via ListDocuments (unary).
func docRefIDs(t *testing.T, ctx context.Context, col *firestore.CollectionRef) map[string]bool {
	t.Helper()
	ids := map[string]bool{}
	it := col.DocumentRefs(ctx)
	for {
		r, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		ids[r.ID] = true
	}
	return ids
}

func TestSDKFirestoreCRUD(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	col := client.Collection(unique("cities"))

	doc := col.Doc("SF")

	// Create (Commit with exists=false precondition).
	_, err := doc.Create(ctx, map[string]any{"name": "San Francisco", "population": 800000})
	require.NoError(t, err)

	// Update (Commit with update mask).
	_, err = doc.Update(ctx, []firestore.Update{{Path: "population", Value: 900000}})
	require.NoError(t, err)

	// List documents (ListDocuments — unary): the doc must be present.
	require.True(t, docRefIDs(t, ctx, col)["SF"], "document should be listed via ListDocuments")

	// List collection IDs (ListCollectionIds — unary): the collection must be
	// present.
	var foundColl bool
	colls := client.Collections(ctx)
	for {
		c, err := colls.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		if c.ID == col.ID {
			foundColl = true
		}
	}
	require.True(t, foundColl, "collection should be listed via ListCollectionIds")

	// Delete (Commit with delete write).
	_, err = doc.Delete(ctx)
	require.NoError(t, err)

	// The document must be gone from the ListDocuments result.
	require.False(t, docRefIDs(t, ctx, col)["SF"], "document should be gone after delete")
}

func TestSDKFirestoreTransaction(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	col := client.Collection(unique("counters"))

	// Write-only transaction: BeginTransaction + Commit(transaction). The read
	// path (BatchGetDocuments) is streaming and deferred, so the transaction
	// work here only writes.
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := tx.Set(col.Doc("a"), map[string]any{"value": 1}); err != nil {
			return err
		}
		return tx.Set(col.Doc("b"), map[string]any{"value": 2})
	})
	require.NoError(t, err)

	ids := docRefIDs(t, ctx, col)
	require.True(t, ids["a"], "transaction write a should be committed")
	require.True(t, ids["b"], "transaction write b should be committed")
}

func TestSDKFirestoreReadsQueriesTxn(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	col := client.Collection(unique("todos"))

	// Seed documents.
	_, err := col.Doc("a").Set(ctx, map[string]any{"done": false, "prio": 1})
	require.NoError(t, err)
	_, err = col.Doc("b").Set(ctx, map[string]any{"done": false, "prio": 2})
	require.NoError(t, err)
	_, err = col.Doc("c").Set(ctx, map[string]any{"done": true, "prio": 3})
	require.NoError(t, err)

	// DocRef.Get — single-document read via BatchGetDocuments (streaming).
	snap, err := col.Doc("a").Get(ctx)
	require.NoError(t, err)
	require.True(t, snap.Exists())
	require.Equal(t, false, snap.Data()["done"])

	// Query — Where + Limit via RunQuery (streaming).
	docs, err := col.Where("done", "==", false).Limit(10).Documents(ctx).GetAll()
	require.NoError(t, err)
	require.Len(t, docs, 2)

	// Read-write transaction: BeginTransaction + BatchGetDocuments read +
	// Commit. Reads doc "a" and writes doc "d" based on its value.
	err = client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		ds, err := tx.Get(col.Doc("a"))
		if err != nil {
			return err
		}
		if ds.Data()["done"] == true {
			return nil
		}
		return tx.Set(col.Doc("d"), map[string]any{"done": true, "prio": 0})
	})
	require.NoError(t, err)

	// The transaction write must be visible on a subsequent read.
	snap, err = col.Doc("d").Get(ctx)
	require.NoError(t, err)
	require.True(t, snap.Exists())
}

// TestSDKFirestoreCursorsAndUpdateMissing covers the Java-compat gaps J17/R12
// (startAfter/endBefore exclusivity) and J18/R13 (update on a missing document
// → NOT_FOUND) through the official Go client.
func TestSDKFirestoreCursorsAndUpdateMissing(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	col := client.Collection(unique("cursors"))

	for _, age := range []int64{10, 20, 30, 40} {
		_, err := col.Doc(fmt.Sprintf("doc-%d", age)).Set(ctx, map[string]any{"age": age})
		require.NoError(t, err)
	}

	ages := func(q firestore.Query) []int64 {
		docs, err := q.Documents(ctx).GetAll()
		require.NoError(t, err)
		out := make([]int64, len(docs))
		for i, d := range docs {
			out[i] = d.Data()["age"].(int64)
		}
		return out
	}

	// startAfter is exclusive.
	require.Equal(t, []int64{30, 40}, ages(col.OrderBy("age", firestore.Asc).StartAfter(int64(20))))
	// startAt/endAt are inclusive.
	require.Equal(t, []int64{20, 30}, ages(col.OrderBy("age", firestore.Asc).StartAt(int64(20)).EndAt(int64(30))))
	// endBefore is exclusive.
	require.Equal(t, []int64{10, 20}, ages(col.OrderBy("age", firestore.Asc).EndBefore(int64(30))))
	// Descending startAfter.
	require.Equal(t, []int64{20, 10}, ages(col.OrderBy("age", firestore.Desc).StartAfter(int64(30))))

	// Update on a missing document must fail with NOT_FOUND, not an upsert.
	_, err := col.Doc("does-not-exist").Update(ctx, []firestore.Update{{Path: "age", Value: int64(1)}})
	require.Error(t, err)
	require.ErrorContains(t, err, "No document to update")

	// The failed update must not have created the document.
	require.False(t, docRefIDs(t, ctx, col)["does-not-exist"])
}

// TestSDKFirestoreDeletePrecondition covers J40 through the official Go client:
// deleting a missing document guarded by firestore.Exists is NOT_FOUND
// ("No document to update"), and a create (currentDocument.exists=false) of an
// already-present document is ALREADY_EXISTS.
func TestSDKFirestoreDeletePrecondition(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	col := client.Collection(unique("deleteprecond"))

	// Delete of a missing document guarded by Exists → NOT_FOUND.
	_, err := col.Doc("missing").Delete(ctx, firestore.Exists)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.ErrorContains(t, err, "No document to update")

	// Create an existing document (exists=false precondition) → ALREADY_EXISTS.
	doc := col.Doc("present")
	_, err = doc.Create(ctx, map[string]any{"v": int64(1)})
	require.NoError(t, err)
	_, err = doc.Create(ctx, map[string]any{"v": int64(2)})
	require.Error(t, err)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.ErrorContains(t, err, "already exists")

	// The plain delete of a present document still succeeds.
	_, err = doc.Delete(ctx)
	require.NoError(t, err)
	require.False(t, docRefIDs(t, ctx, col)["present"])
}

// TestSDKFirestoreSnapshots exercises the high-level Snapshots listener. It
// verifies the emulator emits a TargetChange NO_CHANGE frame after CURRENT so
// the SDK concludes a snapshot instead of hanging, and that subsequent writes
// surface as real-time snapshot updates.
func TestSDKFirestoreSnapshots(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	col := client.Collection(unique("snap"))

	_, _, err := col.Add(ctx, map[string]any{"n": int64(1)})
	require.NoError(t, err)

	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	it := col.Snapshots(sctx)
	defer it.Stop()

	// Initial snapshot must be delivered promptly (without NO_CHANGE the SDK
	// never concludes a snapshot and Next() blocks until the context deadline).
	snap, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, 1, snap.Size)

	// A subsequent write must surface as a real-time snapshot update.
	_, _, err = col.Add(ctx, map[string]any{"n": int64(2)})
	require.NoError(t, err)
	snap, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, 2, snap.Size)
}
