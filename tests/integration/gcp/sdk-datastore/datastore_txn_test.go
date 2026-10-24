// Package datastore_test — transaction and query-shape depth. The base suite
// covers CRUD, cursors, ancestors, namespaces and named databases; this file adds
// the official client's read-modify-write transaction path (RunInTransaction,
// including rollback-on-error) and the no-lost-update guarantee at the wire
// level, plus keys-only / projection / ordered-page query shapes.
package datastore_test

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/datastore"
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestDatastoreTransactions exercises the official client's transactional
// read-modify-write path: a successful RunInTransaction commits atomically and is
// visible afterwards, while returning an error rolls the mutation back.
func TestDatastoreTransactions(t *testing.T) {
	ctx := context.Background()
	client := DatastoreClient(ctx)
	defer client.Close()

	suffix := uniqueName("ds-txn")
	key := datastore.NameKey("Task", "txn-"+suffix, nil)
	t.Cleanup(func() { client.Delete(ctx, key) })

	// Two sequential read-modify-write transactions must both land (the
	// second must observe the first's committed value).
	for want := 1; want <= 2; want++ {
		_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			var task Task
			if err := tx.Get(key, &task); err != nil && err != datastore.ErrNoSuchEntity {
				return err
			}
			task.Priority++
			_, err := tx.Put(key, &task)
			return err
		})
		require.NoError(t, err)

		var got Task
		require.NoError(t, client.Get(ctx, key, &got))
		assert.Equal(t, want, got.Priority, "transaction %d must have committed", want)
	}

	// A transaction that returns an error is rolled back: the mutation must
	// not be visible.
	sentinel := errors.New("abort this transaction")
	_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var task Task
		if err := tx.Get(key, &task); err != nil {
			return err
		}
		task.Description = "should-not-commit"
		if _, err := tx.Put(key, &task); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	var after Task
	require.NoError(t, client.Get(ctx, key, &after))
	assert.Empty(t, after.Description, "a rolled-back transaction must not apply")
}

// TestDatastoreQueryShapes covers the documented query result shapes through the
// official client: keys-only (no entity data), projection (only projected
// fields), and an ordered page with limit+offset.
func TestDatastoreQueryShapes(t *testing.T) {
	ctx := context.Background()
	client := DatastoreClient(ctx)
	defer client.Close()

	kind := "ShapeTask-" + uniqueName("ds")
	names := []string{"a", "b", "c", "d"}
	for i, n := range names {
		_, err := client.Put(ctx, datastore.NameKey(kind, n, nil), &Task{Description: n, Priority: i})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, n := range names {
			client.Delete(ctx, datastore.NameKey(kind, n, nil))
		}
	})

	// Keys-only: keys come back, entity data does not.
	keys, err := client.GetAll(ctx, datastore.NewQuery(kind).KeysOnly(), nil)
	require.NoError(t, err)
	require.Len(t, keys, len(names))

	// Projection: only the projected field is populated.
	var projected []Task
	_, err = client.GetAll(ctx, datastore.NewQuery(kind).Project("Description"), &projected)
	require.NoError(t, err)
	require.Len(t, projected, len(names))
	for _, task := range projected {
		assert.NotEmpty(t, task.Description, "projected field must be present")
		assert.Equal(t, 0, task.Priority, "un-projected field must be zero")
	}

	// Ordered page: priority ascending, offset 1, limit 2 → b, c.
	var page []Task
	_, err = client.GetAll(ctx, datastore.NewQuery(kind).Order("Priority").Offset(1).Limit(2), &page)
	require.NoError(t, err)
	require.Len(t, page, 2)
	assert.Equal(t, []int{1, 2}, []int{page[0].Priority, page[1].Priority})
}

// TestDatastoreBatchMutations covers GetMulti/PutMulti through the client's
// batched (non-transactional) path.
func TestDatastoreBatchMutations(t *testing.T) {
	ctx := context.Background()
	client := DatastoreClient(ctx)
	defer client.Close()

	suffix := uniqueName("ds-batch")
	keys := []*datastore.Key{
		datastore.NameKey("Task", "batch-a-"+suffix, nil),
		datastore.NameKey("Task", "batch-b-"+suffix, nil),
	}
	t.Cleanup(func() { client.DeleteMulti(ctx, keys) })

	tasks := []*Task{{Description: "A", Priority: 1}, {Description: "B", Priority: 2}}
	_, err := client.PutMulti(ctx, keys, tasks)
	require.NoError(t, err)

	got := make([]Task, len(keys))
	require.NoError(t, client.GetMulti(ctx, keys, got))
	assert.Equal(t, "A", got[0].Description)
	assert.Equal(t, "B", got[1].Description)
}

// TestDatastoreTransactionConflictAborts is the wire-level no-lost-update
// guarantee: a transaction that read an entity, then commits after a concurrent
// writer changed it, is ABORTED and applies nothing. Driven through the generated
// client because the high-level Go client hides the raw read-set commit.
func TestDatastoreTransactionConflictAborts(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.NewClient(emulatorAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	c := datastorepb.NewDatastoreClient(conn)

	project := ProjectID()
	id := "conflict-" + uniqueName("ds")
	key := &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: project},
		Path:        []*datastorepb.Key_PathElement{{Kind: "Task", IdType: &datastorepb.Key_PathElement_Name{Name: id}}},
	}
	entity := func(n int64) *datastorepb.Entity {
		return &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{
			"Priority": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}},
		}}
	}
	readOpts := func(txn []byte) *datastorepb.ReadOptions {
		return &datastorepb.ReadOptions{ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: txn}}
	}

	// Seed version 1.
	_, err = c.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: project,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: entity(1)}}},
	})
	require.NoError(t, err)

	begin, err := c.BeginTransaction(ctx, &datastorepb.BeginTransactionRequest{ProjectId: project})
	require.NoError(t, err)
	txn := begin.GetTransaction()

	// Transactional read records version 1 in the read-set.
	lookup, err := c.Lookup(ctx, &datastorepb.LookupRequest{
		ProjectId: project, Keys: []*datastorepb.Key{key}, ReadOptions: readOpts(txn),
	})
	require.NoError(t, err)
	require.Len(t, lookup.GetFound(), 1)

	// A concurrent non-transactional writer bumps the entity after the read.
	_, err = c.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: project,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: entity(2)}}},
	})
	require.NoError(t, err)

	// The transactional commit now conflicts → ABORTED.
	_, err = c.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId:           project,
		Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
		TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: txn},
		Mutations:           []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Update{Update: entity(99)}}},
	})
	require.Equal(t, codes.Aborted, status.Code(err), "err=%v", err)

	// The winner's value survived; the aborted write did not.
	after, err := c.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: project, Keys: []*datastorepb.Key{key}})
	require.NoError(t, err)
	require.Len(t, after.GetFound(), 1)
	require.EqualValues(t, 2, after.GetFound()[0].GetEntity().GetProperties()["Priority"].GetIntegerValue())

	// Cleanup.
	_, err = c.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: project,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: key}}},
	})
	require.NoError(t, err)
}
