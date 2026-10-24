// depth_test.go closes DC7 (plan_docs/gcp-depth-closure-wave-plan.md W2.5): the
// datastore p3 cell. The base suite already asserts p2 (transaction conflict →
// ABORTED, no lost update), p4 (cursor paging) and p5 (batch multi-entity
// boundary); p3 — the official client's typed error envelope on the documented
// negatives — was the only cell left exempt, so this file asserts it through the
// generated Datastore v1 client (the only official client that can drive a raw
// insert/update/transactional commit) and the shared depth-test kit's
// StatusOf/AssertError catalogue (tests/integration/gcp/testdepth, DC2).
//
// The negatives, all real-Datastore documented behaviours (Firestore in
// Datastore mode, "Errors and Error Handling"):
//   - an incomplete Lookup key ("Key path is incomplete") and a negative query
//     offset are INVALID_ARGUMENT;
//   - a commit insert on a key that already exists is ALREADY_EXISTS;
//   - an update mutation on an entity that does not exist is NOT_FOUND;
//   - a transactional commit whose read-set was modified concurrently is
//     ABORTED (the no-lost-update guarantee the p2 suite pins, asserted here
//     through the same envelope catalogue).
//
// The update-missing negatives (DC18) cover both commit paths: the
// non-transactional one and the transactional one. (That a transactional miss
// also terminates the transaction handle is asserted at the core level in
// internal/gcp/service/datastore/txn_test.go.)
package datastore_test

import (
	"context"
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	testdepth "jaiscloud-gcp-testdepth"
)

// depthKey builds a complete Task key name in the emulator's default partition.
func depthKey(project, name string) *datastorepb.Key {
	return &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: project},
		Path:        []*datastorepb.Key_PathElement{{Kind: "Task", IdType: &datastorepb.Key_PathElement_Name{Name: name}}},
	}
}

// depthEntity wraps an integer Priority property on the given key.
func depthEntity(key *datastorepb.Key, n int64) *datastorepb.Entity {
	return &datastorepb.Entity{Key: key, Properties: map[string]*datastorepb.Value{
		"Priority": {ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}},
	}}
}

// TestDatastoreDepthErrors is datastore p3: the generated client's typed error
// envelope on the documented negatives.
func TestDatastoreDepthErrors(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.NewClient(emulatorAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	c := datastorepb.NewDatastoreClient(conn)

	project := ProjectID()

	// A seeded, existing entity for the ALREADY_EXISTS case.
	existing := depthKey(project, "deptherr-"+uniqueName("ds"))
	_, err = c.Commit(ctx, &datastorepb.CommitRequest{
		ProjectId: project,
		Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
		Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: depthEntity(existing, 1)}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = c.Commit(ctx, &datastorepb.CommitRequest{
			ProjectId: project,
			Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: existing}}},
		})
	})

	// An incomplete key: a kind with neither an id nor a name.
	incomplete := &datastorepb.Key{
		PartitionId: &datastorepb.PartitionId{ProjectId: project},
		Path:        []*datastorepb.Key_PathElement{{Kind: "Task"}},
	}

	testdepth.RunNegatives(t, []testdepth.NegativeCase{
		{
			Name: "IncompleteLookupKeyIsInvalidArgument",
			Want: testdepth.SpecInvalidArgument(),
			Run: func() error {
				_, err := c.Lookup(ctx, &datastorepb.LookupRequest{ProjectId: project, Keys: []*datastorepb.Key{incomplete}})
				return err
			},
		},
		{
			Name: "NegativeQueryOffsetIsInvalidArgument",
			Want: testdepth.SpecInvalidArgument(),
			Run: func() error {
				_, err := c.RunQuery(ctx, &datastorepb.RunQueryRequest{
					ProjectId: project,
					QueryType: &datastorepb.RunQueryRequest_Query{Query: &datastorepb.Query{
						Kind:   []*datastorepb.KindExpression{{Name: "Task"}},
						Offset: -1,
					}},
				})
				return err
			},
		},
		{
			Name: "InsertExistingKeyIsAlreadyExists",
			Want: testdepth.SpecAlreadyExists(),
			Run: func() error {
				_, err := c.Commit(ctx, &datastorepb.CommitRequest{
					ProjectId: project,
					Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
					Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Insert{Insert: depthEntity(existing, 2)}}},
				})
				return err
			},
		},
		{
			Name: "UpdateMissingEntityIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				_, err := c.Commit(ctx, &datastorepb.CommitRequest{
					ProjectId: project,
					Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
					Mutations: []*datastorepb.Mutation{{
						Operation: &datastorepb.Mutation_Update{Update: depthEntity(depthKey(project, "deptherrmissing-"+uniqueName("ds")), 1)},
					}},
				})
				return err
			},
		},
		{
			Name: "TransactionalUpdateMissingEntityIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				begin, err := c.BeginTransaction(ctx, &datastorepb.BeginTransactionRequest{ProjectId: project})
				if err != nil {
					return err
				}
				_, err = c.Commit(ctx, &datastorepb.CommitRequest{
					ProjectId:           project,
					Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
					TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: begin.GetTransaction()},
					Mutations: []*datastorepb.Mutation{{
						Operation: &datastorepb.Mutation_Update{Update: depthEntity(depthKey(project, "deptherrmissing-"+uniqueName("ds")), 1)},
					}},
				})
				return err
			},
		},
		{
			Name: "TransactionConflictIsAborted",
			Want: testdepth.SpecAborted(),
			Run: func() error {
				key := depthKey(project, "deptherabort-"+uniqueName("ds"))
				// Seed version 1 (upsert is idempotent).
				if _, err := c.Commit(ctx, &datastorepb.CommitRequest{
					ProjectId: project,
					Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
					Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: depthEntity(key, 1)}}},
				}); err != nil {
					return err
				}
				t.Cleanup(func() {
					_, _ = c.Commit(ctx, &datastorepb.CommitRequest{
						ProjectId: project,
						Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Delete{Delete: key}}},
					})
				})

				begin, err := c.BeginTransaction(ctx, &datastorepb.BeginTransactionRequest{ProjectId: project})
				if err != nil {
					return err
				}
				txn := begin.GetTransaction()

				// The transactional read records version 1 in the read-set.
				if _, err := c.Lookup(ctx, &datastorepb.LookupRequest{
					ProjectId: project,
					Keys:      []*datastorepb.Key{key},
					ReadOptions: &datastorepb.ReadOptions{
						ConsistencyType: &datastorepb.ReadOptions_Transaction{Transaction: txn},
					},
				}); err != nil {
					return err
				}

				// A concurrent non-transactional writer bumps the entity.
				if _, err := c.Commit(ctx, &datastorepb.CommitRequest{
					ProjectId: project,
					Mode:      datastorepb.CommitRequest_NON_TRANSACTIONAL,
					Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Upsert{Upsert: depthEntity(key, 2)}}},
				}); err != nil {
					return err
				}

				_, err = c.Commit(ctx, &datastorepb.CommitRequest{
					ProjectId:           project,
					Mode:                datastorepb.CommitRequest_TRANSACTIONAL,
					TransactionSelector: &datastorepb.CommitRequest_Transaction{Transaction: txn},
					Mutations:           []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Update{Update: depthEntity(key, 99)}}},
				})
				return err
			},
		},
	})
}
