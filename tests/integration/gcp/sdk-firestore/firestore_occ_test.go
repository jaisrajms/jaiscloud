// Package sdk_firestore_test — optimistic-concurrency-control depth. Firestore's
// correctness guarantees rest on OCC: an explicit `updateTime` precondition on a
// write, and a transaction's read-set validated at commit. Both are modelled by
// the emulator and exercised here through the generated gRPC client (the
// high-level SDK hides the raw transaction/precondition wiring).
package sdk_firestore_test

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// occDocID mints a unique document id so a re-run never collides with prior state.
func occDocID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func occDocName(id string) string { return dbName + "/documents/occ/" + id }

// TestFirestoreUpdateTimePrecondition drives the explicit `currentDocument.
// updateTime` compare-and-swap: a write carrying the document's current
// update_time is accepted (and advances the version); a write carrying the now
// stale update_time is rejected FAILED_PRECONDITION without touching the
// document.
func TestFirestoreUpdateTimePrecondition(t *testing.T) {
	ctx := context.Background()
	c := newRawClient(t)
	id := occDocID("uptime")
	name := occDocName(id)

	created, err := c.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       dbName + "/documents",
		CollectionId: "occ",
		DocumentId:   id,
		Document: &firestorepb.Document{Fields: map[string]*firestorepb.Value{
			"v": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 1}},
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, created.GetUpdateTime())
	stale := created.GetUpdateTime()

	update := func(v int64, pre *firestorepb.Precondition) (*firestorepb.CommitResponse, error) {
		return c.Commit(ctx, &firestorepb.CommitRequest{
			Database: dbName,
			Writes: []*firestorepb.Write{{
				Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
					Name: name,
					Fields: map[string]*firestorepb.Value{
						"v": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}},
					},
				}},
				CurrentDocument: pre,
			}},
		})
	}

	// Matching update_time → accepted, returns a fresh update_time.
	ok, err := update(2, &firestorepb.Precondition{
		ConditionType: &firestorepb.Precondition_UpdateTime{UpdateTime: stale},
	})
	require.NoError(t, err)
	require.Len(t, ok.GetWriteResults(), 1)
	require.NotNil(t, ok.GetWriteResults()[0].GetUpdateTime())

	// Stale update_time → FAILED_PRECONDITION; the document keeps v=2.
	_, err = update(3, &firestorepb.Precondition{
		ConditionType: &firestorepb.Precondition_UpdateTime{UpdateTime: stale},
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "err=%v", err)

	got, err := c.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	require.NoError(t, err)
	require.EqualValues(t, 2, got.GetFields()["v"].GetIntegerValue(), "rejected write must not apply")
}

// TestFirestoreTransactionReadConflictAborts pins the no-lost-update guarantee:
// a transaction that reads a document and then commits after a concurrent writer
// changed it is aborted (ABORTED — the code the SDK retries), and its write is
// discarded. The transaction token is single-use, exactly as real Firestore
// requires a fresh BeginTransaction to retry.
func TestFirestoreTransactionReadConflictAborts(t *testing.T) {
	ctx := context.Background()
	c := newRawClient(t)
	name := occDocName(occDocID("txn"))

	upsert := func(v int64) {
		_, err := c.Commit(ctx, &firestorepb.CommitRequest{
			Database: dbName,
			Writes: []*firestorepb.Write{{
				Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
					Name: name,
					Fields: map[string]*firestorepb.Value{
						"v": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: v}},
					},
				}},
			}},
		})
		require.NoError(t, err)
	}
	upsert(1)

	begin, err := c.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: dbName})
	require.NoError(t, err)
	txn := begin.GetTransaction()
	require.NotEmpty(t, txn)

	// Read the document inside the transaction to record its version.
	stream, err := c.BatchGetDocuments(ctx, &firestorepb.BatchGetDocumentsRequest{
		Database:  dbName,
		Documents: []string{name},
		ConsistencySelector: &firestorepb.BatchGetDocumentsRequest_Transaction{
			Transaction: txn,
		},
	})
	require.NoError(t, err)
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// A concurrent writer bumps the version after the transactional read.
	upsert(2)

	// Committing the transaction's write now conflicts → ABORTED, nothing applied.
	_, err = c.Commit(ctx, &firestorepb.CommitRequest{
		Database:    dbName,
		Transaction: txn,
		Writes: []*firestorepb.Write{{
			Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{
				Name: name,
				Fields: map[string]*firestorepb.Value{
					"v": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 99}},
				},
			}},
		}},
	})
	require.Equal(t, codes.Aborted, status.Code(err), "err=%v", err)

	got, err := c.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	require.NoError(t, err)
	require.EqualValues(t, 2, got.GetFields()["v"].GetIntegerValue(), "aborted transaction must not apply")
}
