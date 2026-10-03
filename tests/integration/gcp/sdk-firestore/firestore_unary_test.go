// Package sdk_firestore_test — low-level unary RPC coverage. The high-level
// cloud.google.com/go/firestore client routes all document reads and queries
// through the (deferred) streaming RPCs, so the unary GetDocument /
// CreateDocument / UpdateDocument / DeleteDocument / BatchWrite / Rollback RPCs
// are exercised here directly against firestorepb.
package sdk_firestore_test

import (
	"context"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const dbName = "projects/proj/databases/(default)"

func newRawClient(t *testing.T) firestorepb.FirestoreClient {
	t.Helper()
	conn, err := grpc.NewClient("localhost:8081", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return firestorepb.NewFirestoreClient(conn)
}

func TestUnaryDocumentRPCs(t *testing.T) {
	ctx := context.Background()
	c := newRawClient(t)

	parent := dbName + "/documents"
	name := parent + "/cities/SF"

	// CreateDocument (unary).
	created, err := c.CreateDocument(ctx, &firestorepb.CreateDocumentRequest{
		Parent:       parent,
		CollectionId: "cities",
		DocumentId:   "SF",
		Document: &firestorepb.Document{
			Fields: map[string]*firestorepb.Value{
				"name": {ValueType: &firestorepb.Value_StringValue{StringValue: "San Francisco"}},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, name, created.GetName())

	// GetDocument (unary).
	got, err := c.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	require.NoError(t, err)
	require.Equal(t, "San Francisco", got.GetFields()["name"].GetStringValue())

	// UpdateDocument (unary, patch semantics).
	updated, err := c.UpdateDocument(ctx, &firestorepb.UpdateDocumentRequest{
		Document: &firestorepb.Document{
			Name:   name,
			Fields: map[string]*firestorepb.Value{"population": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 900000}}},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"population"}},
	})
	require.NoError(t, err)
	require.EqualValues(t, 900000, updated.GetFields()["population"].GetIntegerValue())

	// DeleteDocument (unary).
	_, err = c.DeleteDocument(ctx, &firestorepb.DeleteDocumentRequest{Name: name})
	require.NoError(t, err)

	// GetDocument after delete → NotFound.
	_, err = c.GetDocument(ctx, &firestorepb.GetDocumentRequest{Name: name})
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestUnaryBatchWriteAndTxn(t *testing.T) {
	ctx := context.Background()
	c := newRawClient(t)

	// BatchWrite (unary, non-atomic).
	bw, err := c.BatchWrite(ctx, &firestorepb.BatchWriteRequest{
		Database: dbName,
		Writes: []*firestorepb.Write{
			{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{Name: dbName + "/documents/cities/a", Fields: map[string]*firestorepb.Value{"n": {ValueType: &firestorepb.Value_IntegerValue{IntegerValue: 1}}}}}},
			{Operation: &firestorepb.Write_Delete{Delete: dbName + "/documents/cities/nope"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, bw.GetWriteResults(), 2)
	require.Len(t, bw.GetStatus(), 2)
	require.EqualValues(t, 0, bw.GetStatus()[0].GetCode())

	// BeginTransaction + Rollback (unary).
	begin, err := c.BeginTransaction(ctx, &firestorepb.BeginTransactionRequest{Database: dbName})
	require.NoError(t, err)
	require.NotEmpty(t, begin.GetTransaction())

	_, err = c.Rollback(ctx, &firestorepb.RollbackRequest{Database: dbName, Transaction: begin.GetTransaction()})
	require.NoError(t, err)
}
