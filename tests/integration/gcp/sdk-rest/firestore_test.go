// Package sdkrest_test exercises the Firestore documents REST surface through the
// official apiary client (google.golang.org/api/firestore/v1).
package sdkrest_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	firestore "google.golang.org/api/firestore/v1"

	"github.com/stretchr/testify/require"
)

const fsParent = "projects/proj/databases/(default)/documents"

func fsStr(v string) *firestore.Value { return &firestore.Value{StringValue: v} }
func fsInt(v int64) *firestore.Value  { return &firestore.Value{IntegerValue: v} }

func TestSDKFirestoreCRUD(t *testing.T) {
	ctx := context.Background()
	svc, err := firestore.NewService(ctx, opts()...)
	require.NoError(t, err)

	coll := unique("cities")

	// createDocument with an explicit documentId.
	doc, err := svc.Projects.Databases.Documents.CreateDocument(fsParent, coll, &firestore.Document{
		Fields: map[string]firestore.Value{
			"name":       *fsStr("San Francisco"),
			"population": *fsInt(800000),
		},
	}).DocumentId("SF").Do()
	require.NoError(t, err)
	require.NotEmpty(t, doc.Name)
	require.Equal(t, "San Francisco", doc.Fields["name"].StringValue)

	// get.
	got, err := svc.Projects.Databases.Documents.Get(doc.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "San Francisco", got.Fields["name"].StringValue)
	require.EqualValues(t, 800000, got.Fields["population"].IntegerValue)

	// patch with updateMask.
	patched, err := svc.Projects.Databases.Documents.Patch(doc.Name, &firestore.Document{
		Fields: map[string]firestore.Value{"population": *fsInt(900000)},
	}).UpdateMaskFieldPaths("population").Do()
	require.NoError(t, err)
	require.EqualValues(t, 900000, patched.Fields["population"].IntegerValue)

	// list.
	list, err := svc.Projects.Databases.Documents.List(fsParent, coll).Do()
	require.NoError(t, err)
	require.Len(t, list.Documents, 1)

	// delete.
	_, err = svc.Projects.Databases.Documents.Delete(doc.Name).Do()
	require.NoError(t, err)

	// get after delete → 404.
	_, err = svc.Projects.Databases.Documents.Get(doc.Name).Do()
	require.Error(t, err)
}

func TestSDKFirestoreCommitBatchWrite(t *testing.T) {
	ctx := context.Background()
	svc, err := firestore.NewService(ctx, opts()...)
	require.NoError(t, err)

	coll := unique("cities")

	// commit: two creates.
	db := "projects/proj/databases/(default)"
	commit, err := svc.Projects.Databases.Documents.Commit(db, &firestore.CommitRequest{
		Writes: []*firestore.Write{
			{Update: &firestore.Document{Name: fsParent + "/" + coll + "/a", Fields: map[string]firestore.Value{"n": *fsInt(1)}}},
			{Update: &firestore.Document{Name: fsParent + "/" + coll + "/b", Fields: map[string]firestore.Value{"n": *fsInt(2)}}},
		},
	}).Do()
	require.NoError(t, err)
	require.Len(t, commit.WriteResults, 2)
	require.NotEmpty(t, commit.CommitTime)

	// batchWrite: one valid update, one delete.
	bw, err := svc.Projects.Databases.Documents.BatchWrite(db, &firestore.BatchWriteRequest{
		Writes: []*firestore.Write{
			{Update: &firestore.Document{Name: fsParent + "/" + coll + "/c", Fields: map[string]firestore.Value{"n": *fsInt(3)}}},
			{Delete: fsParent + "/" + coll + "/a"},
		},
	}).Do()
	require.NoError(t, err)
	require.Len(t, bw.Status, 2)
	require.EqualValues(t, 0, bw.Status[0].Code)
	require.EqualValues(t, 0, bw.Status[1].Code)
}

func TestSDKFirestoreTransaction(t *testing.T) {
	ctx := context.Background()
	svc, err := firestore.NewService(ctx, opts()...)
	require.NoError(t, err)

	db := "projects/proj/databases/(default)"
	begin, err := svc.Projects.Databases.Documents.BeginTransaction(db, &firestore.BeginTransactionRequest{}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, begin.Transaction)

	_, err = svc.Projects.Databases.Documents.Rollback(db, &firestore.RollbackRequest{Transaction: begin.Transaction}).Do()
	require.NoError(t, err)
}

// TestSDKFirestoreRunQuery exercises runQuery via a raw HTTP call, because the
// apiary RunQuery.Do decodes a single object while Firestore's server-streaming
// REST response is newline-delimited JSON (one object per line).
func TestSDKFirestoreRunQuery(t *testing.T) {
	ctx := context.Background()
	svc, err := firestore.NewService(ctx, opts()...)
	require.NoError(t, err)

	coll := unique("cities")
	for _, id := range []string{"a", "b", "c"} {
		_, err := svc.Projects.Databases.Documents.CreateDocument(fsParent, coll, &firestore.Document{
			Fields: map[string]firestore.Value{"pop": *fsInt(int64(len(id) * 100))},
		}).DocumentId(id).Do()
		require.NoError(t, err)
	}

	reqBody, _ := json.Marshal(map[string]any{
		"structuredQuery": map[string]any{
			"from": []map[string]any{{"collectionId": coll}},
			"where": map[string]any{
				"fieldFilter": map[string]any{
					"field": map[string]any{"fieldPath": "pop"},
					"op":    "GREATER_THAN",
					"value": map[string]any{"integerValue": "50"},
				},
			},
			"orderBy": []map[string]any{
				{"field": map[string]any{"fieldPath": "pop"}, "direction": "ASCENDING"},
			},
		},
	})

	url := strings.TrimRight(endpoint(), "/") + "/v1/" + fsParent + ":runQuery"
	resp, err := http.Post(url, "application/json", strings.NewReader(string(reqBody)))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var docs []map[string]any
	var done bool
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var msg map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &msg))
		if _, ok := msg["document"]; ok {
			docs = append(docs, msg)
		}
		if d, ok := msg["done"].(bool); ok && d {
			done = true
		}
	}
	require.Len(t, docs, 3)
	require.True(t, done)
}

// TestSDKFirestoreCompositeIndexRejected verifies strict composite-index
// enforcement: a multi-field orderBy without a registered index is rejected
// with FAILED_PRECONDITION (HTTP 400).
func TestSDKFirestoreCompositeIndexRejected(t *testing.T) {
	coll := unique("cities")
	reqBody, _ := json.Marshal(map[string]any{
		"structuredQuery": map[string]any{
			"from": []map[string]any{{"collectionId": coll}},
			"orderBy": []map[string]any{
				{"field": map[string]any{"fieldPath": "a"}, "direction": "ASCENDING"},
				{"field": map[string]any{"fieldPath": "b"}, "direction": "ASCENDING"},
			},
		},
	})
	url := strings.TrimRight(endpoint(), "/") + "/v1/" + fsParent + ":runQuery"
	resp, err := http.Post(url, "application/json", strings.NewReader(string(reqBody)))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "FAILED_PRECONDITION")
}
