package firestore

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/wire"
)

// ndjsonLines extracts the newline-delimited JSON response lines carried under
// wire.RawJSONKey.
func ndjsonLines(t *testing.T, data map[string]any) []string {
	t.Helper()
	raw, ok := data[wire.RawJSONKey].(json.RawMessage)
	if !ok {
		t.Fatalf("expected raw JSON in response data, got %T", data[wire.RawJSONKey])
	}
	if len(raw) == 0 {
		return nil
	}
	return strings.Split(string(raw), "\n")
}

func TestRunQuerySubcollectionScoping(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	sub := "projects/proj/databases/(default)/documents/cities/SF/landmarks/a"
	root := "projects/proj/databases/(default)/documents/cities/SF"
	for _, name := range []string{sub, root} {
		if err := p.store.CreateDocument(ctx, firestorestore.Document{
			Name:   name,
			Fields: map[string]*firestorestore.Value{"x": intField(1)},
		}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	runQuery := func(parent string, from []any) ([]string, error) {
		nr := testNR()
		nr.Params["name"] = parent
		nr.Params["body"] = map[string]any{"structuredQuery": map[string]any{"from": from}}
		resp, err := p.RunQuery(ctx, nr)
		if err != nil {
			return nil, err
		}
		return ndjsonLines(t, resp.Data), nil
	}

	// A subcollection query scoped to a document matches its subcollection doc.
	lines, err := runQuery("databases/(default)/documents/cities/SF",
		[]any{map[string]any{"collectionId": "landmarks"}})
	if err != nil {
		t.Fatalf("runQuery subcollection: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines (doc + done), got %d: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "landmarks/a") {
		t.Errorf("expected subcollection doc in first line, got %q", lines[0])
	}
	if !strings.Contains(lines[1], `"done":true`) {
		t.Errorf("expected final done line, got %q", lines[1])
	}

	// A root-level collection is NOT matched by a subcollection-scoped query.
	lines, err = runQuery("databases/(default)/documents/cities/SF",
		[]any{map[string]any{"collectionId": "cities"}})
	if err != nil {
		t.Fatalf("runQuery root collection from subcollection scope: %v", err)
	}
	if len(lines) != 1 || !strings.Contains(lines[0], `"done":true`) {
		t.Errorf("expected only a done line (no match), got %v", lines)
	}

	// Collection-group query (allDescendants) scoped under the same parent.
	lines, err = runQuery("databases/(default)/documents/cities/SF",
		[]any{map[string]any{"collectionId": "landmarks", "allDescendants": true}})
	if err != nil {
		t.Fatalf("runQuery collection group: %v", err)
	}
	if len(lines) != 2 || !strings.Contains(lines[0], "landmarks/a") {
		t.Errorf("expected collection-group match, got %v", lines)
	}
}
