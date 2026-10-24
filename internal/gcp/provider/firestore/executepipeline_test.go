package firestore

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
)

// pipelineBody builds the protojson-shaped request body the REST codec hands the
// handler for documents.executePipeline.
func pipelineBody(stages ...any) map[string]any {
	return map[string]any{
		"structuredPipeline": map[string]any{
			"pipeline": map[string]any{"stages": stages},
		},
	}
}

func refArg(ref string) map[string]any  { return map[string]any{"referenceValue": ref} }
func intArgVal(n string) map[string]any { return map[string]any{"integerValue": n} }

func fnArg(name string, args ...any) map[string]any {
	return map[string]any{"functionValue": map[string]any{"name": name, "args": args}}
}

// TestExecutePipelineREST exercises documents.executePipeline over REST: a
// collection source, a where filter, an ordering and a limit, decoded from the
// protojson request body.
func TestExecutePipelineREST(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	for _, tc := range []struct {
		id    string
		price int64
	}{{"a", 5}, {"b", 20}, {"c", 30}} {
		name := "projects/proj/databases/(default)/documents/books/" + tc.id
		if err := p.store.CreateDocument(ctx, firestorestore.Document{
			Name:   name,
			Fields: map[string]*firestorestore.Value{"title": strField(tc.id), "price": intField(tc.price)},
		}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents"
	nr.Params["body"] = pipelineBody(
		map[string]any{"name": "collection", "args": []any{refArg("/books")}},
		map[string]any{"name": "where", "args": []any{
			fnArg("greater_than", map[string]any{"fieldReferenceValue": "price"}, intArgVal("10")),
		}},
		map[string]any{"name": "sort", "args": []any{
			map[string]any{"mapValue": map[string]any{"fields": map[string]any{
				"direction":  map[string]any{"stringValue": "descending"},
				"expression": map[string]any{"fieldReferenceValue": "price"},
			}}},
		}},
		map[string]any{"name": "limit", "args": []any{intArgVal("1")}},
	)
	resp, err := p.ExecutePipeline(ctx, nr)
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Fatalf("status = %d, want 200", resp.HTTPStatus)
	}
	if _, ok := resp.Data["executionTime"].(string); !ok {
		t.Fatalf("executionTime missing/not a string: %v", resp.Data["executionTime"])
	}
	results, ok := resp.Data["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("results = %v, want 1 document", resp.Data["results"])
	}
	doc, _ := results[0].(map[string]any)
	fields, _ := doc["fields"].(map[string]any)
	title, _ := fields["title"].(map[string]any)
	if got := title["stringValue"]; got != "c" {
		t.Fatalf("title = %v, want c (highest price after descending sort + limit 1)", got)
	}
	if _, ok := resp.Data["transaction"]; ok {
		t.Fatalf("no newTransaction was requested, got transaction %v", resp.Data["transaction"])
	}
}

// TestExecutePipelineRESTNewTransaction asserts a newTransaction request returns
// a base64 transaction id (the proto bytes field's REST encoding).
func TestExecutePipelineRESTNewTransaction(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := testNR()
	nr.Params["name"] = "databases/(default)/documents"
	body := pipelineBody(
		map[string]any{"name": "collection", "args": []any{refArg("/books")}},
	)
	body["newTransaction"] = map[string]any{}
	nr.Params["body"] = body

	resp, err := p.ExecutePipeline(ctx, nr)
	if err != nil {
		t.Fatalf("ExecutePipeline: %v", err)
	}
	txn, _ := resp.Data["transaction"].(string)
	if txn == "" {
		t.Fatal("expected a transaction id in the response")
	}
	if _, err := base64.StdEncoding.DecodeString(txn); err != nil {
		t.Fatalf("transaction is not standard base64: %v", err)
	}
}

// TestExecutePipelineRESTErrors asserts the fail-loud contract: a missing
// pipeline is InvalidArgument(400) and an unsupported stage is
// Unimplemented(501) — never a silently empty result.
func TestExecutePipelineRESTErrors(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	missing := testNR()
	missing.Params["name"] = "databases/(default)/documents"
	missing.Params["body"] = map[string]any{}
	if _, err := p.ExecutePipeline(ctx, missing); err == nil {
		t.Fatal("expected InvalidArgument for a missing pipeline")
	} else if pe := providerErr(t, err); pe.HTTPStatus != 400 {
		t.Fatalf("missing pipeline status = %d, want 400", pe.HTTPStatus)
	}

	unsupported := testNR()
	unsupported.Params["name"] = "databases/(default)/documents"
	unsupported.Params["body"] = pipelineBody(
		map[string]any{"name": "collection", "args": []any{refArg("/books")}},
		map[string]any{"name": "aggregate", "args": []any{refArg("title")}},
	)
	if _, err := p.ExecutePipeline(ctx, unsupported); err == nil {
		t.Fatal("expected Unimplemented for an unsupported stage")
	} else if pe := providerErr(t, err); pe.HTTPStatus != 501 {
		t.Fatalf("unsupported stage status = %d, want 501", pe.HTTPStatus)
	}
}

func providerErr(t *testing.T, err error) *model.ProviderError {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected a ProviderError, got %T: %v", err, err)
	}
	return pe
}
