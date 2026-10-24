package eventarc

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// TestOperationsSurface covers the Eventarc-host long-running-operations surface
// (reached only under the eventarc host — see adapter.operationsHostService).
// Every completed mutation's done operation is persisted by the core, so list
// returns it, get renders it, delete removes it, and an unknown name is NotFound.
func TestOperationsSurface(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	resp, err := p.CreateTrigger(ctx, newNR(map[string]any{
		"location":  "us-central1",
		"triggerId": "trig",
		"body": map[string]any{
			"eventFilters": []any{map[string]any{"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"}},
			"destination":  map[string]any{"cloudRun": map[string]any{"service": "svc", "region": "us-central1"}},
		},
	}))
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	opName, ok := resp.Data["name"].(string)
	if !ok || opName == "" {
		t.Fatalf("create response is not an operation: %v", resp.Data)
	}

	list, err := p.ListOperations(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	ops, ok := list.Data["operations"].([]any)
	if !ok || len(ops) != 1 {
		t.Fatalf("operations = %v, want one persisted operation", list.Data["operations"])
	}

	got, err := p.GetOperation(ctx, newNR(map[string]any{"name": opName}))
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if got.Data["done"] != true {
		t.Fatalf("operation not done: %v", got.Data)
	}
	if resp, ok := got.Data["response"].(map[string]any); !ok || resp["@type"] == nil {
		t.Fatalf("operation response = %v", got.Data["response"])
	}

	// Cancel of a settled operation is a no-op that reports it.
	if _, err := p.CancelOperation(ctx, newNR(map[string]any{"name": opName})); err != nil {
		t.Fatalf("CancelOperation: %v", err)
	}

	if _, err := p.DeleteOperation(ctx, newNR(map[string]any{"name": opName})); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}
	for _, tc := range []struct {
		name string
		fn   func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error)
	}{
		{"get", p.GetOperation},
		{"delete", p.DeleteOperation},
		{"cancel", p.CancelOperation},
	} {
		_, err := tc.fn(ctx, newNR(map[string]any{"name": opName}))
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 404 {
			t.Errorf("%s after delete: err = %v, want 404 NotFound", tc.name, err)
		}
	}
}
