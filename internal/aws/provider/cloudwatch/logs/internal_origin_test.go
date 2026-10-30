package logs

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

func internalReq(action string, params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{Service: "logs", Action: action, Origin: model.OriginInternal, Params: params}
}

// Internal callers (workload log streamers) rely on create-if-absent for the
// group and the stream, and a batch for a missing group is dropped, not an error.
func TestInternalOriginKeepsTheStreamerSemantics(t *testing.T) {
	p := New()
	ctx := context.Background()
	events := []any{map[string]any{"timestamp": float64(1700000000000), "message": "hello"}}
	put := func() error {
		_, err := p.PutLogEvents(ctx, internalReq("PutLogEvents", map[string]any{
			"logGroupName": "/g", "logStreamName": "s", "logEvents": events}))
		return err
	}

	if err := put(); err != nil {
		t.Fatalf("missing group must drop silently, got %v", err)
	}
	for i := 0; i < 2; i++ { // idempotent
		if _, err := p.CreateLogGroup(ctx, internalReq("CreateLogGroup", map[string]any{"logGroupName": "/g"})); err != nil {
			t.Fatalf("internal CreateLogGroup #%d: %v", i, err)
		}
	}
	if err := put(); err != nil {
		t.Fatal(err)
	}
	resp, err := p.GetLogEvents(ctx, &model.NormalizedRequest{Service: "logs", Action: "GetLogEvents",
		Params: map[string]any{"logGroupName": "/g", "logStreamName": "s"}})
	if err != nil {
		t.Fatalf("stream must have been created on demand: %v", err)
	}
	if got, _ := resp.Data["events"].([]any); len(got) != 1 {
		t.Fatalf("events = %v, want the one ingested line", resp.Data["events"])
	}
}
