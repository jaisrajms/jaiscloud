package workflows

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	w := Workflow{ID: "a", SourceContents: "main:\n  steps:\n    - r:\n        return: ${x}", State: "ACTIVE",
		Labels: map[string]string{"team": "x"}, RevisionID: "000001-a4d"}
	if err := s.CreateWorkflow(ctx, "proj", "us-central1", "a", w); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	start := time.Now().UTC().Truncate(time.Microsecond)
	e := Execution{ID: "e1", State: "FAILED", Argument: `{"x":1}`, Result: "",
		Error:              &ExecutionError{Payload: `{"message":"boom"}`, Context: "boom"},
		StartTime:          start,
		EndTime:            start,
		Duration:           "0.1s",
		WorkflowRevisionID: "000001-a4d",
		CurrentSteps:       []Step{{Routine: "main", Step: "r"}},
	}
	if err := s.CreateExecution(ctx, "proj", "us-central1", "a", "e1", e); err != nil {
		t.Fatalf("create execution: %v", err)
	}
	_ = s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op1", Done: true, Response: `{}`})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := dst.GetWorkflow(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("get workflow after restore: %v", err)
	}
	// Source contents must survive byte-for-byte.
	if got.SourceContents != w.SourceContents {
		t.Fatalf("sourceContents not preserved verbatim:\n%q\nvs\n%q", got.SourceContents, w.SourceContents)
	}
	if got.Labels["team"] != "x" {
		t.Fatalf("labels not preserved: %+v", got.Labels)
	}

	gex, err := dst.GetExecution(ctx, "proj", "us-central1", "a", "e1")
	if err != nil {
		t.Fatalf("get execution after restore: %v", err)
	}
	if gex.Argument != `{"x":1}` || gex.Result != "" {
		t.Fatalf("argument/result not preserved verbatim: %+v", gex)
	}
	if gex.Error == nil || gex.Error.Payload != `{"message":"boom"}` {
		t.Fatalf("error not preserved: %+v", gex.Error)
	}
	if len(gex.CurrentSteps) != 1 || gex.CurrentSteps[0].Step != "r" {
		t.Fatalf("currentSteps not preserved: %+v", gex.CurrentSteps)
	}

	_, err = dst.GetOperation(ctx, "proj", "us-central1", "op1")
	if err != nil {
		t.Fatalf("get operation after restore: %v", err)
	}
}
