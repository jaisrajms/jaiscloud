package eventarc

import (
	"context"
	"encoding/json"
	"testing"

	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/store"
)

// TestOperationsPersisted proves a control-plane mutation records its done
// operation, so the shared google.longrunning surface can read, list and
// delete it, and that the recorded response survives the target's deletion.
func TestOperationsPersisted(t *testing.T) {
	ctx := context.Background()
	svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())

	body := json.RawMessage(`{"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}],` +
		`"destination":{"cloudRun":{"service":"svc","region":"us-central1"}}}`)
	trigger, op, err := svc.CreateTrigger(ctx, "proj", "us-central1", "trig", body, false)
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if op.ID == "" || len(op.Response) == 0 {
		t.Fatalf("create operation not recorded: %+v", op)
	}

	got, err := svc.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if got.Verb != "create" || got.Target == "" {
		t.Fatalf("operation = %+v", got)
	}
	var resp map[string]any
	if err := json.Unmarshal(got.Response, &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if resp["@type"] != TriggerTypeURL || resp["name"] == "" {
		t.Fatalf("operation response = %v", resp)
	}

	list, next, err := svc.ListOperations(ctx, "proj", "us-central1", 0, "")
	if err != nil || next != "" || len(list) != 1 || list[0].ID != op.ID {
		t.Fatalf("ListOperations = %+v, %q, %v", list, next, err)
	}

	// The delete mutation records the deleted resource's body, so the operation
	// still renders after the trigger is gone.
	_, delOp, err := svc.DeleteTrigger(ctx, "proj", "us-central1", trigger.Name, "", false)
	if err != nil {
		t.Fatalf("DeleteTrigger: %v", err)
	}
	gotDel, err := svc.GetOperation(ctx, "proj", "us-central1", delOp.ID)
	if err != nil {
		t.Fatalf("GetOperation(delete): %v", err)
	}
	if gotDel.Verb != "delete" || len(gotDel.Response) == 0 {
		t.Fatalf("delete operation = %+v", gotDel)
	}

	if err := svc.DeleteOperation(ctx, "proj", "us-central1", op.ID); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}
	if _, err := svc.GetOperation(ctx, "proj", "us-central1", op.ID); maskStatus(err) != 404 {
		t.Fatalf("GetOperation after delete err = %v, want 404", err)
	}
	if err := svc.DeleteOperation(ctx, "proj", "us-central1", "missing"); maskStatus(err) != 404 {
		t.Fatalf("DeleteOperation(missing) err = %v, want 404", err)
	}

	// validateOnly creates no operation.
	if _, voOp, err := svc.CreateTrigger(ctx, "proj", "us-central1", "dry", body, true); err != nil || voOp.ID == "" {
		t.Fatalf("validateOnly create = %+v, %v", voOp, err)
	} else if _, err := svc.GetOperation(ctx, "proj", "us-central1", voOp.ID); maskStatus(err) != 404 {
		t.Fatalf("validateOnly operation was persisted: %v", err)
	}
}
