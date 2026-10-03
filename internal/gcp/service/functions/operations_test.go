package functions

import (
	"context"
	"errors"
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func assertNotFound(t *testing.T, err error) {
	t.Helper()
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if perr.Code != "NotFound" || perr.HTTPStatus != 404 {
		t.Fatalf("code=%q status=%d, want NotFound/404", perr.Code, perr.HTTPStatus)
	}
}

// TestServiceOperationStore covers the persisted long-running operation
// lifecycle: mutations store an operation that get/list/wait read back with the
// version-appropriate typed response, and unknown ids are NotFound.
func TestServiceOperationStore(t *testing.T) {
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())

	in := FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}, V1)
	f, op, err := s.CreateFunction(ctx, "proj", "us-central1", "hello", in, V1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if f.ID != "hello" || op.Verb != "create" {
		t.Fatalf("create returned %+v / %+v", f, op)
	}

	name := OperationName(V1, "proj", op)
	got, err := s.GetOperationJSON(ctx, "proj", name, V1)
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if resp, _ := got["response"].(map[string]any); resp["@type"] != functionTypeURLV1 {
		t.Errorf("v1 response @type = %v, want %v", resp["@type"], functionTypeURLV1)
	}
	// The same operation read as v2 renders the v2 Function shape.
	gotV2, err := s.GetOperationJSON(ctx, "proj", name, V2)
	if err != nil {
		t.Fatalf("get op v2: %v", err)
	}
	if resp, _ := gotV2["response"].(map[string]any); resp["@type"] != functionTypeURLV2 {
		t.Errorf("v2 response @type = %v, want %v", resp["@type"], functionTypeURLV2)
	}

	// :wait returns the same persisted, done operation.
	if _, err := s.WaitOperation(ctx, "proj", name, V2); err != nil {
		t.Fatalf("wait: %v", err)
	}

	page, next, err := s.ListOperations(ctx, "proj", "us-central1", V1, "", 0, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 1 || next != "" || page[0].ID != op.ID {
		t.Fatalf("list = %+v next=%q", page, next)
	}

	// Update and delete persist further operations.
	if _, uop, err := s.UpdateFunction(ctx, "proj", "us-central1", "hello",
		FunctionInputFromMap(map[string]any{"entryPoint": "next"}, V1), []string{"entryPoint"}, V1); err != nil {
		t.Fatalf("update: %v", err)
	} else if uop.Verb != "update" {
		t.Fatalf("update op verb = %q", uop.Verb)
	}
	if _, err := s.DeleteFunction(ctx, "proj", "us-central1", "hello"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	page, _, err = s.ListOperations(ctx, "proj", "us-central1", V1, "", 0, "")
	if err != nil {
		t.Fatalf("list after mutations: %v", err)
	}
	if len(page) != 3 {
		t.Fatalf("expected 3 persisted operations, got %d: %+v", len(page), page)
	}

	// Unknown operations are NotFound for get/wait/cancel/delete.
	missing := "projects/proj/locations/us-central1/operations/missing"
	if _, err := s.GetOperationJSON(ctx, "proj", missing, V2); err == nil {
		t.Errorf("expected NotFound on get")
	} else {
		assertNotFound(t, err)
	}
	if err := s.CancelOperation(ctx, "proj", missing); err == nil {
		t.Errorf("expected NotFound on cancel")
	} else {
		assertNotFound(t, err)
	}
	if err := s.DeleteOperation(ctx, "proj", missing); err == nil {
		t.Errorf("expected NotFound on delete")
	} else {
		assertNotFound(t, err)
	}

	// A malformed operation name is InvalidArgument, not NotFound.
	if _, err := s.GetOperationJSON(ctx, "proj", "bogus", V2); err == nil {
		t.Errorf("expected InvalidArgument for malformed operation name")
	}
}

// TestOperationNameVersionShape covers J60: v1 operation names are top-level
// (operations/{id}), v2 names are location-scoped.
func TestOperationNameVersionShape(t *testing.T) {
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	_, op, err := s.CreateFunction(ctx, "proj", "us-central1", "hello",
		FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}, V1), V1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	v1name := OperationName(V1, "proj", op)
	if v1name != "operations/"+op.ID {
		t.Fatalf("v1 name = %q, want operations/%s", v1name, op.ID)
	}
	v2name := OperationName(V2, "proj", op)
	if v2name != "projects/proj/locations/us-central1/operations/"+op.ID {
		t.Fatalf("v2 name = %q", v2name)
	}
	// The v1 top-level name reads the persisted operation back.
	if _, err := s.GetOperationJSON(ctx, "proj", v1name, V1); err != nil {
		t.Fatalf("get by v1 name: %v", err)
	}
}

// TestOperationV1LookupIsProjectIndependent covers the J60 follow-up: a v1
// top-level name (operations/{id}) carries no project, so the persisted
// operation is found regardless of the caller's project.
func TestOperationV1LookupIsProjectIndependent(t *testing.T) {
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	_, op, err := s.CreateFunction(ctx, "proj", "us-central1", "hello",
		FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}, V1), V1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	name := OperationName(V1, "proj", op)
	if _, err := s.GetOperationJSON(ctx, "another-project", name, V1); err != nil {
		t.Fatalf("v1 lookup by id should not be project-scoped: %v", err)
	}
}
