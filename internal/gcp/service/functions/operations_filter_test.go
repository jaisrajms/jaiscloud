package functions

import (
	"context"
	"errors"
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func assertInvalidArgument(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected InvalidArgument, got nil")
	}
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("code=%q status=%d, want InvalidArgument/400", perr.Code, perr.HTTPStatus)
	}
}

// newOperationFilterService returns a service with three persisted done
// operations in us-central1 so filter combinations have something to narrow.
func newOperationFilterService(t *testing.T) (*Service, []Operation) {
	t.Helper()
	ctx := context.Background()
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	var ops []Operation
	for _, id := range []string{"alpha", "beta", "gamma"} {
		_, op, err := s.CreateFunction(ctx, "proj", "us-central1", id,
			FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}, V1), V1)
		if err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
		ops = append(ops, op)
	}
	return s, ops
}

// TestListOperationsFilter covers the supported AIP-160 subset over the
// persisted function operations: done and name, the boolean operators, and the
// AND/OR/NOT grammar.
func TestListOperationsFilter(t *testing.T) {
	ctx := context.Background()
	s, ops := newOperationFilterService(t)

	cases := []struct {
		name   string
		filter string
		want   int
	}{
		{"empty matches all", "", 3},
		{"done true", "done=true", 3},
		{"done true quoted", `done = "true"`, 3},
		{"done false", "done=false", 0},
		{"not done", "NOT done=true", 0},
		{"minus negation", "-done=true", 0},
		{"minus negation matches", "-done=false", 3},
		{"name exact", `name="projects/proj/locations/us-central1/operations/` + ops[0].ID + `"`, 1},
		{"name contains", `name:"operations/"`, 3},
		{"and", `done=true AND name:"` + ops[1].ID + `"`, 1},
		// AIP-160: OR binds tighter than AND, so this is
		// done=false AND (name:id0 OR done=true) == false for every operation.
		{"or binds tighter than and", `done=false AND name:"` + ops[0].ID + `" OR done=true`, 0},
		{"parens", `(done=true OR done=false) AND name:"` + ops[2].ID + `"`, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			page, _, err := s.ListOperations(ctx, "proj", "us-central1", V2, tc.filter, 0, "")
			if err != nil {
				t.Fatalf("list %q: %v", tc.filter, err)
			}
			if len(page) != tc.want {
				t.Fatalf("list %q = %d ops, want %d: %+v", tc.filter, len(page), tc.want, page)
			}
		})
	}

	// The v1 name form is matched when the caller renders v1 operations.
	page, _, err := s.ListOperations(ctx, "proj", "us-central1", V1, `name="operations/`+ops[0].ID+`"`, 0, "")
	if err != nil {
		t.Fatalf("v1 name filter: %v", err)
	}
	if len(page) != 1 || page[0].ID != ops[0].ID {
		t.Fatalf("v1 name filter = %+v, want %s", page, ops[0].ID)
	}
}

// TestListOperationsFilterInvalid rejects a filter the emulator cannot
// evaluate rather than silently returning unfiltered (or empty) results.
func TestListOperationsFilterInvalid(t *testing.T) {
	ctx := context.Background()
	s, _ := newOperationFilterService(t)

	for _, filter := range []string{
		"bogus=true",            // unknown field
		"done=maybe",            // non-boolean done value
		"done",                  // missing operator
		"done=",                 // missing value
		"done=true AND",         // dangling operator
		"name!=x OR (done=true", // unbalanced paren
		`name="unterminated`,    // unterminated string
		"done=true OR",          // trailing operator
	} {
		if _, _, err := s.ListOperations(ctx, "proj", "us-central1", V2, filter, 0, ""); err == nil {
			t.Errorf("filter %q: expected InvalidArgument", filter)
		} else {
			assertInvalidArgument(t, err)
		}
	}
}

// TestListOperationsFilterComposesWithPaging verifies the filter is applied
// before pagination, so a page is a page of matching results and the cursor
// continues across the filtered set.
func TestListOperationsFilterComposesWithPaging(t *testing.T) {
	ctx := context.Background()
	s, _ := newOperationFilterService(t)

	first, next, err := s.ListOperations(ctx, "proj", "us-central1", V2, "done=true", 1, "")
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first) != 1 || next == "" {
		t.Fatalf("first page = %+v next=%q, want one op and a cursor", first, next)
	}
	second, next2, err := s.ListOperations(ctx, "proj", "us-central1", V2, "done=true", 1, next)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(second) != 1 || second[0].ID == first[0].ID || next2 == "" {
		t.Fatalf("second page = %+v next=%q", second, next2)
	}
	// A filter that matches nothing returns an empty page, never the unfiltered set.
	none, _, err := s.ListOperations(ctx, "proj", "us-central1", V2, "done=false", 10, "")
	if err != nil {
		t.Fatalf("done=false: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("done=false = %+v, want empty", none)
	}
}
