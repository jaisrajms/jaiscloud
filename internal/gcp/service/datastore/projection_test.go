package datastore

import (
	"context"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// TestRunQueryProjection locks the projection contract: a query with no
// projection returns full entities, a projection returns only the named
// properties (key always present), and a keys-only projection (the clients'
// "__key__" form) returns no properties. The gRPC/REST EntityResultType is
// derived from QueryResult.ResultType, so it is asserted too.
func TestRunQueryProjection(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	entity := func(n int64, desc string) dsstore.Entity {
		return dsstore.Entity{Properties: map[string]dsstore.Value{
			"n":    {IntegerValue: ptrInt64(n)},
			"desc": {StringValue: &desc},
		}}
	}
	upsert(t, s, "p", nameKey("Task", "a"), entity(1, "one"))
	upsert(t, s, "p", nameKey("Task", "b"), entity(2, "two"))

	full, err := s.RunQuery(ctx, "p", &Query{Kind: "Task"}, nil)
	if err != nil {
		t.Fatalf("full RunQuery: %v", err)
	}
	if full.ResultType != ResultFull {
		t.Fatalf("full ResultType = %v, want ResultFull", full.ResultType)
	}
	for _, e := range full.Entities {
		if len(e.Entity.Properties) != 2 {
			t.Fatalf("full entity properties = %v, want both", e.Entity.Properties)
		}
	}

	proj, err := s.RunQuery(ctx, "p", &Query{Kind: "Task", Projection: &Projection{Properties: []string{"desc"}}}, nil)
	if err != nil {
		t.Fatalf("projected RunQuery: %v", err)
	}
	if proj.ResultType != ResultProjection {
		t.Fatalf("projected ResultType = %v, want ResultProjection", proj.ResultType)
	}
	if len(proj.Entities) != 2 {
		t.Fatalf("projected entities = %d, want 2", len(proj.Entities))
	}
	for _, e := range proj.Entities {
		if _, ok := e.Entity.Properties["n"]; ok {
			t.Fatal("projected entity must not carry the un-projected property n")
		}
		if _, ok := e.Entity.Properties["desc"]; !ok {
			t.Fatal("projected entity must carry the projected property desc")
		}
	}

	keys, err := s.RunQuery(ctx, "p", &Query{Kind: "Task", Projection: &Projection{Properties: []string{"__key__"}}}, nil)
	if err != nil {
		t.Fatalf("keys-only RunQuery: %v", err)
	}
	if keys.ResultType != ResultKeysOnly {
		t.Fatalf("keys-only ResultType = %v, want ResultKeysOnly", keys.ResultType)
	}
	for _, e := range keys.Entities {
		if len(e.Entity.Properties) != 0 {
			t.Fatalf("keys-only entity properties = %v, want none", e.Entity.Properties)
		}
		if e.Entity.Key == "" {
			t.Fatal("keys-only entity must still carry its key")
		}
	}
}

// TestParseGQLProjection verifies the GQL parser surfaces the SELECT list as a
// structured projection (previously parsed and discarded).
func TestParseGQLProjection(t *testing.T) {
	q, err := ParseGQL(GQLQuery{QueryString: "SELECT desc FROM Task"})
	if err != nil {
		t.Fatalf("ParseGQL: %v", err)
	}
	if q.Projection == nil || len(q.Projection.Properties) != 1 || q.Projection.Properties[0] != "desc" {
		t.Fatalf("GQL projection = %+v, want [desc]", q.Projection)
	}
	if q.ResultType() != ResultProjection {
		t.Fatalf("GQL projection ResultType = %v, want ResultProjection", q.ResultType())
	}

	star, err := ParseGQL(GQLQuery{QueryString: "SELECT * FROM Task"})
	if err != nil {
		t.Fatalf("ParseGQL(*): %v", err)
	}
	if star.Projection != nil {
		t.Fatalf("SELECT * must not set a projection, got %+v", star.Projection)
	}
}
