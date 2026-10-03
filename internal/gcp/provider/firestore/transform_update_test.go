package firestore

import (
	"context"
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// seedTransformDoc creates {n:10, keep:"me"} under the given name.
func seedTransformDoc(t *testing.T, p *Provider, name string) {
	t.Helper()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	if err := p.store.CreateDocument(context.Background(), firestorestore.Document{
		Name: name,
		Fields: map[string]*firestorestore.Value{
			"n":    firestorestore.IntVal(10),
			"keep": firestorestore.StringVal("me"),
		},
		CreateTime: now, UpdateTime: now,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// TestBuildUpdateThreeMaskStates asserts the nil / non-empty / empty mask
// distinction: replace-all, patch, and transform-only respectively.
func TestBuildUpdateThreeMaskStates(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)

	seedTransformDoc(t, p, name)

	// 1. nil mask → replace-all: unrelated field keep is dropped.
	doc, _, _, err := p.buildUpdate(ctx, &documentWire{
		Name:   name,
		Fields: map[string]*firestorestore.Value{"n": firestorestore.IntVal(1)},
	}, nil, nil, nil, now)
	if err != nil {
		t.Fatalf("replace-all buildUpdate: %v", err)
	}
	if _, ok := doc.Fields["keep"]; ok {
		t.Errorf("replace-all should drop unrelated field keep, got %+v", doc.Fields)
	}

	// 2. non-empty mask → patch: only n is replaced, keep preserved.
	doc, _, _, err = p.buildUpdate(ctx, &documentWire{
		Name:   name,
		Fields: map[string]*firestorestore.Value{"n": firestorestore.IntVal(2)},
	}, &documentMaskWire{FieldPaths: []string{"n"}}, nil, nil, now)
	if err != nil {
		t.Fatalf("patch buildUpdate: %v", err)
	}
	if v, ok := doc.Fields["keep"].AsString(); !ok || v != "me" {
		t.Errorf("patch should preserve keep, got %+v", doc.Fields)
	}
	if v, ok := doc.Fields["n"].AsInt64(); !ok || v != 2 {
		t.Errorf("patch n: got %v, want 2", v)
	}

	// 3. empty (present) mask + transform → transforms only on existing base.
	doc, _, _, err = p.buildUpdate(ctx, &documentWire{Name: name},
		&documentMaskWire{}, []fieldTransformWire{{FieldPath: "n", Increment: firestorestore.IntVal(5)}}, nil, now)
	if err != nil {
		t.Fatalf("transform-only buildUpdate: %v", err)
	}
	if v, ok := doc.Fields["n"].AsInt64(); !ok || v != 15 {
		t.Errorf("transform-only n: got %v, want 15", v)
	}
	if v, ok := doc.Fields["keep"].AsString(); !ok || v != "me" {
		t.Errorf("transform-only should preserve keep, got %+v", doc.Fields)
	}
}

// TestTransformOnlyUpdateCommit exercises the full REST commit path for a
// transform-only update (empty document.fields + empty updateMask + transforms),
// asserting unrelated fields survive.
func TestTransformOnlyUpdateCommit(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	name := "projects/proj/databases/(default)/documents/cities/SF"

	seedTransformDoc(t, p, name)

	commit := func(t *testing.T, transforms []any) {
		t.Helper()
		nr := testNR()
		nr.Params["body"] = map[string]any{
			"writes": []any{
				map[string]any{
					"update":           map[string]any{"name": name},
					"updateMask":       map[string]any{},
					"updateTransforms": transforms,
				},
			},
		}
		if _, err := p.Commit(ctx, nr); err != nil {
			t.Fatalf("transform-only commit: %v", err)
		}
	}

	t.Run("Increment", func(t *testing.T) {
		commit(t, []any{
			map[string]any{"fieldPath": "n", "increment": map[string]any{"integerValue": "5"}},
		})
		doc, err := p.store.GetDocument(ctx, name)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if v, ok := doc.Fields["n"].AsInt64(); !ok || v != 15 {
			t.Errorf("increment: got %v, want 15 (not 5)", v)
		}
		if v, ok := doc.Fields["keep"].AsString(); !ok || v != "me" {
			t.Errorf("keep lost: got %+v", doc.Fields)
		}
	})

	t.Run("ServerTimestamp", func(t *testing.T) {
		// Server timestamp is applied via SetToServerValue; assert it is set and
		// keep survives.
		commit(t, []any{
			map[string]any{"fieldPath": "created", "setToServerValue": "REQUEST_TIME"},
		})
		doc, err := p.store.GetDocument(ctx, name)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if v, ok := doc.Fields["created"].AsTimestamp(); !ok || v.IsZero() {
			t.Errorf("server timestamp: got %+v, want a non-zero timestamp", doc.Fields["created"])
		}
		if v, ok := doc.Fields["keep"].AsString(); !ok || v != "me" {
			t.Errorf("keep lost: got %+v", doc.Fields)
		}
	})

	t.Run("ArrayUnion", func(t *testing.T) {
		// Seed an array field, then ArrayUnion onto it (appendMissingElements).
		now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
		if err := p.store.UpdateDocument(ctx, firestorestore.Document{
			Name: name,
			Fields: map[string]*firestorestore.Value{
				"n":    firestorestore.IntVal(15),
				"keep": firestorestore.StringVal("me"),
				"tags": firestorestore.ArrayVal(firestorestore.StringVal("a")),
			},
			CreateTime: now, UpdateTime: now,
		}); err != nil {
			t.Fatalf("seed array: %v", err)
		}
		commit(t, []any{
			map[string]any{
				"fieldPath": "tags",
				"appendMissingElements": map[string]any{
					"values": []any{map[string]any{"stringValue": "b"}},
				},
			},
		})
		doc, err := p.store.GetDocument(ctx, name)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		arr, ok := doc.Fields["tags"].AsArray()
		if !ok || len(arr) != 2 {
			t.Errorf("arrayUnion: got %+v, want [a b]", doc.Fields["tags"])
		}
		if v, ok := doc.Fields["keep"].AsString(); !ok || v != "me" {
			t.Errorf("keep lost: got %+v", doc.Fields)
		}
	})
}
