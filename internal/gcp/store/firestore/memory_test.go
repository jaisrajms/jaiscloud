package firestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

func TestValueJSONRoundTrip(t *testing.T) {
	ts := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		val   *Value
		check func(*Value) bool
	}{
		{"null", NullVal(), func(v *Value) bool { return v.NullValue != nil && *v.NullValue == NullEnumValue }},
		{"bool", BoolVal(true), func(v *Value) bool { b, ok := v.AsBool(); return ok && b }},
		{"int", IntVal(-42), func(v *Value) bool { n, ok := v.AsInt64(); return ok && n == -42 }},
		{"double", DoubleVal(3.5), func(v *Value) bool { f, ok := v.AsFloat64(); return ok && f == 3.5 }},
		{"timestamp", TimestampVal(ts), func(v *Value) bool { t, ok := v.AsTimestamp(); return ok && t.Equal(ts) }},
		{"string", StringVal("hello"), func(v *Value) bool { s, ok := v.AsString(); return ok && s == "hello" }},
		{"bytes", BytesVal([]byte{0x01, 0x02}), func(v *Value) bool { b, ok := v.AsBytes(); return ok && bytes.Equal(b, []byte{0x01, 0x02}) }},
		{"reference", ReferenceVal("projects/p/databases/(default)/documents/c/d"), func(v *Value) bool {
			s, ok := v.AsReference()
			return ok && s == "projects/p/databases/(default)/documents/c/d"
		}},
		{"geopoint", GeoPointVal(37.77, -122.41), func(v *Value) bool {
			g, ok := v.AsGeoPoint()
			return ok && g.Latitude == 37.77 && g.Longitude == -122.41
		}},
		{"array", ArrayVal(StringVal("a"), IntVal(1)), func(v *Value) bool {
			a, ok := v.AsArray()
			return ok && len(a) == 2
		}},
		{"map", MapVal(map[string]*Value{"k": StringVal("v")}), func(v *Value) bool {
			m, ok := v.AsMap()
			return ok && m["k"] != nil
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.val)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var got Value
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !tc.check(&got) {
				t.Fatalf("round-trip mismatch: %s", b)
			}
		})
	}
}

// TestValueIntegerWireEncoding asserts integerValue is a decimal string on the
// wire (split int/double, unlike DynamoDB's arbitrary-precision number).
func TestValueIntegerWireEncoding(t *testing.T) {
	b, err := json.Marshal(IntVal(9007199254740993))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := raw["integerValue"]; got != "9007199254740993" {
		t.Fatalf("expected integerValue as string, got %T %v", got, got)
	}
}

func TestMemoryStoreCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)

	name := "projects/p/databases/(default)/documents/cities/SF"
	doc := Document{
		Name:       name,
		Fields:     map[string]*Value{"name": StringVal("San Francisco")},
		CreateTime: now,
		UpdateTime: now,
	}

	if err := s.CreateDocument(ctx, doc); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Duplicate → ErrDocumentExists.
	if err := s.CreateDocument(ctx, doc); !errors.Is(err, ErrDocumentExists) {
		t.Fatalf("expected ErrDocumentExists, got %v", err)
	}

	got, err := s.GetDocument(ctx, name)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.CollectionID != "cities" {
		t.Errorf("expected CollectionID cities, got %q", got.CollectionID)
	}
	if got.ParentPath != "projects/p/databases/(default)/documents/cities" {
		t.Errorf("unexpected ParentPath %q", got.ParentPath)
	}
	if v, ok := got.Fields["name"].AsString(); !ok || v != "San Francisco" {
		t.Errorf("unexpected fields: %+v", got.Fields)
	}

	// Get missing → ErrDocumentNotFound.
	if _, err := s.GetDocument(ctx, "projects/p/databases/(default)/documents/cities/NOPE"); !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("expected ErrDocumentNotFound, got %v", err)
	}

	// Update.
	updated := Document{
		Name:       name,
		Fields:     map[string]*Value{"name": StringVal("SF"), "state": StringVal("CA")},
		CreateTime: now,
		UpdateTime: now.Add(time.Minute),
	}
	if err := s.UpdateDocument(ctx, updated); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ = s.GetDocument(ctx, name)
	if v, _ := got.Fields["name"].AsString(); v != "SF" {
		t.Errorf("expected updated name SF, got %q", v)
	}
	if !got.UpdateTime.Equal(now.Add(time.Minute)) {
		t.Errorf("unexpected UpdateTime %v", got.UpdateTime)
	}

	// Update missing → ErrDocumentNotFound.
	if err := s.UpdateDocument(ctx, Document{Name: "projects/p/databases/(default)/documents/cities/NOPE"}); !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("expected ErrDocumentNotFound on update, got %v", err)
	}

	// ListDocuments scoped by project+database.
	s.CreateDocument(ctx, Document{Name: "projects/p/databases/(default)/documents/cities/LA", CreateTime: now, UpdateTime: now})
	s.CreateDocument(ctx, Document{Name: "projects/q/databases/(default)/documents/cities/NY", CreateTime: now, UpdateTime: now})
	list, err := s.ListDocuments(ctx, "p", "(default)")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 docs in project p, got %d", len(list))
	}
	if list[0].Name != "projects/p/databases/(default)/documents/cities/LA" ||
		list[1].Name != "projects/p/databases/(default)/documents/cities/SF" {
		t.Errorf("unexpected list order: %+v", list)
	}

	// Delete is idempotent.
	if err := s.DeleteDocument(ctx, name); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.DeleteDocument(ctx, name); err != nil {
		t.Fatalf("delete should be idempotent, got %v", err)
	}
	if _, err := s.GetDocument(ctx, name); !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("expected ErrDocumentNotFound after delete, got %v", err)
	}
}

func TestMemoryStoreSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)

	s := NewMemoryStore()
	s.CreateDocument(ctx, Document{
		Name:       "projects/p/databases/(default)/documents/cities/SF",
		Fields:     map[string]*Value{"name": StringVal("San Francisco"), "n": IntVal(7)},
		CreateTime: now,
		UpdateTime: now,
	})
	s.CreateDocument(ctx, Document{
		Name:       "projects/p/databases/(default)/documents/cities/LA",
		Fields:     map[string]*Value{"name": StringVal("Los Angeles")},
		CreateTime: now,
		UpdateTime: now,
	})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Wipe, then restore.
	s.Reset(ctx)
	if empty, err := s.IsEmpty(ctx); err != nil || !empty {
		t.Fatalf("expected empty after reset, got empty=%v err=%v", empty, err)
	}

	restored := NewMemoryStore()
	if err := restored.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	doc, err := restored.GetDocument(ctx, "projects/p/databases/(default)/documents/cities/SF")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if v, ok := doc.Fields["n"].AsInt64(); !ok || v != 7 {
		t.Errorf("unexpected restored field: %+v", doc.Fields)
	}
	// CollectionID/ParentPath are re-derived on restore.
	if doc.CollectionID != "cities" || doc.ParentPath != "projects/p/databases/(default)/documents/cities" {
		t.Errorf("derived fields not restored: collectionID=%q parentPath=%q", doc.CollectionID, doc.ParentPath)
	}
}

// TestNormalizeDocumentDefaultsTimestamp asserts normalizeDocument fills
// timestamps from the (frozen) global clock when unset.
func TestNormalizeDocumentDefaultsTimestamp(t *testing.T) {
	frozen := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: frozen})
	defer clock.SetGlobalClock(clock.RealClock{})

	doc := &Document{Name: "projects/p/databases/(default)/documents/cities/SF"}
	normalizeDocument(doc)

	if !doc.CreateTime.Equal(frozen) || !doc.UpdateTime.Equal(frozen) {
		t.Errorf("expected frozen timestamp, got create=%v update=%v", doc.CreateTime, doc.UpdateTime)
	}
	if doc.Fields == nil {
		t.Errorf("expected non-nil Fields after normalize")
	}
}
