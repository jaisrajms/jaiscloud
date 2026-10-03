package logging

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemorySinkAndExclusionCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Now().UTC()

	sink := LogSink{Name: "s1", Destination: "storage.googleapis.com/b", Filter: "severity>=ERROR", CreateTime: now, UpdateTime: now}
	if err := s.CreateSink(ctx, "projects/p", sink); err != nil {
		t.Fatalf("CreateSink: %v", err)
	}
	if err := s.CreateSink(ctx, "projects/p", sink); !errors.Is(err, ErrSinkExists) {
		t.Fatalf("duplicate CreateSink = %v, want ErrSinkExists", err)
	}
	if got, err := s.GetSink(ctx, "projects/p", "s1"); err != nil || got.Destination != sink.Destination {
		t.Fatalf("GetSink = %+v, %v", got, err)
	}
	if _, err := s.GetSink(ctx, "projects/p", "missing"); !errors.Is(err, ErrSinkNotFound) {
		t.Fatalf("GetSink missing = %v, want ErrSinkNotFound", err)
	}
	sink.Filter = "severity>=WARNING"
	if err := s.UpdateSink(ctx, "projects/p", sink); err != nil {
		t.Fatalf("UpdateSink: %v", err)
	}
	if err := s.UpdateSink(ctx, "projects/p", LogSink{Name: "missing"}); !errors.Is(err, ErrSinkNotFound) {
		t.Fatalf("UpdateSink missing = %v", err)
	}
	if list, err := s.ListSinks(ctx, "projects/p"); err != nil || len(list) != 1 {
		t.Fatalf("ListSinks = %v, %v", list, err)
	}
	if err := s.DeleteSink(ctx, "projects/p", "s1"); err != nil {
		t.Fatalf("DeleteSink: %v", err)
	}
	if err := s.DeleteSink(ctx, "projects/p", "s1"); !errors.Is(err, ErrSinkNotFound) {
		t.Fatalf("double DeleteSink = %v", err)
	}

	excl := LogExclusion{Name: "e1", Filter: "severity<DEBUG", CreateTime: now, UpdateTime: now}
	if err := s.CreateExclusion(ctx, "projects/p", excl); err != nil {
		t.Fatalf("CreateExclusion: %v", err)
	}
	if err := s.CreateExclusion(ctx, "projects/p", excl); !errors.Is(err, ErrExclusionExists) {
		t.Fatalf("duplicate CreateExclusion = %v", err)
	}
	if got, err := s.GetExclusion(ctx, "projects/p", "e1"); err != nil || got.Filter != excl.Filter {
		t.Fatalf("GetExclusion = %+v, %v", got, err)
	}
	if _, err := s.GetExclusion(ctx, "projects/p", "missing"); !errors.Is(err, ErrExclusionNotFound) {
		t.Fatalf("GetExclusion missing = %v", err)
	}
	if list, err := s.ListExclusions(ctx, "projects/p"); err != nil || len(list) != 1 {
		t.Fatalf("ListExclusions = %v, %v", list, err)
	}
	if err := s.DeleteExclusion(ctx, "projects/p", "e1"); err != nil {
		t.Fatalf("DeleteExclusion: %v", err)
	}
}

func TestMemorySnapshotRoundTripWithConfig(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Now().UTC()
	if err := s.Write(ctx, "projects/p", testEntry("projects/p/logs/l", "hi", 200, now)); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateSink(ctx, "projects/p", LogSink{Name: "s", Destination: "d", CreateTime: now, UpdateTime: now}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateExclusion(ctx, "projects/p", LogExclusion{Name: "e", Filter: "severity<DEBUG", CreateTime: now, UpdateTime: now}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if list, err := dst.List(ctx, "projects/p"); err != nil || len(list) != 1 {
		t.Fatalf("restored entries = %v, %v", list, err)
	}
	if sink, err := dst.GetSink(ctx, "projects/p", "s"); err != nil || sink.Destination != "d" {
		t.Fatalf("restored sink = %+v, %v", sink, err)
	}
	if e, err := dst.GetExclusion(ctx, "projects/p", "e"); err != nil || e.Filter != "severity<DEBUG" {
		t.Fatalf("restored exclusion = %+v, %v", e, err)
	}
	if empty, err := dst.IsEmpty(ctx); err != nil || empty {
		t.Fatalf("IsEmpty = %v, %v, want false", empty, err)
	}
}

func TestMemoryRestoreLegacySnapshot(t *testing.T) {
	ctx := context.Background()
	dst := NewMemoryStore()
	legacy := `{"projects/p":[{"id":3,"logName":"projects/p/logs/l","severity":200,"payloadType":"text","textPayload":"old","timestamp":"2026-01-01T00:00:00Z"}]}`
	if err := dst.Restore(ctx, bytes.NewBufferString(legacy)); err != nil {
		t.Fatalf("Restore legacy: %v", err)
	}
	list, err := dst.List(ctx, "projects/p")
	if err != nil || len(list) != 1 || list[0].TextPayload != "old" {
		t.Fatalf("legacy entries = %v, %v", list, err)
	}
	if list, err := dst.ListSinks(ctx, "projects/p"); err != nil || len(list) != 0 {
		t.Fatalf("legacy sinks = %v, %v", list, err)
	}
}
