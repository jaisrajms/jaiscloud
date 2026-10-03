package logging

import (
	"context"
	"strings"
	"testing"

	loggingstore "jaiscloud/internal/gcp/store/logging"
)

func TestSinkCRUD(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	created, err := s.CreateSink(ctx, "projects/p", loggingstore.LogSink{
		Name:        "my-sink",
		Destination: "storage.googleapis.com/bucket",
		Filter:      "severity>=WARNING",
	}, true, "")
	if err != nil {
		t.Fatalf("CreateSink: %v", err)
	}
	if created.Name != "my-sink" {
		t.Fatalf("Name = %q, want my-sink", created.Name)
	}
	if created.WriterIdentity == "" {
		t.Fatal("WriterIdentity is empty")
	}
	if created.CreateTime.IsZero() || created.UpdateTime.IsZero() {
		t.Fatal("timestamps not assigned")
	}

	got, err := s.GetSink(ctx, "projects/p/sinks/my-sink")
	if err != nil {
		t.Fatalf("GetSink: %v", err)
	}
	if got.Destination != "storage.googleapis.com/bucket" {
		t.Fatalf("Destination = %q", got.Destination)
	}

	// A masked update changes only the named field.
	updated, err := s.UpdateSink(ctx, "projects/p/sinks/my-sink",
		loggingstore.LogSink{Filter: "severity>=ERROR"}, []string{"filter"}, false, "")
	if err != nil {
		t.Fatalf("UpdateSink: %v", err)
	}
	if updated.Filter != "severity>=ERROR" {
		t.Fatalf("Filter = %q, want severity>=ERROR", updated.Filter)
	}
	if updated.Destination != "storage.googleapis.com/bucket" {
		t.Fatalf("masked update cleared Destination: %q", updated.Destination)
	}

	list, next, err := s.ListSinks(ctx, "projects/p", 0, "")
	if err != nil {
		t.Fatalf("ListSinks: %v", err)
	}
	if next != "" || len(list) != 1 {
		t.Fatalf("ListSinks = %d entries, next=%q", len(list), next)
	}

	if err := s.DeleteSink(ctx, "projects/p/sinks/my-sink"); err != nil {
		t.Fatalf("DeleteSink: %v", err)
	}
	if _, err := s.GetSink(ctx, "projects/p/sinks/my-sink"); err == nil {
		t.Fatal("GetSink after delete = nil error")
	}
}

func TestSinkCreateValidation(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	cases := []struct {
		name string
		sink loggingstore.LogSink
	}{
		{"missing destination", loggingstore.LogSink{Name: "s"}},
		{"invalid name", loggingstore.LogSink{Name: "bad name!", Destination: "d"}},
		{"empty name", loggingstore.LogSink{Destination: "d"}},
		{"unsupported filter", loggingstore.LogSink{Name: "s2", Destination: "d", Filter: "resource.labels.zone=\"x\""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.CreateSink(ctx, "projects/p", tc.sink, false, ""); err == nil {
				t.Fatalf("CreateSink(%+v) = nil error", tc.sink)
			}
		})
	}

	if _, err := s.CreateSink(ctx, "projects/p", loggingstore.LogSink{Name: "dup", Destination: "d"}, false, ""); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := s.CreateSink(ctx, "projects/p", loggingstore.LogSink{Name: "dup", Destination: "d"}, false, ""); err == nil {
		t.Fatal("duplicate create = nil error, want AlreadyExists")
	}
}

func TestSinkUpdateUnsupportedMaskFailsLoud(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateSink(ctx, "projects/p", loggingstore.LogSink{Name: "s", Destination: "d"}, false, ""); err != nil {
		t.Fatalf("CreateSink: %v", err)
	}
	_, err := s.UpdateSink(ctx, "projects/p/sinks/s", loggingstore.LogSink{}, []string{"writerIdentity"}, false, "")
	if err == nil {
		t.Fatal("unsupported mask = nil error")
	}
	if !strings.Contains(err.Error(), "unsupported update_mask") {
		t.Fatalf("error = %v, want unsupported update_mask", err)
	}
}

func TestExclusionCRUD(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	created, err := s.CreateExclusion(ctx, "projects/p", loggingstore.LogExclusion{
		Name:   "no-debug",
		Filter: "severity<DEBUG",
	})
	if err != nil {
		t.Fatalf("CreateExclusion: %v", err)
	}
	if created.Name != "no-debug" || created.CreateTime.IsZero() {
		t.Fatalf("created = %+v", created)
	}

	got, err := s.GetExclusion(ctx, "projects/p/exclusions/no-debug")
	if err != nil {
		t.Fatalf("GetExclusion: %v", err)
	}
	if got.Filter != "severity<DEBUG" {
		t.Fatalf("Filter = %q", got.Filter)
	}

	updated, err := s.UpdateExclusion(ctx, "projects/p/exclusions/no-debug",
		loggingstore.LogExclusion{Disabled: true}, []string{"disabled"})
	if err != nil {
		t.Fatalf("UpdateExclusion: %v", err)
	}
	if !updated.Disabled || updated.Filter != "severity<DEBUG" {
		t.Fatalf("updated = %+v", updated)
	}

	list, _, err := s.ListExclusions(ctx, "projects/p", 0, "")
	if err != nil {
		t.Fatalf("ListExclusions: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("ListExclusions = %d", len(list))
	}

	if err := s.DeleteExclusion(ctx, "projects/p/exclusions/no-debug"); err != nil {
		t.Fatalf("DeleteExclusion: %v", err)
	}
	if _, err := s.GetExclusion(ctx, "projects/p/exclusions/no-debug"); err == nil {
		t.Fatal("GetExclusion after delete = nil error")
	}
}

func TestExclusionCreateRequiresFilter(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateExclusion(ctx, "projects/p", loggingstore.LogExclusion{Name: "e"}); err == nil {
		t.Fatal("CreateExclusion without filter = nil error")
	}
}

func TestRouteEntry(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	scope := "projects/p"

	if _, err := s.CreateSink(ctx, scope, loggingstore.LogSink{
		Name:        "errors",
		Destination: "storage.googleapis.com/errors",
		Filter:      "severity>=ERROR",
	}, false, ""); err != nil {
		t.Fatalf("CreateSink errors: %v", err)
	}
	if _, err := s.CreateSink(ctx, scope, loggingstore.LogSink{
		Name:        "all-else",
		Destination: "storage.googleapis.com/all",
	}, false, ""); err != nil {
		t.Fatalf("CreateSink all-else: %v", err)
	}
	if _, err := s.CreateSink(ctx, scope, loggingstore.LogSink{
		Name:        "disabled",
		Destination: "storage.googleapis.com/disabled",
		Disabled:    true,
	}, false, ""); err != nil {
		t.Fatalf("CreateSink disabled: %v", err)
	}
	// A sink whose inline exclusion suppresses the matching entry.
	if _, err := s.CreateSink(ctx, scope, loggingstore.LogSink{
		Name:        "with-inline",
		Destination: "storage.googleapis.com/inline",
		Exclusions:  []loggingstore.LogExclusion{{Name: "quiet", Filter: `textPayload:"noisy"`}},
	}, false, ""); err != nil {
		t.Fatalf("CreateSink with-inline: %v", err)
	}

	entry := textEntry("projects/p/logs/l", "boom", 500)

	decision, err := s.RouteEntry(ctx, scope, entry)
	if err != nil {
		t.Fatalf("RouteEntry: %v", err)
	}
	if decision.Excluded {
		t.Fatal("entry unexpectedly excluded")
	}
	want := map[string]bool{
		"projects/p/sinks/errors":      true,
		"projects/p/sinks/all-else":    true,
		"projects/p/sinks/with-inline": true,
	}
	if len(decision.Sinks) != len(want) {
		t.Fatalf("matched sinks = %v, want %v", decision.Sinks, want)
	}
	for _, s := range decision.Sinks {
		if !want[s] {
			t.Fatalf("unexpected matched sink %q", s)
		}
	}

	// A project-level exclusion suppresses every sink, evaluated first.
	if _, err := s.CreateExclusion(ctx, scope, loggingstore.LogExclusion{
		Name: "exclude-boom", Filter: `textPayload:"boom"`,
	}); err != nil {
		t.Fatalf("CreateExclusion: %v", err)
	}
	decision, err = s.RouteEntry(ctx, scope, entry)
	if err != nil {
		t.Fatalf("RouteEntry: %v", err)
	}
	if !decision.Excluded || len(decision.Sinks) != 0 {
		t.Fatalf("decision = %+v, want excluded with no sinks", decision)
	}

	// An inline sink exclusion drops only the matching sink.
	noisy := textEntry("projects/p/logs/l", "noisy", 500)
	if err := s.DeleteExclusion(ctx, "projects/p/exclusions/exclude-boom"); err != nil {
		t.Fatalf("DeleteExclusion: %v", err)
	}
	decision, err = s.RouteEntry(ctx, scope, noisy)
	if err != nil {
		t.Fatalf("RouteEntry: %v", err)
	}
	for _, sink := range decision.Sinks {
		if sink == "projects/p/sinks/with-inline" {
			t.Fatalf("inline exclusion did not suppress sink: %v", decision.Sinks)
		}
	}
}

func TestSinkUpdateDefaultMaskPreservesFields(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	created, err := s.CreateSink(ctx, "projects/p", loggingstore.LogSink{
		Name:        "s",
		Destination: "storage.googleapis.com/old",
		Filter:      "severity>=WARNING",
		Description: "keep me",
		Exclusions:  []loggingstore.LogExclusion{{Name: "quiet", Filter: `textPayload:"noisy"`}},
	}, false, "")
	if err != nil {
		t.Fatalf("CreateSink: %v", err)
	}

	// An empty update mask uses the proto default destination,filter,includeChildren.
	updated, err := s.UpdateSink(ctx, "projects/p/sinks/s", loggingstore.LogSink{
		Destination: "storage.googleapis.com/new",
		Filter:      "severity>=ERROR",
	}, nil, false, "")
	if err != nil {
		t.Fatalf("UpdateSink: %v", err)
	}
	if updated.Destination != "storage.googleapis.com/new" || updated.Filter != "severity>=ERROR" {
		t.Fatalf("default-mask update = %+v", updated)
	}
	if updated.Description != "keep me" {
		t.Fatalf("default-mask update cleared description: %q", updated.Description)
	}
	if len(updated.Exclusions) != 1 || updated.Exclusions[0].Name != "quiet" {
		t.Fatalf("default-mask update cleared exclusions: %+v", updated.Exclusions)
	}
	if !updated.CreateTime.Equal(created.CreateTime) {
		t.Fatalf("default-mask update changed createTime: %v -> %v", created.CreateTime, updated.CreateTime)
	}
	if updated.WriterIdentity == "" {
		t.Fatal("default-mask update cleared writerIdentity")
	}
}

func TestExclusionUpdateRequiresMask(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateExclusion(ctx, "projects/p", loggingstore.LogExclusion{Name: "e", Filter: "severity<DEBUG"}); err != nil {
		t.Fatalf("CreateExclusion: %v", err)
	}
	if _, err := s.UpdateExclusion(ctx, "projects/p/exclusions/e", loggingstore.LogExclusion{Disabled: true}, nil); err == nil {
		t.Fatal("UpdateExclusion without mask = nil error, want InvalidArgument")
	}
}

func TestSinkWriterIdentityModes(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	shared, err := s.CreateSink(ctx, "projects/p", loggingstore.LogSink{Name: "shared", Destination: "d"}, false, "")
	if err != nil {
		t.Fatalf("CreateSink shared: %v", err)
	}
	unique, err := s.CreateSink(ctx, "projects/p", loggingstore.LogSink{Name: "unique", Destination: "d"}, true, "")
	if err != nil {
		t.Fatalf("CreateSink unique: %v", err)
	}
	if shared.WriterIdentity == "" || unique.WriterIdentity == "" {
		t.Fatal("empty writer identity")
	}
	if shared.WriterIdentity == unique.WriterIdentity {
		t.Fatal("unique_writer_identity produced the shared identity")
	}
	custom, err := s.CreateSink(ctx, "projects/p", loggingstore.LogSink{Name: "custom", Destination: "d"}, false, "serviceAccount:me@example.iam.gserviceaccount.com")
	if err != nil {
		t.Fatalf("CreateSink custom: %v", err)
	}
	if custom.WriterIdentity != "serviceAccount:me@example.iam.gserviceaccount.com" {
		t.Fatalf("custom writer identity = %q", custom.WriterIdentity)
	}
}

func TestConfigResourceNameParsing(t *testing.T) {
	if scope, id, err := ParseSinkName("projects/p/sinks/x"); err != nil || scope != "projects/p" || id != "x" {
		t.Fatalf("ParseSinkName = %q,%q,%v", scope, id, err)
	}
	if _, _, err := ParseSinkName("projects/p/exclusions/x"); err == nil {
		t.Fatal("ParseSinkName on exclusion = nil error")
	}
	if scope, id, err := ParseExclusionName("organizations/12/exclusions/e"); err != nil || scope != "organizations/12" || id != "e" {
		t.Fatalf("ParseExclusionName = %q,%q,%v", scope, id, err)
	}
	if _, _, err := ParseExclusionName("projects/p/logs/l"); err == nil {
		t.Fatal("ParseExclusionName on log = nil error")
	}
}
