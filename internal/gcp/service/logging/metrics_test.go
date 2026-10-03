package logging

import (
	"context"
	"errors"
	"testing"

	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/model"
)

func providerErr(t *testing.T, err error) *model.ProviderError {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("error %v is not a *model.ProviderError", err)
	}
	return pe
}

func TestMetricCreateViewAndDefaults(t *testing.T) {
	svc := NewService(loggingstore.NewMemoryStore(), "test")
	ctx := context.Background()

	m, err := svc.CreateMetric(ctx, "projects/test", loggingstore.LogMetric{
		Name:   "nginx/requests",
		Filter: "severity>=ERROR",
	})
	if err != nil {
		t.Fatalf("CreateMetric: %v", err)
	}
	if m.Name != "nginx/requests" {
		t.Fatalf("name = %q", m.Name)
	}
	if m.Descriptor.MetricKind != "DELTA" || m.Descriptor.ValueType != "INT64" || m.Descriptor.Unit != "1" {
		t.Fatalf("descriptor defaults = %+v", m.Descriptor)
	}
	if want := "logging.googleapis.com/user/nginx/requests"; m.Descriptor.Type != want {
		t.Fatalf("descriptor.type = %q, want %q", m.Descriptor.Type, want)
	}
	if want := "projects/test/metricDescriptors/logging.googleapis.com/user/nginx/requests"; m.Descriptor.Name != want {
		t.Fatalf("descriptor.name = %q, want %q", m.Descriptor.Name, want)
	}

	// The id may be fetched by its canonical percent-encoded resource name.
	got, err := svc.GetMetric(ctx, "projects/test/metrics/nginx%2Frequests")
	if err != nil || got.Name != "nginx/requests" {
		t.Fatalf("GetMetric encoded = %+v, %v", got, err)
	}
	// A raw slash spelling (multi-segment path) also resolves.
	if got, err := svc.GetMetric(ctx, "projects/test/metrics/nginx/requests"); err != nil || got.Name != "nginx/requests" {
		t.Fatalf("GetMetric raw = %+v, %v", got, err)
	}

	if _, err := svc.CreateMetric(ctx, "projects/test", loggingstore.LogMetric{Name: "nginx/requests", Filter: "severity>=INFO"}); err == nil {
		t.Fatal("duplicate CreateMetric = nil error")
	} else if pe := providerErr(t, err); pe.HTTPStatus != 409 {
		t.Fatalf("duplicate status = %d, want 409", pe.HTTPStatus)
	}
}

func TestMetricValidation(t *testing.T) {
	svc := NewService(loggingstore.NewMemoryStore(), "test")
	ctx := context.Background()

	cases := []struct {
		name  string
		scope string
		in    loggingstore.LogMetric
	}{
		{"empty filter", "projects/test", loggingstore.LogMetric{Name: "m"}},
		{"bad name", "projects/test", loggingstore.LogMetric{Name: "/leading", Filter: "severity>=ERROR"}},
		{"bad filter", "projects/test", loggingstore.LogMetric{Name: "m", Filter: "severity>>ERROR"}},
		{"distribution without extractor", "projects/test", loggingstore.LogMetric{
			Name:       "m",
			Filter:     "severity>=ERROR",
			Descriptor: loggingstore.LogMetricDescriptor{ValueType: "DISTRIBUTION"},
		}},
		{"extractor without label", "projects/test", loggingstore.LogMetric{
			Name:            "m",
			Filter:          "severity>=ERROR",
			LabelExtractors: map[string]string{"code": "EXTRACT(x)"},
		}},
		{"label without extractor", "projects/test", loggingstore.LogMetric{
			Name:       "m",
			Filter:     "severity>=ERROR",
			Descriptor: loggingstore.LogMetricDescriptor{Labels: []loggingstore.LogMetricLabel{{Key: "code"}}},
		}},
		{"bad scope", "nonsense/test", loggingstore.LogMetric{Name: "m", Filter: "severity>=ERROR"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := svc.CreateMetric(ctx, tc.scope, tc.in); err == nil {
				t.Fatal("CreateMetric = nil error, want InvalidArgument")
			} else if pe := providerErr(t, err); pe.HTTPStatus != 400 {
				t.Fatalf("status = %d, want 400", pe.HTTPStatus)
			}
		})
	}
}

func TestMetricUpdateImmutableKindAndLabels(t *testing.T) {
	svc := NewService(loggingstore.NewMemoryStore(), "test")
	ctx := context.Background()

	created, err := svc.CreateMetric(ctx, "projects/test", loggingstore.LogMetric{
		Name:            "m",
		Filter:          "severity>=ERROR",
		ValueExtractor:  "EXTRACT(jsonPayload.latency)",
		LabelExtractors: map[string]string{"code": "EXTRACT(jsonPayload.code)"},
		Descriptor: loggingstore.LogMetricDescriptor{
			ValueType: "DISTRIBUTION",
			Unit:      "ms",
			Labels:    []loggingstore.LogMetricLabel{{Key: "code", ValueType: "INT64"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateMetric: %v", err)
	}
	name := "projects/test/metrics/m"

	// metric_kind is immutable.
	if _, err := svc.UpdateMetric(ctx, name, loggingstore.LogMetric{
		Filter:          "severity>=WARNING",
		ValueExtractor:  created.ValueExtractor,
		LabelExtractors: created.LabelExtractors,
		Descriptor: loggingstore.LogMetricDescriptor{
			MetricKind: "GAUGE", ValueType: "DISTRIBUTION", Unit: "ms",
			Labels: []loggingstore.LogMetricLabel{{Key: "code", ValueType: "INT64"}},
		},
	}); err == nil {
		t.Fatal("metric_kind change = nil error")
	}

	// Existing label value_type is immutable.
	if _, err := svc.UpdateMetric(ctx, name, loggingstore.LogMetric{
		Filter:          "severity>=WARNING",
		ValueExtractor:  created.ValueExtractor,
		LabelExtractors: created.LabelExtractors,
		Descriptor: loggingstore.LogMetricDescriptor{
			ValueType: "DISTRIBUTION", Unit: "ms",
			Labels: []loggingstore.LogMetricLabel{{Key: "code", ValueType: "STRING"}},
		},
	}); err == nil {
		t.Fatal("label value_type change = nil error")
	}

	// A new label may be added; a mutable field is updated.
	updated, err := svc.UpdateMetric(ctx, name, loggingstore.LogMetric{
		Filter:          "severity>=WARNING",
		ValueExtractor:  created.ValueExtractor,
		LabelExtractors: map[string]string{"code": "EXTRACT(jsonPayload.code)", "host": "EXTRACT(resource.labels.host)"},
		Descriptor: loggingstore.LogMetricDescriptor{
			ValueType: "DISTRIBUTION", Unit: "ms",
			Labels: []loggingstore.LogMetricLabel{
				{Key: "code", ValueType: "INT64"},
				{Key: "host", ValueType: "STRING"},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateMetric add label: %v", err)
	}
	if updated.Filter != "severity>=WARNING" || len(updated.Descriptor.Labels) != 2 {
		t.Fatalf("updated metric = %+v", updated)
	}
}

func TestMetricUpdateUpsertsAndPreservesImmutableDefaults(t *testing.T) {
	svc := NewService(loggingstore.NewMemoryStore(), "test")
	ctx := context.Background()

	// update on an absent metric creates it (metrics.update is create-or-update).
	created, err := svc.UpdateMetric(ctx, "projects/test/metrics/new", loggingstore.LogMetric{Filter: "severity>=ERROR"})
	if err != nil || created.Name != "new" {
		t.Fatalf("upsert UpdateMetric = %+v, %v", created, err)
	}

	// a body name that disagrees with the resource name is rejected.
	if _, err := svc.UpdateMetric(ctx, "projects/test/metrics/new", loggingstore.LogMetric{Name: "other", Filter: "severity>=ERROR"}); err == nil {
		t.Fatal("mismatched name = nil error")
	} else if pe := providerErr(t, err); pe.HTTPStatus != 400 {
		t.Fatalf("mismatched name status = %d, want 400", pe.HTTPStatus)
	}

	// create a GAUGE metric, then update without metric_kind: the stored kind is
	// preserved rather than being misread as a change to the DELTA default.
	if _, err := svc.CreateMetric(ctx, "projects/test", loggingstore.LogMetric{
		Name:       "g",
		Filter:     "severity>=ERROR",
		Descriptor: loggingstore.LogMetricDescriptor{MetricKind: "GAUGE", ValueType: "INT64"},
	}); err != nil {
		t.Fatalf("create gauge: %v", err)
	}
	updated, err := svc.UpdateMetric(ctx, "projects/test/metrics/g", loggingstore.LogMetric{
		Filter:     "severity>=WARNING",
		Descriptor: loggingstore.LogMetricDescriptor{ValueType: "INT64"},
	})
	if err != nil {
		t.Fatalf("update without metric_kind: %v", err)
	}
	if updated.Descriptor.MetricKind != "GAUGE" || updated.Filter != "severity>=WARNING" {
		t.Fatalf("updated = %+v", updated)
	}
}

func TestMetricListNonPositivePageSize(t *testing.T) {
	svc := NewService(loggingstore.NewMemoryStore(), "test")
	ctx := context.Background()
	if _, err := svc.CreateMetric(ctx, "projects/test", loggingstore.LogMetric{Name: "m", Filter: "severity>=ERROR"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// The metrics list treats a non-positive page size as unset (not an error).
	page, _, err := svc.ListMetrics(ctx, "projects/test", -1, "")
	if err != nil || len(page) != 1 {
		t.Fatalf("ListMetrics(-1) = %+v, %v", page, err)
	}
}

func TestMetricListPagination(t *testing.T) {
	svc := NewService(loggingstore.NewMemoryStore(), "test")
	ctx := context.Background()
	for _, name := range []string{"a", "b", "c"} {
		if _, err := svc.CreateMetric(ctx, "projects/test", loggingstore.LogMetric{Name: name, Filter: "severity>=ERROR"}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	page, next, err := svc.ListMetrics(ctx, "projects/test", 2, "")
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	if len(page) != 2 || page[0].Name != "a" || page[1].Name != "b" || next == "" {
		t.Fatalf("page1 = %+v, next=%q", page, next)
	}
	page2, next2, err := svc.ListMetrics(ctx, "projects/test", 2, next)
	if err != nil || len(page2) != 1 || page2[0].Name != "c" || next2 != "" {
		t.Fatalf("page2 = %+v, next=%q, err=%v", page2, next2, err)
	}
}
