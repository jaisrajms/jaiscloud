package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestMemoryMetricCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Now().UTC()

	m := LogMetric{
		Name:            "nginx/requests",
		Description:     "count",
		Filter:          "severity>=ERROR",
		ValueExtractor:  "EXTRACT(jsonPayload.latency)",
		LabelExtractors: map[string]string{"code": "EXTRACT(jsonPayload.code)"},
		BucketOptions:   json.RawMessage(`{"linearBuckets":{"numFiniteBuckets":3,"width":1,"offset":0}}`),
		Descriptor: LogMetricDescriptor{
			MetricKind: "DELTA",
			ValueType:  "DISTRIBUTION",
			Unit:       "ms",
			Labels:     []LogMetricLabel{{Key: "code", ValueType: "INT64"}},
		},
		CreateTime: now,
		UpdateTime: now,
	}
	if err := s.CreateMetric(ctx, "projects/p", m); err != nil {
		t.Fatalf("CreateMetric: %v", err)
	}
	if err := s.CreateMetric(ctx, "projects/p", m); !errors.Is(err, ErrMetricExists) {
		t.Fatalf("duplicate CreateMetric = %v, want ErrMetricExists", err)
	}
	got, err := s.GetMetric(ctx, "projects/p", "nginx/requests")
	if err != nil || got.Name != m.Name || got.Descriptor.ValueType != "DISTRIBUTION" {
		t.Fatalf("GetMetric = %+v, %v", got, err)
	}
	if string(got.BucketOptions) != string(m.BucketOptions) {
		t.Fatalf("bucketOptions = %s, want %s", got.BucketOptions, m.BucketOptions)
	}
	// Mutating the returned metric must not affect the stored one (clone).
	got.LabelExtractors["code"] = "mutated"
	got.Descriptor.Labels[0].Description = "mutated"
	again, _ := s.GetMetric(ctx, "projects/p", "nginx/requests")
	if again.LabelExtractors["code"] != m.LabelExtractors["code"] || again.Descriptor.Labels[0].Description != "" {
		t.Fatalf("stored metric mutated through GetMetric: %+v", again)
	}
	if _, err := s.GetMetric(ctx, "projects/p", "missing"); !errors.Is(err, ErrMetricNotFound) {
		t.Fatalf("GetMetric missing = %v, want ErrMetricNotFound", err)
	}
	m.Filter = "severity>=WARNING"
	if err := s.UpdateMetric(ctx, "projects/p", m); err != nil {
		t.Fatalf("UpdateMetric: %v", err)
	}
	if err := s.UpdateMetric(ctx, "projects/p", LogMetric{Name: "missing"}); !errors.Is(err, ErrMetricNotFound) {
		t.Fatalf("UpdateMetric missing = %v", err)
	}
	if list, err := s.ListMetrics(ctx, "projects/p"); err != nil || len(list) != 1 {
		t.Fatalf("ListMetrics = %v, %v", list, err)
	}
	if err := s.DeleteMetric(ctx, "projects/p", "nginx/requests"); err != nil {
		t.Fatalf("DeleteMetric: %v", err)
	}
	if err := s.DeleteMetric(ctx, "projects/p", "nginx/requests"); !errors.Is(err, ErrMetricNotFound) {
		t.Fatalf("double DeleteMetric = %v", err)
	}
}

func TestMemorySnapshotRoundTripWithMetrics(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Now().UTC()
	if err := s.CreateMetric(ctx, "projects/p", LogMetric{
		Name:       "m1",
		Filter:     "severity>=ERROR",
		Descriptor: LogMetricDescriptor{MetricKind: "DELTA", ValueType: "INT64", Unit: "1"},
		CreateTime: now,
		UpdateTime: now,
	}); err != nil {
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
	if m, err := dst.GetMetric(ctx, "projects/p", "m1"); err != nil || m.Filter != "severity>=ERROR" {
		t.Fatalf("restored metric = %+v, %v", m, err)
	}
	if empty, err := dst.IsEmpty(ctx); err != nil || empty {
		t.Fatalf("IsEmpty = %v, %v, want false", empty, err)
	}
}
