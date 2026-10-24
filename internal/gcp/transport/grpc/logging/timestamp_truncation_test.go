package logging

import (
	"testing"
	"time"

	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// TestEntryTimesKeepNanoseconds pins that the gRPC adapter preserves a log
// entry's own timestamp/receiveTimestamp: both are proto Timestamps with
// nanosecond accuracy (the Discovery), so unlike the server metadata fields
// below they are not truncated. TD48.
func TestEntryTimesKeepNanoseconds(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	e := entryToProto(loggingstore.LogEntry{Timestamp: instant, ReceiveTimestamp: instant})
	if got := e.GetTimestamp().AsTime(); !got.Equal(instant) {
		t.Fatalf("timestamp = %v, want %v (nanoseconds preserved)", got, instant)
	}
	if got := e.GetReceiveTimestamp().AsTime(); !got.Equal(instant) {
		t.Fatalf("receiveTimestamp = %v, want %v (nanoseconds preserved)", got, instant)
	}
}

// TestAdminTimesMicrosecondTruncation pins the same for a sink, an exclusion, a
// metric and a bucket (whose stored times are UnixNano).
func TestAdminTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	check := func(name string, got time.Time) {
		t.Helper()
		if !got.Equal(want) {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}

	sink := sinkToProto(loggingstore.LogSink{Name: "s1", CreateTime: instant, UpdateTime: instant})
	check("sink.createTime", sink.GetCreateTime().AsTime())
	check("sink.updateTime", sink.GetUpdateTime().AsTime())

	metric := metricToProto(loggingstore.LogMetric{Name: "m1", CreateTime: instant, UpdateTime: instant})
	check("metric.createTime", metric.GetCreateTime().AsTime())
	check("metric.updateTime", metric.GetUpdateTime().AsTime())

	bucket := bucketToProto("projects/proj", loggingstore.LogBucket{
		Name: "b1", CreateTime: instant.UnixNano(), UpdateTime: instant.UnixNano(),
	})
	check("bucket.createTime", bucket.GetCreateTime().AsTime())
	check("bucket.updateTime", bucket.GetUpdateTime().AsTime())
}
