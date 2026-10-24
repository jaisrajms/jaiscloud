package monitoring

import (
	"testing"
	"time"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// TestPointTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// server google-datetime instant to the microsecond the REST renderer
// (transport/rest/monitoring) emits.
func TestPointTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	p := pointToProto(monitoringstore.Point{StartTime: instant, EndTime: instant})
	if got := p.GetInterval().GetStartTime().AsTime(); !got.Equal(want) {
		t.Fatalf("interval.startTime = %v, want %v", got, want)
	}
	if got := p.GetInterval().GetEndTime().AsTime(); !got.Equal(want) {
		t.Fatalf("interval.endTime = %v, want %v", got, want)
	}
}

// TestNotificationChannelTimesMicrosecondTruncation pins the same for a channel's
// creation/mutation records.
func TestNotificationChannelTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	c := notificationChannelToProto(monitoringstore.NotificationChannel{
		ID: "c1", CreateTime: instant, UpdateTime: instant,
	}, "proj")
	if got := c.GetCreationRecord().GetMutateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("creationRecord.mutateTime = %v, want %v", got, want)
	}
	if len(c.GetMutationRecords()) != 1 {
		t.Fatalf("mutationRecords = %d, want 1", len(c.GetMutationRecords()))
	}
	if got := c.GetMutationRecords()[0].GetMutateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("mutationRecords[0].mutateTime = %v, want %v", got, want)
	}
}
