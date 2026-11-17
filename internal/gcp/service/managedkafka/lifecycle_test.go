package managedkafka

import (
	"context"
	"testing"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// TestParkStopsBrokersAndKeepsMetadata proves Park tears down broker workloads
// (through ParkCluster, not the reaping StopCluster) while every cluster and
// topic record survives, and that Resume recreates each broker.
func TestParkStopsBrokersAndKeepsMetadata(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))

	for _, c := range []struct{ project, location, name string }{
		{"proj", "us-central1", "c1"},
		{"proj", "europe-west1", "c2"},
		{"other", "us-central1", "c3"},
	} {
		if _, _, err := s.CreateCluster(ctx, c.project, c.location, c.name, clusterIn(nil)); err != nil {
			t.Fatalf("CreateCluster %s: %v", c.name, err)
		}
	}
	if _, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "orders", topicIn(1, 1)); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	fb.ensured = nil // only count the resume calls

	res, err := s.Park(ctx, "proj")
	if err != nil {
		t.Fatalf("Park: %v", err)
	}
	if got := res["brokersStopped"]; got != 2 {
		t.Fatalf("brokersStopped = %v, want 2", got)
	}
	// A real broker has no PVC, so its topics go with the broker workloads; the
	// result must say so rather than implying a lossless park.
	if got, ok := res["topicDataLost"]; !ok || got != true {
		t.Fatalf("topicDataLost = %v (present=%v), want true with a real broker", got, ok)
	}
	if len(fb.parked) != 2 || fb.parked[0] != "proj/europe-west1/c2" || fb.parked[1] != "proj/us-central1/c1" {
		t.Fatalf("ParkCluster calls = %v", fb.parked)
	}
	if len(fb.stopped) != 0 {
		t.Fatalf("Park used the reaping StopCluster: %v", fb.stopped)
	}

	// Metadata survives the park.
	if _, err := s.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("cluster record lost by Park: %v", err)
	}
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "orders"); err != nil {
		t.Fatalf("topic record lost by Park: %v", err)
	}

	// Resume recreates every broker in the project (and only that project).
	fb.ensured = nil // GetCluster rehydrates its broker; count only resume calls
	if err := s.Resume(ctx, "proj"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if len(fb.ensured) != 2 || fb.ensured[0] != "proj/europe-west1/c2" || fb.ensured[1] != "proj/us-central1/c1" {
		t.Fatalf("EnsureCluster calls on resume = %v", fb.ensured)
	}
}

// TestParkWithoutBrokerIsMetadataOnly documents the mock topology: there is no
// broker manager to stop, so no broker is reported stopped; Park succeeds and
// every cluster record survives.
func TestParkWithoutBrokerIsMetadataOnly(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	res, err := s.Park(ctx, "proj")
	if err != nil {
		t.Fatalf("Park: %v", err)
	}
	if got := res["brokersStopped"]; got != 0 {
		t.Fatalf("brokersStopped = %v, want 0 (no broker manager in mock mode)", got)
	}
	// Mock mode has no broker and therefore no topic data to lose: the field is
	// omitted rather than claimed as false.
	if got, ok := res["topicDataLost"]; ok {
		t.Fatalf("topicDataLost = %v in mock mode, want the field omitted (no broker, no topic data)", got)
	}
	if err := s.Resume(ctx, "proj"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if s.Name() != "managedkafka" {
		t.Fatalf("Name() = %q, want managedkafka", s.Name())
	}
}
