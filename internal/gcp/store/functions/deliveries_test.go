package functions

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestMemoryStoreDeliveryCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	d := Delivery{
		ID: "d1", Location: "us-central1", FunctionID: "fn", Source: "pubsub",
		EventType: "google.pubsub.topic.publish", Resource: "projects/proj/topics/t",
		Data: "hello", Attributes: map[string]string{"k": "v"}, Status: DeliveryPending,
	}
	if err := s.CreateDelivery(ctx, "proj", "us-central1", d); err != nil {
		t.Fatalf("create delivery: %v", err)
	}
	got, err := s.GetDelivery(ctx, "proj", "us-central1", "d1")
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if got.FunctionID != "fn" || got.Status != DeliveryPending || got.Attributes["k"] != "v" || got.CreateTime.IsZero() {
		t.Fatalf("delivery = %+v", got)
	}

	got.Status = DeliveryDelivered
	got.Attempts = 1
	got.Result = "hello"
	got.UpdateTime = time.Now()
	if err := s.UpdateDelivery(ctx, "proj", "us-central1", got); err != nil {
		t.Fatalf("update delivery: %v", err)
	}
	if got, _ = s.GetDelivery(ctx, "proj", "us-central1", "d1"); got.Status != DeliveryDelivered || got.Result != "hello" {
		t.Fatalf("updated delivery = %+v", got)
	}

	// Location- and project-scoped.
	if _, err := s.GetDelivery(ctx, "proj", "europe-west1", "d1"); err != ErrNoSuchDelivery {
		t.Fatalf("expected ErrNoSuchDelivery for other location, got %v", err)
	}
	if err := s.UpdateDelivery(ctx, "proj", "us-central1", Delivery{ID: "missing"}); err != ErrNoSuchDelivery {
		t.Fatalf("expected ErrNoSuchDelivery updating missing, got %v", err)
	}

	list, err := s.ListDeliveries(ctx, "proj", "us-central1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list deliveries = %v, %v", list, err)
	}

	s.Reset(ctx)
	if _, err := s.GetDelivery(ctx, "proj", "us-central1", "d1"); err != ErrNoSuchDelivery {
		t.Fatalf("expected ErrNoSuchDelivery after reset, got %v", err)
	}
}

func TestMemoryStoreSnapshotKeepsDeliveries(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateDelivery(ctx, "proj", "us-central1", Delivery{
		ID: "d1", Location: "us-central1", FunctionID: "fn", Status: DeliveryDeadLetter, Attempts: 3,
	}); err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := dst.GetDelivery(ctx, "proj", "us-central1", "d1")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.Status != DeliveryDeadLetter || got.Attempts != 3 {
		t.Fatalf("restored delivery = %+v", got)
	}
	empty, err := dst.IsEmpty(ctx)
	if err != nil || empty {
		t.Fatalf("store with a delivery reported empty: empty=%v err=%v", empty, err)
	}
}
