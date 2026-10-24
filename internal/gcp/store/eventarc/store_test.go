package eventarc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// jsonEqual compares two JSON documents by value. The memory store keeps raw
// bytes while Postgres stores JSONB (which canonicalizes whitespace and key
// order), so byte equality is not part of the store contract — value equality
// is. Both backends must agree semantically. Numbers are decoded as
// json.Number so "1" and "1.0" stay distinct (no float64 coercion).
func jsonEqual(a, b []byte) bool {
	av, aok := decodeJSON(a)
	bv, bok := decodeJSON(b)
	if !aok || !bok {
		return string(a) == string(b)
	}
	return reflect.DeepEqual(av, bv)
}

func decodeJSON(b []byte) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	return v, true
}

// runStoreTests exercises a Store against the shared test matrix. Backend tests
// (memory/postgres) call this so both implement the identical contract.
func runStoreTests(t *testing.T, s Store) {
	ctx := context.Background()
	defer s.Reset(ctx)

	if _, err := s.GetTrigger(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchTrigger {
		t.Fatalf("expected ErrNoSuchTrigger, got %v", err)
	}
	if _, err := s.GetChannel(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchChannel {
		t.Fatalf("expected ErrNoSuchChannel, got %v", err)
	}

	tr := Trigger{
		Name:   "my-trigger",
		Config: []byte(`{"destination":{"workflow":"projects/proj/locations/us-central1/workflows/w1"},"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`),
		Labels: map[string]string{"env": "dev"},
		UID:    "uid-1",
		Etag:   "etag-1",
	}
	if err := s.CreateTrigger(ctx, "proj", "us-central1", tr); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	if err := s.CreateTrigger(ctx, "proj", "us-central1", tr); err != ErrAlreadyExists {
		t.Fatalf("expected trigger ErrAlreadyExists, got %v", err)
	}

	got, err := s.GetTrigger(ctx, "proj", "us-central1", "my-trigger")
	if err != nil {
		t.Fatalf("get trigger: %v", err)
	}
	if got.Labels["env"] != "dev" || got.UID != "uid-1" || got.Etag != "etag-1" {
		t.Fatalf("trigger fields lost: %+v", got)
	}
	if !jsonEqual(got.Config, []byte(`{"destination":{"workflow":"projects/proj/locations/us-central1/workflows/w1"},"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`)) {
		t.Fatalf("config not verbatim: %s", got.Config)
	}

	// Atomic update preserves untouched fields (UID/Etag stay stable).
	updated, err := s.UpdateTriggerAtomic(ctx, "proj", "us-central1", "my-trigger", func(cur Trigger) (Trigger, error) {
		cur.Config = []byte(`{"destination":{"workflow":"projects/proj/locations/us-central1/workflows/w2"}}`)
		cur.Labels = map[string]string{"env": "prod"}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("update trigger atomic: %v", err)
	}
	if updated.UID != "uid-1" || updated.Etag != "etag-1" {
		t.Fatalf("uid/etag not stable across atomic update: %+v", updated)
	}
	if updated.Labels["env"] != "prod" {
		t.Fatalf("labels not updated: %+v", updated)
	}

	list, err := s.ListTriggers(ctx, "proj", "us-central1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list triggers: %v %d", err, len(list))
	}

	// Atomic delete aborts on a guard error without removing the trigger.
	guardErr := errors.New("guard refused")
	if err := s.DeleteTriggerAtomic(ctx, "proj", "us-central1", "my-trigger", func(Trigger) error { return guardErr }); !errors.Is(err, guardErr) {
		t.Fatalf("expected guard error, got %v", err)
	}
	if _, err := s.GetTrigger(ctx, "proj", "us-central1", "my-trigger"); err != nil {
		t.Fatalf("guard-aborted delete removed the trigger: %v", err)
	}
	if err := s.DeleteTrigger(ctx, "proj", "us-central1", "my-trigger"); err != nil {
		t.Fatalf("delete trigger: %v", err)
	}
	if _, err := s.GetTrigger(ctx, "proj", "us-central1", "my-trigger"); err != ErrNoSuchTrigger {
		t.Fatalf("expected ErrNoSuchTrigger after delete, got %v", err)
	}
	if err := s.DeleteTriggerAtomic(ctx, "proj", "us-central1", "my-trigger", func(Trigger) error { return nil }); err != ErrNoSuchTrigger {
		t.Fatalf("expected ErrNoSuchTrigger from atomic delete of missing trigger, got %v", err)
	}

	// Channels.
	ch := Channel{
		Name:            "my-channel",
		Config:          []byte(`{"provider":"projects/proj/locations/us-central1/providers/some.saas"}`),
		UID:             "uid-c",
		ActivationToken: "tok-c",
	}
	if err := s.CreateChannel(ctx, "proj", "us-central1", ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := s.CreateChannel(ctx, "proj", "us-central1", ch); err != ErrAlreadyExists {
		t.Fatalf("expected channel ErrAlreadyExists, got %v", err)
	}
	gotCh, err := s.GetChannel(ctx, "proj", "us-central1", "my-channel")
	if err != nil || gotCh.UID != "uid-c" || gotCh.ActivationToken != "tok-c" {
		t.Fatalf("get channel: %v %+v", err, gotCh)
	}
	chList, err := s.ListChannels(ctx, "proj", "us-central1")
	if err != nil || len(chList) != 1 {
		t.Fatalf("list channels: %v %d", err, len(chList))
	}

	// Cross-location lists (back the location-optional console): a second
	// channel in another location is returned, ordered by location then id.
	if err := s.CreateChannel(ctx, "proj", "europe-west1", Channel{Name: "aaa-channel", UID: "uid-c2"}); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	allCh, err := s.ListChannelsAllLocations(ctx, "proj")
	if err != nil || len(allCh) != 2 {
		t.Fatalf("list channels all locations: %v %d", err, len(allCh))
	}
	if allCh[0].Location != "europe-west1" || allCh[0].Name != "aaa-channel" || allCh[1].Location != "us-central1" {
		t.Fatalf("cross-location channel ordering wrong: %+v", allCh)
	}
	if other, err := s.ListChannelsAllLocations(ctx, "other-proj"); err != nil || len(other) != 0 {
		t.Fatalf("channels leaked across projects: %v %d", err, len(other))
	}
	if err := s.DeleteChannel(ctx, "proj", "europe-west1", "aaa-channel"); err != nil {
		t.Fatalf("delete second channel: %v", err)
	}
	if err := s.DeleteChannelAtomic(ctx, "proj", "us-central1", "my-channel", func(Channel) error { return guardErr }); !errors.Is(err, guardErr) {
		t.Fatalf("expected channel guard error, got %v", err)
	}
	if _, err := s.GetChannel(ctx, "proj", "us-central1", "my-channel"); err != nil {
		t.Fatalf("guard-aborted channel delete removed the channel: %v", err)
	}
	if err := s.DeleteChannel(ctx, "proj", "us-central1", "my-channel"); err != nil {
		t.Fatalf("delete channel: %v", err)
	}
	if _, err := s.GetChannel(ctx, "proj", "us-central1", "my-channel"); err != ErrNoSuchChannel {
		t.Fatalf("expected ErrNoSuchChannel after delete, got %v", err)
	}
	if err := s.DeleteChannelAtomic(ctx, "proj", "us-central1", "my-channel", func(Channel) error { return nil }); err != ErrNoSuchChannel {
		t.Fatalf("expected ErrNoSuchChannel from atomic delete of missing channel, got %v", err)
	}

	// Operations: the persisted done operations of control-plane mutations.
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "nope"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation, got %v", err)
	}
	op := Operation{
		ID:       "op-1",
		Verb:     "create",
		Target:   "projects/proj/locations/us-central1/triggers/my-trigger",
		Response: []byte(`{"@type":"type.googleapis.com/google.cloud.eventarc.v1.Trigger","name":"projects/proj/locations/us-central1/triggers/my-trigger"}`),
	}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	gotOp, err := s.GetOperation(ctx, "proj", "us-central1", "op-1")
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if gotOp.Verb != "create" || gotOp.Target != op.Target || !jsonEqual(gotOp.Response, op.Response) {
		t.Fatalf("operation fields lost: %+v", gotOp)
	}
	ops, err := s.ListOperations(ctx, "proj", "us-central1")
	if err != nil || len(ops) != 1 || ops[0].ID != "op-1" {
		t.Fatalf("list operations = %+v, %v", ops, err)
	}
	// Operations in another location are not listed.
	if ops, err := s.ListOperations(ctx, "proj", "europe-west1"); err != nil || len(ops) != 0 {
		t.Fatalf("other location operations = %+v, %v", ops, err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op-1"); err != nil {
		t.Fatalf("delete operation: %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op-1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation after delete, got %v", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op-1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation from delete of missing operation, got %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, NewMemoryStore())
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateTrigger(ctx, "p", "l", Trigger{Name: "t", Labels: map[string]string{"k": "v"}, Config: []byte(`{"eventFilters":[]}`), UID: "u", Etag: "e"})
	_ = s.CreateChannel(ctx, "p", "l", Channel{Name: "c", UID: "uc", Etag: "ec", ActivationToken: "tc", Config: []byte(`{"provider":"x"}`)})
	_ = s.CreateOperation(ctx, "p", "l", Operation{ID: "op", Verb: "create", Target: "target", Response: []byte(`{"@type":"x"}`)})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := s2.GetTrigger(ctx, "p", "l", "t")
	if err != nil || got.Labels["k"] != "v" || got.UID != "u" {
		t.Fatalf("trigger lost after restore: %v %+v", err, got)
	}
	gotCh, err := s2.GetChannel(ctx, "p", "l", "c")
	if err != nil || gotCh.ActivationToken != "tc" || gotCh.Etag != "ec" {
		t.Fatalf("channel lost after restore: %v %+v", err, gotCh)
	}
	gotOp, err := s2.GetOperation(ctx, "p", "l", "op")
	if err != nil || gotOp.Verb != "create" || !jsonEqual(gotOp.Response, []byte(`{"@type":"x"}`)) {
		t.Fatalf("operation lost after restore: %v %+v", err, gotOp)
	}
}
