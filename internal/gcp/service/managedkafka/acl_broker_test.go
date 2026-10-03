package managedkafka

import (
	"context"
	"errors"
	"testing"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

var errBrokerDown = errors.New("broker down")

func aclEntry(principal, permission, operation string) AclEntryInput {
	return AclEntryInput{Principal: principal, PermissionType: permission, Operation: operation, Host: "*"}
}

// lastAclCall returns the most recent broker ACL mirror call.
func lastAclCall(t *testing.T, fb *fakeBroker) fakeAclCall {
	t.Helper()
	if len(fb.aclCalls) == 0 {
		t.Fatal("no ACL mirror call reached the broker")
	}
	return fb.aclCalls[len(fb.aclCalls)-1]
}

func TestAclMirrorsToBroker(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	alice := aclEntry("User:alice", "ALLOW", "READ")
	bob := aclEntry("User:bob", "DENY", "WRITE")

	a, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{alice}})
	if err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}
	c := lastAclCall(t, fb)
	if c.ResourceType != "TOPIC" || c.ResourceName != "orders" || c.PatternType != "LITERAL" {
		t.Fatalf("create mirror = %+v", c)
	}
	if len(c.Entries) != 1 || c.Entries[0].Principal != "User:alice" || c.Entries[0].PermissionType != "ALLOW" || c.Entries[0].Operation != "READ" || c.Entries[0].Host != "*" {
		t.Fatalf("create entries = %+v", c.Entries)
	}

	// Adding an entry redeploys the whole set.
	updated, created, err := s.AddAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", bob)
	if err != nil || created {
		t.Fatalf("AddAclEntry = created %v, err %v", created, err)
	}
	if entries := lastAclCall(t, fb).Entries; len(entries) != 2 || entries[1].Principal != "User:bob" {
		t.Fatalf("add mirror entries = %+v", entries)
	}

	// Update replaces the set under the etag OCC.
	if _, err := s.UpdateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{Etag: updated.Etag, AclEntries: []AclEntryInput{alice}}); err != nil {
		t.Fatalf("UpdateAcl: %v", err)
	}
	if entries := lastAclCall(t, fb).Entries; len(entries) != 1 || entries[0].Principal != "User:alice" {
		t.Fatalf("update mirror entries = %+v", entries)
	}

	// Removing the last entry deletes the broker ACL (empty entry set).
	if _, deleted, err := s.RemoveAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", alice); err != nil || !deleted {
		t.Fatalf("RemoveAclEntry = deleted %v, err %v", deleted, err)
	}
	if entries := lastAclCall(t, fb).Entries; len(entries) != 0 {
		t.Fatalf("remove-last mirror entries = %+v, want empty", entries)
	}
	if _, err := s.GetAcl(ctx, "proj", "us-central1", "c1", "topic/orders"); err == nil {
		t.Fatal("acl survived removing its last entry")
	}

	// A plain delete removes the broker bindings before the metadata.
	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "cluster", AclInput{AclEntries: []AclEntryInput{alice}}); err != nil {
		t.Fatalf("CreateAcl(cluster): %v", err)
	}
	before := len(fb.aclCalls)
	if err := s.DeleteAcl(ctx, "proj", "us-central1", "c1", "cluster"); err != nil {
		t.Fatalf("DeleteAcl: %v", err)
	}
	if len(fb.aclCalls) != before+1 {
		t.Fatalf("DeleteAcl made %d mirror calls, want 1", len(fb.aclCalls)-before)
	}
	if entries := lastAclCall(t, fb).Entries; len(entries) != 0 {
		t.Fatalf("delete mirror entries = %+v, want empty", entries)
	}
	if a.ResourceType != "TOPIC" || a.ResourceName != "orders" || a.PatternType != "LITERAL" {
		t.Errorf("stored pattern = %s/%s/%s", a.ResourceType, a.ResourceName, a.PatternType)
	}
}

func TestAclCreateRollsBackOnBrokerFailure(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092", replaceAclErr: errBrokerDown}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	_, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{aclEntry("User:a", "ALLOW", "READ")}})
	assertInternal(t, err)
	if _, err := s.GetAcl(ctx, "proj", "us-central1", "c1", "topic/orders"); err == nil {
		t.Fatal("acl survived a failed broker mirror")
	}
	// The failed mirror may have written partial bindings; a compensation call
	// tries to clear them.
	if c := lastAclCall(t, fb); len(c.Entries) != 0 {
		t.Fatalf("compensation entries = %+v, want empty", c.Entries)
	}
}

func TestAclUpdateRollsBackOnBrokerFailure(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	alice := aclEntry("User:alice", "ALLOW", "READ")
	a, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{alice}})
	if err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}

	fb.replaceAclErr = errBrokerDown
	_, err = s.UpdateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{Etag: a.Etag, AclEntries: []AclEntryInput{aclEntry("User:b", "DENY", "WRITE")}})
	assertInternal(t, err)
	got, gerr := s.GetAcl(ctx, "proj", "us-central1", "c1", "topic/orders")
	if gerr != nil {
		t.Fatalf("GetAcl after rollback: %v", gerr)
	}
	if len(got.AclEntries) != 1 || got.AclEntries[0].Principal != "User:alice" {
		t.Fatalf("entries = %+v, want the pre-update alice entry restored", got.AclEntries)
	}
	// The broker is re-told the pre-update entries, because the failed full
	// replace already deleted them.
	if c := lastAclCall(t, fb); len(c.Entries) != 1 || c.Entries[0].Principal != "User:alice" {
		t.Fatalf("compensation entries = %+v, want alice", c.Entries)
	}
}

func TestAclAddRemoveNoOpSkipsBroker(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	alice := aclEntry("User:alice", "ALLOW", "READ")
	if _, _, err := s.AddAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", alice); err != nil {
		t.Fatalf("AddAclEntry(create): %v", err)
	}
	calls := len(fb.aclCalls)

	// Re-adding an existing entry is a no-op and must not touch the broker.
	if _, created, err := s.AddAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", alice); err != nil || created {
		t.Fatalf("AddAclEntry(dup) = created %v, err %v", created, err)
	}
	if len(fb.aclCalls) != calls {
		t.Fatalf("duplicate add made %d extra broker calls", len(fb.aclCalls)-calls)
	}

	// Removing an entry that is not present is a no-op too.
	if _, _, err := s.RemoveAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", aclEntry("User:bob", "DENY", "WRITE")); err != nil {
		t.Fatalf("RemoveAclEntry(absent): %v", err)
	}
	if len(fb.aclCalls) != calls {
		t.Fatalf("absent remove made %d extra broker calls", len(fb.aclCalls)-calls)
	}
}

func TestAclDeleteBrokerFailureKeepsMetadata(t *testing.T) {
	ctx := context.Background()
	fb := &fakeBroker{endpoint: "broker:9092"}
	s := NewService(mkstore.NewMemoryStore(), WithBroker(fb))
	withCluster(t, s)

	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{aclEntry("User:a", "ALLOW", "READ")}}); err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}
	fb.replaceAclErr = errBrokerDown
	assertInternal(t, s.DeleteAcl(ctx, "proj", "us-central1", "c1", "topic/orders"))
	if _, err := s.GetAcl(ctx, "proj", "us-central1", "c1", "topic/orders"); err != nil {
		t.Fatalf("metadata removed despite broker delete failure: %v", err)
	}
}

func TestAclNoBrokerIsMetadataOnly(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	withCluster(t, s)

	entry := aclEntry("User:a", "ALLOW", "READ")
	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{entry}}); err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}
	if err := s.DeleteAcl(ctx, "proj", "us-central1", "c1", "topic/orders"); err != nil {
		t.Fatalf("DeleteAcl: %v", err)
	}
}

func TestAclValidationRejectsUnmappableEntries(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())
	withCluster(t, s)

	for name, entry := range map[string]AclEntryInput{
		"empty principal": {Principal: "", PermissionType: "ALLOW", Operation: "READ", Host: "*"},
		"missing User:":   {Principal: "alice", PermissionType: "ALLOW", Operation: "READ", Host: "*"},
		"bad host":        {Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: "10.0.0.1"},
		"empty host":      {Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: ""},
		"bad permission":  {Principal: "User:a", PermissionType: "MAYBE", Operation: "READ", Host: "*"},
		"bad operation":   {Principal: "User:a", PermissionType: "ALLOW", Operation: "FLY", Host: "*"},
	} {
		_, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{entry}})
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "InvalidArgument" {
			t.Fatalf("%s: error = %v, want InvalidArgument", name, err)
		}
		if _, gerr := s.GetAcl(ctx, "proj", "us-central1", "c1", "topic/orders"); gerr == nil {
			t.Fatalf("%s: invalid acl was stored", name)
		}
	}

	// Case-insensitive permission/operation values are accepted (the API
	// documents them so).
	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/x", AclInput{AclEntries: []AclEntryInput{
		{Principal: "User:a", PermissionType: "allow", Operation: "idempotent_write", Host: "*"},
	}}); err != nil {
		t.Fatalf("case-insensitive entry rejected: %v", err)
	}

	// The documented 100-entry cap is enforced.
	entries := make([]AclEntryInput, 0, maxAclEntries+1)
	for i := 0; i <= maxAclEntries; i++ {
		entries = append(entries, AclEntryInput{Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: "*"})
	}
	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/many", AclInput{AclEntries: entries}); err == nil {
		t.Fatal("an acl with more than 100 entries was accepted")
	}
}
