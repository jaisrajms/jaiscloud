package managedkafka

import (
	"context"
	"testing"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// TestMockConsumerGroupFromGroupAcl proves the mock topology's consumer-group
// registry: without a live broker there is no group coordinator, so a LITERAL
// GROUP ACL (the control-plane reference that names a group) seeds the group
// record, after which get/update/delete work and an offset reset is visible.
func TestMockConsumerGroupFromGroupAcl(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())

	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	// Before the GROUP ACL the group does not exist.
	if _, err := s.GetConsumerGroup(ctx, "proj", "us-central1", "c1", "g1"); err == nil {
		t.Fatal("GetConsumerGroup succeeded before the group was seeded")
	}
	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "consumerGroup/g1", AclInput{
		AclEntries: []AclEntryInput{{Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: "*"}},
	}); err != nil {
		t.Fatalf("CreateAcl(group): %v", err)
	}

	got, err := s.GetConsumerGroup(ctx, "proj", "us-central1", "c1", "g1")
	if err != nil {
		t.Fatalf("GetConsumerGroup: %v", err)
	}
	if got.Name != "g1" || len(got.Topics) != 0 {
		t.Fatalf("seeded group = %+v, want an empty group", got)
	}
	ids, _, err := s.ListConsumerGroups(ctx, "proj", "us-central1", "c1", ConsumerGroupViewBasic, "", 0, "")
	if err != nil || len(ids) != 1 || ids[0].Name != "g1" {
		t.Fatalf("ListConsumerGroups = %+v, %v", ids, err)
	}

	updated, err := s.UpdateConsumerGroup(ctx, "proj", "us-central1", "c1", "g1", ConsumerGroupInput{
		Offsets: []ConsumerGroupOffset{{Topic: "orders", Partition: 0, Offset: 4}},
	}, []string{"topics"})
	if err != nil {
		t.Fatalf("UpdateConsumerGroup: %v", err)
	}
	if len(updated.Topics) != 1 || updated.Topics["projects/proj/locations/us-central1/clusters/c1/topics/orders"] == nil {
		t.Fatalf("updated topics = %+v", updated.Topics)
	}

	read, err := s.GetConsumerGroup(ctx, "proj", "us-central1", "c1", "g1")
	if err != nil {
		t.Fatalf("GetConsumerGroup after update: %v", err)
	}
	if p := read.Topics["projects/proj/locations/us-central1/clusters/c1/topics/orders"]; p == nil || p.Partitions[0].Offset != 4 {
		t.Fatalf("committed offsets not visible: %+v", read.Topics)
	}

	// A wildcard ACL names no concrete group, so it seeds nothing.
	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "allConsumerGroups", AclInput{
		AclEntries: []AclEntryInput{{Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: "*"}},
	}); err != nil {
		t.Fatalf("CreateAcl(allConsumerGroups): %v", err)
	}
	if _, err := s.GetConsumerGroup(ctx, "proj", "us-central1", "c1", "*"); err == nil {
		t.Fatal("wildcard ACL seeded a group named *")
	}

	if err := s.DeleteConsumerGroup(ctx, "proj", "us-central1", "c1", "g1"); err != nil {
		t.Fatalf("DeleteConsumerGroup: %v", err)
	}
	if _, err := s.GetConsumerGroup(ctx, "proj", "us-central1", "c1", "g1"); err == nil {
		t.Fatal("GetConsumerGroup succeeded after delete")
	}
}

// TestMockConsumerGroupsDroppedWithCluster proves a recreated cluster does not
// inherit the previous cluster's mock groups.
func TestMockConsumerGroupsDroppedWithCluster(t *testing.T) {
	ctx := context.Background()
	s := NewService(mkstore.NewMemoryStore())

	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "consumerGroup/g1", AclInput{
		AclEntries: []AclEntryInput{{Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: "*"}},
	}); err != nil {
		t.Fatalf("CreateAcl(group): %v", err)
	}
	if _, err := s.DeleteCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("recreate cluster: %v", err)
	}
	if _, err := s.GetConsumerGroup(ctx, "proj", "us-central1", "c1", "g1"); err == nil {
		t.Fatal("recreated cluster inherited a mock consumer group")
	}
}
