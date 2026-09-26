package managedkafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	mkstore "jaiscloud/internal/gcp/store/managedkafka"
	"jaiscloud/internal/model"
)

func newCore() *Service { return NewService(mkstore.NewMemoryStore()) }

func clusterIn(labels map[string]string) ClusterInput {
	return ClusterInput{Labels: labels}
}

func topicIn(partitionCount, replicationFactor int) TopicInput {
	return TopicInput{PartitionCount: partitionCount, ReplicationFactor: replicationFactor}
}

func TestClusterCRUDAndLRO(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	c, op, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(map[string]string{"env": "test"}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if !op.Done {
		t.Fatalf("operation not done: %+v", op)
	}
	if c.Labels["env"] != "test" {
		t.Errorf("labels = %v, want env=test", c.Labels)
	}
	opJSON := OperationJSON(op, "proj")
	if opJSON["done"] != true {
		t.Errorf("done = %v, want true", opJSON["done"])
	}
	if meta, _ := opJSON["metadata"].(map[string]any); meta["verb"] != "create" {
		t.Errorf("metadata verb = %v, want create", meta["verb"])
	}
	if resp, _ := opJSON["response"].(map[string]any); resp["name"] != "projects/proj/locations/us-central1/clusters/c1" {
		t.Errorf("response name = %v", resp["name"])
	}
	if resp, _ := opJSON["response"].(map[string]any); resp["bootstrapAddress"] != "bootstrap.c1.us-central1.managedkafka.proj.cloud.goog" {
		t.Errorf("response bootstrapAddress = %v", resp["bootstrapAddress"])
	}

	// The operation is persisted and retrievable by id.
	got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if got.Verb != "create" || got.Target != "projects/proj/locations/us-central1/clusters/c1" {
		t.Errorf("operation = %+v", got)
	}

	if _, err := s.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("GetCluster: %v", err)
	}

	upd, updOp, err := s.UpdateCluster(ctx, "proj", "us-central1", "c1", clusterIn(map[string]string{"env": "prod"}))
	if err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	if updOp.Verb != "update" || upd.Labels["env"] != "prod" {
		t.Errorf("update = %+v op.verb=%q", upd, updOp.Verb)
	}

	ops, _, err := s.ListOperations(ctx, "proj", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(ops))
	}

	clusters, _, err := s.ListClusters(ctx, "proj", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(clusters))
	}

	delOp, err := s.DeleteCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if !delOp.Done || delOp.Verb != "delete" {
		t.Errorf("delete op = %+v", delOp)
	}
	if _, err := s.GetCluster(ctx, "proj", "us-central1", "c1"); err == nil {
		t.Fatal("expected NotFound after delete")
	}
}

func TestCreateClusterAlreadyExists(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil))
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "AlreadyExists" {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestCreateClusterMissingParams(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	for _, tc := range []struct{ location, id string }{{"", "c1"}, {"us-central1", ""}} {
		_, _, err := s.CreateCluster(ctx, "proj", tc.location, tc.id, clusterIn(nil))
		if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
			t.Errorf("location=%q id=%q: expected InvalidArgument, got %v", tc.location, tc.id, err)
		}
	}
}

func TestTopicCRUD(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	tp, err := s.CreateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(3, 2))
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if tp.PartitionCount != 3 || tp.ReplicationFactor != 2 {
		t.Errorf("topic = %+v", tp)
	}

	got, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1")
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.ReplicationFactor != 2 {
		t.Errorf("replicationFactor = %d, want 2", got.ReplicationFactor)
	}

	upd, err := s.UpdateTopic(ctx, "proj", "us-central1", "c1", "t1", topicIn(6, 0))
	if err != nil {
		t.Fatalf("UpdateTopic: %v", err)
	}
	if upd.PartitionCount != 6 || upd.ReplicationFactor != 2 {
		t.Errorf("updated topic = %+v (replicationFactor should be preserved)", upd)
	}

	topics, _, err := s.ListTopics(ctx, "proj", "us-central1", "c1", 0, "")
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}

	if err := s.DeleteTopic(ctx, "proj", "us-central1", "c1", "t1"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if _, err := s.GetTopic(ctx, "proj", "us-central1", "c1", "t1"); err == nil {
		t.Fatal("expected NotFound after delete")
	}
}

func TestCreateTopicClusterNotFound(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	_, err := s.CreateTopic(ctx, "proj", "us-central1", "missing", "t1", topicIn(1, 1))
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

// TestUpdateClusterConcurrentDisjointFieldsNoLostUpdate proves UpdateCluster's
// atomic get-merge-write cannot lose one of two concurrent disjoint-field
// updates.
func TestUpdateClusterConcurrentDisjointFieldsNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
		Labels: map[string]string{"env": "dev"},
		Config: []byte(`{"gcpConfig":{"kmsKey":"orig"}}`),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, _, errs[i] = s.UpdateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
				Labels: map[string]string{"i": string(rune('a' + i%26))},
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _, errs[perField+i] = s.UpdateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{
				Config: []byte(`{"gcpConfig":{"kmsKey":"` + string(rune('A'+i%26)) + `"}}`),
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	got, err := s.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Labels["env"] == "dev" {
		t.Errorf("labels reverted to stale value: %v", got.Labels)
	}
	js := ClusterJSON(got, "proj")
	if gcp, _ := js["gcpConfig"].(map[string]any); gcp["kmsKey"] == "orig" {
		t.Errorf("gcpConfig.kmsKey reverted to stale value: %v", gcp)
	}
}

func TestACLCRUDAndAddRemoveEntry(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	entry := AclEntryInput{Principal: "User:alice@example.com", PermissionType: "ALLOW", Operation: "READ", Host: "*"}
	a, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{entry}})
	if err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}
	if a.ResourceType != "TOPIC" || a.ResourceName != "orders" || a.PatternType != "LITERAL" {
		t.Errorf("derived = %s/%s/%s", a.ResourceType, a.ResourceName, a.PatternType)
	}
	if a.Etag == "" {
		t.Error("empty etag")
	}

	got, err := s.GetAcl(ctx, "proj", "us-central1", "c1", "topic/orders")
	if err != nil {
		t.Fatalf("GetAcl: %v", err)
	}
	if len(got.AclEntries) != 1 {
		t.Fatalf("entries = %v", got.AclEntries)
	}

	// Add a new entry.
	entry2 := AclEntryInput{Principal: "User:bob@example.com", PermissionType: "DENY", Operation: "WRITE", Host: "*"}
	updated, created, err := s.AddAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", entry2)
	if err != nil {
		t.Fatalf("AddAclEntry: %v", err)
	}
	if created {
		t.Error("acl should already exist")
	}
	if len(updated.AclEntries) != 2 {
		t.Fatalf("entries after add = %v", updated.AclEntries)
	}

	// Adding the same entry is a no-op.
	updated, _, err = s.AddAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", entry2)
	if err != nil {
		t.Fatalf("AddAclEntry dup: %v", err)
	}
	if len(updated.AclEntries) != 2 {
		t.Fatalf("entries after dup add = %v", updated.AclEntries)
	}

	// AddAclEntry creates a missing acl.
	createdAcl, created, err := s.AddAclEntry(ctx, "proj", "us-central1", "c1", "cluster", entry)
	if err != nil {
		t.Fatalf("AddAclEntry create: %v", err)
	}
	if !created || createdAcl.ResourceType != "CLUSTER" || createdAcl.ResourceName != "kafka-cluster" {
		t.Errorf("created acl = %+v created=%v", createdAcl, created)
	}

	// Update requires the matching etag.
	if _, err := s.UpdateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{entry}, Etag: "stale"}); err == nil {
		t.Fatal("expected Aborted on etag mismatch")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "Aborted" {
		t.Fatalf("expected Aborted, got %v", err)
	}
	if _, err := s.UpdateAcl(ctx, "proj", "us-central1", "c1", "topic/orders", AclInput{AclEntries: []AclEntryInput{entry2}, Etag: updated.Etag}); err != nil {
		t.Fatalf("UpdateAcl: %v", err)
	}

	// Remove both entries: the second removal deletes the acl.
	if _, deleted, err := s.RemoveAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", entry); err != nil || deleted {
		t.Fatalf("RemoveAclEntry first: deleted=%v err=%v", deleted, err)
	}
	acl, deleted, err := s.RemoveAclEntry(ctx, "proj", "us-central1", "c1", "topic/orders", entry2)
	if err != nil {
		t.Fatalf("RemoveAclEntry last: %v", err)
	}
	if !deleted || acl != nil {
		t.Fatalf("expected acl deletion, got deleted=%v acl=%v", deleted, acl)
	}
	if _, err := s.GetAcl(ctx, "proj", "us-central1", "c1", "topic/orders"); err == nil {
		t.Fatal("expected NotFound after acl delete")
	}

	// Acls list.
	acls, _, err := s.ListAcls(ctx, "proj", "us-central1", "c1", 0, "")
	if err != nil {
		t.Fatalf("ListAcls: %v", err)
	}
	if len(acls) != 1 {
		t.Fatalf("expected 1 acl, got %d", len(acls))
	}

	// Delete.
	if err := s.DeleteAcl(ctx, "proj", "us-central1", "c1", "cluster"); err != nil {
		t.Fatalf("DeleteAcl: %v", err)
	}
}

func TestCreateAclInvalidID(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	_, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "bogus", AclInput{AclEntries: []AclEntryInput{{Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: "*"}}})
	if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestCreateAndUpdateAclRejectEmptyEntries(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/x", AclInput{}); err == nil {
		t.Fatal("expected InvalidArgument for empty aclEntries")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	entry := AclEntryInput{Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: "*"}
	a, err := s.CreateAcl(ctx, "proj", "us-central1", "c1", "topic/x", AclInput{AclEntries: []AclEntryInput{entry}})
	if err != nil {
		t.Fatalf("CreateAcl: %v", err)
	}
	if _, err := s.UpdateAcl(ctx, "proj", "us-central1", "c1", "topic/x", AclInput{Etag: a.Etag}); err == nil {
		t.Fatal("expected InvalidArgument for empty aclEntries on update")
	}
}

func TestTopicJSONEchoesConfigs(t *testing.T) {
	tp := mkstore.Topic{
		Location: "us-central1", ClusterName: "c1", Name: "t1",
		PartitionCount: 3, ReplicationFactor: 2,
		Config: []byte(`{"configs":{"cleanup.policy":"compact"}}`),
	}
	js := TopicJSON(tp, "proj")
	cfg, ok := js["configs"].(map[string]any)
	if !ok || cfg["cleanup.policy"] != "compact" {
		t.Errorf("configs = %#v, want cleanup.policy=compact", js["configs"])
	}
}

func TestBootstrapAddress(t *testing.T) {
	if got, want := BootstrapAddress("proj", "us-central1", "c1"), "bootstrap.c1.us-central1.managedkafka.proj.cloud.goog"; got != want {
		t.Errorf("BootstrapAddress = %q, want %q", got, want)
	}
}

// TestClusterJSONIncludesBootstrapAddress guards the Java-compat gap where
// GetCluster omitted bootstrapAddress for an ACTIVE cluster.
func TestClusterJSONIncludesBootstrapAddress(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "europe-west1", "my-cluster", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := s.GetCluster(ctx, "proj", "europe-west1", "my-cluster")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	js := ClusterJSON(c, "proj")
	if js["bootstrapAddress"] != "bootstrap.my-cluster.europe-west1.managedkafka.proj.cloud.goog" {
		t.Errorf("bootstrapAddress = %v", js["bootstrapAddress"])
	}
}

func TestDeriveAclPattern(t *testing.T) {
	cases := map[string][3]string{
		"cluster":                   {"CLUSTER", "kafka-cluster", "LITERAL"},
		"allTopics":                 {"TOPIC", "*", "LITERAL"},
		"allConsumerGroups":         {"GROUP", "*", "LITERAL"},
		"allTransactionalIds":       {"TRANSACTIONAL_ID", "*", "LITERAL"},
		"topic/x":                   {"TOPIC", "x", "LITERAL"},
		"topicPrefixed/x":           {"TOPIC", "x", "PREFIXED"},
		"consumerGroup/x":           {"GROUP", "x", "LITERAL"},
		"consumerGroupPrefixed/x":   {"GROUP", "x", "PREFIXED"},
		"transactionalId/x":         {"TRANSACTIONAL_ID", "x", "LITERAL"},
		"transactionalIdPrefixed/x": {"TRANSACTIONAL_ID", "x", "PREFIXED"},
	}
	for id, want := range cases {
		rt, rn, pt, err := deriveAclPattern(id)
		if err != nil {
			t.Errorf("%s: %v", id, err)
			continue
		}
		if rt != want[0] || rn != want[1] || pt != want[2] {
			t.Errorf("%s = %s/%s/%s, want %v", id, rt, rn, pt, want)
		}
	}
	if _, _, _, err := deriveAclPattern("nope"); err == nil {
		t.Error("expected error for invalid acl id")
	}
}

func TestConsumerGroups(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	groups, next, err := s.ListConsumerGroups(ctx, "proj", "us-central1", "c1", 0, "")
	if err != nil {
		t.Fatalf("ListConsumerGroups: %v", err)
	}
	if len(groups) != 0 || next != "" {
		t.Errorf("groups=%v next=%q, want empty", groups, next)
	}

	// Listing under a missing cluster is NotFound.
	if _, _, err := s.ListConsumerGroups(ctx, "proj", "us-central1", "missing", 0, ""); err == nil {
		t.Error("expected NotFound for a missing cluster")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Errorf("expected NotFound, got %v", err)
	}

	for _, err := range []error{
		s.GetConsumerGroup(ctx, "proj", "us-central1", "c1", "g1"),
		s.UpdateConsumerGroup(ctx, "proj", "us-central1", "c1", "g1"),
		s.DeleteConsumerGroup(ctx, "proj", "us-central1", "c1", "g1"),
	} {
		var perr *model.ProviderError
		if !errors.As(err, &perr) || perr.Code != "NotFound" {
			t.Errorf("expected NotFound, got %v", err)
		}
	}
}

func TestReset(t *testing.T) {
	ctx := context.Background()
	s := newCore()
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c1", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	s.Reset(ctx)
	if _, err := s.GetCluster(ctx, "proj", "us-central1", "c1"); err == nil {
		t.Fatal("expected NotFound after Reset")
	}
}
