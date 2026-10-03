package grpcconformance

import (
	"context"
	"fmt"

	managedkafka "cloud.google.com/go/managedkafka/apiv1"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// managedKafkaChecks covers the Managed Kafka v1 surface
// (google.cloud.managedkafka.v1.ManagedKafka) via the official generated
// cloud.google.com/go/managedkafka/apiv1 client: cluster CRUD (create/update/
// delete are long-running operations with the response packed inline, so the
// client's Wait observes it without polling), topic CRUD, ACL CRUD plus
// add/remove entry, and consumer-group list/get/update/delete. Consumer groups
// have no create RPC — a client creates one by committing offsets to a live
// broker — so in the hermetic (mock-broker) conformance environment the list is
// empty and the item methods report NotFound; the broker-backed data plane is
// exercised by tests/persistent_mode/gcp/managedkafka-broker (the MK6 gate).
//
// Every probe is self-contained: it ensures a run-unique cluster exists and
// then exercises one RPC. Names are run-unique via cfg.ResourceName, so a
// long-lived emulator never sees cross-run collisions.
func managedKafkaChecks() []Check {
	return []Check{
		{Service: "managedkafka", RPC: "CreateCluster", Method: "CreateCluster", KeyField: "LRO done + ACTIVE cluster", Run: checkMKCreateCluster},
		{Service: "managedkafka", RPC: "GetCluster", Method: "GetCluster", KeyField: "name/state round-trip", Run: checkMKGetCluster},
		{Service: "managedkafka", RPC: "ListClusters", Method: "ListClusters", KeyField: "created cluster present", Run: checkMKListClusters},
		{Service: "managedkafka", RPC: "UpdateCluster", Method: "UpdateCluster", KeyField: "labels updated via LRO", Run: checkMKUpdateCluster},
		{Service: "managedkafka", RPC: "DeleteCluster", Method: "DeleteCluster", KeyField: "LRO done + NotFound after", Run: checkMKDeleteCluster},
		{Service: "managedkafka", RPC: "CreateTopic", Method: "CreateTopic", KeyField: "partition/replication round-trip", Run: checkMKCreateTopic},
		{Service: "managedkafka", RPC: "GetTopic", Method: "GetTopic", KeyField: "name/fields round-trip", Run: checkMKGetTopic},
		{Service: "managedkafka", RPC: "ListTopics", Method: "ListTopics", KeyField: "created topic present", Run: checkMKListTopics},
		{Service: "managedkafka", RPC: "UpdateTopic", Method: "UpdateTopic", KeyField: "partitionCount updated", Run: checkMKUpdateTopic},
		{Service: "managedkafka", RPC: "DeleteTopic", Method: "DeleteTopic", KeyField: "NotFound after delete", Run: checkMKDeleteTopic},
		{Service: "managedkafka", RPC: "CreateAcl", Method: "CreateAcl", KeyField: "resourceType/etag derived", Run: checkMKCreateAcl},
		{Service: "managedkafka", RPC: "GetAcl", Method: "GetAcl", KeyField: "entries/etag round-trip", Run: checkMKGetAcl},
		{Service: "managedkafka", RPC: "ListAcls", Method: "ListAcls", KeyField: "created acl present", Run: checkMKListAcls},
		{Service: "managedkafka", RPC: "UpdateAcl", Method: "UpdateAcl", KeyField: "etag grows on update", Run: checkMKUpdateAcl},
		{Service: "managedkafka", RPC: "DeleteAcl", Method: "DeleteAcl", KeyField: "NotFound after delete", Run: checkMKDeleteAcl},
		{Service: "managedkafka", RPC: "AddAclEntry", Method: "AddAclEntry", KeyField: "entry appended", Run: checkMKAddAclEntry},
		{Service: "managedkafka", RPC: "RemoveAclEntry", Method: "RemoveAclEntry", KeyField: "last entry deletes acl", Run: checkMKRemoveAclEntry},
		{Service: "managedkafka", RPC: "ListConsumerGroups", Method: "ListConsumerGroups", KeyField: "empty set (no broker)", Run: checkMKListConsumerGroups},
		{Service: "managedkafka", RPC: "GetConsumerGroup", Method: "GetConsumerGroup", KeyField: "NotFound for an absent group", Run: checkMKGetConsumerGroup},
		{Service: "managedkafka", RPC: "UpdateConsumerGroup", Method: "UpdateConsumerGroup", KeyField: "NotFound for an absent group", Run: checkMKUpdateConsumerGroup},
		{Service: "managedkafka", RPC: "DeleteConsumerGroup", Method: "DeleteConsumerGroup", KeyField: "NotFound for an absent group", Run: checkMKDeleteConsumerGroup},
	}
}

const managedKafkaLocation = "us-central1"

// newManagedKafkaClient dials the emulator and returns the official generated
// Managed Kafka client.
func newManagedKafkaClient(ctx context.Context, cfg Config) (*managedkafka.Client, error) {
	return managedkafka.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// managedKafkaClusterName is the run-unique cluster the probes share.
func managedKafkaClusterName(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s/clusters/%s", cfg.Project, managedKafkaLocation, cfg.ResourceName("gcpc-grpc-mk"))
}

// ensureManagedKafkaCluster creates the run-unique probe cluster, treating
// AlreadyExists as success so repeated probes are idempotent.
func ensureManagedKafkaCluster(ctx context.Context, client *managedkafka.Client, cfg Config) (string, error) {
	op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    fmt.Sprintf("projects/%s/locations/%s", cfg.Project, managedKafkaLocation),
		ClusterId: cfg.ResourceName("gcpc-grpc-mk"),
		Cluster: &managedkafkapb.Cluster{
			Labels:         map[string]string{"probe": "gcpc"},
			CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472},
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return managedKafkaClusterName(cfg), nil
		}
		return "", fmt.Errorf("create cluster: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait cluster: %w", err)
	}
	return managedKafkaClusterName(cfg), nil
}

// Check 1: CreateCluster returns a done operation whose response is the ACTIVE
// cluster.
func checkMKCreateCluster(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-mk-create")
	wantName := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", cfg.Project, managedKafkaLocation, id)
	op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    fmt.Sprintf("projects/%s/locations/%s", cfg.Project, managedKafkaLocation),
		ClusterId: id,
		Cluster: &managedkafkapb.Cluster{
			Labels:         map[string]string{"probe": "gcpc"},
			CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateCluster operation not done")
	}
	meta, err := op.Metadata()
	if err != nil {
		return fmt.Errorf("operation metadata: %w", err)
	}
	if meta.GetVerb() != "create" {
		return fmt.Errorf("operation verb = %q, want create", meta.GetVerb())
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetState() != managedkafkapb.Cluster_ACTIVE {
		return fmt.Errorf("cluster state = %v, want ACTIVE", cluster.GetState())
	}
	if cluster.GetName() != wantName {
		return fmt.Errorf("cluster name = %q, want %q", cluster.GetName(), wantName)
	}
	if cluster.GetLabels()["probe"] != "gcpc" {
		return fmt.Errorf("cluster labels = %v", cluster.GetLabels())
	}
	return nil
}

// Check 2: GetCluster round-trips.
func checkMKGetCluster(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	got, err := client.GetCluster(ctx, &managedkafkapb.GetClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetName() != name || got.GetState() != managedkafkapb.Cluster_ACTIVE {
		return fmt.Errorf("GetCluster = %+v", got)
	}
	return nil
}

// Check 3: ListClusters includes the probe cluster.
func checkMKListClusters(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	it := client.ListClusters(ctx, &managedkafkapb.ListClustersRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s", cfg.Project, managedKafkaLocation),
	})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListClusters did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListClusters: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

// Check 4: UpdateCluster applies labels through an LRO.
func checkMKUpdateCluster(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.UpdateCluster(ctx, &managedkafkapb.UpdateClusterRequest{
		Cluster:    &managedkafkapb.Cluster{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateCluster: %w", err)
	}
	cluster, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if cluster.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("updated labels = %v", cluster.GetLabels())
	}
	return nil
}

// Check 5: DeleteCluster returns a done operation and the cluster is then gone.
func checkMKDeleteCluster(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	id := cfg.ResourceName("gcpc-grpc-mk-del")
	name := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", cfg.Project, managedKafkaLocation, id)
	if op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    fmt.Sprintf("projects/%s/locations/%s", cfg.Project, managedKafkaLocation),
		ClusterId: id,
		Cluster:   &managedkafkapb.Cluster{CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472}},
	}); err != nil {
		return fmt.Errorf("create: %w", err)
	} else if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("create wait: %w", err)
	}
	delOp, err := client.DeleteCluster(ctx, &managedkafkapb.DeleteClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteCluster: %w", err)
	}
	if !delOp.Done() {
		return fmt.Errorf("delete operation not done")
	}
	if err := delOp.Wait(ctx); err != nil {
		return fmt.Errorf("delete wait: %w", err)
	}
	if _, err := client.GetCluster(ctx, &managedkafkapb.GetClusterRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetCluster after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── Topics ───────────────────────────────────────────────────────────────────

func managedKafkaTopicName(cluster, topicID string) string { return cluster + "/topics/" + topicID }

func ensureManagedKafkaTopic(ctx context.Context, client *managedkafka.Client, cluster, topicID string) (string, error) {
	t, err := client.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
		Parent:  cluster,
		TopicId: topicID,
		Topic:   &managedkafkapb.Topic{PartitionCount: 3, ReplicationFactor: 2},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return managedKafkaTopicName(cluster, topicID), nil
		}
		return "", fmt.Errorf("CreateTopic: %w", err)
	}
	return t.GetName(), nil
}

// Check 6: CreateTopic round-trips.
func checkMKCreateTopic(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	id := cfg.ResourceName("gcpc-grpc-topic")
	t, err := client.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
		Parent:  cluster,
		TopicId: id,
		Topic:   &managedkafkapb.Topic{PartitionCount: 3, ReplicationFactor: 2},
	})
	if err != nil {
		return fmt.Errorf("CreateTopic: %w", err)
	}
	if t.GetName() != managedKafkaTopicName(cluster, id) {
		return fmt.Errorf("topic name = %q", t.GetName())
	}
	if t.GetPartitionCount() != 3 || t.GetReplicationFactor() != 2 {
		return fmt.Errorf("topic = %+v", t)
	}
	return nil
}

// Check 7: GetTopic round-trips.
func checkMKGetTopic(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureManagedKafkaTopic(ctx, client, cluster, cfg.ResourceName("gcpc-grpc-topic-get"))
	if err != nil {
		return err
	}
	got, err := client.GetTopic(ctx, &managedkafkapb.GetTopicRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetTopic: %w", err)
	}
	if got.GetName() != name || got.GetReplicationFactor() != 2 {
		return fmt.Errorf("GetTopic = %+v", got)
	}
	return nil
}

// Check 8: ListTopics includes the probe topic.
func checkMKListTopics(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureManagedKafkaTopic(ctx, client, cluster, cfg.ResourceName("gcpc-grpc-topic-list"))
	if err != nil {
		return err
	}
	it := client.ListTopics(ctx, &managedkafkapb.ListTopicsRequest{Parent: cluster})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListTopics did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListTopics: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

// Check 9: UpdateTopic changes the partition count.
func checkMKUpdateTopic(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureManagedKafkaTopic(ctx, client, cluster, cfg.ResourceName("gcpc-grpc-topic-upd"))
	if err != nil {
		return err
	}
	got, err := client.UpdateTopic(ctx, &managedkafkapb.UpdateTopicRequest{
		Topic:      &managedkafkapb.Topic{Name: name, PartitionCount: 6},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"partition_count"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateTopic: %w", err)
	}
	if got.GetPartitionCount() != 6 {
		return fmt.Errorf("partitionCount = %d, want 6", got.GetPartitionCount())
	}
	return nil
}

// Check 10: DeleteTopic removes the topic.
func checkMKDeleteTopic(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	name, err := ensureManagedKafkaTopic(ctx, client, cluster, cfg.ResourceName("gcpc-grpc-topic-del"))
	if err != nil {
		return err
	}
	if err := client.DeleteTopic(ctx, &managedkafkapb.DeleteTopicRequest{Name: name}); err != nil {
		return fmt.Errorf("DeleteTopic: %w", err)
	}
	if _, err := client.GetTopic(ctx, &managedkafkapb.GetTopicRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetTopic after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── ACLs ─────────────────────────────────────────────────────────────────────

func managedKafkaAclName(cluster, aclID string) string { return cluster + "/acls/" + aclID }

func managedKafkaAclEntry(principal, permission, operation string) *managedkafkapb.AclEntry {
	return &managedkafkapb.AclEntry{Principal: principal, PermissionType: permission, Operation: operation, Host: "*"}
}

func ensureManagedKafkaAcl(ctx context.Context, client *managedkafka.Client, cluster, aclID string, entries ...*managedkafkapb.AclEntry) (*managedkafkapb.Acl, error) {
	acl, err := client.CreateAcl(ctx, &managedkafkapb.CreateAclRequest{
		Parent: cluster,
		AclId:  aclID,
		Acl:    &managedkafkapb.Acl{AclEntries: entries},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return client.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: managedKafkaAclName(cluster, aclID)})
		}
		return nil, fmt.Errorf("CreateAcl: %w", err)
	}
	return acl, nil
}

// Check 11: CreateAcl derives the resource pattern and an etag.
func checkMKCreateAcl(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	acl, err := client.CreateAcl(ctx, &managedkafkapb.CreateAclRequest{
		Parent: cluster,
		AclId:  "topic/" + cfg.ResourceName("gcpc-grpc-acl"),
		Acl:    &managedkafkapb.Acl{AclEntries: []*managedkafkapb.AclEntry{managedKafkaAclEntry("User:probe@example.com", "ALLOW", "READ")}},
	})
	if err != nil {
		return fmt.Errorf("CreateAcl: %w", err)
	}
	if acl.GetResourceType() != "TOPIC" || acl.GetPatternType() != "LITERAL" {
		return fmt.Errorf("acl pattern = %s/%s", acl.GetResourceType(), acl.GetPatternType())
	}
	if acl.GetEtag() == "" {
		return fmt.Errorf("acl etag is empty")
	}
	if len(acl.GetAclEntries()) != 1 {
		return fmt.Errorf("acl entries = %v", acl.GetAclEntries())
	}
	return nil
}

// Check 12: GetAcl round-trips.
func checkMKGetAcl(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	aclID := "topic/" + cfg.ResourceName("gcpc-grpc-acl-get")
	created, err := ensureManagedKafkaAcl(ctx, client, cluster, aclID, managedKafkaAclEntry("User:probe@example.com", "ALLOW", "READ"))
	if err != nil {
		return err
	}
	got, err := client.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: created.GetName()})
	if err != nil {
		return fmt.Errorf("GetAcl: %w", err)
	}
	if got.GetName() != managedKafkaAclName(cluster, aclID) || len(got.GetAclEntries()) != 1 {
		return fmt.Errorf("GetAcl = %+v", got)
	}
	return nil
}

// Check 13: ListAcls includes the probe acl.
func checkMKListAcls(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	aclID := "topic/" + cfg.ResourceName("gcpc-grpc-acl-list")
	if _, err := ensureManagedKafkaAcl(ctx, client, cluster, aclID, managedKafkaAclEntry("User:probe@example.com", "ALLOW", "READ")); err != nil {
		return err
	}
	it := client.ListAcls(ctx, &managedkafkapb.ListAclsRequest{Parent: cluster})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListAcls did not include %q", aclID)
		}
		if err != nil {
			return fmt.Errorf("ListAcls: %w", err)
		}
		if got.GetName() == managedKafkaAclName(cluster, aclID) {
			return nil
		}
	}
}

// Check 14: UpdateAcl advances the etag.
func checkMKUpdateAcl(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	aclID := "topic/" + cfg.ResourceName("gcpc-grpc-acl-upd")
	created, err := ensureManagedKafkaAcl(ctx, client, cluster, aclID, managedKafkaAclEntry("User:probe@example.com", "ALLOW", "READ"))
	if err != nil {
		return err
	}
	updated, err := client.UpdateAcl(ctx, &managedkafkapb.UpdateAclRequest{
		Acl: &managedkafkapb.Acl{
			Name:       created.GetName(),
			Etag:       created.GetEtag(),
			AclEntries: []*managedkafkapb.AclEntry{managedKafkaAclEntry("User:probe@example.com", "DENY", "WRITE")},
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateAcl: %w", err)
	}
	if updated.GetEtag() == created.GetEtag() {
		return fmt.Errorf("UpdateAcl did not advance the etag")
	}
	if got := updated.GetAclEntries(); len(got) != 1 || got[0].GetPermissionType() != "DENY" {
		return fmt.Errorf("updated entries = %v", got)
	}
	return nil
}

// Check 15: DeleteAcl removes the acl.
func checkMKDeleteAcl(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	aclID := "topic/" + cfg.ResourceName("gcpc-grpc-acl-del")
	created, err := ensureManagedKafkaAcl(ctx, client, cluster, aclID, managedKafkaAclEntry("User:probe@example.com", "ALLOW", "READ"))
	if err != nil {
		return err
	}
	if err := client.DeleteAcl(ctx, &managedkafkapb.DeleteAclRequest{Name: created.GetName()}); err != nil {
		return fmt.Errorf("DeleteAcl: %w", err)
	}
	if _, err := client.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetAcl after delete = %v, want NotFound", err)
	}
	return nil
}

// Check 16: AddAclEntry appends an entry.
func checkMKAddAclEntry(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	aclID := "topic/" + cfg.ResourceName("gcpc-grpc-acl-add")
	created, err := ensureManagedKafkaAcl(ctx, client, cluster, aclID, managedKafkaAclEntry("User:probe@example.com", "ALLOW", "READ"))
	if err != nil {
		return err
	}
	resp, err := client.AddAclEntry(ctx, &managedkafkapb.AddAclEntryRequest{
		Acl:      created.GetName(),
		AclEntry: managedKafkaAclEntry("User:probe2@example.com", "DENY", "WRITE"),
	})
	if err != nil {
		return fmt.Errorf("AddAclEntry: %w", err)
	}
	if resp.GetAclCreated() {
		return fmt.Errorf("AddAclEntry reported aclCreated=true for an existing acl")
	}
	if len(resp.GetAcl().GetAclEntries()) != 2 {
		return fmt.Errorf("entries after add = %v", resp.GetAcl().GetAclEntries())
	}
	return nil
}

// Check 17: RemoveAclEntry on the last entry deletes the acl.
func checkMKRemoveAclEntry(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	aclID := "topic/" + cfg.ResourceName("gcpc-grpc-acl-rm")
	entry := managedKafkaAclEntry("User:probe@example.com", "ALLOW", "READ")
	created, err := ensureManagedKafkaAcl(ctx, client, cluster, aclID, entry)
	if err != nil {
		return err
	}
	resp, err := client.RemoveAclEntry(ctx, &managedkafkapb.RemoveAclEntryRequest{
		Acl:      created.GetName(),
		AclEntry: entry,
	})
	if err != nil {
		return fmt.Errorf("RemoveAclEntry: %w", err)
	}
	if !resp.GetAclDeleted() {
		return fmt.Errorf("removing the last entry did not delete the acl: %+v", resp)
	}
	if _, err := client.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetAcl after remove = %v, want NotFound", err)
	}
	return nil
}

// Check 18: ListConsumerGroups is empty (no broker data plane).
func checkMKListConsumerGroups(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	it := client.ListConsumerGroups(ctx, &managedkafkapb.ListConsumerGroupsRequest{Parent: cluster})
	groups := 0
	for {
		_, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListConsumerGroups: %w", err)
		}
		groups++
	}
	if groups != 0 {
		return fmt.Errorf("ListConsumerGroups returned %d groups, want 0", groups)
	}
	return nil
}

// Check 19: GetConsumerGroup on an absent group is NotFound. There is no create
// RPC, so the hermetic (mock-broker) environment has no groups.
func checkMKGetConsumerGroup(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	name := cluster + "/consumerGroups/" + cfg.ResourceName("gcpc-grpc-cg-get")
	if _, err := client.GetConsumerGroup(ctx, &managedkafkapb.GetConsumerGroupRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetConsumerGroup = %v, want NotFound", err)
	}
	return nil
}

// Check 20: UpdateConsumerGroup on an absent group is NotFound (the group must
// be created by a committing consumer on a live broker).
func checkMKUpdateConsumerGroup(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	name := cluster + "/consumerGroups/" + cfg.ResourceName("gcpc-grpc-cg-upd")
	_, err = client.UpdateConsumerGroup(ctx, &managedkafkapb.UpdateConsumerGroupRequest{
		ConsumerGroup: &managedkafkapb.ConsumerGroup{Name: name},
		UpdateMask:    &fieldmaskpb.FieldMask{Paths: []string{"topics"}},
	})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("UpdateConsumerGroup = %v, want NotFound", err)
	}
	return nil
}

// Check 21: DeleteConsumerGroup on an absent group is NotFound.
func checkMKDeleteConsumerGroup(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	cluster, err := ensureManagedKafkaCluster(ctx, client, cfg)
	if err != nil {
		return err
	}
	name := cluster + "/consumerGroups/" + cfg.ResourceName("gcpc-grpc-cg-del")
	if err := client.DeleteConsumerGroup(ctx, &managedkafkapb.DeleteConsumerGroupRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("DeleteConsumerGroup = %v, want NotFound", err)
	}
	return nil
}
