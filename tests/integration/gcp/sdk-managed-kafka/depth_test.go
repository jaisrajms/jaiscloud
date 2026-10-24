// depth_test.go closes TD10's Managed Kafka p2–p5 gap. kafka_test.go proves the
// baseline lifecycle over the REST transport; this file drives the service
// through the official cloud.google.com/go/managedkafka/apiv1 client, which is
// gRPC-only, so it also gives the gRPC transport (internal/gcp/transport/grpc/
// managedkafka) its first behavioural suite.
//
//	p2 concurrency  - six goroutines race one clusterId (one winner, five
//	                  AlreadyExists losers); concurrent distinct topic creates
//	                  all land; concurrent AddAclEntry calls onto one ACL
//	                  neither lose an entry nor duplicate one; the client runs
//	                  under -race.
//	p3 errors       - the gRPC status codes for an unknown cluster/topic/ACL/
//	                  consumer group (NotFound), a duplicate create
//	                  (AlreadyExists), invalid topic and ACL input
//	                  (InvalidArgument) and a stale ACL etag (Aborted).
//	p4 listing      - cluster, topic and ACL pages walked with
//	                  pageSize/pageToken through the shared depth-test kit
//	                  (tests/integration/gcp/testdepth), asserting the
//	                  ascending name order across page boundaries, the terminal
//	                  empty token and a stable second walk. This is the kit's
//	                  reference adapter for a gRPC iterator client.
//	p5 scale        - a many-topic cluster plus the cluster create → operation →
//	                  get/update/delete long-running lifecycle (done inline,
//	                  persisted under operations.get).
//
// The Kafka data plane (produce/consume over the wire, consumer-group
// coordination and streamed record reads) needs a live broker, which the
// hermetic CI topology does not start; that half of p2/p4/p5 stays the local
// Docker gate in tests/persistent_mode/gcp/managedkafka-broker (see its
// docker_e2e_test.go, which this session extends with concurrent
// producers/consumers and the consumer-group API assertions).
//
// The pinned cloud.google.com/go/managedkafka proto (this module pins v1.2.0;
// v1.1.0, the emulator's pin, is field-identical) has no
// Cluster.bootstrap_address field, so the gRPC read-back is asserted on the
// metadata the proto does carry; the REST suite owns the bootstrapAddress
// assertion (J55/AUD3-11).
package kafka_test

import (
	"context"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	managedkafka "cloud.google.com/go/managedkafka/apiv1"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	testdepth "jaiscloud-gcp-testdepth"
)

// kafkaGRPCEndpoint returns the emulator's gRPC listener (8081 by default),
// stripping any scheme the caller included.
func kafkaGRPCEndpoint() string {
	host := os.Getenv("GCP_EMULATOR_ENDPOINT_GRPC")
	if host == "" {
		host = "localhost:8081"
	}
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	return host
}

func kafkaGRPCClient(t *testing.T) *managedkafka.Client {
	t.Helper()
	client, err := managedkafka.NewClient(context.Background(),
		option.WithEndpoint(kafkaGRPCEndpoint()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// kafkaGRPCParent is the location parent every resource in this file lives
// under.
func kafkaGRPCParent() string {
	return "projects/" + kafkaProject() + "/locations/us-central1"
}

func kafkaClusterResource(id string) string { return kafkaGRPCParent() + "/clusters/" + id }
func kafkaTopicResource(cluster, topic string) string {
	return kafkaClusterResource(cluster) + "/topics/" + topic
}
func kafkaAclResource(cluster, aclID string) string {
	return kafkaClusterResource(cluster) + "/acls/" + aclID
}
func kafkaConsumerGroupResource(cluster, group string) string {
	return kafkaClusterResource(cluster) + "/consumerGroups/" + group
}

// leaf returns the last path segment of a resource name.
func leaf(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// sortedCopy returns a name-sorted copy of ids, so a paged walk (which the
// service orders by name) can be compared against a creation-ordered set.
func sortedCopy(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

// kafkaClusterSpec is the minimal cluster body the API accepts.
func kafkaClusterSpec() *managedkafkapb.Cluster {
	return &managedkafkapb.Cluster{
		CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472},
		PlatformConfig: &managedkafkapb.Cluster_GcpConfig{
			GcpConfig: &managedkafkapb.GcpConfig{AccessConfig: &managedkafkapb.AccessConfig{}},
		},
	}
}

// createKafkaDepthCluster creates a cluster and waits for its done operation,
// registering cleanup.
func createKafkaDepthCluster(t *testing.T, client *managedkafka.Client) string {
	t.Helper()
	id := uniqueName("depth-cluster")
	op, err := client.CreateCluster(context.Background(), &managedkafkapb.CreateClusterRequest{
		Parent:    kafkaGRPCParent(),
		ClusterId: id,
		Cluster:   kafkaClusterSpec(),
	})
	require.NoError(t, err)
	cluster, err := op.Wait(context.Background())
	require.NoError(t, err)
	require.Equal(t, managedkafkapb.Cluster_ACTIVE, cluster.GetState())
	t.Cleanup(func() { deleteKafkaDepthCluster(t, client, id) })
	return id
}

// deleteKafkaDepthCluster deletes a cluster in us-central1, ignoring a cluster
// that is already gone so cleanup never fails the test.
func deleteKafkaDepthCluster(t *testing.T, client *managedkafka.Client, id string) {
	t.Helper()
	deleteKafkaDepthClusterAt(t, client, kafkaGRPCParent(), id)
}

// deleteKafkaDepthClusterAt deletes a cluster under an explicit parent.
func deleteKafkaDepthClusterAt(t *testing.T, client *managedkafka.Client, parent, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	op, err := client.DeleteCluster(ctx, &managedkafkapb.DeleteClusterRequest{Name: parent + "/clusters/" + id})
	if err != nil {
		return
	}
	_ = op.Wait(ctx)
}

// deleteAllClustersIn removes every cluster in a location, so a paging test
// over that location is deterministic against an emulator carrying a prior
// run's leftovers.
func deleteAllClustersIn(t *testing.T, client *managedkafka.Client, parent string) {
	t.Helper()
	ctx := context.Background()
	var ids []string
	it := client.ListClusters(ctx, &managedkafkapb.ListClustersRequest{Parent: parent})
	for {
		cluster, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		ids = append(ids, leaf(cluster.GetName()))
	}
	for _, id := range ids {
		deleteKafkaDepthClusterAt(t, client, parent, id)
	}
}

// wantGRPCCode asserts err carries the given gRPC status code.
func wantGRPCCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	require.Error(t, err)
	assert.Equal(t, code, status.Code(err), "unexpected status for %v", err)
}

// TestManagedKafkaDepthConcurrency is the p2 class: parallel writers against
// the control plane under -race — the create race, a lost-update-free topic
// burst and the ACL entry add contention the store's atomic update serialises.
func TestManagedKafkaDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	client := kafkaGRPCClient(t)

	// (1) Six goroutines race one clusterId: exactly one create wins and every
	// other call is AlreadyExists (the store's create is the arbiter, not the
	// client).
	t.Run("ClusterCreateRace", func(t *testing.T) {
		raceID := uniqueName("depth-race")
		defer deleteKafkaDepthCluster(t, client, raceID)

		const racers = 6
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners int
			losers  int
			other   []error
		)
		start := make(chan struct{})
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
					Parent:    kafkaGRPCParent(),
					ClusterId: raceID,
					Cluster:   kafkaClusterSpec(),
				})
				if err == nil {
					_, err = op.Wait(ctx)
				}
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					winners++
				case status.Code(err) == codes.AlreadyExists:
					losers++
				default:
					other = append(other, err)
				}
			}()
		}
		close(start)
		wg.Wait()

		require.Empty(t, other, "unexpected create errors")
		assert.Equal(t, 1, winners)
		assert.Equal(t, racers-1, losers)
	})

	// (2) Concurrent distinct topic creates against one cluster all land: no
	// create is lost and each read-back agrees with what was written.
	t.Run("TopicBurst", func(t *testing.T) {
		cluster := createKafkaDepthCluster(t, client)
		const topics = 8
		ids := make([]string, topics)
		errs := make([]error, topics)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < topics; i++ {
			ids[i] = uniqueName("depth-topic")
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, errs[i] = client.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
					Parent:  kafkaClusterResource(cluster),
					TopicId: ids[i],
					Topic:   &managedkafkapb.Topic{PartitionCount: 3, ReplicationFactor: 1},
				})
			}(i)
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			require.NoError(t, err, "topic %d", i)
		}
		for _, id := range ids {
			got, err := client.GetTopic(ctx, &managedkafkapb.GetTopicRequest{Name: kafkaTopicResource(cluster, id)})
			require.NoError(t, err)
			assert.Equal(t, int32(3), got.GetPartitionCount())
		}
	})

	// (3) Concurrent AddAclEntry calls onto one ACL neither lose nor duplicate
	// an entry: distinct principals all land, an entry added many times stays
	// single.
	t.Run("AclEntryContention", func(t *testing.T) {
		cluster := createKafkaDepthCluster(t, client)
		const (
			aclID   = "topic/orders"
			entries = 8
		)

		aclEntry := func(principal, operation string) *managedkafkapb.AclEntry {
			return &managedkafkapb.AclEntry{
				Principal:      "User:" + principal,
				PermissionType: "ALLOW",
				Operation:      operation,
				Host:           "*",
			}
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, entries)
		for i := 0; i < entries; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, errs[i] = client.AddAclEntry(ctx, &managedkafkapb.AddAclEntryRequest{
					Acl:      kafkaAclResource(cluster, aclID),
					AclEntry: aclEntry(string(rune('a'+i))+"@example.com", "READ"),
				})
			}(i)
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			require.NoError(t, err, "add %d", i)
		}
		got, err := client.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: kafkaAclResource(cluster, aclID)})
		require.NoError(t, err)
		assert.Len(t, got.GetAclEntries(), entries, "every distinct entry must land exactly once")

		// The same entry added concurrently many times is idempotent.
		start = make(chan struct{})
		dupErrs := make([]error, entries)
		for i := 0; i < entries; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, dupErrs[i] = client.AddAclEntry(ctx, &managedkafkapb.AddAclEntryRequest{
					Acl:      kafkaAclResource(cluster, aclID),
					AclEntry: aclEntry("dup@example.com", "WRITE"),
				})
			}(i)
		}
		close(start)
		wg.Wait()
		for i, err := range dupErrs {
			require.NoError(t, err, "duplicate add %d", i)
		}
		got, err = client.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: kafkaAclResource(cluster, aclID)})
		require.NoError(t, err)
		assert.Len(t, got.GetAclEntries(), entries+1, "a repeated entry must not be duplicated")

		// The acl id decides the resource pattern the API derives, and the
		// gRPC read-back must report it.
		assert.Equal(t, "TOPIC", got.GetResourceType())
		assert.Equal(t, "orders", got.GetResourceName())
		assert.Equal(t, "LITERAL", got.GetPatternType())
	})
}

// TestManagedKafkaDepthErrors is the p3 class: the gRPC status codes the real
// API returns for a missing resource, a duplicate, invalid input and a stale
// concurrency token.
func TestManagedKafkaDepthErrors(t *testing.T) {
	ctx := context.Background()
	client := kafkaGRPCClient(t)
	cluster := createKafkaDepthCluster(t, client)

	t.Run("UnknownCluster", func(t *testing.T) {
		missing := kafkaClusterResource(uniqueName("depth-missing"))
		_, err := client.GetCluster(ctx, &managedkafkapb.GetClusterRequest{Name: missing})
		wantGRPCCode(t, err, codes.NotFound)
		_, err = client.DeleteCluster(ctx, &managedkafkapb.DeleteClusterRequest{Name: missing})
		wantGRPCCode(t, err, codes.NotFound)
		// A child create under a cluster that does not exist is NotFound, not a
		// silent success.
		_, err = client.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
			Parent:  missing,
			TopicId: "orders",
			Topic:   &managedkafkapb.Topic{PartitionCount: 1, ReplicationFactor: 1},
		})
		wantGRPCCode(t, err, codes.NotFound)
		// A list is cluster-scoped (unlike a get, which is acl-scoped), so this
		// is the assertion that fails because the *cluster* is missing.
		aclIt := client.ListAcls(ctx, &managedkafkapb.ListAclsRequest{Parent: missing})
		_, err = aclIt.Next()
		wantGRPCCode(t, err, codes.NotFound)
		// A get is acl-scoped: NOT_FOUND for the absent acl.
		_, err = client.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: missing + "/acls/cluster"})
		wantGRPCCode(t, err, codes.NotFound)
		_, err = client.GetConsumerGroup(ctx, &managedkafkapb.GetConsumerGroupRequest{Name: missing + "/consumerGroups/g"})
		wantGRPCCode(t, err, codes.NotFound)
	})

	t.Run("DuplicateCluster", func(t *testing.T) {
		op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
			Parent:    kafkaGRPCParent(),
			ClusterId: cluster,
			Cluster:   kafkaClusterSpec(),
		})
		if err == nil {
			// A loser is rejected synchronously; treat a done-but-failed
			// operation as the equivalent failure.
			_, err = op.Wait(ctx)
		}
		wantGRPCCode(t, err, codes.AlreadyExists)
	})

	t.Run("InvalidTopic", func(t *testing.T) {
		_, err := client.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
			Parent:  kafkaClusterResource(cluster),
			TopicId: uniqueName("depth-bad"),
			Topic:   &managedkafkapb.Topic{PartitionCount: 0, ReplicationFactor: 1},
		})
		wantGRPCCode(t, err, codes.InvalidArgument)
	})

	t.Run("UnknownTopic", func(t *testing.T) {
		_, err := client.GetTopic(ctx, &managedkafkapb.GetTopicRequest{Name: kafkaTopicResource(cluster, "no-such-topic")})
		wantGRPCCode(t, err, codes.NotFound)
		err = client.DeleteTopic(ctx, &managedkafkapb.DeleteTopicRequest{Name: kafkaTopicResource(cluster, "no-such-topic")})
		wantGRPCCode(t, err, codes.NotFound)
	})

	t.Run("InvalidAcl", func(t *testing.T) {
		entry := &managedkafkapb.AclEntry{Principal: "User:x@example.com", PermissionType: "ALLOW", Operation: "READ", Host: "*"}

		// A bad acl id is InvalidArgument (the id defines the resource pattern).
		_, err := client.CreateAcl(ctx, &managedkafkapb.CreateAclRequest{
			Parent: kafkaClusterResource(cluster),
			AclId:  "not-a-pattern",
			Acl:    &managedkafkapb.Acl{AclEntries: []*managedkafkapb.AclEntry{entry}},
		})
		wantGRPCCode(t, err, codes.InvalidArgument)

		// An entry the authorizer could not map is InvalidArgument.
		_, err = client.CreateAcl(ctx, &managedkafkapb.CreateAclRequest{
			Parent: kafkaClusterResource(cluster),
			AclId:  "topic/bad",
			Acl:    &managedkafkapb.Acl{AclEntries: []*managedkafkapb.AclEntry{{Principal: "x@example.com", PermissionType: "ALLOW", Operation: "READ", Host: "*"}}},
		})
		wantGRPCCode(t, err, codes.InvalidArgument)

		// An update with no etag is InvalidArgument; a stale one is Aborted.
		_, err = client.CreateAcl(ctx, &managedkafkapb.CreateAclRequest{
			Parent: kafkaClusterResource(cluster),
			AclId:  "topic/orders",
			Acl:    &managedkafkapb.Acl{AclEntries: []*managedkafkapb.AclEntry{entry}},
		})
		require.NoError(t, err)
		_, err = client.UpdateAcl(ctx, &managedkafkapb.UpdateAclRequest{
			Acl: &managedkafkapb.Acl{Name: kafkaAclResource(cluster, "topic/orders"), AclEntries: []*managedkafkapb.AclEntry{entry}},
		})
		wantGRPCCode(t, err, codes.InvalidArgument)

		_, err = client.UpdateAcl(ctx, &managedkafkapb.UpdateAclRequest{
			Acl: &managedkafkapb.Acl{Name: kafkaAclResource(cluster, "topic/orders"), Etag: "stale", AclEntries: []*managedkafkapb.AclEntry{entry}},
		})
		wantGRPCCode(t, err, codes.Aborted)
	})

	t.Run("UnknownAcl", func(t *testing.T) {
		_, err := client.GetAcl(ctx, &managedkafkapb.GetAclRequest{Name: kafkaAclResource(cluster, "topic/absent")})
		wantGRPCCode(t, err, codes.NotFound)
		err = client.DeleteAcl(ctx, &managedkafkapb.DeleteAclRequest{Name: kafkaAclResource(cluster, "topic/absent")})
		wantGRPCCode(t, err, codes.NotFound)
	})

	// With no live broker the consumer-group list is empty (a group exists only
	// once a client commits offsets), and get/update/delete of a group are
	// NotFound. The broker-present case lives in the local Docker gate.
	t.Run("AbsentConsumerGroup", func(t *testing.T) {
		groupIt := client.ListConsumerGroups(ctx, &managedkafkapb.ListConsumerGroupsRequest{Parent: kafkaClusterResource(cluster)})
		_, err := groupIt.Next()
		assert.ErrorIs(t, err, iterator.Done, "a brokerless cluster has no consumer groups")
		_, err = client.GetConsumerGroup(ctx, &managedkafkapb.GetConsumerGroupRequest{Name: kafkaConsumerGroupResource(cluster, "no-group")})
		wantGRPCCode(t, err, codes.NotFound)
		_, err = client.UpdateConsumerGroup(ctx, &managedkafkapb.UpdateConsumerGroupRequest{
			ConsumerGroup: &managedkafkapb.ConsumerGroup{Name: kafkaConsumerGroupResource(cluster, "no-group")},
		})
		wantGRPCCode(t, err, codes.NotFound)
		err = client.DeleteConsumerGroup(ctx, &managedkafkapb.DeleteConsumerGroupRequest{Name: kafkaConsumerGroupResource(cluster, "no-group")})
		wantGRPCCode(t, err, codes.NotFound)
	})
}

// TestManagedKafkaDepthListing is the p4 class: pageSize/pageToken walks over
// clusters, topics and ACLs, asserting every resource is returned exactly once
// in name-ascending order across page boundaries.
func TestManagedKafkaDepthListing(t *testing.T) {
	ctx := context.Background()
	client := kafkaGRPCClient(t)

	t.Run("Topics", func(t *testing.T) {
		cluster := createKafkaDepthCluster(t, client)
		const topics = 7
		want := make([]string, 0, topics)
		for i := 0; i < topics; i++ {
			id := uniqueName("depth-page")
			_, err := client.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
				Parent:  kafkaClusterResource(cluster),
				TopicId: id,
				Topic:   &managedkafkapb.Topic{PartitionCount: 1, ReplicationFactor: 1},
			})
			require.NoError(t, err)
			want = append(want, id)
		}
		assert.Equal(t, sortedCopy(want), walkTopicPages(t, client, cluster, 3), "a paged walk returns every topic in ascending order")
	})

	t.Run("Clusters", func(t *testing.T) {
		// A dedicated location keeps the cluster page deterministic; any cluster
		// a prior (interrupted) run leaked there is swept first so the exact-set
		// comparison holds against a persistent emulator too.
		location := "europe-west3"
		parent := "projects/" + kafkaProject() + "/locations/" + location
		deleteAllClustersIn(t, client, parent)
		const clusters = 5
		want := make([]string, 0, clusters)
		for i := 0; i < clusters; i++ {
			id := uniqueName("depth-page")
			op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
				Parent:    parent,
				ClusterId: id,
				Cluster:   kafkaClusterSpec(),
			})
			require.NoError(t, err)
			_, err = op.Wait(ctx)
			require.NoError(t, err)
			want = append(want, id)
			defer deleteKafkaDepthClusterAt(t, client, parent, id)
		}
		assert.Equal(t, sortedCopy(want), walkClusterPages(t, client, parent, 2))
	})

	t.Run("Acls", func(t *testing.T) {
		cluster := createKafkaDepthCluster(t, client)
		const acls = 5
		entry := &managedkafkapb.AclEntry{Principal: "User:x@example.com", PermissionType: "ALLOW", Operation: "READ", Host: "*"}
		want := make([]string, 0, acls)
		for i := 0; i < acls; i++ {
			id := "topic/" + uniqueName("depth-page")
			_, err := client.CreateAcl(ctx, &managedkafkapb.CreateAclRequest{
				Parent: kafkaClusterResource(cluster),
				AclId:  id,
				Acl:    &managedkafkapb.Acl{AclEntries: []*managedkafkapb.AclEntry{entry}},
			})
			require.NoError(t, err)
			want = append(want, id)
		}
		assert.Equal(t, sortedCopy(want), walkAclPages(t, client, cluster, 2))
	})
}

// TestManagedKafkaDepthScale is the p5 class: a many-topic cluster and the
// long-running cluster lifecycle (create/update return a done operation, the
// operation is readable back by name, and the updated metadata is durable).
func TestManagedKafkaDepthScale(t *testing.T) {
	ctx := context.Background()
	client := kafkaGRPCClient(t)

	t.Run("ManyTopics", func(t *testing.T) {
		cluster := createKafkaDepthCluster(t, client)
		const topics = 40
		for i := 0; i < topics; i++ {
			_, err := client.CreateTopic(ctx, &managedkafkapb.CreateTopicRequest{
				Parent:  kafkaClusterResource(cluster),
				TopicId: uniqueName("depth-scale"),
				Topic:   &managedkafkapb.Topic{PartitionCount: 1, ReplicationFactor: 1},
			})
			require.NoError(t, err)
		}
		assert.Len(t, walkTopicPages(t, client, cluster, 0), topics)
	})

	t.Run("ClusterOperationLifecycle", func(t *testing.T) {
		id := uniqueName("depth-lro")
		op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
			Parent:    kafkaGRPCParent(),
			ClusterId: id,
			Cluster:   kafkaClusterSpec(),
		})
		require.NoError(t, err)
		defer deleteKafkaDepthCluster(t, client, id)
		_, err = op.Wait(ctx)
		require.NoError(t, err)
		meta, err := op.Metadata()
		if assert.NoError(t, err) {
			assert.Equal(t, "create", meta.GetVerb())
		}

		// The operation is persisted under operations.get.
		_, err = client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: op.Name()})
		require.NoError(t, err)

		updated, err := client.UpdateCluster(ctx, &managedkafkapb.UpdateClusterRequest{
			Cluster: &managedkafkapb.Cluster{
				Name:           kafkaClusterResource(id),
				Labels:         map[string]string{"depth": "scale"},
				CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 6, MemoryBytes: 6442450944},
			},
		})
		require.NoError(t, err)
		_, err = updated.Wait(ctx)
		require.NoError(t, err)

		got, err := client.GetCluster(ctx, &managedkafkapb.GetClusterRequest{Name: kafkaClusterResource(id)})
		require.NoError(t, err)
		assert.Equal(t, "scale", got.GetLabels()["depth"])
		assert.Equal(t, int64(6), got.GetCapacityConfig().GetVcpuCount())
	})
}

// walkTopicPages walks every topic under a cluster (pageSize 0 = the default
// page size) through the shared depth-test kit, which asserts the paging
// contract — each resource exactly once, ascending resource name across page
// boundaries, no page larger than requested, no empty non-terminal page, an
// empty final token, and an identical second walk — and returns the leaf ids in
// service order.
func walkTopicPages(t *testing.T, client *managedkafka.Client, cluster string, pageSize int32) []string {
	t.Helper()
	ctx := context.Background()
	cfg := testdepth.Paging[*managedkafkapb.Topic]{
		Name:     func(topic *managedkafkapb.Topic) string { return topic.GetName() },
		PageSize: int64(pageSize),
		Order:    testdepth.OrderAscendingName,
		// The emulator fills every non-terminal page (the assertion this suite
		// made before the kit); AIP-158 permits a zero-result mid-collection page,
		// hence the explicit opt-in.
		RequireNonEmptyPages: true,
	}
	topics := testdepth.AssertPaging(t, func() testdepth.Lister[*managedkafkapb.Topic] {
		it := client.ListTopics(ctx, &managedkafkapb.ListTopicsRequest{Parent: kafkaClusterResource(cluster)})
		return func(req testdepth.ListRequest) (testdepth.Page[*managedkafkapb.Topic], error) {
			items, next, err := it.InternalFetch(int(req.PageSize), req.PageToken)
			return testdepth.Page[*managedkafkapb.Topic]{Items: items, NextPageToken: next}, err
		}
	}, cfg)
	out := make([]string, 0, len(topics))
	for _, topic := range topics {
		out = append(out, leaf(topic.GetName()))
	}
	return out
}

// walkClusterPages walks a location's clusters through the shared depth-test kit
// and returns the leaf ids.
func walkClusterPages(t *testing.T, client *managedkafka.Client, parent string, pageSize int32) []string {
	t.Helper()
	ctx := context.Background()
	cfg := testdepth.Paging[*managedkafkapb.Cluster]{
		Name:     func(cluster *managedkafkapb.Cluster) string { return cluster.GetName() },
		PageSize: int64(pageSize),
		Order:    testdepth.OrderAscendingName,
		// The emulator fills every non-terminal page (the assertion this suite
		// made before the kit); AIP-158 permits a zero-result mid-collection page,
		// hence the explicit opt-in.
		RequireNonEmptyPages: true,
	}
	clusters := testdepth.AssertPaging(t, func() testdepth.Lister[*managedkafkapb.Cluster] {
		it := client.ListClusters(ctx, &managedkafkapb.ListClustersRequest{Parent: parent})
		return func(req testdepth.ListRequest) (testdepth.Page[*managedkafkapb.Cluster], error) {
			items, next, err := it.InternalFetch(int(req.PageSize), req.PageToken)
			return testdepth.Page[*managedkafkapb.Cluster]{Items: items, NextPageToken: next}, err
		}
	}, cfg)
	out := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		out = append(out, leaf(cluster.GetName()))
	}
	return out
}

// walkAclPages walks a cluster's ACLs through the shared depth-test kit and
// returns the acl ids (the `acls/` suffix, which itself contains a slash:
// `topic/<name>`).
func walkAclPages(t *testing.T, client *managedkafka.Client, cluster string, pageSize int32) []string {
	t.Helper()
	ctx := context.Background()
	prefix := kafkaClusterResource(cluster) + "/acls/"
	cfg := testdepth.Paging[*managedkafkapb.Acl]{
		Name:     func(acl *managedkafkapb.Acl) string { return acl.GetName() },
		PageSize: int64(pageSize),
		Order:    testdepth.OrderAscendingName,
		// The emulator fills every non-terminal page (the assertion this suite
		// made before the kit); AIP-158 permits a zero-result mid-collection page,
		// hence the explicit opt-in.
		RequireNonEmptyPages: true,
	}
	acls := testdepth.AssertPaging(t, func() testdepth.Lister[*managedkafkapb.Acl] {
		it := client.ListAcls(ctx, &managedkafkapb.ListAclsRequest{Parent: kafkaClusterResource(cluster)})
		return func(req testdepth.ListRequest) (testdepth.Page[*managedkafkapb.Acl], error) {
			items, next, err := it.InternalFetch(int(req.PageSize), req.PageToken)
			return testdepth.Page[*managedkafkapb.Acl]{Items: items, NextPageToken: next}, err
		}
	}, cfg)
	out := make([]string, 0, len(acls))
	for _, acl := range acls {
		out = append(out, strings.TrimPrefix(acl.GetName(), prefix))
	}
	return out
}
