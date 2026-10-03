package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"k8s.io/client-go/kubernetes/fake"

	core "jaiscloud/internal/gcp/service/managedkafka"
)

func TestAclOperationMapping(t *testing.T) {
	cases := map[string]kadm.ACLOperation{
		"all":              kadm.OpAll,
		"READ":             kadm.OpRead,
		"write":            kadm.OpWrite,
		"CREATE":           kadm.OpCreate,
		"DELETE":           kadm.OpDelete,
		"ALTER":            kadm.OpAlter,
		"DESCRIBE":         kadm.OpDescribe,
		"CLUSTER_ACTION":   kadm.OpClusterAction,
		"DESCRIBE_CONFIGS": kadm.OpDescribeConfigs,
		"ALTER_CONFIGS":    kadm.OpAlterConfigs,
		"IDEMPOTENT_WRITE": kadm.OpIdempotentWrite,
	}
	for in, want := range cases {
		got, err := aclOperation(in)
		if err != nil || got != want {
			t.Errorf("aclOperation(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := aclOperation("FLY"); err == nil {
		t.Error("aclOperation accepted an unknown operation")
	}
}

func TestAclPatternAndHelpers(t *testing.T) {
	if p, err := aclPattern("literal"); err != nil || p != kadm.ACLPatternLiteral {
		t.Errorf("aclPattern(literal) = %v, %v", p, err)
	}
	if p, err := aclPattern("PREFIXED"); err != nil || p != kadm.ACLPatternPrefixed {
		t.Errorf("aclPattern(PREFIXED) = %v, %v", p, err)
	}
	if _, err := aclPattern("regex"); err == nil {
		t.Error("aclPattern accepted an unknown pattern type")
	}

	if got := ensureUserPrefix("alice"); got != "User:alice" {
		t.Errorf("ensureUserPrefix(alice) = %q", got)
	}
	if got := ensureUserPrefix("User:*"); got != "User:*" {
		t.Errorf("ensureUserPrefix(User:*) = %q", got)
	}
	if got := aclHost(""); got != "*" {
		t.Errorf("aclHost(empty) = %q, want *", got)
	}
	if got := aclHost("10.0.0.1"); got != "10.0.0.1" {
		t.Errorf("aclHost = %q", got)
	}
}

// TestAclBuildersValidate proves the builders we hand kadm pass its own
// create/delete validation, which fails loudly for an empty operation set or an
// unset filter (a common way to build a delete that silently matches nothing).
func TestAclBuildersValidate(t *testing.T) {
	spec := aclSpec{ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL"}
	pattern, err := aclPattern(spec.PatternType)
	if err != nil {
		t.Fatalf("aclPattern: %v", err)
	}
	del, err := aclDeleteFilter(spec, pattern)
	if err != nil {
		t.Fatalf("aclDeleteFilter: %v", err)
	}
	if err := del.ValidateDelete(); err != nil {
		t.Fatalf("delete filter invalid: %v", err)
	}
	create, err := aclBuilder(spec, pattern)
	if err != nil {
		t.Fatalf("aclBuilder: %v", err)
	}
	if err := create.Operations(kadm.OpRead).Allow("User:a").AllowHosts("*").ValidateCreate(); err != nil {
		t.Fatalf("create builder invalid: %v", err)
	}

	for _, rt := range []string{"CLUSTER", "TOPIC", "GROUP", "TRANSACTIONAL_ID"} {
		if _, err := aclBuilder(aclSpec{ResourceType: rt, ResourceName: "x"}, kadm.ACLPatternLiteral); err != nil {
			t.Errorf("aclBuilder(%s): %v", rt, err)
		}
	}
	if _, err := aclBuilder(aclSpec{ResourceType: "SUBJECT"}, kadm.ACLPatternLiteral); err == nil {
		t.Error("aclBuilder accepted an unknown resource type")
	}
}

func TestFranzAdminReplaceACLs(t *testing.T) {
	ctx := context.Background()
	api := &fakeAdminAPI{}
	a := &franzAdmin{client: api}
	spec := aclSpec{
		ResourceType: "TOPIC",
		ResourceName: "orders",
		PatternType:  "LITERAL",
		Entries: []core.AclBinding{
			{Principal: "User:alice", PermissionType: "ALLOW", Operation: "READ", Host: "*"},
			{Principal: "bob", PermissionType: "DENY", Operation: "WRITE", Host: "*"},
		},
	}
	if err := a.ReplaceACLs(ctx, spec); err != nil {
		t.Fatalf("ReplaceACLs: %v", err)
	}
	if len(api.deleteACLBuilders) != 1 {
		t.Fatalf("delete calls = %d, want 1", len(api.deleteACLBuilders))
	}
	if len(api.createACLBuilders) != 2 {
		t.Fatalf("create calls = %d, want 2 (one per entry)", len(api.createACLBuilders))
	}

	// An empty entry set is a pure delete.
	api = &fakeAdminAPI{}
	a = &franzAdmin{client: api}
	if err := a.ReplaceACLs(ctx, aclSpec{ResourceType: "CLUSTER", ResourceName: "kafka-cluster", PatternType: "LITERAL"}); err != nil {
		t.Fatalf("ReplaceACLs(delete): %v", err)
	}
	if len(api.deleteACLBuilders) != 1 || len(api.createACLBuilders) != 0 {
		t.Fatalf("delete-only calls = %d delete, %d create", len(api.deleteACLBuilders), len(api.createACLBuilders))
	}
}

func TestFranzAdminReplaceACLsSurfacesErrors(t *testing.T) {
	ctx := context.Background()
	spec := aclSpec{ResourceType: "TOPIC", ResourceName: "orders", PatternType: "LITERAL"}

	// A request-level delete failure.
	a := &franzAdmin{client: &fakeAdminAPI{deleteACLErr: errors.New("network")}}
	if err := a.ReplaceACLs(ctx, spec); err == nil {
		t.Fatal("ReplaceACLs swallowed a delete request error")
	}

	// A per-filter delete rejection.
	a = &franzAdmin{client: &fakeAdminAPI{deleteACLResp: kadm.DeleteACLsResults{{Err: kerr.ClusterAuthorizationFailed}}}}
	if err := a.ReplaceACLs(ctx, spec); !errors.Is(err, kerr.ClusterAuthorizationFailed) {
		t.Fatalf("ReplaceACLs delete error = %v, want ClusterAuthorizationFailed", err)
	}

	// A per-matching-ACL delete rejection (a filter matches several ACLs, each
	// with its own error) must not be swallowed.
	a = &franzAdmin{client: &fakeAdminAPI{deleteACLResp: kadm.DeleteACLsResults{{Deleted: kadm.DeletedACLs{{Err: kerr.ClusterAuthorizationFailed}}}}}}
	if err := a.ReplaceACLs(ctx, spec); !errors.Is(err, kerr.ClusterAuthorizationFailed) {
		t.Fatalf("ReplaceACLs per-matching-ACL error = %v, want ClusterAuthorizationFailed", err)
	}

	// A per-creation rejection (the top-level error is request-level only).
	spec.Entries = []core.AclBinding{{Principal: "User:a", PermissionType: "ALLOW", Operation: "READ"}}
	a = &franzAdmin{client: &fakeAdminAPI{createACLResp: kadm.CreateACLsResults{{Err: kerr.TopicAuthorizationFailed}}}}
	if err := a.ReplaceACLs(ctx, spec); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("ReplaceACLs create error = %v, want TopicAuthorizationFailed", err)
	}

	// Unmappable input is rejected before touching the broker.
	if err := a.ReplaceACLs(ctx, aclSpec{ResourceType: "TOPIC", ResourceName: "t", PatternType: "REGEX"}); err == nil {
		t.Fatal("ReplaceACLs accepted an unknown pattern type")
	}
	spec.Entries = []core.AclBinding{{Principal: "User:a", PermissionType: "ALLOW", Operation: "FLY"}}
	if err := a.ReplaceACLs(ctx, spec); err == nil {
		t.Fatal("ReplaceACLs accepted an unknown operation")
	}
}

func TestReplaceACLsNoOpWithoutEndpoint(t *testing.T) {
	factory, created := recordingFactory()
	p := newAdminPool(factory)
	if err := replaceACLs(context.Background(), p, "", aclSpec{ResourceType: "TOPIC", ResourceName: "t"}); err != nil {
		t.Fatalf("replaceACLs: %v", err)
	}
	if created() != 0 {
		t.Fatalf("no-op replaceACLs built %d admin clients, want 0", created())
	}
}

func TestK8sBrokerReplaceAclNoOpWithoutEndpoint(t *testing.T) {
	factory, created := recordingFactory()
	b := newK8sBroker(fake.NewSimpleClientset(), "jaiscloud", "redpanda:test", discardLogger())
	b.admins = newAdminPool(factory)

	if err := b.ReplaceAcl(context.Background(), "p", "l", "c1", "TOPIC", "t", "LITERAL", nil); err != nil {
		t.Fatalf("ReplaceAcl: %v", err)
	}
	if created() != 0 {
		t.Fatalf("no live broker but built %d admin clients, want 0", created())
	}
}

func TestK8sBrokerDelegatesReplaceAcl(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.probe = func(string) bool { return true }

	fa := &fakeKafkaAdmin{}
	b.admins = newAdminPool(func(string) (kafkaAdmin, error) { return fa, nil })

	ctx := context.Background()
	if _, err := b.EnsureCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	entries := []core.AclBinding{{Principal: "User:a", PermissionType: "ALLOW", Operation: "READ", Host: "*"}}
	if err := b.ReplaceAcl(ctx, "p", "l", "c1", "TOPIC", "orders", "PREFIXED", entries); err != nil {
		t.Fatalf("ReplaceAcl: %v", err)
	}
	if len(fa.acls) != 1 {
		t.Fatalf("admin ReplaceACLs calls = %d, want 1", len(fa.acls))
	}
	got := fa.acls[0]
	if got.ResourceType != "TOPIC" || got.ResourceName != "orders" || got.PatternType != "PREFIXED" || len(got.Entries) != 1 || got.Entries[0].Principal != "User:a" {
		t.Fatalf("mirrored spec = %+v", got)
	}
}
