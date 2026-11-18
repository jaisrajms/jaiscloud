package kafka

import (
	"context"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"jaiscloud/internal/k8shelpers"
)

// TestProvisionNamespaceBindsResolvedEmulatorIdentity proves the per-cluster
// namespace's executor RoleBinding binds the ServiceAccount the emulator
// process itself runs as. A GCP substrate pod runs as e.g. `jaiscloud-gcp` in
// namespace `jaiscloud-gcp`; binding the historical `jaiscloud/jaiscloud`
// constant would be accepted by the API server and then fail the broker Pod
// create with Forbidden.
func TestProvisionNamespaceBindsResolvedEmulatorIdentity(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	b.ownServiceAccount = func() (string, string) { return "jaiscloud-gcp", "jaiscloud-gcp" }

	ctx := context.Background()
	key := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "c1"}
	ns, owned := b.provisionNamespace(ctx, key)
	if ns != ClusterNamespace(key.Project, key.Location, key.Cluster) || !owned {
		t.Fatalf("provisionNamespace = (%q, %v), want the derived owned namespace", ns, owned)
	}

	rb, err := client.RbacV1().RoleBindings(ns).Get(ctx, k8shelpers.ExecutorRoleBindingName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("executor RoleBinding not created in %q: %v", ns, err)
	}
	want := []rbacv1.Subject{{Kind: "ServiceAccount", Name: "jaiscloud-gcp", Namespace: "jaiscloud-gcp"}}
	if len(rb.Subjects) != 1 || rb.Subjects[0] != want[0] {
		t.Fatalf("RoleBinding subjects = %+v, want %+v", rb.Subjects, want)
	}
	if rb.RoleRef.Name != k8shelpers.ExecutorClusterRoleName {
		t.Errorf("RoleRef = %q, want %q", rb.RoleRef.Name, k8shelpers.ExecutorClusterRoleName)
	}
}

// TestNewK8sBrokerDefaultsToResolvedIdentity documents that the broker wires the
// process resolver (k8shelpers.ResolveOwnServiceAccount) rather than a constant.
func TestNewK8sBrokerDefaultsToResolvedIdentity(t *testing.T) {
	b := newK8sBroker(fake.NewSimpleClientset(), "jaiscloud", "redpanda:test", discardLogger())
	if b.ownServiceAccount == nil {
		t.Fatal("newK8sBroker left ownServiceAccount nil")
	}
	// The field must resolve, not panic or return empty values; a per-namespace
	// RoleBinding with an empty subject would silently grant nobody.
	name, ns := b.ownServiceAccount()
	if name == "" || ns == "" {
		t.Fatalf("resolved identity = (%q, %q), want non-empty values", name, ns)
	}
}
