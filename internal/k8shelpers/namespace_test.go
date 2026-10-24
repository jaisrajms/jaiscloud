package k8shelpers

import (
	"context"
	"errors"
	"regexp"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func TestNamespaceNameDeterministicAndValid(t *testing.T) {
	name := NamespaceName("dataproc", "my-project", "cluster-1")
	if name != NamespaceName("dataproc", "my-project", "cluster-1") {
		t.Fatal("NamespaceName is not deterministic")
	}
	if len(name) > maxNamespaceLength {
		t.Fatalf("namespace %q is %d chars, over the %d limit", name, len(name), maxNamespaceLength)
	}
	if !dns1123Label.MatchString(name) {
		t.Fatalf("namespace %q is not a valid DNS-1123 label", name)
	}
	other := NamespaceName("dataproc", "my-project", "cluster-2")
	if name == other {
		t.Fatalf("distinct ids share namespace %q", name)
	}
}

func TestNamespaceNameUniquenessAfterSanitize(t *testing.T) {
	// Two ids that sanitize to the same base must still get distinct namespaces
	// because the hash covers the raw inputs.
	a := NamespaceName("dataproc", "proj", "a.b/c")
	b := NamespaceName("dataproc", "proj", "a-b-c")
	if a == b {
		t.Fatalf("sanitize collision: %q == %q", a, b)
	}
}

func TestNamespaceNameTruncatesLongInputs(t *testing.T) {
	long := ""
	for i := 0; i < 200; i++ {
		long += "x"
	}
	name := NamespaceName("dataproc", long, long)
	if len(name) > maxNamespaceLength {
		t.Fatalf("long-input namespace %q is %d chars", name, len(name))
	}
	if !dns1123Label.MatchString(name) {
		t.Fatalf("long-input namespace %q is not a valid DNS-1123 label", name)
	}
}

func TestEnsureManagedNamespaceCreatesThenAdopts(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	created, err := EnsureManagedNamespace(ctx, client, "gcp-dataproc-proj-c1-deadbeef", "dataproc")
	if err != nil || !created {
		t.Fatalf("first EnsureManagedNamespace = %v, %v; want true, nil", created, err)
	}
	ns, err := client.CoreV1().Namespaces().Get(ctx, "gcp-dataproc-proj-c1-deadbeef", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("namespace not created: %v", err)
	}
	if !IsManagedNamespace(ns, "dataproc") {
		t.Fatalf("created namespace missing ownership labels: %v", ns.Labels)
	}

	created, err = EnsureManagedNamespace(ctx, client, "gcp-dataproc-proj-c1-deadbeef", "dataproc")
	if err != nil || created {
		t.Fatalf("second EnsureManagedNamespace = %v, %v; want false, nil", created, err)
	}
}

func TestEnsureManagedNamespaceAdoptsPreExisting(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Labels: map[string]string{"team": "platform"}},
	})
	created, err := EnsureManagedNamespace(context.Background(), client, "shared", "dataproc")
	if err != nil || created {
		t.Fatalf("EnsureManagedNamespace on pre-existing = %v, %v; want false, nil", created, err)
	}
	ns, _ := client.CoreV1().Namespaces().Get(context.Background(), "shared", metav1.GetOptions{})
	if IsManagedNamespace(ns, "dataproc") {
		t.Fatal("adopted namespace was relabelled as emulator-owned")
	}
}

func TestEnsureManagedNamespaceForbiddenReturnsSentinel(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "ns", errors.New("forbidden"))
	})
	created, err := EnsureManagedNamespace(context.Background(), client, "ns", "dataproc")
	if created {
		t.Fatal("created should be false on Forbidden")
	}
	if !errors.Is(err, ErrNamespaceForbidden) {
		t.Fatalf("err = %v; want ErrNamespaceForbidden", err)
	}
}

func TestDeleteManagedNamespaceOnlyOwned(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "owned", Labels: OwnerNamespaceLabels("dataproc")}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "adopted", Labels: map[string]string{"team": "x"}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "other-svc", Labels: OwnerNamespaceLabels("kafka")}},
	)
	ctx := context.Background()

	deleted, err := DeleteManagedNamespace(ctx, client, "owned", "dataproc")
	if err != nil || !deleted {
		t.Fatalf("DeleteManagedNamespace(owned) = %v, %v; want true, nil", deleted, err)
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, "owned", metav1.GetOptions{}); err == nil {
		t.Fatal("owned namespace survived delete")
	}

	for _, name := range []string{"adopted", "other-svc", "missing"} {
		deleted, err := DeleteManagedNamespace(ctx, client, name, "dataproc")
		if err != nil || deleted {
			t.Fatalf("DeleteManagedNamespace(%s) = %v, %v; want false, nil", name, deleted, err)
		}
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, "adopted", metav1.GetOptions{}); err != nil {
		t.Fatal("adopted namespace was deleted")
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, "other-svc", metav1.GetOptions{}); err != nil {
		t.Fatal("another service's namespace was deleted")
	}
}

func TestListAndSweepManagedNamespaces(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "dp-1", Labels: OwnerNamespaceLabels("dataproc")}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "dp-2", Labels: OwnerNamespaceLabels("dataproc")}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kafka-1", Labels: OwnerNamespaceLabels("kafka")}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "plain", Labels: map[string]string{"team": "x"}}},
	)
	ctx := context.Background()

	names, err := ListManagedNamespaces(ctx, client, "dataproc")
	if err != nil {
		t.Fatalf("ListManagedNamespaces: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("ListManagedNamespaces = %v; want 2 dataproc namespaces", names)
	}

	n, err := SweepManagedNamespaces(ctx, client, "dataproc")
	if err != nil {
		t.Fatalf("SweepManagedNamespaces: %v", err)
	}
	if n != 2 {
		t.Fatalf("SweepManagedNamespaces deleted %d; want 2", n)
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, "kafka-1", metav1.GetOptions{}); err != nil {
		t.Fatal("sweep deleted another service's namespace")
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, "plain", metav1.GetOptions{}); err != nil {
		t.Fatal("sweep deleted an unmanaged namespace")
	}
}

func TestEnsureNamespaceRBACForSubjects(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	err := EnsureNamespaceRBACForSubjects(ctx, client, "gcp-dataproc-p-c-1",
		RBACSubject{Name: "jaiscloud", Namespace: "jaiscloud"},
		RBACSubject{Name: "spark-driver", Namespace: "gcp-dataproc-p-c-1"},
		RBACSubject{Name: "jaiscloud", Namespace: "jaiscloud"}, // duplicate
		RBACSubject{Name: ""}, // skipped
	)
	if err != nil {
		t.Fatalf("EnsureNamespaceRBACForSubjects: %v", err)
	}
	rb, err := client.RbacV1().RoleBindings("gcp-dataproc-p-c-1").Get(ctx, ExecutorRoleBindingName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("rolebinding not created: %v", err)
	}
	if len(rb.Subjects) != 2 {
		t.Fatalf("subjects = %+v; want 2 (deduped, empty skipped)", rb.Subjects)
	}
}

func TestEnsureNamespaceRBACCreatesRoleBinding(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	if err := EnsureNamespaceRBAC(ctx, client, "gcp-dataproc-p-c-1", "jaiscloud", "jaiscloud"); err != nil {
		t.Fatalf("EnsureNamespaceRBAC: %v", err)
	}
	rb, err := client.RbacV1().RoleBindings("gcp-dataproc-p-c-1").Get(ctx, ExecutorRoleBindingName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("rolebinding not created: %v", err)
	}
	if rb.RoleRef.Kind != "ClusterRole" || rb.RoleRef.Name != ExecutorClusterRoleName {
		t.Fatalf("roleRef = %+v", rb.RoleRef)
	}
	if len(rb.Subjects) != 1 || rb.Subjects[0].Name != "jaiscloud" || rb.Subjects[0].Namespace != "jaiscloud" {
		t.Fatalf("subjects = %+v", rb.Subjects)
	}
	// Idempotent.
	if err := EnsureNamespaceRBAC(ctx, client, "gcp-dataproc-p-c-1", "jaiscloud", "jaiscloud"); err != nil {
		t.Fatalf("second EnsureNamespaceRBAC: %v", err)
	}
}

// TestEnsureManagedNamespaceRecreatesTerminating covers the fast-delete/recreate
// race: a Dataproc cluster (or Kafka broker) with a derived namespace name is
// deleted and immediately recreated, and the old namespace is still Terminating.
// The create must wait for it to disappear and recreate it rather than adopting
// a Terminating namespace (which would reject every later create in it).
func TestEnsureManagedNamespaceRecreatesTerminating(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	name := "gcp-dataproc-proj-c1-feedface"
	now := metav1.Now()
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Labels:            OwnerNamespaceLabels("dataproc"),
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes"},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed terminating namespace: %v", err)
	}

	// The first Get observes the Terminating namespace; once the wait loop polls,
	// terminate it (delete from the tracker) and report NotFound.
	gets := 0
	client.PrependReactor("get", "namespaces", func(action k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 2 {
			_ = client.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, "", name)
			return true, nil, k8serrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, name)
		}
		return false, nil, nil
	})

	created, err := EnsureManagedNamespace(ctx, client, name, "dataproc")
	if err != nil || !created {
		t.Fatalf("EnsureManagedNamespace over a terminating namespace = %v, %v; want true, nil", created, err)
	}
	ns, err := client.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("recreated namespace not found: %v", err)
	}
	if ns.DeletionTimestamp != nil {
		t.Fatalf("namespace is still Terminating after recreate: %+v", ns.DeletionTimestamp)
	}
}
