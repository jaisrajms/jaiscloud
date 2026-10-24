package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"jaiscloud/internal/k8shelpers"
)

func gceClusterInput() ClusterInput {
	return ClusterInput{Config: json.RawMessage(`{"gceClusterConfig":{}}`)}
}

func TestCreateClusterProvisionsPerClusterNamespace(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	ctx := context.Background()

	c1, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gceClusterInput())
	require.NoError(t, err)
	require.NotEmpty(t, c1.Namespace)
	require.True(t, c1.NamespaceOwned, "emulator-created namespace must be owned")

	ns, err := client.CoreV1().Namespaces().Get(ctx, c1.Namespace, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "dataproc", ns.Labels["jaiscloud.io/service"])

	// The per-namespace RoleBinding binds both the emulator SA and the Spark
	// driver SA (default when unset).
	rb, err := client.RbacV1().RoleBindings(c1.Namespace).Get(ctx, k8shelpers.ExecutorRoleBindingName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, rb.Subjects, 2)
	// The new namespace is watched for executor-pod ownership.
	p.nsPatchersMu.Lock()
	_, watched := p.nsPatchers[c1.Namespace]
	p.nsPatchersMu.Unlock()
	require.True(t, watched, "per-cluster namespace must be watched by an ownership patcher")

	c2, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c2", gceClusterInput())
	require.NoError(t, err)
	require.NotEqual(t, c1.Namespace, c2.Namespace, "distinct clusters must get distinct namespaces")

	// The effective namespace is persisted and read back.
	got, err := p.GetCluster(ctx, "proj", "us-central1", "c1")
	require.NoError(t, err)
	require.Equal(t, c1.Namespace, got.Namespace)
	require.True(t, got.NamespaceOwned)
}

func TestCreateClusterHonorsExplicitKubernetesNamespace(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	ctx := context.Background()

	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gkeInput(t, mustJSONMap([]byte(gkeVirtualClusterConfig))))
	require.NoError(t, err)
	require.Equal(t, "dataproc", c.Namespace)
	require.True(t, c.NamespaceOwned)
	if _, err := client.CoreV1().Namespaces().Get(ctx, "dataproc", metav1.GetOptions{}); err != nil {
		t.Fatalf("requested namespace not provisioned: %v", err)
	}
}

func TestDeleteClusterRemovesOwnedNamespaceAndWorkloads(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	ctx := context.Background()

	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gceClusterInput())
	require.NoError(t, err)
	ns := c.Namespace

	_, err = client.BatchV1().Jobs(ns).Create(ctx, &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "jc-spark-c1",
			Namespace: ns,
			Labels: map[string]string{
				"jaiscloud.io/provider":     "dataproc",
				"jaiscloud.io/cluster-name": "c1",
			},
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = p.DeleteCluster(ctx, "proj", "us-central1", "c1")
	require.NoError(t, err)
	// The next read settles DELETING -> record removed; teardown runs in the
	// background.
	_, err = p.GetCluster(ctx, "proj", "us-central1", "c1")
	require.Error(t, err)

	require.Eventually(t, func() bool {
		_, err := client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		return k8serrors.IsNotFound(err)
	}, 5*time.Second, 20*time.Millisecond, "owned namespace survived cluster delete")
	require.Eventually(t, func() bool {
		_, err := client.BatchV1().Jobs(ns).Get(ctx, "jc-spark-c1", metav1.GetOptions{})
		return k8serrors.IsNotFound(err)
	}, 5*time.Second, 20*time.Millisecond, "cluster job survived cluster delete")
}

func TestDeleteClusterLeavesAdoptedNamespace(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	_, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-ns", Labels: map[string]string{"team": "platform"}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	p := newK8sProvider(t, client)
	vcc := map[string]any{"kubernetesClusterConfig": map[string]any{
		"kubernetesNamespace": "shared-ns",
		"gkeClusterConfig":    map[string]any{"gkeClusterTarget": "projects/proj/locations/us-central1/clusters/gke-1"},
	}}
	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gkeInput(t, vcc))
	require.NoError(t, err)
	require.Equal(t, "shared-ns", c.Namespace)
	require.False(t, c.NamespaceOwned, "a pre-existing namespace must be adopted, not owned")

	_, err = p.DeleteCluster(ctx, "proj", "us-central1", "c1")
	require.NoError(t, err)
	_, err = p.GetCluster(ctx, "proj", "us-central1", "c1")
	require.Error(t, err)

	if _, err := client.CoreV1().Namespaces().Get(ctx, "shared-ns", metav1.GetOptions{}); err != nil {
		t.Fatalf("adopted namespace was deleted: %v", err)
	}
}

func TestCreateClusterFallsBackWhenNamespaceForbidden(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	client.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "ns", errors.New("forbidden"))
	})
	ctx := context.Background()

	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gceClusterInput())
	require.NoError(t, err)
	require.Equal(t, "jaiscloud", c.Namespace, "must fall back to the process-wide namespace")
	require.False(t, c.NamespaceOwned)
}

func TestResetSweepsOwnedNamespaces(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	ctx := context.Background()

	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gceClusterInput())
	require.NoError(t, err)
	require.True(t, c.NamespaceOwned)

	p.Reset(ctx)
	if _, err := client.CoreV1().Namespaces().Get(ctx, c.Namespace, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Fatalf("Reset did not sweep the owned namespace: %v", err)
	}
}

func TestSubmitJobRunsInClusterNamespace(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	ctx := context.Background()

	c, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gceClusterInput())
	require.NoError(t, err)

	fw := prependPodWatch(t, client)
	_, err = p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j-ns"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	require.NoError(t, err)

	var jobName string
	require.Eventually(t, func() bool {
		jobs, err := client.BatchV1().Jobs(c.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil || len(jobs.Items) == 0 {
			return false
		}
		jobName = jobs.Items[0].Name
		return true
	}, 5*time.Second, 10*time.Millisecond, "spark-submit job was not created in the cluster namespace")
	fw.Add(succeededDriverPod("driver-ns", jobName))
}

// TestNamespaceNameUsedForDerivedClusters documents the naming contract the
// cluster record relies on.
func TestNamespaceNameUsedForDerivedClusters(t *testing.T) {
	want := k8shelpers.NamespaceName("dataproc", "proj", "c1")
	client := fake.NewSimpleClientset()
	p := newK8sProvider(t, client)
	c, _, err := p.CreateCluster(context.Background(), "proj", "us-central1", "c1", gceClusterInput())
	require.NoError(t, err)
	require.Equal(t, want, c.Namespace)
}
