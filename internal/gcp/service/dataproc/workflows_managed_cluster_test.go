package dataproc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/k8shelpers"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// workflowWithManagedCluster builds an inline workflow whose placement manages a
// cluster the way a WorkflowTemplatePlacement does.
func workflowWithManagedCluster(mc map[string]any) map[string]any {
	return map[string]any{
		"placement": map[string]any{"managedCluster": mc},
		"jobs": []any{
			map[string]any{"stepId": "a", "pysparkJob": map[string]any{"mainPythonFileUri": "gs://bucket/main.py"}},
		},
	}
}

// normaliseClusterRead strips the fields that legitimately differ between two
// clusters (the name, the per-instance UUID and the state transition wall
// times) so two reads can be compared field for field.
func normaliseClusterRead(m map[string]any) map[string]any {
	delete(m, "clusterName")
	delete(m, "clusterUuid")
	if st, ok := m["status"].(map[string]any); ok {
		delete(st, "stateStartTime")
	}
	if hist, ok := m["statusHistory"].([]any); ok {
		for _, h := range hist {
			if hm, ok := h.(map[string]any); ok {
				delete(hm, "stateStartTime")
			}
		}
	}
	return m
}

// TestWorkflowManagedClusterRejectedLikeCreate verifies a managedCluster that
// clusters.create would reject is rejected by instantiate with the same error:
// the workflow placement has no placement of its own to fall back on, so the
// "exactly one of config or virtualClusterConfig" rule must apply unchanged.
func TestWorkflowManagedClusterRejectedLikeCreate(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()

	_, _, createErr := p.CreateCluster(ctx, "proj", "us-central1", "direct", ClusterInput{})
	require.Error(t, createErr)

	_, wfErr := p.InstantiateInlineWorkflowTemplate(ctx, "proj", "us-central1",
		workflowWithManagedCluster(map[string]any{"clusterName": "wf-no-config"}))
	require.Error(t, wfErr)
	require.Equal(t, createErr.Error(), wfErr.Error(),
		"a managedCluster must fail exactly as clusters.create fails")
	var pe *model.ProviderError
	require.ErrorAs(t, wfErr, &pe)
	require.Equal(t, 400, pe.HTTPStatus)

	// The rejected create persists nothing.
	var nf *model.ProviderError
	_, err := p.GetCluster(ctx, "proj", "us-central1", "wf-no-config")
	require.ErrorAs(t, err, &nf)
	require.Equal(t, 404, nf.HTTPStatus)
}

// TestWorkflowManagedClusterMatchesDirectCreate verifies a valid managedCluster
// is created through the same core entry point as clusters.create, so its stored
// config/labels and its rendered read shape are identical to a direct create of
// the same name, config and labels.
func TestWorkflowManagedClusterMatchesDirectCreate(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	config := map[string]any{
		"gceClusterConfig": map[string]any{"zoneUri": "us-central1-a", "networkUri": "default"},
		"workerConfig":     map[string]any{"numInstances": float64(3), "machineTypeUri": "n1-standard-4"},
	}

	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "direct-c", ClusterInput{
		Config: mustJSON(config), Labels: map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	direct, err := p.GetCluster(ctx, "proj", "us-central1", "direct-c")
	require.NoError(t, err)

	_, err = p.InstantiateInlineWorkflowTemplate(ctx, "proj", "us-central1",
		workflowWithManagedCluster(map[string]any{
			"clusterName": "wf-c",
			"config":      config,
			"labels":      map[string]any{"env": "test"},
		}))
	require.NoError(t, err)
	managed, err := p.GetCluster(ctx, "proj", "us-central1", "wf-c")
	require.NoError(t, err)

	require.Equal(t, direct.Status.State, managed.Status.State, "the read state must match a direct create's")
	require.Equal(t, normaliseClusterRead(ClusterJSON(direct)), normaliseClusterRead(ClusterJSON(managed)),
		"a workflow-managed cluster must read back exactly like a directly created one")
}

// TestWorkflowManagedClusterCreateFailureLeavesNothing verifies a create that
// fails after the cluster's namespace was provisioned leaves neither a cluster
// record nor the namespace behind: the create path's rollback is what the
// workflow gets, instead of a half-written placement-less record.
func TestWorkflowManagedClusterCreateFailureLeavesNothing(t *testing.T) {
	client := fake.NewSimpleClientset()
	p := NewService(&errClusterCreateStore{Store: dpstore.NewMemoryStore()}, store.NewMemoryResourceStore(),
		WithK8s(client, "jaiscloud", nil))
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	ctx := context.Background()

	_, err := p.InstantiateInlineWorkflowTemplate(ctx, "proj", "us-central1",
		workflowWithManagedCluster(managedClusterDef("wf-broken")))
	require.Error(t, err)

	var nf *model.ProviderError
	_, err = p.GetCluster(ctx, "proj", "us-central1", "wf-broken")
	require.ErrorAs(t, err, &nf)
	require.Equal(t, 404, nf.HTTPStatus)

	ns := k8shelpers.NamespaceName(dataprocService, "proj", "wf-broken")
	_, err = client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	require.True(t, k8serrors.IsNotFound(err), "failed create leaked namespace %s: %v", ns, err)
}

// TestWorkflowManagedClusterReuse verifies the create-if-absent/reuse-if-present
// contract and the workflow-owned flag: an existing cluster is reused unchanged
// and survives the workflow, while a cluster the workflow created is deleted
// when the workflow ends.
func TestWorkflowManagedClusterReuse(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()

	existing, _, err := p.CreateCluster(ctx, "proj", "us-central1", "wf-existing", ClusterInput{
		Config: mustJSON(map[string]any{"gceClusterConfig": map[string]any{"zoneUri": "us-east1-b"}}),
		Labels: map[string]string{"team": "data"},
	})
	require.NoError(t, err)

	// The placement names a different config and labels; a reused cluster keeps
	// what it already is.
	op, err := p.InstantiateInlineWorkflowTemplate(ctx, "proj", "us-central1",
		workflowWithManagedCluster(map[string]any{
			"clusterName": "wf-existing",
			"config":      managedClusterDef("ignored")["config"],
			"labels":      map[string]any{"team": "other"},
		}))
	require.NoError(t, err)
	pollWorkflowDone(t, p, "proj", "us-central1", op.ID)

	reused, err := p.GetCluster(ctx, "proj", "us-central1", "wf-existing")
	require.NoError(t, err, "a reused cluster must survive the workflow")
	require.Equal(t, existing.ClusterUUID, reused.ClusterUUID, "a reused cluster must not be recreated")
	require.Equal(t, map[string]string{"team": "data"}, reused.Labels)

	// A managed cluster the workflow created is workflow-owned and removed when
	// the workflow ends.
	op, err = p.InstantiateInlineWorkflowTemplate(ctx, "proj", "us-central1",
		workflowWithManagedCluster(managedClusterDef("wf-owned")))
	require.NoError(t, err)
	pollWorkflowDone(t, p, "proj", "us-central1", op.ID)

	var nf *model.ProviderError
	_, err = p.GetCluster(ctx, "proj", "us-central1", "wf-owned")
	require.ErrorAs(t, err, &nf)
	require.Equal(t, 404, nf.HTTPStatus, "workflow-owned cluster must be deleted when the workflow ends")
}

// errClusterCreateStore fails every cluster write after CreateCluster's
// namespace provisioning, the shape a Postgres commit failure has.
type errClusterCreateStore struct {
	dpstore.Store
}

func (s *errClusterCreateStore) CreateCluster(context.Context, string, string, dpstore.Cluster) error {
	return errors.New("simulated cluster write failure")
}
