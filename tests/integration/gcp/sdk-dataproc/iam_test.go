// Package sdk_dataproc_test — resource IAM (TF1-2) driven by the official
// Google REST apiary client.
//
// The Dataproc v1 Discovery document publishes the google.iam.v1.IAMPolicy
// mixin on the regional clusters, jobs and workflowTemplates collections.
// Terraform's google_dataproc_{cluster,job}_iam_{policy,binding,member}
// resources drive exactly these methods, so this case exercises the same wire
// path:
//
//	getIamPolicy      -> the empty default policy (version 1, etag, no bindings)
//	setIamPolicy      -> a conditional binding stored at version 3 (AIP-149)
//	getIamPolicy      -> the stored binding read back with its condition
//	testIamPermissions-> every requested permission granted (no authz)
//	setIamPolicy      -> a stale etag is 409 ABORTED and changes nothing
//	absent resource   -> 404 NotFound before the policy plane is touched
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_dataproc_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/dataproc/v1"
	"google.golang.org/api/googleapi"
)

// requireStaleEtag asserts an IAM write with an etag that no longer matches was
// rejected with 409 ABORTED and did not overwrite the stored policy.
func requireStaleEtag(t *testing.T, err error) {
	t.Helper()
	var gerr *googleapi.Error
	require.ErrorAs(t, err, &gerr)
	require.Equal(t, 409, gerr.Code)
}

func TestSDKDataprocResourceIAM(t *testing.T) {
	ctx := context.Background()
	svc, err := dataproc.NewService(ctx, opts()...)
	require.NoError(t, err)

	cluster := newCluster(t, svc)
	jobID := unique("job")
	submitJob(t, svc, cluster, jobID)

	condition := &dataproc.Expr{
		Title:       "expires-soon",
		Description: "temporary access",
		Expression:  `request.time < timestamp("2030-01-01T00:00:00Z")`,
	}
	binding := func(role, member string, cond *dataproc.Expr) *dataproc.Policy {
		return &dataproc.Policy{
			Bindings: []*dataproc.Binding{{Role: role, Members: []string{member}, Condition: cond}},
		}
	}

	t.Run("cluster", func(t *testing.T) {
		c := svc.Projects.Regions.Clusters
		name := "projects/" + dpProject + "/regions/" + dpRegion + "/clusters/" + cluster

		empty, err := c.GetIamPolicy(name, &dataproc.GetIamPolicyRequest{}).Do()
		require.NoError(t, err)
		require.Equal(t, int64(1), empty.Version)
		require.NotEmpty(t, empty.Etag)
		require.Empty(t, empty.Bindings)

		set, err := c.SetIamPolicy(name, &dataproc.SetIamPolicyRequest{
			Policy: binding("roles/dataproc.worker", "user:cluster-iam@example.com", condition),
		}).Do()
		require.NoError(t, err)
		require.Equal(t, int64(3), set.Version)
		require.NotEmpty(t, set.Etag)

		got, err := c.GetIamPolicy(name, &dataproc.GetIamPolicyRequest{}).Do()
		require.NoError(t, err)
		require.Equal(t, int64(3), got.Version)
		require.Len(t, got.Bindings, 1)
		require.Equal(t, "roles/dataproc.worker", got.Bindings[0].Role)
		require.Equal(t, []string{"user:cluster-iam@example.com"}, got.Bindings[0].Members)
		require.NotNil(t, got.Bindings[0].Condition)
		require.Equal(t, condition.Expression, got.Bindings[0].Condition.Expression)

		perms, err := c.TestIamPermissions(name, &dataproc.TestIamPermissionsRequest{
			Permissions: []string{"dataproc.clusters.get", "dataproc.clusters.delete"},
		}).Do()
		require.NoError(t, err)
		require.Equal(t, []string{"dataproc.clusters.get", "dataproc.clusters.delete"}, perms.Permissions)

		stale := binding("roles/viewer", "user:stale@example.com", nil)
		stale.Etag = "stale"
		_, err = c.SetIamPolicy(name, &dataproc.SetIamPolicyRequest{Policy: stale}).Do()
		requireStaleEtag(t, err)

		after, err := c.GetIamPolicy(name, &dataproc.GetIamPolicyRequest{}).Do()
		require.NoError(t, err)
		require.Equal(t, got.Etag, after.Etag)
		require.Len(t, after.Bindings, 1)

		absent := "projects/" + dpProject + "/regions/" + dpRegion + "/clusters/" + unique("absent")
		_, err = c.GetIamPolicy(absent, &dataproc.GetIamPolicyRequest{}).Do()
		var gerr *googleapi.Error
		require.ErrorAs(t, err, &gerr)
		require.Equal(t, 404, gerr.Code)
	})

	t.Run("job", func(t *testing.T) {
		jobs := svc.Projects.Regions.Jobs
		name := "projects/" + dpProject + "/regions/" + dpRegion + "/jobs/" + jobID

		empty, err := jobs.GetIamPolicy(name, &dataproc.GetIamPolicyRequest{}).Do()
		require.NoError(t, err)
		require.Equal(t, int64(1), empty.Version)
		require.Empty(t, empty.Bindings)

		set, err := jobs.SetIamPolicy(name, &dataproc.SetIamPolicyRequest{
			Policy: binding("roles/dataproc.viewer", "user:job-iam@example.com", nil),
		}).Do()
		require.NoError(t, err)
		require.NotEmpty(t, set.Etag)

		got, err := jobs.GetIamPolicy(name, &dataproc.GetIamPolicyRequest{}).Do()
		require.NoError(t, err)
		require.Len(t, got.Bindings, 1)
		require.Equal(t, "roles/dataproc.viewer", got.Bindings[0].Role)

		perms, err := jobs.TestIamPermissions(name, &dataproc.TestIamPermissionsRequest{
			Permissions: []string{"dataproc.jobs.get"},
		}).Do()
		require.NoError(t, err)
		require.Equal(t, []string{"dataproc.jobs.get"}, perms.Permissions)

		stale := binding("roles/editor", "user:stale@example.com", nil)
		stale.Etag = "stale"
		_, err = jobs.SetIamPolicy(name, &dataproc.SetIamPolicyRequest{Policy: stale}).Do()
		requireStaleEtag(t, err)

		absent := "projects/" + dpProject + "/regions/" + dpRegion + "/jobs/" + unique("absent")
		_, err = jobs.GetIamPolicy(absent, &dataproc.GetIamPolicyRequest{}).Do()
		var gerr *googleapi.Error
		require.ErrorAs(t, err, &gerr)
		require.Equal(t, 404, gerr.Code)
	})
}
