// Package sdk_bigquery_test — TF1-1: table-level IAM driven through the
// official apiary client (tables.getIamPolicy / setIamPolicy /
// testIamPermissions). This proves the wire shape the real SDK parses — an
// unwrapped Policy with version/etag/bindings and a permission array — and the
// documented fail-open testIamPermissions for an absent table.
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -run TableIAM ./...
package sdk_bigquery_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

func TestSDKBigQueryTableIAM(t *testing.T) {
	ctx := context.Background()
	svc, err := bigquery.NewService(ctx, opts()...)
	require.NoError(t, err)

	const project = "test-project"
	datasetID := unique("ds_iam")
	tableID := unique("tbl_iam")
	resource := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, datasetID, tableID)

	_, err = svc.Datasets.Insert(project, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: datasetID},
		Location:         "US",
	}).Do()
	require.NoError(t, err)
	_, err = svc.Tables.Insert(project, datasetID, &bigquery.Table{
		TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: datasetID, TableId: tableID},
		Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
			{Name: "id", Type: "INTEGER"},
		}},
	}).Do()
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Datasets.Delete(project, datasetID).Do() })

	policy, err := svc.Tables.GetIamPolicy(resource, &bigquery.GetIamPolicyRequest{}).Do()
	require.NoError(t, err)
	require.Equal(t, int64(1), policy.Version)
	require.Empty(t, policy.Bindings)
	require.NotEmpty(t, policy.Etag)

	set, err := svc.Tables.SetIamPolicy(resource, &bigquery.SetIamPolicyRequest{
		Policy: &bigquery.Policy{
			Version: 1,
			Bindings: []*bigquery.Binding{{
				Role:    "roles/bigquery.dataViewer",
				Members: []string{"user:sdk@example.com"},
			}},
		},
	}).Do()
	require.NoError(t, err)
	require.Len(t, set.Bindings, 1)
	require.Equal(t, "roles/bigquery.dataViewer", set.Bindings[0].Role)
	require.Equal(t, []string{"user:sdk@example.com"}, set.Bindings[0].Members)
	require.NotEmpty(t, set.Etag)

	// A write carrying the current etag succeeds; re-using it afterwards is a
	// 409 (ABORTED) optimistic-concurrency failure.
	next := &bigquery.Policy{
		Version: 1,
		Etag:    set.Etag,
		Bindings: []*bigquery.Binding{{
			Role:    "roles/bigquery.dataViewer",
			Members: []string{"user:other@example.com"},
		}},
	}
	updated, err := svc.Tables.SetIamPolicy(resource, &bigquery.SetIamPolicyRequest{Policy: next}).Do()
	require.NoError(t, err)
	require.Equal(t, []string{"user:other@example.com"}, updated.Bindings[0].Members)
	require.NotEqual(t, set.Etag, updated.Etag)

	_, err = svc.Tables.SetIamPolicy(resource, &bigquery.SetIamPolicyRequest{Policy: next}).Do()
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 409, apiErr.Code)
	require.Equal(t, "aborted", apiErr.Errors[0].Reason)

	perms, err := svc.Tables.TestIamPermissions(resource, &bigquery.TestIamPermissionsRequest{
		Permissions: []string{"bigquery.tables.get", "bigquery.tables.getData"},
	}).Do()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bigquery.tables.get", "bigquery.tables.getData"}, perms.Permissions)

	// Discovery documents an empty permission set (not NOT_FOUND) for an absent
	// resource; the emulator honours that fail-open shape.
	absent := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, datasetID, tableID+"_absent")
	perms, err = svc.Tables.TestIamPermissions(absent, &bigquery.TestIamPermissionsRequest{
		Permissions: []string{"bigquery.tables.get"},
	}).Do()
	require.NoError(t, err)
	require.Empty(t, perms.Permissions)

	// get/set on an absent table are a real 404.
	_, err = svc.Tables.GetIamPolicy(absent, &bigquery.GetIamPolicyRequest{}).Do()
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code)
}
