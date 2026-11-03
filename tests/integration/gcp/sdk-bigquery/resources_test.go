// Package sdk_bigquery_test — NF31: dataset-scoped routines and table-scoped
// row access policies driven through the official apiary client. This proves
// the wire shape the real SDK parses (Routine/RoutineReference with int64
// millisecond timestamps, RowAccessPolicy/RowAccessPolicyReference with
// google-datetime strings and input-only grantees), not just the emulator's own
// view of it.
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -run RoutinesAndRowAccess ./...
package sdk_bigquery_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

func TestSDKBigQueryRoutinesAndRowAccessPolicies(t *testing.T) {
	ctx := context.Background()
	svc, err := bigquery.NewService(ctx, opts()...)
	require.NoError(t, err)

	const project = "test-project"
	datasetID := unique("ds_routine")
	tableID := unique("tbl_policy")
	routineID := unique("routine")
	policyID := unique("policy")

	_, err = svc.Datasets.Insert(project, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: datasetID},
		Location:         "US",
	}).Do()
	require.NoError(t, err)
	_, err = svc.Tables.Insert(project, datasetID, &bigquery.Table{
		TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: datasetID, TableId: tableID},
		Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
			{Name: "region", Type: "STRING"},
		}},
	}).Do()
	require.NoError(t, err)
	extraRoutines := []string{routineID + "_p1", routineID + "_p2"}
	extraPolicies := []string{policyID + "_p1", policyID + "_p2"}
	t.Cleanup(func() {
		for _, id := range append([]string{routineID}, extraRoutines...) {
			_ = svc.Routines.Delete(project, datasetID, id).Do()
		}
		for _, id := range append([]string{policyID}, extraPolicies...) {
			_ = svc.RowAccessPolicies.Delete(project, datasetID, tableID, id).Do()
		}
		_ = svc.Datasets.Delete(project, datasetID).Do()
	})

	routine := &bigquery.Routine{
		RoutineReference: &bigquery.RoutineReference{
			ProjectId: project, DatasetId: datasetID, RoutineId: routineID,
		},
		RoutineType:    "SCALAR_FUNCTION",
		Language:       "SQL",
		DefinitionBody: "x * 2",
		Description:    "sdk routine",
		Arguments: []*bigquery.Argument{{
			Name:         "x",
			ArgumentKind: "FIXED_TYPE",
			Mode:         "IN",
			DataType:     &bigquery.StandardSqlDataType{TypeKind: "INT64"},
		}},
		ReturnType: &bigquery.StandardSqlDataType{TypeKind: "INT64"},
	}

	t.Run("RoutineCreateGetListUpdateDelete", func(t *testing.T) {
		created, err := svc.Routines.Insert(project, datasetID, routine).Do()
		require.NoError(t, err)
		require.Equal(t, routineID, created.RoutineReference.RoutineId)
		require.Equal(t, "SCALAR_FUNCTION", created.RoutineType)
		require.Equal(t, "x * 2", created.DefinitionBody)
		// creationTime/lastModifiedTime are int64 milliseconds since the epoch.
		require.NotEmpty(t, created.CreationTime)
		require.NotEmpty(t, created.LastModifiedTime)
		require.Len(t, created.Arguments, 1)
		require.Equal(t, "INT64", created.Arguments[0].DataType.TypeKind)

		got, err := svc.Routines.Get(project, datasetID, routineID).Do()
		require.NoError(t, err)
		require.Equal(t, "x * 2", got.DefinitionBody)
		require.Equal(t, created.Etag, got.Etag)

		list, err := svc.Routines.List(project, datasetID).Do()
		require.NoError(t, err)
		found := false
		for _, r := range list.Routines {
			if r.RoutineReference != nil && r.RoutineReference.RoutineId == routineID {
				found = true
				require.Equal(t, "SCALAR_FUNCTION", r.RoutineType)
			}
		}
		require.True(t, found, "routine missing from routines.list")

		updated := *routine
		updated.DefinitionBody = "x * 3"
		updated.Etag = created.Etag
		up, err := svc.Routines.Update(project, datasetID, routineID, &updated).Do()
		require.NoError(t, err)
		require.Equal(t, "x * 3", up.DefinitionBody)
		require.NotEqual(t, created.Etag, up.Etag)

		require.NoError(t, svc.Routines.Delete(project, datasetID, routineID).Do())
		_, err = svc.Routines.Get(project, datasetID, routineID).Do()
		require.Error(t, err)
		var apiErr *googleapi.Error
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, 404, apiErr.Code)
	})

	t.Run("RowAccessPolicyCreateGetListUpdateDelete", func(t *testing.T) {
		policy := &bigquery.RowAccessPolicy{
			RowAccessPolicyReference: &bigquery.RowAccessPolicyReference{
				ProjectId: project, DatasetId: datasetID, TableId: tableID, PolicyId: policyID,
			},
			FilterPredicate: `region = "EU"`,
			// Input only: the API never echoes grantees.
			Grantees: []string{"user:sdk@example.com"},
		}
		created, err := svc.RowAccessPolicies.Insert(project, datasetID, tableID, policy).Do()
		require.NoError(t, err)
		require.Equal(t, policyID, created.RowAccessPolicyReference.PolicyId)
		require.Equal(t, `region = "EU"`, created.FilterPredicate)
		require.NotEmpty(t, created.CreationTime, "creationTime is a google-datetime string")
		require.NotEmpty(t, created.LastModifiedTime)
		require.Empty(t, created.Grantees, "grantees is input-only and must not be echoed")

		got, err := svc.RowAccessPolicies.Get(project, datasetID, tableID, policyID).Do()
		require.NoError(t, err)
		require.Equal(t, created.Etag, got.Etag)

		list, err := svc.RowAccessPolicies.List(project, datasetID, tableID).PageSize(10).Do()
		require.NoError(t, err)
		found := false
		for _, p := range list.RowAccessPolicies {
			if p.RowAccessPolicyReference != nil && p.RowAccessPolicyReference.PolicyId == policyID {
				found = true
			}
		}
		require.True(t, found, "policy missing from rowAccessPolicies.list")

		updated := *policy
		updated.FilterPredicate = `region = "US"`
		updated.Etag = created.Etag
		up, err := svc.RowAccessPolicies.Update(project, datasetID, tableID, policyID, &updated).Do()
		require.NoError(t, err)
		require.Equal(t, `region = "US"`, up.FilterPredicate)
		require.Equal(t, created.CreationTime, up.CreationTime)

		require.NoError(t, svc.RowAccessPolicies.Delete(project, datasetID, tableID, policyID).Do())
		_, err = svc.RowAccessPolicies.Get(project, datasetID, tableID, policyID).Do()
		require.Error(t, err)
		var apiErr *googleapi.Error
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, 404, apiErr.Code)
	})

	t.Run("DuplicateAndPaging", func(t *testing.T) {
		// The lifecycle subtest deleted the routine, so re-create it and then
		// assert a second create of the same ID is the API's duplicate (409).
		_, err := svc.Routines.Insert(project, datasetID, routine).Do()
		require.NoError(t, err)
		_, err = svc.Routines.Insert(project, datasetID, routine).Do()
		require.Error(t, err)
		var apiErr *googleapi.Error
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, 409, apiErr.Code)
		require.NotEmpty(t, apiErr.Errors, "the legacy errors[] reason must be present")
		require.Equal(t, "duplicate", apiErr.Errors[0].Reason)

		policy := &bigquery.RowAccessPolicy{
			RowAccessPolicyReference: &bigquery.RowAccessPolicyReference{
				ProjectId: project, DatasetId: datasetID, TableId: tableID, PolicyId: policyID,
			},
			FilterPredicate: `region = "EU"`,
		}
		// The lifecycle subtest deleted the policy, so create it again and then
		// assert the duplicate.
		_, err = svc.RowAccessPolicies.Insert(project, datasetID, tableID, policy).Do()
		require.NoError(t, err)
		_, err = svc.RowAccessPolicies.Insert(project, datasetID, tableID, policy).Do()
		require.Error(t, err)
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, 409, apiErr.Code)

		// Paging fixtures: two extra routines and two extra policies.
		for _, id := range extraRoutines {
			r := *routine
			r.RoutineReference = &bigquery.RoutineReference{ProjectId: project, DatasetId: datasetID, RoutineId: id}
			_, err := svc.Routines.Insert(project, datasetID, &r).Do()
			require.NoError(t, err)
		}
		for _, id := range extraPolicies {
			p := *policy
			p.RowAccessPolicyReference = &bigquery.RowAccessPolicyReference{
				ProjectId: project, DatasetId: datasetID, TableId: tableID, PolicyId: id,
			}
			_, err := svc.RowAccessPolicies.Insert(project, datasetID, tableID, &p).Do()
			require.NoError(t, err)
		}
		// maxResults+pageToken (routines) and pageSize+pageToken (policies)
		// walk the whole set without skipping or duplicating an ID.
		seen := map[string]bool{}
		pageToken := ""
		for {
			call := svc.Routines.List(project, datasetID).MaxResults(2)
			if pageToken != "" {
				call = call.PageToken(pageToken)
			}
			page, err := call.Do()
			require.NoError(t, err)
			for _, r := range page.Routines {
				seen[r.RoutineReference.RoutineId] = true
			}
			if page.NextPageToken == "" {
				break
			}
			pageToken = page.NextPageToken
		}
		for _, id := range append([]string{routineID}, extraRoutines...) {
			require.True(t, seen[id], "routine %s missing from a paged list", id)
		}

		seenPolicies := map[string]bool{}
		pageToken = ""
		for {
			call := svc.RowAccessPolicies.List(project, datasetID, tableID).PageSize(2)
			if pageToken != "" {
				call = call.PageToken(pageToken)
			}
			page, err := call.Do()
			require.NoError(t, err)
			for _, pol := range page.RowAccessPolicies {
				seenPolicies[pol.RowAccessPolicyReference.PolicyId] = true
			}
			if page.NextPageToken == "" {
				break
			}
			pageToken = page.NextPageToken
		}
		for _, id := range append([]string{policyID}, extraPolicies...) {
			require.True(t, seenPolicies[id], "policy %s missing from a paged list", id)
		}
	})

	// models stays an explicit 501; the SDK surfaces it as an HTTP 501 with the
	// legacy errors[] reason, not a 404 route miss.
	t.Run("ModelsUnimplemented", func(t *testing.T) {
		_, err := svc.Models.Get(project, datasetID, "missing_model").Do()
		require.Error(t, err)
		var apiErr *googleapi.Error
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, 501, apiErr.Code)
	})
}
