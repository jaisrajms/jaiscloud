// Package sdk_bigquery_test exercises the jaiscloud-gcp emulator's BigQuery
// v2 REST surface through the official Google apiary client. This validates
// wire-level parity with the real SDK: the emulator is metadata-not-engine, so
// jobs.query returns jobComplete=true with empty results, and tabledata
// insertAll/list round-trips streamed rows.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_bigquery_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func TestSDKBigQuery(t *testing.T) {
	ctx := context.Background()
	svc, err := bigquery.NewService(ctx, opts()...)
	require.NoError(t, err)

	const project = "test-project"
	datasetID := unique("ds")
	tableID := unique("tbl")
	jobID := unique("job")

	t.Cleanup(func() {
		_ = svc.Tables.Delete(project, datasetID, tableID).Do()
		_ = svc.Datasets.Delete(project, datasetID).Do()
		_ = svc.Jobs.Delete(project, jobID).Do()
	})

	t.Run("CreateDataset", func(t *testing.T) {
		ds, err := svc.Datasets.Insert(project, &bigquery.Dataset{
			DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: datasetID},
			FriendlyName:     "SDK Test Dataset",
			Location:         "US",
		}).Do()
		require.NoError(t, err)
		require.Equal(t, datasetID, ds.DatasetReference.DatasetId)
		require.Equal(t, project, ds.DatasetReference.ProjectId)
	})

	t.Run("CreateTableWithSchema", func(t *testing.T) {
		tbl, err := svc.Tables.Insert(project, datasetID, &bigquery.Table{
			TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: datasetID, TableId: tableID},
			Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "STRING"},
			}},
		}).Do()
		require.NoError(t, err)
		require.Equal(t, tableID, tbl.TableReference.TableId)
		require.Equal(t, "TABLE", tbl.Type)
		require.Len(t, tbl.Schema.Fields, 2)
	})

	t.Run("UpdateDataset", func(t *testing.T) {
		up, err := svc.Datasets.Update(project, datasetID, &bigquery.Dataset{
			DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: datasetID},
			FriendlyName:     "SDK Updated Dataset",
		}).Do()
		require.NoError(t, err)
		require.Equal(t, "SDK Updated Dataset", up.FriendlyName)
		require.NotEmpty(t, up.Etag)

		got, err := svc.Datasets.Get(project, datasetID).Do()
		require.NoError(t, err)
		require.Equal(t, "SDK Updated Dataset", got.FriendlyName)
	})

	t.Run("UpdateTable", func(t *testing.T) {
		up, err := svc.Tables.Update(project, datasetID, tableID, &bigquery.Table{
			TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: datasetID, TableId: tableID},
			FriendlyName:   "SDK Updated Table",
			Description:    "updated via PUT",
		}).Do()
		require.NoError(t, err)
		require.Equal(t, "SDK Updated Table", up.FriendlyName)
		require.Equal(t, "updated via PUT", up.Description)
		require.NotEmpty(t, up.Etag)
	})

	t.Run("InsertAllRows", func(t *testing.T) {
		resp, err := svc.Tabledata.InsertAll(project, datasetID, tableID, &bigquery.TableDataInsertAllRequest{
			Rows: []*bigquery.TableDataInsertAllRequestRows{
				{InsertId: "1", Json: map[string]bigquery.JsonValue{"id": 1, "name": "alice"}},
				{InsertId: "2", Json: map[string]bigquery.JsonValue{"id": 2, "name": "bob"}},
			},
		}).Do()
		require.NoError(t, err)
		require.Len(t, resp.InsertErrors, 0)
	})

	t.Run("ListRowsRoundTrip", func(t *testing.T) {
		data, err := svc.Tabledata.List(project, datasetID, tableID).Do()
		require.NoError(t, err)
		require.Len(t, data.Rows, 2)
		require.Equal(t, int64(2), data.TotalRows)
		// Schema-ordered cells: id (INTEGER) then name (STRING). Every
		// primitive cell is a string on the BigQuery wire.
		require.Equal(t, "1", data.Rows[0].F[0].V)
		require.Equal(t, "alice", data.Rows[0].F[1].V)
		require.Equal(t, "2", data.Rows[1].F[0].V)
		require.Equal(t, "bob", data.Rows[1].F[1].V)
	})

	t.Run("JobsQueryEmptyResults", func(t *testing.T) {
		qr, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        "SELECT * FROM somewhere",
			UseLegacySql: googleapi.Bool(false),
		}).Do()
		require.NoError(t, err)
		require.True(t, qr.JobComplete)
		require.Empty(t, qr.Rows)
		require.Equal(t, uint64(0), qr.TotalRows)
		require.NotEmpty(t, qr.JobReference.JobId)

		// The query job is stored: getQueryResults + get both work.
		gqr, err := svc.Jobs.GetQueryResults(project, qr.JobReference.JobId).Do()
		require.NoError(t, err)
		require.True(t, gqr.JobComplete)
		require.Empty(t, gqr.Rows)

		got, err := svc.Jobs.Get(project, qr.JobReference.JobId).Do()
		require.NoError(t, err)
		require.Equal(t, "DONE", got.Status.State)
	})

	t.Run("JobsInsertAndCancel", func(t *testing.T) {
		ins, err := svc.Jobs.Insert(project, &bigquery.Job{
			JobReference: &bigquery.JobReference{ProjectId: project, JobId: jobID},
			Configuration: &bigquery.JobConfiguration{
				Query: &bigquery.JobConfigurationQuery{Query: "SELECT 2", UseLegacySql: googleapi.Bool(false)},
			},
		}).Do()
		require.NoError(t, err)
		require.Equal(t, "DONE", ins.Status.State)
		require.Equal(t, jobID, ins.JobReference.JobId)

		cancel, err := svc.Jobs.Cancel(project, jobID).Do()
		require.NoError(t, err)
		require.Equal(t, "DONE", cancel.Job.Status.State)
	})

	t.Run("JobsInsertRequiresJobId", func(t *testing.T) {
		_, err := svc.Jobs.Insert(project, &bigquery.Job{
			Configuration: &bigquery.JobConfiguration{
				Query: &bigquery.JobConfigurationQuery{Query: "SELECT 1", UseLegacySql: googleapi.Bool(false)},
			},
		}).Do()
		require.Error(t, err)
		var gerr *googleapi.Error
		require.True(t, errors.As(err, &gerr))
		require.Equal(t, 400, gerr.Code)
	})

	t.Run("ListAndGet", func(t *testing.T) {
		dsList, err := svc.Datasets.List(project).Do()
		require.NoError(t, err)
		require.NotEmpty(t, dsList.Datasets)

		tblList, err := svc.Tables.List(project, datasetID).Do()
		require.NoError(t, err)
		require.NotEmpty(t, tblList.Tables)

		jobList, err := svc.Jobs.List(project).Do()
		require.NoError(t, err)
		require.NotEmpty(t, jobList.Jobs)

		ds, err := svc.Datasets.Get(project, datasetID).Do()
		require.NoError(t, err)
		require.Equal(t, datasetID, ds.DatasetReference.DatasetId)

		tbl, err := svc.Tables.Get(project, datasetID, tableID).Do()
		require.NoError(t, err)
		require.Equal(t, uint64(2), tbl.NumRows)
	})

	t.Run("Delete", func(t *testing.T) {
		require.NoError(t, svc.Tables.Delete(project, datasetID, tableID).Do())
		require.NoError(t, svc.Datasets.Delete(project, datasetID).Do())
		require.NoError(t, svc.Jobs.Delete(project, jobID).Do())
	})
}
