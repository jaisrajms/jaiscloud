// Package sdk_bigquery_test exercises the executed-SQL surface of the
// jaiscloud-gcp BigQuery v2 REST API through the official Google apiary
// client. This is the SDK half of the BQ3 conformance gate: the BQ1/BQ2
// engine evaluates a bounded Standard SQL subset, so the official client must
// observe real rows, DDL/DML statement statistics, and fail-loud errors for
// constructs outside the frozen subset.
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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
)

// TestSDKBigQuerySQL drives the executed-SQL path end-to-end: it seeds rows
// through tabledata.insertAll, queries them with the official client, then
// runs CREATE TABLE AS SELECT / INSERT / UPDATE / DELETE / TRUNCATE and checks
// the Discovery statement statistics and that the store stays the source of
// truth. Unsupported statements and dry runs are checked too.
//
// The pinned apiary client does not model QueryResponse.statementType (it is
// declared in the Discovery document), so the statement type of a jobs.query
// is read back from the persisted job statistics via jobs.get — the same place
// the emulator records it. DML affected-row counts are read from the
// jobs.query response itself (QueryResponse.numDmlAffectedRows).
func TestSDKBigQuerySQL(t *testing.T) {
	ctx := context.Background()
	svc, err := bigquery.NewService(ctx, opts()...)
	require.NoError(t, err)

	const project = "test-project"
	datasetID := unique("sql_ds")
	srcTable := unique("src")
	dstTable := unique("dst")

	t.Cleanup(func() {
		_ = svc.Tables.Delete(project, datasetID, srcTable).Do()
		_ = svc.Tables.Delete(project, datasetID, dstTable).Do()
		_ = svc.Datasets.Delete(project, datasetID).Do()
	})

	_, err = svc.Datasets.Insert(project, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: datasetID},
	}).Do()
	require.NoError(t, err)

	tableRef := &bigquery.TableReference{ProjectId: project, DatasetId: datasetID, TableId: srcTable}
	_, err = svc.Tables.Insert(project, datasetID, &bigquery.Table{
		TableReference: tableRef,
		Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "STRING"},
			{Name: "score", Type: "FLOAT"},
		}},
	}).Do()
	require.NoError(t, err)

	_, err = svc.Tabledata.InsertAll(project, datasetID, srcTable, &bigquery.TableDataInsertAllRequest{
		Rows: []*bigquery.TableDataInsertAllRequestRows{
			{InsertId: "1", Json: map[string]bigquery.JsonValue{"id": 1, "name": "alice", "score": 1.5}},
			{InsertId: "2", Json: map[string]bigquery.JsonValue{"id": 2, "name": "bob", "score": 2.5}},
			{InsertId: "3", Json: map[string]bigquery.JsonValue{"id": 3, "name": "carol", "score": 3.5}},
		},
	}).Do()
	require.NoError(t, err)

	// numRows reads the store directly (tables.get), independent of the query
	// engine, so it is the "source of truth" oracle for DDL/DML.
	numRows := func(id string) uint64 {
		t.Helper()
		tbl, err := svc.Tables.Get(project, datasetID, id).Do()
		require.NoError(t, err)
		return tbl.NumRows
	}

	query := func(q string) *bigquery.QueryResponse {
		t.Helper()
		qr, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        q,
			UseLegacySql: googleapi.Bool(false),
		}).Do()
		require.NoError(t, err)
		require.True(t, qr.JobComplete)
		require.NotNil(t, qr.JobReference)
		return qr
	}

	// statementType reads the persisted job statistics (jobs.get) — the
	// statement type of a jobs.query is stored there by the emulator.
	statementType := func(qr *bigquery.QueryResponse) string {
		t.Helper()
		j, err := svc.Jobs.Get(project, qr.JobReference.JobId).Do()
		require.NoError(t, err)
		require.NotNil(t, j.Statistics)
		require.NotNil(t, j.Statistics.Query)
		return j.Statistics.Query.StatementType
	}

	t.Run("SelectExpressionAndOrder", func(t *testing.T) {
		qr := query(fmt.Sprintf(
			"SELECT name, score * 2 AS doubled FROM `%s.%s.%s` WHERE score >= 2.5 ORDER BY score",
			project, datasetID, srcTable))
		require.Equal(t, "SELECT", statementType(qr))
		require.Equal(t, uint64(2), qr.TotalRows)
		require.Len(t, qr.Rows, 2)
		require.Equal(t, "bob", qr.Rows[0].F[0].V)
		// score is FLOAT64 and score * 2 is inferred FLOAT64 from the plan.
		require.Equal(t, "5", qr.Rows[0].F[1].V)
		require.Equal(t, "7", qr.Rows[1].F[1].V)
		require.Len(t, qr.Schema.Fields, 2)
		require.Equal(t, "doubled", qr.Schema.Fields[1].Name)
		require.Equal(t, "FLOAT", qr.Schema.Fields[1].Type)
	})

	t.Run("CreateTableAsSelect", func(t *testing.T) {
		qr := query(fmt.Sprintf(
			"CREATE TABLE `%s.%s.%s` AS SELECT id, name, score FROM `%s.%s.%s` WHERE score >= 2.5",
			project, datasetID, dstTable, project, datasetID, srcTable))
		require.Equal(t, "CREATE_TABLE_AS_SELECT", statementType(qr))
		require.Len(t, qr.Rows, 0)
		require.Equal(t, uint64(2), numRows(dstTable))
	})

	t.Run("Insert", func(t *testing.T) {
		qr := query(fmt.Sprintf(
			"INSERT INTO `%s.%s.%s` (id, name, score) VALUES (4, 'dave', 4.5)",
			project, datasetID, dstTable))
		require.Equal(t, "INSERT", statementType(qr))
		require.Equal(t, int64(1), qr.NumDmlAffectedRows)
		require.Equal(t, uint64(3), numRows(dstTable))

		// The DML is visible through tabledata.list, not just the engine.
		data, err := svc.Tabledata.List(project, datasetID, dstTable).Do()
		require.NoError(t, err)
		require.Equal(t, int64(3), data.TotalRows)
	})

	t.Run("Update", func(t *testing.T) {
		qr := query(fmt.Sprintf(
			"UPDATE `%s.%s.%s` SET score = score + 1 WHERE id = 4",
			project, datasetID, dstTable))
		require.Equal(t, "UPDATE", statementType(qr))
		require.Equal(t, int64(1), qr.NumDmlAffectedRows)

		check := query(fmt.Sprintf(
			"SELECT score FROM `%s.%s.%s` WHERE id = 4", project, datasetID, dstTable))
		require.Len(t, check.Rows, 1)
		require.Equal(t, "5.5", check.Rows[0].F[0].V)
	})

	t.Run("Delete", func(t *testing.T) {
		qr := query(fmt.Sprintf(
			"DELETE FROM `%s.%s.%s` WHERE id = 2", project, datasetID, dstTable))
		require.Equal(t, "DELETE", statementType(qr))
		require.Equal(t, int64(1), qr.NumDmlAffectedRows)
		require.Equal(t, uint64(2), numRows(dstTable))
	})

	t.Run("InsertJobStatisticsPersistWithoutReapply", func(t *testing.T) {
		jobID := unique("dml_job")
		ins, err := svc.Jobs.Insert(project, &bigquery.Job{
			JobReference: &bigquery.JobReference{ProjectId: project, JobId: jobID},
			Configuration: &bigquery.JobConfiguration{
				Query: &bigquery.JobConfigurationQuery{
					Query: fmt.Sprintf(
						"INSERT INTO `%s.%s.%s` (id, name, score) VALUES (9, 'erin', 9.5)",
						project, datasetID, dstTable),
					UseLegacySql: googleapi.Bool(false),
				},
			},
		}).Do()
		require.NoError(t, err)
		require.Equal(t, "DONE", ins.Status.State)
		require.NotNil(t, ins.Statistics)
		require.NotNil(t, ins.Statistics.Query)
		require.Equal(t, "INSERT", ins.Statistics.Query.StatementType)
		require.Equal(t, int64(1), ins.Statistics.Query.NumDmlAffectedRows)

		before := numRows(dstTable)
		// getQueryResults must return the persisted statistics, never run the
		// INSERT again.
		gqr, err := svc.Jobs.GetQueryResults(project, jobID).Do()
		require.NoError(t, err)
		require.True(t, gqr.JobComplete)
		require.Equal(t, int64(1), gqr.NumDmlAffectedRows)
		require.Len(t, gqr.Rows, 0)
		require.Equal(t, before, numRows(dstTable))
		_ = svc.Jobs.Delete(project, jobID).Do()
	})

	t.Run("DryRunDoesNotMutate", func(t *testing.T) {
		before := numRows(dstTable)
		qr, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query: fmt.Sprintf(
				"INSERT INTO `%s.%s.%s` (id, name, score) VALUES (10, 'frank', 1.0)",
				project, datasetID, dstTable),
			UseLegacySql: googleapi.Bool(false),
			DryRun:       true,
		}).Do()
		require.NoError(t, err)
		require.True(t, qr.JobComplete)
		require.Equal(t, before, numRows(dstTable))

		// A dry run persists no job, so the synthesized reference 404s.
		_, err = svc.Jobs.Get(project, qr.JobReference.JobId).Do()
		require.Error(t, err)
		var gerr *googleapi.Error
		require.True(t, errors.As(err, &gerr))
		require.Equal(t, 404, gerr.Code)
	})

	t.Run("UnsupportedFailsLoud", func(t *testing.T) {
		for _, q := range []string{
			fmt.Sprintf("MERGE `%s.%s.%s` T USING `%s.%s.%s` S ON T.id = S.id", project, datasetID, srcTable, project, datasetID, srcTable),
			fmt.Sprintf("SELECT * FROM `%s.%s.INFORMATION_SCHEMA.TABLES`", project, datasetID),
		} {
			_, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
				Query:        q,
				UseLegacySql: googleapi.Bool(false),
			}).Do()
			require.Error(t, err, "query should fail loud: %s", q)
			var gerr *googleapi.Error
			require.True(t, errors.As(err, &gerr), "query %q: expected *googleapi.Error, got %v", q, err)
			require.Equal(t, 400, gerr.Code, "query %q", q)
		}
	})

	t.Run("Truncate", func(t *testing.T) {
		qr := query(fmt.Sprintf("TRUNCATE TABLE `%s.%s.%s`", project, datasetID, dstTable))
		require.Equal(t, "TRUNCATE_TABLE", statementType(qr))
		require.Equal(t, uint64(0), numRows(dstTable))
	})
}
