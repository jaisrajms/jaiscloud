// Package sdk_bigquery_test — behavioural depth (p2–p5) for the BigQuery v2
// surface, driven by the official apiary client against the emulator. Where
// bigquery_test.go/sql_test.go prove the baseline lifecycle and the executed-SQL
// subset, this file exercises the behaviour classes docs/gcp/testability.md §10
// names:
//
//	p3 errors & negatives  — duplicate dataset/table 409, unknown dataset/table
//	                         and unknown job 404, subset rejection 400, paging
//	                         input rejection 400.
//	p4 listing & paging    — datasets/tables/jobs listing by maxResults +
//	                         pageToken, tabledata.list by maxResults +
//	                         startIndex, and jobs.query/getQueryResults
//	                         result-set paging.
//	p2 concurrency         — parallel metadata churn and concurrent WRITE_APPEND
//	                         load jobs against one destination table, asserted
//	                         from a -race test client.
//	p5 scale & boundary    — a real gs:// load (multi-object wildcard, gzip
//	                         NEWLINE_DELIMITED_JSON, autodetected schema), a
//	                         many-row load and a bounded large-result query.
//
// Run with the GCP binary running:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -race ./...
package sdk_bigquery_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// bqService returns an apiary BigQuery client pointed at the emulator.
func bqService(t *testing.T) *bigquery.Service {
	t.Helper()
	svc, err := bigquery.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

// storageClient returns a Cloud Storage client for seeding gs:// load sources.
// The high-level client only honors the emulator through STORAGE_EMULATOR_HOST,
// so mirror GCP_EMULATOR_ENDPOINT onto it when it is unset.
func storageClient(t *testing.T) *storage.Client {
	t.Helper()
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimRight(endpoint(), "/"))
	}
	c, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// cleanupBucket removes a seeded bucket and its objects when the test ends, so
// repeated runs against one emulator do not accumulate GCS state.
func cleanupBucket(t *testing.T, ctx context.Context, c *storage.Client, bucket string) {
	t.Helper()
	t.Cleanup(func() {
		it := c.Bucket(bucket).Objects(ctx, nil)
		for {
			attrs, err := it.Next()
			if err != nil {
				break
			}
			_ = c.Bucket(bucket).Object(attrs.Name).Delete(ctx)
		}
		_ = c.Bucket(bucket).Delete(ctx)
	})
}

// gerrCode asserts err is a *googleapi.Error with the given HTTP status.
func gerrCode(t *testing.T, err error, code int, what string) {
	t.Helper()
	require.Error(t, err, what)
	var gerr *googleapi.Error
	require.True(t, errors.As(err, &gerr), "%s: expected *googleapi.Error, got %v", what, err)
	require.Equal(t, code, gerr.Code, "%s: %v", what, gerr)
}

// TestSDKBigQueryErrors covers the p3 class: the real error envelope and codes
// for duplicates, missing resources, subset rejection and invalid paging input.
func TestSDKBigQueryErrors(t *testing.T) {
	svc := bqService(t)
	const project = "test-project"

	ds := unique("err_ds")
	_, err := svc.Datasets.Insert(project, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: ds},
	}).Do()
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Datasets.Delete(project, ds).Do() })

	tbl := unique("err_tbl")
	_, err = svc.Tables.Insert(project, ds, &bigquery.Table{
		TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: tbl},
		Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
			{Name: "id", Type: "INTEGER"},
		}},
	}).Do()
	require.NoError(t, err)

	t.Run("DuplicateDatasetIs409", func(t *testing.T) {
		_, err := svc.Datasets.Insert(project, &bigquery.Dataset{
			DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: ds},
		}).Do()
		gerrCode(t, err, 409, "duplicate dataset")
	})

	t.Run("DuplicateTableIs409", func(t *testing.T) {
		_, err := svc.Tables.Insert(project, ds, &bigquery.Table{
			TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: tbl},
		}).Do()
		gerrCode(t, err, 409, "duplicate table")
	})

	t.Run("MissingResourcesAre404", func(t *testing.T) {
		_, err := svc.Datasets.Get(project, unique("nope_ds")).Do()
		gerrCode(t, err, 404, "get missing dataset")

		_, err = svc.Tables.Get(project, ds, unique("nope_tbl")).Do()
		gerrCode(t, err, 404, "get missing table")

		_, err = svc.Jobs.Get(project, unique("nope_job")).Do()
		gerrCode(t, err, 404, "get missing job")

		_, err = svc.Jobs.GetQueryResults(project, unique("nope_job")).Do()
		gerrCode(t, err, 404, "getQueryResults of missing job")

		// A streamed insert into a table that does not exist is a 404, not a
		// silently-dropped row.
		_, err = svc.Tabledata.InsertAll(project, ds, unique("nope_tbl"), &bigquery.TableDataInsertAllRequest{
			Rows: []*bigquery.TableDataInsertAllRequestRows{
				{InsertId: "1", Json: map[string]bigquery.JsonValue{"id": 1}},
			},
		}).Do()
		gerrCode(t, err, 404, "insertAll into missing table")
	})

	t.Run("UnknownDatasetInQueryIs404", func(t *testing.T) {
		_, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        fmt.Sprintf("SELECT * FROM `%s.%s.%s`", project, unique("nope_ds"), tbl),
			UseLegacySql: googleapi.Bool(false),
		}).Do()
		gerrCode(t, err, 404, "query unknown dataset")
	})

	t.Run("SubsetRejectedFailsLoud", func(t *testing.T) {
		for _, q := range []string{
			"SELECT * FROM UNNEST([1, 2, 3]) AS id",
			fmt.Sprintf("SELECT * FROM `%s.%s.events_*`", project, ds),
			"SELECT SAFE.DIVIDE(1, 0) AS x",
		} {
			_, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
				Query:        q,
				UseLegacySql: googleapi.Bool(false),
			}).Do()
			gerrCode(t, err, 400, "unsupported query: "+q)
		}
		// Legacy SQL is rejected before execution (never a successful-looking
		// wrong answer).
		_, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        fmt.Sprintf("SELECT * FROM [%s.%s.%s]", project, ds, tbl),
			UseLegacySql: googleapi.Bool(true),
		}).Do()
		gerrCode(t, err, 400, "legacy SQL")
	})

	t.Run("JobIdRequiredIs400", func(t *testing.T) {
		_, err := svc.Jobs.Insert(project, &bigquery.Job{
			Configuration: &bigquery.JobConfiguration{
				Query: &bigquery.JobConfigurationQuery{Query: "SELECT 1", UseLegacySql: googleapi.Bool(false)},
			},
		}).Do()
		gerrCode(t, err, 400, "jobs.insert without jobId")
	})

	t.Run("InvalidPagingInputIs400", func(t *testing.T) {
		qr, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        fmt.Sprintf("SELECT id FROM `%s.%s.%s`", project, ds, tbl),
			UseLegacySql: googleapi.Bool(false),
		}).Do()
		require.NoError(t, err)

		_, err = svc.Jobs.GetQueryResults(project, qr.JobReference.JobId).MaxResults(-1).Do()
		gerrCode(t, err, 400, "getQueryResults with maxResults=-1")

		_, err = svc.Tabledata.List(project, ds, tbl).StartIndex(0).MaxResults(-1).Do()
		gerrCode(t, err, 400, "tabledata.list with maxResults=-1")

		_, err = svc.Datasets.List(project).MaxResults(-1).Do()
		gerrCode(t, err, 400, "datasets.list with maxResults=-1")

		_, err = svc.Tables.List(project, ds).MaxResults(-1).Do()
		gerrCode(t, err, 400, "tables.list with maxResults=-1")

		_, err = svc.Jobs.List(project).MaxResults(-1).Do()
		gerrCode(t, err, 400, "jobs.list with maxResults=-1")
	})
}

// TestSDKBigQueryListingPaging covers the p4 class over the list/read surfaces:
// datasets, tables and jobs walk by maxResults + pageToken, and tabledata.list
// offsets by startIndex.
func TestSDKBigQueryListingPaging(t *testing.T) {
	ctx := context.Background()
	svc := bqService(t)
	const project = "test-project"

	ds := unique("page_ds")
	_, err := svc.Datasets.Insert(project, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: ds},
	}).Do()
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Datasets.Delete(project, ds).Do() })

	// Five tables in the dataset: tables.list is scoped to the dataset, so the
	// page contents are deterministic.
	tableIDs := make(map[string]bool)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%s_t%d", ds, i)
		tableIDs[id] = true
		_, err := svc.Tables.Insert(project, ds, &bigquery.Table{
			TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: id},
			Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{Name: "id", Type: "INTEGER"},
			}},
		}).Do()
		require.NoError(t, err)
	}

	t.Run("TablesListWalksPages", func(t *testing.T) {
		seen := map[string]bool{}
		token := ""
		pages := 0
		for {
			call := svc.Tables.List(project, ds).MaxResults(2).Context(ctx)
			if token != "" {
				call = call.PageToken(token)
			}
			page, err := call.Do()
			require.NoError(t, err)
			pages++
			require.LessOrEqual(t, len(page.Tables), 2, "maxResults=2 must bound the page")
			for _, tb := range page.Tables {
				id := tb.TableReference.TableId
				require.False(t, seen[id], "table %s returned on more than one page", id)
				seen[id] = true
			}
			if page.NextPageToken == "" {
				break
			}
			token = page.NextPageToken
			require.Less(t, pages, 10, "pagination did not terminate")
		}
		require.Equal(t, tableIDs, seen, "the walk must cover exactly the created tables")
	})

	t.Run("DatasetsListIsStableAcrossPages", func(t *testing.T) {
		walk := func() []string {
			var ids []string
			token := ""
			for {
				call := svc.Datasets.List(project).MaxResults(2).Context(ctx)
				if token != "" {
					call = call.PageToken(token)
				}
				page, err := call.Do()
				require.NoError(t, err)
				for _, d := range page.Datasets {
					ids = append(ids, d.DatasetReference.DatasetId)
				}
				if page.NextPageToken == "" {
					return ids
				}
				token = page.NextPageToken
			}
		}
		first, second := walk(), walk()
		require.Equal(t, first, second, "two identical walks must agree (cross-page stability)")
		require.Contains(t, first, ds, "the created dataset must appear in the walk")
	})

	t.Run("JobsListCoversCreatedJobs", func(t *testing.T) {
		created := map[string]bool{}
		for i := 0; i < 5; i++ {
			id := unique(fmt.Sprintf("page_job%d", i))
			created[id] = true
			_, err := svc.Jobs.Insert(project, &bigquery.Job{
				JobReference: &bigquery.JobReference{ProjectId: project, JobId: id},
				Configuration: &bigquery.JobConfiguration{
					Query: &bigquery.JobConfigurationQuery{Query: "SELECT 1", UseLegacySql: googleapi.Bool(false)},
				},
			}).Do()
			require.NoError(t, err)
		}
		seen := map[string]int{}
		token := ""
		for {
			call := svc.Jobs.List(project).MaxResults(2).Context(ctx)
			if token != "" {
				call = call.PageToken(token)
			}
			page, err := call.Do()
			require.NoError(t, err)
			for _, j := range page.Jobs {
				seen[j.JobReference.JobId]++
			}
			if page.NextPageToken == "" {
				break
			}
			token = page.NextPageToken
		}
		for id := range created {
			require.Equal(t, 1, seen[id], "job %s must appear exactly once across pages", id)
		}
	})

	t.Run("TableDataPagingAndStartIndex", func(t *testing.T) {
		rowTable := unique("page_rows")
		_, err := svc.Tables.Insert(project, ds, &bigquery.Table{
			TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: rowTable},
			Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{Name: "id", Type: "INTEGER"},
			}},
		}).Do()
		require.NoError(t, err)
		rows := make([]*bigquery.TableDataInsertAllRequestRows, 0, 5)
		for i := 1; i <= 5; i++ {
			rows = append(rows, &bigquery.TableDataInsertAllRequestRows{
				InsertId: fmt.Sprint(i), Json: map[string]bigquery.JsonValue{"id": i},
			})
		}
		_, err = svc.Tabledata.InsertAll(project, ds, rowTable, &bigquery.TableDataInsertAllRequest{Rows: rows}).Do()
		require.NoError(t, err)

		p1, err := svc.Tabledata.List(project, ds, rowTable).MaxResults(2).Do()
		require.NoError(t, err)
		require.Len(t, p1.Rows, 2)
		require.Equal(t, int64(5), p1.TotalRows)
		require.NotEmpty(t, p1.PageToken)

		p2, err := svc.Tabledata.List(project, ds, rowTable).MaxResults(2).PageToken(p1.PageToken).Do()
		require.NoError(t, err)
		require.Len(t, p2.Rows, 2)

		// startIndex offsets into the full row set: rows 4 and 5.
		offset, err := svc.Tabledata.List(project, ds, rowTable).StartIndex(3).MaxResults(2).Do()
		require.NoError(t, err)
		require.Len(t, offset.Rows, 2)
		require.Equal(t, "4", offset.Rows[0].F[0].V)

		// An out-of-range startIndex is an empty page, not an error.
		empty, err := svc.Tabledata.List(project, ds, rowTable).StartIndex(99).Do()
		require.NoError(t, err)
		require.Len(t, empty.Rows, 0)
	})
}

// TestSDKBigQueryResultPaging covers the p4 getQueryResults corner: jobs.query
// maxResults truncates the first page, and getQueryResults continues it by
// pageToken or startIndex.
func TestSDKBigQueryResultPaging(t *testing.T) {
	svc := bqService(t)
	const project = "test-project"

	ds := unique("res_ds")
	_, err := svc.Datasets.Insert(project, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: ds},
	}).Do()
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Datasets.Delete(project, ds).Do() })

	tbl := unique("res_tbl")
	_, err = svc.Tables.Insert(project, ds, &bigquery.Table{
		TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: tbl},
		Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
			{Name: "id", Type: "INTEGER"},
		}},
	}).Do()
	require.NoError(t, err)

	rows := make([]*bigquery.TableDataInsertAllRequestRows, 0, 5)
	for i := 1; i <= 5; i++ {
		rows = append(rows, &bigquery.TableDataInsertAllRequestRows{
			InsertId: fmt.Sprint(i), Json: map[string]bigquery.JsonValue{"id": i},
		})
	}
	_, err = svc.Tabledata.InsertAll(project, ds, tbl, &bigquery.TableDataInsertAllRequest{Rows: rows}).Do()
	require.NoError(t, err)

	query := fmt.Sprintf("SELECT id FROM `%s.%s.%s` ORDER BY id", project, ds, tbl)

	ids := func(rows []*bigquery.TableRow) []string {
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			out = append(out, fmt.Sprint(r.F[0].V))
		}
		return out
	}

	// jobs.query with maxResults returns the first page + a pageToken.
	qr, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
		Query: query, UseLegacySql: googleapi.Bool(false), MaxResults: 2,
	}).Do()
	require.NoError(t, err)
	require.True(t, qr.JobComplete)
	require.Equal(t, []string{"1", "2"}, ids(qr.Rows))
	require.Equal(t, uint64(5), qr.TotalRows)
	require.NotEmpty(t, qr.PageToken, "a truncated page must carry a pageToken")

	// getQueryResults continues by pageToken.
	g2, err := svc.Jobs.GetQueryResults(project, qr.JobReference.JobId).MaxResults(2).PageToken(qr.PageToken).Do()
	require.NoError(t, err)
	require.Equal(t, []string{"3", "4"}, ids(g2.Rows))
	require.NotEmpty(t, g2.PageToken)

	g3, err := svc.Jobs.GetQueryResults(project, qr.JobReference.JobId).MaxResults(2).PageToken(g2.PageToken).Do()
	require.NoError(t, err)
	require.Equal(t, []string{"5"}, ids(g3.Rows))
	require.Empty(t, g3.PageToken, "the final page must not carry a pageToken")

	// startIndex selects a zero-based start row.
	g4, err := svc.Jobs.GetQueryResults(project, qr.JobReference.JobId).StartIndex(3).MaxResults(2).Do()
	require.NoError(t, err)
	require.Equal(t, []string{"4", "5"}, ids(g4.Rows))
	require.Empty(t, g4.PageToken)

	// With maxResults unset, getQueryResults returns the whole bounded set.
	all, err := svc.Jobs.GetQueryResults(project, qr.JobReference.JobId).Do()
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2", "3", "4", "5"}, ids(all.Rows))
	require.Empty(t, all.PageToken)

	// An undecodable pageToken is rejected, never silently ignored (which
	// would return page 1 with a 200).
	_, err = svc.Jobs.GetQueryResults(project, qr.JobReference.JobId).PageToken("not-a-real-token").Do()
	gerrCode(t, err, 400, "getQueryResults with an invalid pageToken")
}

// TestSDKBigQueryConcurrency covers the p2 class under -race: parallel metadata
// churn, a duplicate-create race that must have exactly one winner, and
// concurrent WRITE_APPEND load jobs against one destination table.
func TestSDKBigQueryConcurrency(t *testing.T) {
	ctx := context.Background()
	svc := bqService(t)
	const project = "test-project"

	t.Run("DuplicateDatasetRace", func(t *testing.T) {
		ds := unique("race_ds")
		t.Cleanup(func() { _ = svc.Datasets.Delete(project, ds).Do() })

		const writers = 8
		var (
			wg        sync.WaitGroup
			mu        sync.Mutex
			ok, dupes int
			other     []error
		)
		start := make(chan struct{})
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := svc.Datasets.Insert(project, &bigquery.Dataset{
					DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: ds},
				}).Do()
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					ok++
				case isCode(err, 409):
					dupes++
				default:
					other = append(other, err)
				}
			}()
		}
		close(start)
		wg.Wait()
		require.Empty(t, other, "unexpected errors: %v", other)
		require.Equal(t, 1, ok, "exactly one concurrent create must win")
		require.Equal(t, writers-1, dupes, "every loser must be a 409")
	})

	t.Run("ParallelTableChurn", func(t *testing.T) {
		ds := unique("churn_ds")
		_, err := svc.Datasets.Insert(project, &bigquery.Dataset{
			DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: ds},
		}).Do()
		require.NoError(t, err)
		t.Cleanup(func() { _ = svc.Datasets.Delete(project, ds).Do() })

		const writers = 8
		var (
			wg   sync.WaitGroup
			errs []error
			mu   sync.Mutex
		)
		record := func(err error) {
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("churn_t%d", i)
				ref := &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: id}
				if _, err := svc.Tables.Insert(project, ds, &bigquery.Table{
					TableReference: ref,
					Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
						{Name: "id", Type: "INTEGER"},
					}},
				}).Do(); err != nil {
					record(err)
					return
				}
				if _, err := svc.Tables.Get(project, ds, id).Do(); err != nil {
					record(err)
				}
				if _, err := svc.Tables.Update(project, ds, id, &bigquery.Table{
					TableReference: ref, FriendlyName: "updated",
				}).Do(); err != nil {
					record(err)
				}
			}(i)
		}
		wg.Wait()
		require.Empty(t, errs, "unexpected errors: %v", errs)

		page, err := svc.Tables.List(project, ds).Do()
		require.NoError(t, err)
		require.Len(t, page.Tables, writers)
	})

	t.Run("ConcurrentAppendLoads", func(t *testing.T) {
		ds := unique("load_ds")
		_, err := svc.Datasets.Insert(project, &bigquery.Dataset{
			DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: ds},
		}).Do()
		require.NoError(t, err)
		t.Cleanup(func() { _ = svc.Datasets.Delete(project, ds).Do() })

		dest := unique("load_dest")
		// Pre-create the destination so every load only appends (a concurrent
		// create-race is the dataset/table test above).
		_, err = svc.Tables.Insert(project, ds, &bigquery.Table{
			TableReference: &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: dest},
			Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{Name: "id", Type: "INTEGER"},
			}},
		}).Do()
		require.NoError(t, err)

		bucket := unique("load-bucket")
		client := storageClient(t)
		require.NoError(t, client.Bucket(bucket).Create(ctx, project, &storage.BucketAttrs{Location: "US"}))
		cleanupBucket(t, ctx, client, bucket)

		const files = 6
		const rowsPerFile = 4
		// Seed one JSON object per writer.
		for i := 0; i < files; i++ {
			var buf bytes.Buffer
			for r := 0; r < rowsPerFile; r++ {
				fmt.Fprintf(&buf, `{"id": %d}`+"\n", i*rowsPerFile+r)
			}
			w := client.Bucket(bucket).Object(fmt.Sprintf("part-%02d.json", i)).NewWriter(ctx)
			_, err := w.Write(buf.Bytes())
			require.NoError(t, err)
			require.NoError(t, w.Close())
		}

		var wg sync.WaitGroup
		var mu sync.Mutex
		var errs []error
		start := make(chan struct{})
		for i := 0; i < files; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				_, err := svc.Jobs.Insert(project, &bigquery.Job{
					JobReference: &bigquery.JobReference{ProjectId: project, JobId: unique(fmt.Sprintf("loadjob%d", i))},
					Configuration: &bigquery.JobConfiguration{Load: &bigquery.JobConfigurationLoad{
						SourceUris:       []string{fmt.Sprintf("gs://%s/part-%02d.json", bucket, i)},
						DestinationTable: &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: dest},
						SourceFormat:     "NEWLINE_DELIMITED_JSON",
						WriteDisposition: "WRITE_APPEND",
					}},
				}).Do()
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(i)
		}
		close(start)
		wg.Wait()
		require.Empty(t, errs, "unexpected load errors: %v", errs)

		got, err := svc.Tables.Get(project, ds, dest).Do()
		require.NoError(t, err)
		require.Equal(t, uint64(files*rowsPerFile), got.NumRows, "no append may be lost")
	})
}

// TestSDKBigQueryLoadBoundary covers the p5 class: a real gs:// load through
// the official client with a multi-object wildcard, gzip-compressed
// NEWLINE_DELIMITED_JSON, an autodetected schema, a many-row load and a bounded
// large-result query.
func TestSDKBigQueryLoadBoundary(t *testing.T) {
	ctx := context.Background()
	svc := bqService(t)
	const project = "test-project"

	ds := unique("loadb_ds")
	_, err := svc.Datasets.Insert(project, &bigquery.Dataset{
		DatasetReference: &bigquery.DatasetReference{ProjectId: project, DatasetId: ds},
	}).Do()
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Datasets.Delete(project, ds).Do() })

	bucket := unique("load-bucket")
	client := storageClient(t)
	require.NoError(t, client.Bucket(bucket).Create(ctx, project, &storage.BucketAttrs{Location: "US"}))
	cleanupBucket(t, ctx, client, bucket)

	put := func(name string, data []byte) {
		t.Helper()
		w := client.Bucket(bucket).Object(name).NewWriter(ctx)
		_, err := w.Write(data)
		require.NoError(t, err)
		require.NoError(t, w.Close())
	}
	load := func(jobID, tableID string, l *bigquery.JobConfigurationLoad) *bigquery.Job {
		t.Helper()
		l.DestinationTable = &bigquery.TableReference{ProjectId: project, DatasetId: ds, TableId: tableID}
		job, err := svc.Jobs.Insert(project, &bigquery.Job{
			JobReference:  &bigquery.JobReference{ProjectId: project, JobId: jobID},
			Configuration: &bigquery.JobConfiguration{Load: l},
		}).Do()
		require.NoError(t, err)
		require.Equal(t, "DONE", job.Status.State)
		require.NotNil(t, job.Statistics)
		require.NotNil(t, job.Statistics.Load)
		return job
	}
	numRows := func(tableID string) uint64 {
		t.Helper()
		tbl, err := svc.Tables.Get(project, ds, tableID).Do()
		require.NoError(t, err)
		return tbl.NumRows
	}
	ids := func(q string) []string {
		t.Helper()
		qr, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query: q, UseLegacySql: googleapi.Bool(false),
		}).Do()
		require.NoError(t, err)
		out := make([]string, 0, len(qr.Rows))
		for _, r := range qr.Rows {
			out = append(out, fmt.Sprint(r.F[0].V))
		}
		return out
	}

	t.Run("WildcardMultiObjectLoad", func(t *testing.T) {
		put("wld/part-01.json", []byte(`{"id": 1, "name": "a"}`+"\n"))
		put("wld/part-02.json", []byte(`{"id": 2, "name": "b"}`+"\n"))
		// A non-matching object must not be read.
		put("wld/other.txt", []byte("not json\n"))

		table := unique("wld_tbl")
		job := load(unique("wld_job"), table, &bigquery.JobConfigurationLoad{
			SourceUris:   []string{fmt.Sprintf("gs://%s/wld/part-*.json", bucket)},
			SourceFormat: "NEWLINE_DELIMITED_JSON",
			Autodetect:   true,
		})
		require.Equal(t, int64(2), job.Statistics.Load.InputFiles, "both wildcard objects must be read")
		require.Equal(t, int64(2), job.Statistics.Load.OutputRows)
		require.Equal(t, uint64(2), numRows(table))
		require.Equal(t, []string{"a", "b"}, ids(fmt.Sprintf(
			"SELECT name FROM `%s.%s.%s` ORDER BY id", project, ds, table)))
	})

	t.Run("GzipNDJSONAutoDetected", func(t *testing.T) {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, err := zw.Write([]byte(`{"id": 10, "score": 1.5}` + "\n" + `{"id": 11, "score": 2.5}` + "\n"))
		require.NoError(t, err)
		require.NoError(t, zw.Close())
		put("gz/data.json", buf.Bytes())

		table := unique("gz_tbl")
		job := load(unique("gz_job"), table, &bigquery.JobConfigurationLoad{
			SourceUris:   []string{fmt.Sprintf("gs://%s/gz/data.json", bucket)},
			SourceFormat: "NEWLINE_DELIMITED_JSON",
			Autodetect:   true,
		})
		require.Equal(t, int64(2), job.Statistics.Load.OutputRows)
		require.Equal(t, uint64(2), numRows(table))
		// score is inferred FLOAT64 → Discovery FLOAT.
		tbl, err := svc.Tables.Get(project, ds, table).Do()
		require.NoError(t, err)
		require.Equal(t, "FLOAT", tbl.Schema.Fields[1].Type)
	})

	t.Run("ManyRowLoadAndLargeResult", func(t *testing.T) {
		const n = 2000
		var buf bytes.Buffer
		for i := 0; i < n; i++ {
			fmt.Fprintf(&buf, `{"id": %d}`+"\n", i)
		}
		put("big/data.json", buf.Bytes())

		table := unique("big_tbl")
		job := load(unique("big_job"), table, &bigquery.JobConfigurationLoad{
			SourceUris:   []string{fmt.Sprintf("gs://%s/big/data.json", bucket)},
			SourceFormat: "NEWLINE_DELIMITED_JSON",
			Schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{Name: "id", Type: "INTEGER"},
			}},
		})
		require.Equal(t, int64(n), job.Statistics.Load.OutputRows)
		require.Equal(t, uint64(n), numRows(table))

		// A large result returns the whole bounded set in one (unpaged) response
		// with a correct totalRows.
		qr, err := svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        fmt.Sprintf("SELECT id FROM `%s.%s.%s` ORDER BY id", project, ds, table),
			UseLegacySql: googleapi.Bool(false),
		}).Do()
		require.NoError(t, err)
		require.Equal(t, uint64(n), qr.TotalRows)
		require.Len(t, qr.Rows, n)
		require.Equal(t, "0", qr.Rows[0].F[0].V)
		require.Equal(t, fmt.Sprint(n-1), qr.Rows[n-1].F[0].V)

		// Aggregating over the same table exercises the engine at scale.
		qr, err = svc.Jobs.Query(project, &bigquery.QueryRequest{
			Query:        fmt.Sprintf("SELECT COUNT(*) AS n FROM `%s.%s.%s`", project, ds, table),
			UseLegacySql: googleapi.Bool(false),
		}).Do()
		require.NoError(t, err)
		require.Len(t, qr.Rows, 1)
		require.Equal(t, fmt.Sprint(n), qr.Rows[0].F[0].V)
	})
}

// isCode reports whether err is a *googleapi.Error with the given status.
func isCode(err error, code int) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == code
}
