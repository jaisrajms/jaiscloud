// Package bigquery implements the BigQuery v2 provider
// (bigquery.googleapis.com/bigquery/v2). Datasets, tables, jobs and streamed
// rows are stored records, and jobs.query / jobs.insert (query configuration)
// evaluate a bounded Standard SQL subset on an in-process pure-Go SQLite engine
// in both memory and --dsn modes: SELECTs return real rows and DDL/DML mutate
// the store (the source of truth; SQLite is disposable per-query scratch).
// Constructs outside the frozen subset fail loud with 400 invalidQuery.
// tabledata.insertAll stores streamed rows that tabledata.list reads back;
// tabledata.list honors startIndex as an offset into the row set but does not
// echo it in the response (startIndex is a request-only parameter in the
// Discovery TableDataList schema). routines/models/rowAccessPolicies are
// deferred and route to an explicit Unimplemented (501) rather than the
// codec's 404.
//
// jobs.insert also accepts a configuration.load job: one or more gs:// source
// objects (a single object, a single-'*' wildcard, or a list; resolved through
// the injected SourceReader against the emulated GCS) are decoded as
// NEWLINE_DELIMITED_JSON, CSV, Parquet or Avro — gzip is auto-detected for the
// text formats — coerced to the destination schema (explicit or autodetected)
// and written per writeDisposition, synchronously, with statistics.load on the
// job. The same decoder backs the media-upload endpoint the official clients use
// for load_table_from_file: a multipart/related jobs.insert, or a resumable
// session (POST /upload/bigquery/v2/.../jobs then PUT chunks with Content-Range).
// Everything outside that subset (ORC, partitioning/clustering,
// schemaUpdateOptions, a non-default quote/encoding, a multi-'*' wildcard)
// fails loud (400/501); a data-level failure (too many bad records, WRITE_EMPTY
// on a non-empty table) is reported in status.errorResult like real BigQuery.
// See load.go.
//
// Known limitations (emulator simplifications, documented rather than fixed):
//   - tabledata.insertAll honors insertId (best-effort duplicate suppression
//     over a bounded, TTL'd per-table window), skipInvalidRows,
//     ignoreUnknownValues and the table schema (missing REQUIRED fields and
//     unknown fields). Row-level failures never fail the request: the response
//     is always 200 with per-row insertErrors, and when skipInvalidRows is
//     false nothing is inserted and the otherwise-valid rows are reported with
//     reason "stopped" (matching the real API). insertId dedup is process-local
//     and not persisted across store snapshots/restarts. templateSuffix is not
//     supported and field *types* are not enforced (only presence/unknown-field
//     checks).
//   - tabledata.list renders every TableCell the way the client SDKs parse it:
//     primitive values as strings, REPEATED as {"v": [<cell>...]},
//     RECORD/STRUCT as {"v": {"f": [...]}}, and NULL as {"v": null}.
//   - List methods emit the Discovery summary subsets: datasets.list uses
//     DatasetList.datasets, tables.list uses TableList.tables, and jobs.list
//     uses JobList.jobs (ListFormatJob, which omits etag).
//   - jobs.query/getQueryResults honor result-set paging: jobs.query honors
//     maxResults (returning pageRowCount + pageToken) and getQueryResults
//     honors maxResults/pageToken/startIndex. Like real GCP's jobs.query, an
//     unset maxResults imposes no row limit (only the engine's scan guard
//     applies), so the complete bounded result set is returned in one page;
//     getQueryResults likewise returns the whole set when maxResults is unset.
//     getQueryResults re-executes the stored query (BQ6: the store stays the
//     single source of truth, so a write between pages is visible).
//   - projects.getServiceAccount returns a synthetic
//     bq-{project}@gcp-sa-bigquery.iam.gserviceaccount.com email rather than a
//     real service account.
package bigquery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/identity"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/queryengine"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

const kindPrefix = "bigquery#"

// Provider handles BigQuery datasets, tables, jobs, and tabledata.
type Provider struct {
	store bqstore.Store
	// sourceReader resolves gs:// load-job sources against the emulated GCS.
	// It is injected by main.go; nil means load jobs fail loud (Unimplemented).
	sourceReader SourceReader

	// mu guards uploads, the in-progress resumable load-job upload sessions
	// (load_table_from_file with an unknown size). Sessions are in-memory only:
	// a load source is decoded from a whole buffer (see BQF3), so a session is
	// bounded and cleared on Reset.
	mu      sync.Mutex
	uploads map[string]*loadUploadSession
}

// New returns a Provider backed by the given store.
func New(s bqstore.Store, opts ...Option) *Provider {
	p := &Provider{store: s, uploads: map[string]*loadUploadSession{}}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Reset wipes the store and any in-progress resumable load-job upload sessions.
func (p *Provider) Reset(ctx context.Context) {
	p.store.Reset(ctx)
	p.mu.Lock()
	p.uploads = map[string]*loadUploadSession{}
	p.mu.Unlock()
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"BigQuery.CreateDataset":           p.CreateDataset,
		"BigQuery.GetDataset":              p.GetDataset,
		"BigQuery.ListDatasets":            p.ListDatasets,
		"BigQuery.UpdateDataset":           p.UpdateDataset,
		"BigQuery.DeleteDataset":           p.DeleteDataset,
		"BigQuery.CreateTable":             p.CreateTable,
		"BigQuery.GetTable":                p.GetTable,
		"BigQuery.ListTables":              p.ListTables,
		"BigQuery.UpdateTable":             p.UpdateTable,
		"BigQuery.DeleteTable":             p.DeleteTable,
		"BigQuery.InsertAll":               p.InsertAll,
		"BigQuery.ListRows":                p.ListRows,
		"BigQuery.InsertJob":               p.InsertJob,
		"BigQuery.InsertJobResumableStart": p.InsertJobResumableStart,
		"BigQuery.InsertJobResumable":      p.InsertJobResumable,
		"BigQuery.GetJob":                  p.GetJob,
		"BigQuery.ListJobs":                p.ListJobs,
		"BigQuery.DeleteJob":               p.DeleteJob,
		"BigQuery.CancelJob":               p.CancelJob,
		"BigQuery.Query":                   p.Query,
		"BigQuery.GetQueryResults":         p.GetQueryResults,
		"BigQuery.GetServiceAccount":       p.GetServiceAccount,
		"BigQuery.Routines":                p.Routines,
		"BigQuery.Models":                  p.Models,
		"BigQuery.RowAccessPolicies":       p.RowAccessPolicies,
	}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// projectOf returns the project for store scoping. The codec extracts the
// project from the path (nr.Params["project"]) which is authoritative even
// when the WithEndpoint-stripped path lacks a version segment the identity
// package would otherwise resolve; AccountID is the fallback.
func projectOf(nr *model.NormalizedRequest) string {
	if p := strParam(nr, "project"); p != "" {
		return p
	}
	return nr.AccountID
}

func bodyMap(nr *model.NormalizedRequest) map[string]any {
	m, _ := nr.Params["body"].(map[string]any)
	return m
}

func mapValue(m map[string]any, key string) map[string]any {
	if m == nil {
		return nil
	}
	v, _ := m[key].(map[string]any)
	return v
}

func strValue(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// maxResultsBody parses the optional maxResults field off a decoded jobs.query
// request body, defaulting to 0 (no page limit). A present but non-integer or
// negative value is InvalidArgument, matching the real API's rejection.
func maxResultsBody(m map[string]any) (int, error) {
	raw, ok := m["maxResults"]
	if !ok || raw == nil {
		return 0, nil
	}
	switch v := raw.(type) {
	case float64:
		if v < 0 || float64(int(v)) != v {
			return 0, invalidArgument("maxResults must be a non-negative integer")
		}
		return int(v), nil
	case int:
		if v < 0 {
			return 0, invalidArgument("maxResults must be a non-negative integer")
		}
		return v, nil
	case int64:
		if v < 0 {
			return 0, invalidArgument("maxResults must be a non-negative integer")
		}
		return int(v), nil
	case string:
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return 0, invalidArgument("maxResults must be a non-negative integer")
		}
		return n, nil
	}
	return 0, invalidArgument("maxResults must be a non-negative integer")
}

func stringMap(m map[string]any, key string) map[string]string {
	raw := mapValue(m, key)
	if raw == nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "bqjob_" + hex.EncodeToString(b)
}

func millis(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}

func mapErr(err error) error {
	switch {
	case errors.Is(err, bqstore.ErrNoSuchDataset):
		return model.NewProviderError("NotFound", "dataset not found", 404)
	case errors.Is(err, bqstore.ErrNoSuchTable):
		return model.NewProviderError("NotFound", "table not found", 404)
	case errors.Is(err, bqstore.ErrNoSuchJob):
		return model.NewProviderError("NotFound", "job not found", 404)
	case errors.Is(err, bqstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	}
	return err
}

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// --- Wire rendering ---

func (p *Provider) datasetMap(projectID string, d bqstore.Dataset) map[string]any {
	out := map[string]any{}
	if len(d.Config) > 0 {
		_ = json.Unmarshal(d.Config, &out)
	}
	out["kind"] = kindPrefix + "dataset"
	out["datasetReference"] = map[string]any{"projectId": projectID, "datasetId": d.DatasetID}
	out["id"] = projectID + ":" + d.DatasetID
	out["creationTime"] = millis(d.CreateTime)
	out["lastModifiedTime"] = millis(d.UpdateTime)
	out["etag"] = millis(d.UpdateTime)
	if loc, _ := out["location"].(string); loc == "" {
		out["location"] = "US"
	}
	if _, ok := out["access"]; !ok {
		out["access"] = defaultDatasetAccess()
	}
	labels := d.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// datasetSummaryMap renders the DatasetList summary subset the Discovery
// schema models for datasets[]. The full dataset resource (datasets.get/
// insert/update) carries additional fields (creationTime, etag,
// lastModifiedTime, access) that do not appear in a datasets.list response.
func (p *Provider) datasetSummaryMap(projectID string, d bqstore.Dataset) map[string]any {
	full := p.datasetMap(projectID, d)
	out := map[string]any{}
	for _, k := range []string{
		"kind", "id", "datasetReference", "labels", "location",
		"friendlyName", "type", "catalogSource", "externalDatasetReference",
	} {
		if v, ok := full[k]; ok {
			out[k] = v
		}
	}
	return out
}

// defaultDatasetAccess is the ACL real GCP assigns to a dataset created without
// an explicit access list: project writers may edit, project owners own, the
// creating user owns, and project readers may view. The user entry carries a
// synthetic identity because the emulator has no per-request principal.
func defaultDatasetAccess() []any {
	return []any{
		map[string]any{"role": "WRITER", "specialGroup": "projectWriters"},
		map[string]any{"role": "OWNER", "specialGroup": "projectOwners"},
		map[string]any{"role": "OWNER", "userByEmail": identity.DefaultServiceAccount},
		map[string]any{"role": "READER", "specialGroup": "projectReaders"},
	}
}

// withMaxTimeTravel adds the default maxTimeTravelHours (7 days) to a full
// dataset representation unless the caller configured one. Real GCP only
// surfaces this on datasets.get/update — not on datasets.insert, whose
// response omits it — so this is applied by the get/update handlers.
func withMaxTimeTravel(out map[string]any) map[string]any {
	if _, ok := out["maxTimeTravelHours"]; !ok {
		out["maxTimeTravelHours"] = "168"
	}
	return out
}

func (p *Provider) tableMap(projectID string, t bqstore.Table) map[string]any {
	out := map[string]any{}
	if len(t.Config) > 0 {
		_ = json.Unmarshal(t.Config, &out)
	}
	out["kind"] = kindPrefix + "table"
	out["tableReference"] = map[string]any{"projectId": projectID, "datasetId": t.DatasetID, "tableId": t.TableID}
	out["id"] = projectID + ":" + t.DatasetID + "." + t.TableID
	out["creationTime"] = millis(t.CreateTime)
	out["lastModifiedTime"] = millis(t.UpdateTime)
	out["etag"] = millis(t.UpdateTime)
	if len(t.Schema) > 0 {
		var sc map[string]any
		if json.Unmarshal(t.Schema, &sc) == nil {
			out["schema"] = sc
		}
	}
	if _, ok := out["type"].(string); !ok || out["type"] == "" {
		out["type"] = "TABLE"
	}
	labels := t.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	out["numRows"] = strconv.FormatInt(t.NumRows, 10)
	return out
}

// tableSummaryMap renders the TableList summary subset the Discovery schema
// models for tables[]. The full table resource (tables.get/insert/update)
// carries additional fields (etag, lastModifiedTime, numRows, schema) that do
// not appear in a tables.list response.
func (p *Provider) tableSummaryMap(projectID string, t bqstore.Table) map[string]any {
	full := p.tableMap(projectID, t)
	out := map[string]any{}
	for _, k := range []string{
		"kind", "id", "tableReference", "labels", "type", "creationTime",
		"friendlyName", "timePartitioning", "view", "clustering",
		"rangePartitioning", "requirePartitionFilter", "expirationTime",
	} {
		if v, ok := full[k]; ok {
			out[k] = v
		}
	}
	return out
}

func (p *Provider) jobMap(projectID string, j bqstore.Job) map[string]any {
	out := map[string]any{}
	if len(j.Config) > 0 {
		_ = json.Unmarshal(j.Config, &out)
	}
	out["kind"] = kindPrefix + "job"
	out["id"] = projectID + ":" + j.JobID
	out["etag"] = millis(j.CreateTime)
	ref := map[string]any{"projectId": projectID, "jobId": j.JobID}
	if existing, ok := out["jobReference"].(map[string]any); ok {
		if loc, _ := existing["location"].(string); loc != "" {
			ref["location"] = loc
		}
	}
	out["jobReference"] = ref
	if _, ok := out["configuration"]; !ok {
		out["configuration"] = map[string]any{}
	}
	// A job is always terminal here (loads/queries run synchronously), but a
	// data-level failure (WRITE_EMPTY on a non-empty table, maxBadRecords
	// exceeded) is reported in status.errorResult rather than as an HTTP error,
	// matching real BigQuery. Preserve it while forcing state=DONE.
	status := map[string]any{"state": "DONE"}
	if existing, ok := out["status"].(map[string]any); ok {
		for k, v := range existing {
			status[k] = v
		}
		status["state"] = "DONE"
	}
	out["status"] = status
	return out
}

// jobSummaryMap projects a job onto the Discovery JobList.jobs item schema
// (ListFormatJob), which omits etag. The full Job schema returned by
// jobs.get/insert does model etag, so only jobs.list uses this projection.
func (p *Provider) jobSummaryMap(projectID string, j bqstore.Job) map[string]any {
	out := p.jobMap(projectID, j)
	delete(out, "etag")
	return out
}

// pagingParams translates BigQuery's maxResults/pageToken query parameters into
// the shared paging helper's pageSize/pageToken shape (maxResults is BigQuery's
// page-size parameter; paging.PageSize reads pageSize). maxResults is validated
// up front by maxResultsQueryParam — the uint32 binding, so its rejection names
// `max_results` (DC63) — and a legal value above MaxInt32 is capped for the
// int-based pager here instead of being handed to PageSize's int32 binding
// (which would reject it). The `pageSize` arm is the emulator's own alias for
// maxResults; it is bound as if declared, like the other bespoke pagers.
func pagingParams(params map[string]any) map[string]any {
	out := map[string]any{}
	if v, ok := params["pageToken"]; ok {
		out["pageToken"] = v
	}
	if v, ok := params["maxResults"]; ok {
		out["pageSize"] = cappedMaxResults(v)
	} else if v, ok := params["pageSize"]; ok {
		out["pageSize"] = v
	}
	return out
}

// cappedMaxResults maps a legal BigQuery maxResults (uint32) onto the shared
// pager's int without changing its meaning: anything above MaxInt32 is capped
// there (the page can never exceed the available items anyway). The handlers
// validate maxResults before calling pagingParams, so a value the uint32 binding
// would reject never reaches the pager; the pass-through keeps this adapter
// total if that ever changes.
func cappedMaxResults(v any) any {
	switch x := v.(type) {
	case string:
		if n, err := strconv.ParseUint(x, 10, 32); err == nil && n > math.MaxInt32 {
			return int(math.MaxInt32)
		}
	case float64:
		if x == math.Trunc(x) && x > math.MaxInt32 && x <= math.MaxUint32 {
			return int(math.MaxInt32)
		}
	case int:
		if x > math.MaxInt32 {
			return int(math.MaxInt32)
		}
	case int64:
		if x > math.MaxInt32 && x <= math.MaxUint32 {
			return int(math.MaxInt32)
		}
	}
	return v
}

// --- Datasets ---

func (p *Provider) CreateDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyMap(nr)
	datasetID := strValue(mapValue(body, "datasetReference"), "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetReference.datasetId is required")
	}
	if _, err := p.store.GetDataset(ctx, projectOf(nr), datasetID); err == nil {
		return nil, mapErr(bqstore.ErrAlreadyExists)
	}
	now := clock.Now().UTC()
	d := bqstore.Dataset{
		DatasetID:  datasetID,
		Labels:     stringMap(body, "labels"),
		CreateTime: now,
		UpdateTime: now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			d.Config = data
		}
	}
	if err := p.store.CreateDataset(ctx, projectOf(nr), d); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.datasetMap(projectOf(nr), d)), nil
}

func (p *Provider) GetDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetId is required")
	}
	d, err := p.store.GetDataset(ctx, projectOf(nr), datasetID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(withMaxTimeTravel(p.datasetMap(projectOf(nr), d))), nil
}

func (p *Provider) ListDatasets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := maxResultsQueryParam(nr); err != nil {
		return nil, err
	}
	datasets, err := p.store.ListDatasets(ctx, projectOf(nr))
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(datasets, func(d bqstore.Dataset) string { return d.DatasetID }, pagingParams(nr.Params))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, d := range page {
		items = append(items, p.datasetSummaryMap(projectOf(nr), d))
	}
	resp := map[string]any{"kind": kindPrefix + "datasetList", "datasets": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetId is required")
	}
	body := bodyMap(nr)
	d, err := p.store.UpdateDatasetAtomic(ctx, projectOf(nr), datasetID, func(d bqstore.Dataset) (bqstore.Dataset, error) {
		if body != nil {
			stored := map[string]any{}
			if len(d.Config) > 0 {
				_ = json.Unmarshal(d.Config, &stored)
			}
			for k, v := range body {
				stored[k] = v
			}
			if data, err := json.Marshal(stored); err == nil {
				d.Config = data
			}
			if labels := stringMap(body, "labels"); labels != nil {
				d.Labels = labels
			}
		}
		d.UpdateTime = clock.Now().UTC()
		return d, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(withMaxTimeTravel(p.datasetMap(projectOf(nr), d))), nil
}

func (p *Provider) DeleteDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetId is required")
	}
	if err := p.store.DeleteDataset(ctx, projectOf(nr), datasetID); err != nil {
		return nil, mapErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: http.StatusNoContent, Data: map[string]any{}}, nil
}

// --- Tables ---

func (p *Provider) CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetId is required")
	}
	body := bodyMap(nr)
	tableID := strValue(mapValue(body, "tableReference"), "tableId")
	if tableID == "" {
		return nil, invalidArgument("tableReference.tableId is required")
	}
	if _, err := p.store.GetDataset(ctx, projectOf(nr), datasetID); err != nil {
		return nil, mapErr(err)
	}
	if _, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID); err == nil {
		return nil, mapErr(bqstore.ErrAlreadyExists)
	}
	now := clock.Now().UTC()
	t := bqstore.Table{
		DatasetID:  datasetID,
		TableID:    tableID,
		Labels:     stringMap(body, "labels"),
		CreateTime: now,
		UpdateTime: now,
	}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			t.Config = data
		}
		if schema := mapValue(body, "schema"); schema != nil {
			if data, err := json.Marshal(schema); err == nil {
				t.Schema = data
			}
		}
	}
	if err := p.store.CreateTable(ctx, projectOf(nr), datasetID, t); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.tableMap(projectOf(nr), t)), nil
}

func (p *Provider) GetTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	t, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.tableMap(projectOf(nr), t)), nil
}

func (p *Provider) ListTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetId is required")
	}
	if _, err := maxResultsQueryParam(nr); err != nil {
		return nil, err
	}
	tables, err := p.store.ListTables(ctx, projectOf(nr), datasetID)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(tables, func(t bqstore.Table) string { return t.TableID }, pagingParams(nr.Params))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, p.tableSummaryMap(projectOf(nr), t))
	}
	resp := map[string]any{"kind": kindPrefix + "tableList", "tables": items, "totalItems": len(tables)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	body := bodyMap(nr)
	t, err := p.store.UpdateTableAtomic(ctx, projectOf(nr), datasetID, tableID, func(t bqstore.Table) (bqstore.Table, error) {
		if body != nil {
			stored := map[string]any{}
			if len(t.Config) > 0 {
				_ = json.Unmarshal(t.Config, &stored)
			}
			for k, v := range body {
				stored[k] = v
			}
			if data, err := json.Marshal(stored); err == nil {
				t.Config = data
			}
			if schema := mapValue(body, "schema"); schema != nil {
				if data, err := json.Marshal(schema); err == nil {
					t.Schema = data
				}
			}
			if labels := stringMap(body, "labels"); labels != nil {
				t.Labels = labels
			}
		}
		t.UpdateTime = clock.Now().UTC()
		return t, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.tableMap(projectOf(nr), t)), nil
}

func (p *Provider) DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	if err := p.store.DeleteTable(ctx, projectOf(nr), datasetID, tableID); err != nil {
		return nil, mapErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: http.StatusNoContent, Data: map[string]any{}}, nil
}

// --- Tabledata ---

// insertError is one entry in a row's insertErrors[].errors list.
type insertError struct {
	Reason   string
	Location string
	Message  string
}

func (e insertError) toMap() map[string]any {
	return map[string]any{
		"reason":    e.Reason,
		"location":  e.Location,
		"message":   e.Message,
		"debugInfo": "",
	}
}

// schemaField is one top-level field of a table schema.
type schemaField struct {
	Name   string        `json:"name"`
	Mode   string        `json:"mode"`
	Type   string        `json:"type"`
	Fields []schemaField `json:"fields,omitempty"`
}

// parseSchemaFields extracts the top-level fields from a table's schema JSON.
// A missing/empty/unparseable schema yields nil, which callers treat as "no
// schema validation" so tables without one keep accepting any row.
func parseSchemaFields(schema json.RawMessage) []schemaField {
	if len(schema) == 0 {
		return nil
	}
	var s struct {
		Fields []schemaField `json:"fields"`
	}
	if json.Unmarshal(schema, &s) != nil {
		return nil
	}
	return s.Fields
}

// validateRow checks row against the table's top-level fields. A field is
// required only when its mode is explicitly REQUIRED (BigQuery's default mode
// is NULLABLE). Unknown fields are rejected unless ignoreUnknownValues is set.
// Field value types are not checked — only presence and unknown-field names.
func validateRow(row map[string]any, fields []schemaField, ignoreUnknownValues bool) []insertError {
	if len(fields) == 0 {
		return nil
	}
	known := make(map[string]bool, len(fields))
	for _, f := range fields {
		known[f.Name] = true
	}
	var errs []insertError
	for _, f := range fields {
		if !strings.EqualFold(f.Mode, "REQUIRED") {
			continue
		}
		if v, ok := row[f.Name]; !ok || v == nil {
			errs = append(errs, insertError{
				Reason:   "invalid",
				Location: f.Name,
				Message:  fmt.Sprintf("missing required field %q", f.Name),
			})
		}
	}
	if !ignoreUnknownValues {
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !known[k] {
				errs = append(errs, insertError{
					Reason:   "invalid",
					Location: k,
					Message:  fmt.Sprintf("no such field: %s", k),
				})
			}
		}
	}
	return errs
}

func boolValue(m map[string]any, key string) bool {
	b, _ := m[key].(bool)
	return b
}

func (p *Provider) InsertAll(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	body := bodyMap(nr)
	rawRows, _ := body["rows"].([]any)
	skipInvalidRows := boolValue(body, "skipInvalidRows")
	ignoreUnknownValues := boolValue(body, "ignoreUnknownValues")

	t, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID)
	if err != nil {
		return nil, mapErr(err)
	}
	fields := parseSchemaFields(t.Schema)

	type parsedRow struct {
		index    int
		insertID string
		json     map[string]any
	}
	parsed := make([]parsedRow, 0, len(rawRows))
	for i, rr := range rawRows {
		rowObj, _ := rr.(map[string]any)
		jsonObj := mapValue(rowObj, "json")
		if jsonObj == nil {
			jsonObj = map[string]any{}
		}
		parsed = append(parsed, parsedRow{index: i, insertID: strValue(rowObj, "insertId"), json: jsonObj})
	}

	type rowErrors struct {
		index int
		errs  []insertError
	}
	var rowErrs []rowErrors
	validRows := make([]bqstore.Row, 0, len(parsed))
	validIndexes := make([]int, 0, len(parsed))
	for _, pr := range parsed {
		if errs := validateRow(pr.json, fields, ignoreUnknownValues); len(errs) > 0 {
			rowErrs = append(rowErrs, rowErrors{index: pr.index, errs: errs})
			continue
		}
		data, _ := json.Marshal(pr.json)
		validRows = append(validRows, bqstore.Row{InsertID: pr.insertID, Data: data})
		validIndexes = append(validIndexes, pr.index)
	}

	// Row-level failures never fail the request at the HTTP level: the real API
	// answers 200 and reports them through insertErrors. When skipInvalidRows is
	// false nothing is inserted and every otherwise-valid row is reported as
	// reason "stopped"; when it is true the valid rows are inserted and only the
	// invalid ones are reported.
	if len(rowErrs) > 0 && !skipInvalidRows {
		for _, idx := range validIndexes {
			rowErrs = append(rowErrs, rowErrors{
				index: idx,
				errs: []insertError{{
					Reason:  "stopped",
					Message: "The row was not inserted because another row in the request was invalid.",
				}},
			})
		}
	} else if len(validRows) > 0 {
		dups, err := p.store.InsertRows(ctx, projectOf(nr), datasetID, tableID, validRows)
		if err != nil {
			return nil, mapErr(err)
		}
		for _, di := range dups {
			rowErrs = append(rowErrs, rowErrors{
				index: validIndexes[di],
				errs: []insertError{{
					Reason:  "duplicate",
					Message: "row already inserted with the same insertId",
				}},
			})
		}
	}

	sort.Slice(rowErrs, func(i, j int) bool { return rowErrs[i].index < rowErrs[j].index })
	insertErrors := make([]any, 0, len(rowErrs))
	for _, re := range rowErrs {
		errs := make([]any, 0, len(re.errs))
		for _, e := range re.errs {
			errs = append(errs, e.toMap())
		}
		insertErrors = append(insertErrors, map[string]any{"index": re.index, "errors": errs})
	}
	return provider.OK(map[string]any{
		"kind":         kindPrefix + "tableDataInsertAllResponse",
		"insertErrors": insertErrors,
	}), nil
}

// isRecordType reports whether a BigQuery field type is a nested record. The
// standard-SQL name is STRUCT; the legacy Discovery name is RECORD.
func isRecordType(t string) bool {
	return strings.EqualFold(t, "RECORD") || strings.EqualFold(t, "STRUCT")
}

// scalarCellString renders a primitive cell value as the string the BigQuery
// wire format uses: integers and floats as decimal text, booleans as
// "true"/"false". Client SDKs parse every primitive cell as a string (the Java
// FieldValue parser rejects raw JSON numbers and booleans).
func scalarCellString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		// Nested object/array values only reach here on a schema-less table;
		// render them as JSON text rather than Go syntax.
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	}
}

// tableCell renders one value as a BigQuery TableCell, always a {"v": ...}
// wrapper around the field value: a scalar string, [<cell>...] for a REPEATED
// field (each element wrapped in its own cell), {"f": [...]} for a
// RECORD/STRUCT value, or null. Real BigQuery wraps top-level records as
// {"v": {"f": [...]}} (the first-party Python client reads cell["v"]
// unconditionally), and a null must be {"v": null} rather than an empty object
// (the Java FieldValue.fromPb recurses through "v" and treats JSON null as a
// null primitive, while an object with neither "f" nor "v" is unparseable).
func tableCell(f schemaField, value any) map[string]any {
	if strings.EqualFold(f.Mode, "REPEATED") {
		list, _ := value.([]any)
		elems := make([]any, 0, len(list))
		element := schemaField{Name: f.Name, Type: f.Type, Fields: f.Fields}
		for _, e := range list {
			elems = append(elems, tableCell(element, e))
		}
		return map[string]any{"v": elems}
	}
	if value == nil {
		return map[string]any{"v": nil}
	}
	if isRecordType(f.Type) {
		obj, _ := value.(map[string]any)
		return map[string]any{"v": map[string]any{"f": rowCells(f.Fields, obj)}}
	}
	return map[string]any{"v": scalarCellString(value)}
}

// rowCells renders one row (or nested record) in schema order.
func rowCells(fields []schemaField, m map[string]any) []any {
	cells := make([]any, 0, len(fields))
	for _, f := range fields {
		cells = append(cells, tableCell(f, m[f.Name]))
	}
	return cells
}

// rowToTableRow renders a stored row as the BigQuery TableRow wire shape
// {"f": [TableCell...]}. A table without a schema falls back to every stored
// key in sorted order, rendered as scalar cells.
func rowToTableRow(data json.RawMessage, fields []schemaField) map[string]any {
	m := map[string]any{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &m)
	}
	if len(fields) == 0 {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		cells := make([]any, 0, len(keys))
		for _, k := range keys {
			cells = append(cells, tableCell(schemaField{}, m[k]))
		}
		return map[string]any{"f": cells}
	}
	return map[string]any{"f": rowCells(fields, m)}
}

func (p *Provider) ListRows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	startIndex, err := nonNegativeIntParam(nr, "startIndex")
	if err != nil {
		return nil, err
	}
	if _, err := maxResultsQueryParam(nr); err != nil {
		return nil, err
	}
	t, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID)
	if err != nil {
		return nil, mapErr(err)
	}
	fields := parseSchemaFields(t.Schema)
	rows, err := p.store.ListRows(ctx, projectOf(nr), datasetID, tableID)
	if err != nil {
		return nil, err
	}
	total := int64(len(rows))
	// startIndex offsets into the full row set; an out-of-range index yields an
	// empty page rather than an error (matching the real API).
	if startIndex > 0 {
		if startIndex >= len(rows) {
			rows = nil
		} else {
			rows = rows[startIndex:]
		}
	}
	page, next, err := paging.Page(rows, func(r bqstore.Row) string { return fmt.Sprintf("%020d", r.Seq) }, pagingParams(nr.Params))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, r := range page {
		items = append(items, rowToTableRow(r.Data, fields))
	}
	resp := map[string]any{
		"kind":      kindPrefix + "tableDataList",
		"rows":      items,
		"totalRows": strconv.FormatInt(total, 10),
	}
	if next != "" {
		resp["pageToken"] = next
	}
	return provider.OK(resp), nil
}

// nonNegativeIntParam parses an optional non-negative integer query parameter
// that the Discovery document declares as a *string* (`startIndex` is
// `format: uint64, type: string`), defaulting to 0 when absent. A non-numeric or
// negative value is InvalidArgument (real GCP rejects a malformed unsigned
// integer rather than ignoring it). `maxResults` is a uint32 integer binding and
// goes through maxResultsQueryParam instead.
func nonNegativeIntParam(nr *model.NormalizedRequest, key string) (int, error) {
	raw := strParam(nr, key)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, invalidArgument(key + " must be a non-negative integer")
	}
	return n, nil
}

// maxResultsQueryParam parses BigQuery's optional maxResults query parameter
// with the uint32 binding its Discovery document declares on every method that
// takes it (datasets/tables/tabledata/jobs.list and jobs.getQueryResults),
// defaulting to 0 when absent. An unparseable, negative or explicitly empty
// value is the frontend's binding INVALID_ARGUMENT — an empty value is not
// "absent" (DC63) — and the rejection names `max_results`, the field the
// parameter binds to, rather than the shared pager's internal pageSize alias.
func maxResultsQueryParam(nr *model.NormalizedRequest) (int, error) {
	v, ok := nr.Params["maxResults"]
	if !ok || v == nil {
		return 0, nil
	}
	return paging.ParseUintSizeNamed(v, "maxResults")
}

// --- Jobs ---

func (p *Provider) InsertJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyMap(nr)
	jobID := strValue(mapValue(body, "jobReference"), "jobId")
	if jobID == "" {
		return nil, invalidArgument("jobReference.jobId is required")
	}
	if _, err := p.store.GetJob(ctx, projectOf(nr), jobID); err == nil {
		return nil, mapErr(bqstore.ErrAlreadyExists)
	}
	now := clock.Now().UTC()
	// A query job is evaluated synchronously (accepted-risk LROs), exactly like
	// jobs.query, so its statistics are persisted on the job. That also means a
	// later getQueryResults never re-executes DDL/DML through this path.
	if q := mapValue(mapValue(body, "configuration"), "query"); q != nil {
		if boolValue(q, "useLegacySql") {
			return nil, invalidQuery("legacy SQL is not supported; use standard SQL (useLegacySql=false)")
		}
		res, err := p.runQuery(ctx, projectOf(nr), q, false)
		if err != nil {
			return nil, err
		}
		if res.StatementType != "" {
			stats := map[string]any{"statementType": res.StatementType}
			if isDMLStatement(res.StatementType) {
				stats["numDmlAffectedRows"] = strconv.FormatInt(res.NumDMLAffectedRows, 10)
			}
			body["statistics"] = map[string]any{"query": stats}
		}
	}
	// A load job is likewise evaluated synchronously: the source is read from
	// the emulated GCS (or, for a multipart upload, supplied as the request
	// media), decoded, coerced to the destination schema and written to the
	// store before the job is returned (state=DONE). Options outside the
	// documented subset fail loud (400/501); a data-level failure is reported in
	// the job's status.errorResult like real BigQuery.
	if l := mapValue(mapValue(body, "configuration"), "load"); l != nil {
		source, uploaded, err := p.loadSourceBytes(nr)
		if err != nil {
			return nil, err
		}
		var loadStats, jobErr map[string]any
		if uploaded {
			loadStats, jobErr, err = p.runLoadData(ctx, projectOf(nr), l, source, "0", now)
		} else {
			loadStats, jobErr, err = p.runLoad(ctx, projectOf(nr), l, now)
		}
		if err != nil {
			return nil, err
		}
		attachLoadStats(body, loadStats, jobErr, now)
	}
	j := bqstore.Job{JobID: jobID, CreateTime: now}
	if body != nil {
		if data, err := json.Marshal(body); err == nil {
			j.Config = data
		}
	}
	if err := p.store.CreateJob(ctx, projectOf(nr), j); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.jobMap(projectOf(nr), j)), nil
}

// attachLoadStats stamps a load job body with configuration.jobType and the
// statistics/status real BigQuery reports for a completed load. loadStats is the
// statistics.load object; jobErr, when non-nil, is a data-level failure carried
// in status.errorResult rather than as an HTTP error.
func attachLoadStats(body map[string]any, loadStats, jobErr map[string]any, now time.Time) {
	// application/callers expect the job type on the configuration.
	mapValue(body, "configuration")["jobType"] = "LOAD"
	body["statistics"] = map[string]any{
		"creationTime": millis(now),
		"startTime":    millis(now),
		"endTime":      millis(now),
		"load":         loadStats,
	}
	if jobErr != nil {
		body["status"] = map[string]any{"state": "DONE", "errorResult": jobErr, "errors": []any{jobErr}}
	}
}

// loadSourceBytes returns the uploaded file bytes carried on the request for a
// media (multipart) load job, and whether they were present. It reads either the
// buffered wire.MediaKey or the streamed wire.StreamKey, applying the size cap.
func (p *Provider) loadSourceBytes(nr *model.NormalizedRequest) ([]byte, bool, error) {
	if b, ok := nr.Params[wire.MediaKey].([]byte); ok {
		if len(b) > maxLoadUploadBytes {
			return nil, true, model.NewProviderError("InvalidArgument", "uploaded load source exceeds the size cap", 400)
		}
		return b, true, nil
	}
	if rd, ok := nr.Params[wire.StreamKey].(io.Reader); ok {
		data, err := io.ReadAll(io.LimitReader(rd, maxLoadUploadBytes+1))
		if err != nil {
			return nil, true, invalidArgument("failed to read uploaded load source: " + err.Error())
		}
		if len(data) > maxLoadUploadBytes {
			return nil, true, model.NewProviderError("InvalidArgument", "uploaded load source exceeds the size cap", 400)
		}
		return data, true, nil
	}
	return nil, false, nil
}

// --- Resumable load-job uploads (jobs.insert media, load_table_from_file) ---

const (
	// maxLoadUploadSessions caps concurrent resumable load-upload sessions.
	maxLoadUploadSessions = 100
	// loadUploadSessionTTL bounds how long an inactive session is kept.
	loadUploadSessionTTL = time.Hour
	// maxLoadUploadBytes caps an uploaded load source. A load source is decoded
	// from a whole buffer (BQF3), so the upload is bounded rather than spilled.
	maxLoadUploadBytes = 256 << 20 // 256 MiB
)

// loadUploadSession holds the state of an in-progress resumable load-job upload:
// the Job resource supplied at session start plus the accumulated file bytes.
type loadUploadSession struct {
	project    string
	body       map[string]any
	data       []byte
	lastAccess time.Time
}

// sweepLoadUploads drops sessions idle past loadUploadSessionTTL. The caller must
// hold p.mu.
func (p *Provider) sweepLoadUploads(now time.Time) {
	for id, s := range p.uploads {
		if now.Sub(s.lastAccess) > loadUploadSessionTTL {
			delete(p.uploads, id)
		}
	}
}

// InsertJobResumableStart begins a resumable load-job upload. Real BigQuery
// answers 200 with a Location header (the upload session URI) and no body; the
// job resource arrives as the initiate request body and is validated here so an
// unsupported configuration fails before the client uploads any bytes.
func (p *Provider) InsertJobResumableStart(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyMap(nr)
	if body == nil {
		return nil, invalidArgument("a resumable upload requires a job resource body")
	}
	if strValue(mapValue(body, "jobReference"), "jobId") == "" {
		return nil, invalidArgument("jobReference.jobId is required")
	}
	l := mapValue(mapValue(body, "configuration"), "load")
	if l == nil {
		return nil, invalidArgument("a resumable upload requires a configuration.load job resource")
	}
	if err := validateLoadOptions(l); err != nil {
		return nil, err
	}
	if _, err := p.store.GetJob(ctx, projectOf(nr), strValue(mapValue(body, "jobReference"), "jobId")); err == nil {
		return nil, mapErr(bqstore.ErrAlreadyExists)
	}

	id := newUploadID()
	now := clock.RealNow()
	p.mu.Lock()
	p.sweepLoadUploads(now)
	if len(p.uploads) >= maxLoadUploadSessions {
		p.mu.Unlock()
		return nil, model.NewProviderError("ResourceExhausted", "too many concurrent load-upload sessions", 429)
	}
	p.uploads[id] = &loadUploadSession{project: projectOf(nr), body: body, lastAccess: now}
	p.mu.Unlock()

	base, _ := nr.Params[wire.BaseURLKey].(string)
	if base == "" {
		base = "http://localhost"
	}
	loc := fmt.Sprintf("%s/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable&upload_id=%s", base, projectOf(nr), id)
	return &model.ProviderResponse{HTTPStatus: http.StatusOK, Data: map[string]any{wire.LocationKey: loc}}, nil
}

// InsertJobResumable appends a chunk to a load-upload session. An incomplete
// chunk answers 308 with a Range header (bytes=0-N) — the only resume-incomplete
// shape google-resumable-media accepts. The chunk that completes the source runs
// the load and returns the completed Job.
func (p *Provider) InsertJobResumable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	uploadID, _ := nr.Params["upload_id"].(string)
	media, _ := nr.Params[wire.MediaKey].([]byte)
	cr, _ := nr.Params["contentRange"].(string)
	project := projectOf(nr)

	p.mu.Lock()
	sess, ok := p.uploads[uploadID]
	if !ok || sess.project != project {
		p.mu.Unlock()
		return nil, model.NewProviderError("NotFound", "unknown upload_id", 404)
	}
	start, total, hasTotal, perr := parseContentRange(cr)
	if perr != nil {
		p.mu.Unlock()
		return nil, invalidArgument(perr.Error())
	}
	received := len(sess.data)
	// A "bytes */N" status query carries no payload; otherwise the chunk must
	// begin exactly where the previous one ended (concurrent/misordered chunks
	// are not modeled — the client is sequential).
	if start >= 0 && start != int64(received) {
		p.mu.Unlock()
		return nil, invalidArgument(fmt.Sprintf("Content-Range start %d does not match the %d bytes received", start, received))
	}
	if received+len(media) > maxLoadUploadBytes {
		p.mu.Unlock()
		return nil, model.NewProviderError("InvalidArgument", "uploaded load source exceeds the size cap", 400)
	}
	if start >= 0 {
		sess.data = append(sess.data, media...)
	}
	received = len(sess.data)
	sess.lastAccess = clock.RealNow()
	done := hasTotal && received == int(total)
	body := sess.body
	data := sess.data
	if done {
		delete(p.uploads, uploadID)
	}
	p.mu.Unlock()

	if !done {
		// google-resumable-media requires a Range header on every 308 and parses
		// it as "bytes=0-{end}"; with no bytes received there is no range to
		// report, so the body stays empty (real GCS does the same).
		data := map[string]any{}
		if received > 0 {
			data[wire.RangeKey] = fmt.Sprintf("bytes=0-%d", received-1)
		}
		return &model.ProviderResponse{HTTPStatus: http.StatusPermanentRedirect, Data: data}, nil
	}

	now := clock.Now().UTC()
	loadStats, jobErr, err := p.runLoadData(ctx, project, mapValue(mapValue(body, "configuration"), "load"), data, "0", now)
	if err != nil {
		return nil, err
	}
	attachLoadStats(body, loadStats, jobErr, now)
	jobID := strValue(mapValue(body, "jobReference"), "jobId")
	j := bqstore.Job{JobID: jobID, CreateTime: now}
	if raw, merr := json.Marshal(body); merr == nil {
		j.Config = raw
	}
	if err := p.store.CreateJob(ctx, project, j); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.jobMap(project, j)), nil
}

// parseContentRange parses a resumable Content-Range header. Forms:
// "bytes {start}-{end}/{total}", "bytes {start}-{end}/*" (total unknown), and
// "bytes */{total}" (a status query with no payload, start = -1). hasTotal
// reports whether a concrete total was given.
func parseContentRange(cr string) (start, total int64, hasTotal bool, err error) {
	if cr == "" {
		return 0, 0, false, errors.New("Content-Range header is required")
	}
	if !strings.HasPrefix(cr, "bytes ") {
		return 0, 0, false, errors.New("malformed Content-Range header")
	}
	rest := strings.TrimSpace(strings.TrimPrefix(cr, "bytes "))
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return 0, 0, false, errors.New("malformed Content-Range header")
	}
	span, totalStr := rest[:slash], strings.TrimSpace(rest[slash+1:])
	if totalStr != "*" {
		total, err = strconv.ParseInt(totalStr, 10, 64)
		if err != nil || total < 0 {
			return 0, 0, false, errors.New("malformed Content-Range header")
		}
		hasTotal = true
	}
	if span == "*" {
		return -1, total, hasTotal, nil
	}
	dash := strings.IndexByte(span, '-')
	if dash < 0 {
		return 0, 0, false, errors.New("malformed Content-Range header")
	}
	start, err = strconv.ParseInt(strings.TrimSpace(span[:dash]), 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, errors.New("malformed Content-Range header")
	}
	return start, total, hasTotal, nil
}

// newUploadID returns a random resumable load-upload session id.
func newUploadID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "bql_" + hex.EncodeToString(b)
}

func (p *Provider) GetJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobID := strParam(nr, "jobId")
	if jobID == "" {
		return nil, invalidArgument("jobId is required")
	}
	j, err := p.store.GetJob(ctx, projectOf(nr), jobID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.jobMap(projectOf(nr), j)), nil
}

func (p *Provider) ListJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, err := maxResultsQueryParam(nr); err != nil {
		return nil, err
	}
	jobs, err := p.store.ListJobs(ctx, projectOf(nr))
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(jobs, func(j bqstore.Job) string { return j.JobID }, pagingParams(nr.Params))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, j := range page {
		items = append(items, p.jobSummaryMap(projectOf(nr), j))
	}
	resp := map[string]any{"kind": kindPrefix + "jobList", "jobs": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobID := strParam(nr, "jobId")
	if jobID == "" {
		return nil, invalidArgument("jobId is required")
	}
	if err := p.store.DeleteJob(ctx, projectOf(nr), jobID); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) CancelJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobID := strParam(nr, "jobId")
	if jobID == "" {
		return nil, invalidArgument("jobId is required")
	}
	j, err := p.store.GetJob(ctx, projectOf(nr), jobID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{"kind": kindPrefix + "jobCancelResponse", "job": p.jobMap(projectOf(nr), j)}), nil
}

func (p *Provider) Query(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyMap(nr)
	query := strValue(body, "query")
	if query == "" {
		return nil, invalidArgument("query is required")
	}
	if boolValue(body, "useLegacySql") {
		return nil, invalidQuery("legacy SQL is not supported; use standard SQL (useLegacySql=false)")
	}
	maxResults, err := maxResultsBody(body)
	if err != nil {
		return nil, err
	}
	if boolValue(body, "dryRun") {
		// Dry run: validate the query (surfacing the same errors a real run
		// would) but perform no DDL/DML mutation, return no rows, and persist
		// no job.
		res, err := p.runQuery(ctx, projectOf(nr), body, true)
		if err != nil {
			return nil, err
		}
		return provider.OK(encodeQueryResult("queryResponse", projectOf(nr), newID(), strValue(body, "location"), res, true)), nil
	}
	// Evaluate before persisting so a failing query never leaves a job that
	// reports success. A query is synchronous here (accepted-risk LROs).
	res, err := p.runQuery(ctx, projectOf(nr), body, false)
	if err != nil {
		return nil, err
	}
	jobID := newID()
	now := clock.Now().UTC()
	jobCfg := map[string]any{
		"jobReference":  map[string]any{"projectId": projectOf(nr), "jobId": jobID},
		"configuration": map[string]any{"query": body},
	}
	if loc := strValue(body, "location"); loc != "" {
		jobCfg["jobReference"].(map[string]any)["location"] = loc
	}
	// Persist the statement statistics on the job so a later getQueryResults
	// can report the DDL/DML outcome without re-executing it (a second INSERT
	// or UPDATE would otherwise mutate the store twice).
	if res.StatementType != "" {
		stats := map[string]any{"statementType": res.StatementType}
		if isDMLStatement(res.StatementType) {
			stats["numDmlAffectedRows"] = strconv.FormatInt(res.NumDMLAffectedRows, 10)
		}
		jobCfg["statistics"] = map[string]any{"query": stats}
	}
	data, err := json.Marshal(jobCfg)
	if err != nil {
		// Never report jobComplete=true for a job that wasn't actually stored —
		// a subsequent GetJob/GetQueryResults for this jobId would otherwise
		// 404 despite this call having just reported success.
		return nil, model.NewProviderError("Internal", "failed to encode job configuration", 500)
	}
	j := bqstore.Job{JobID: jobID, Config: data, CreateTime: now}
	if err := p.store.CreateJob(ctx, projectOf(nr), j); err != nil {
		return nil, mapErr(err)
	}
	resp := encodeQueryResult("queryResponse", projectOf(nr), jobID, strValue(body, "location"), res, false)
	// maxResults pages the result set: a truncated first page carries a
	// pageToken the client passes to getQueryResults for the next page.
	pageQueryResponse(resp, 0, maxResults, true)
	return provider.OK(resp), nil
}

func (p *Provider) GetQueryResults(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobID := strParam(nr, "jobId")
	if jobID == "" {
		return nil, invalidArgument("jobId is required")
	}
	// Validate the paging input up front so it is rejected the same way for a
	// SELECT job and a DML/DDL job (which returns before executing anything).
	startIndex, err := nonNegativeIntParam(nr, "startIndex")
	if err != nil {
		return nil, err
	}
	maxResults, err := maxResultsQueryParam(nr)
	if err != nil {
		return nil, err
	}
	start, err := resolveResultPageStart(startIndex, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	j, err := p.store.GetJob(ctx, projectOf(nr), jobID)
	if err != nil {
		return nil, mapErr(err)
	}
	// Re-execute the stored query (jobs.query persists the request body on the
	// job). This keeps the store the single source of truth: no result cache,
	// no schema change, and snapshots/--dsn stay untouched.
	q := jobQueryBody(j)
	if q == nil {
		return nil, invalidQuery(fmt.Sprintf("job %s is not a query job", jobID))
	}
	if boolValue(q, "useLegacySql") {
		return nil, invalidQuery("legacy SQL is not supported; use standard SQL (useLegacySql=false)")
	}
	// A DDL/DML job is not re-executed: the mutation already happened at
	// jobs.query time, so replaying it would double-apply. Return the persisted
	// statement statistics instead (the job config stores them). SELECT keeps
	// re-executing (BQ6), because reads are idempotent.
	if typ, affected, ok := jobQueryStats(j); ok && typ != queryengine.StatementSelect {
		return provider.OK(encodeQueryResult("getQueryResultsResponse", projectOf(nr), jobID, jobLocation(j),
			queryengine.Result{StatementType: typ, NumDMLAffectedRows: affected}, false)), nil
	}
	res, err := p.runQuery(ctx, projectOf(nr), q, false)
	if err != nil {
		return nil, err
	}
	resp := encodeQueryResult("getQueryResultsResponse", projectOf(nr), jobID, jobLocation(j), res, false)
	pageQueryResponse(resp, start, maxResults, false)
	return provider.OK(resp), nil
}

// jobQueryStats returns the statementType and DML affected-row count persisted
// on a query job's configuration.statistics.query.
func jobQueryStats(j bqstore.Job) (typ string, affected int64, ok bool) {
	if len(j.Config) == 0 {
		return "", 0, false
	}
	var cfg map[string]any
	if json.Unmarshal(j.Config, &cfg) != nil {
		return "", 0, false
	}
	stats, _ := cfg["statistics"].(map[string]any)
	qs, _ := stats["query"].(map[string]any)
	if qs == nil {
		return "", 0, false
	}
	typ = strValue(qs, "statementType")
	if typ == "" {
		return "", 0, false
	}
	if s := strValue(qs, "numDmlAffectedRows"); s != "" {
		affected, _ = strconv.ParseInt(s, 10, 64)
	}
	return typ, affected, true
}

func (p *Provider) GetServiceAccount(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"kind":  kindPrefix + "getServiceAccountResponse",
		"email": fmt.Sprintf("bq-%s@gcp-sa-bigquery.iam.gserviceaccount.com", projectOf(nr)),
	}), nil
}

// --- Deferred resources ---

// unimplementedResource fails loud with Unimplemented for the resource types
// the emulator deliberately does not model, so clients get an explicit 501
// rather than a misleading 404.
func unimplementedResource(resource string) error {
	return model.NewProviderError("Unimplemented", resource+" is not implemented by this emulator", 501)
}

// Routines handles the deferred dataset-scoped routines surface.
func (p *Provider) Routines(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, unimplementedResource("routines")
}

// Models handles the deferred dataset-scoped models surface.
func (p *Provider) Models(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, unimplementedResource("models")
}

// RowAccessPolicies handles the deferred table-scoped rowAccessPolicies surface.
func (p *Provider) RowAccessPolicies(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, unimplementedResource("rowAccessPolicies")
}
