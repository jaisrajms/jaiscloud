// Package bigquery implements the BigQuery v2 provider
// (bigquery.googleapis.com/bigquery/v2). BigQuery is metadata-not-engine:
// datasets/tables/jobs are logical records only, jobs.query never evaluates
// SQL (it stores the query and reports jobComplete=true with empty results),
// and tabledata.insertAll stores streamed rows that tabledata.list reads back.
// routines/models/rowAccessPolicies are deferred and route to Unimplemented.
//
// Known limitations (emulator simplifications, documented rather than fixed):
//   - tabledata.insertAll does not honor insertId/skipInvalidRows/
//     ignoreUnknownValues/templateSuffix and performs no schema validation: it
//     always streams the rows and returns an empty insertErrors list.
//   - tabledata.list ignores startIndex (rows are returned from the first row).
//   - List methods (datasets/tables/jobs) return full resource objects rather
//     than the discovery-doc summary subsets (over-inclusion, tolerated by the
//     SDK).
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
	"sort"
	"strconv"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

const kindPrefix = "bigquery#"

// Provider handles BigQuery datasets, tables, jobs, and tabledata.
type Provider struct {
	store bqstore.Store
}

// New returns a Provider backed by the given store.
func New(s bqstore.Store) *Provider {
	return &Provider{store: s}
}

// Reset wipes the store.
func (p *Provider) Reset(ctx context.Context) { p.store.Reset(ctx) }

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"BigQuery.CreateDataset":     p.CreateDataset,
		"BigQuery.GetDataset":        p.GetDataset,
		"BigQuery.ListDatasets":      p.ListDatasets,
		"BigQuery.UpdateDataset":     p.UpdateDataset,
		"BigQuery.DeleteDataset":     p.DeleteDataset,
		"BigQuery.CreateTable":       p.CreateTable,
		"BigQuery.GetTable":          p.GetTable,
		"BigQuery.ListTables":        p.ListTables,
		"BigQuery.UpdateTable":       p.UpdateTable,
		"BigQuery.DeleteTable":       p.DeleteTable,
		"BigQuery.InsertAll":         p.InsertAll,
		"BigQuery.ListRows":          p.ListRows,
		"BigQuery.InsertJob":         p.InsertJob,
		"BigQuery.GetJob":            p.GetJob,
		"BigQuery.ListJobs":          p.ListJobs,
		"BigQuery.DeleteJob":         p.DeleteJob,
		"BigQuery.CancelJob":         p.CancelJob,
		"BigQuery.Query":             p.Query,
		"BigQuery.GetQueryResults":   p.GetQueryResults,
		"BigQuery.GetServiceAccount": p.GetServiceAccount,
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
	labels := d.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
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
	out["status"] = map[string]any{"state": "DONE"}
	return out
}

// pagingParams translates BigQuery's maxResults/pageToken query parameters into
// the shared paging helper's pageSize/pageToken shape (maxResults is BigQuery's
// page-size parameter; paging.PageSize reads pageSize).
func pagingParams(params map[string]any) map[string]any {
	out := map[string]any{}
	if v, ok := params["pageToken"]; ok {
		out["pageToken"] = v
	}
	if v, ok := params["maxResults"]; ok {
		out["pageSize"] = v
	} else if v, ok := params["pageSize"]; ok {
		out["pageSize"] = v
	}
	return out
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
	return provider.OK(p.datasetMap(projectOf(nr), d)), nil
}

func (p *Provider) ListDatasets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasets, err := p.store.ListDatasets(ctx, projectOf(nr))
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(datasets, func(d bqstore.Dataset) string { return d.DatasetID }, pagingParams(nr.Params))
	items := make([]any, 0, len(page))
	for _, d := range page {
		items = append(items, p.datasetMap(projectOf(nr), d))
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
	d, err := p.store.GetDataset(ctx, projectOf(nr), datasetID)
	if err != nil {
		return nil, mapErr(err)
	}
	body := bodyMap(nr)
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
	if err := p.store.UpdateDataset(ctx, projectOf(nr), d); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.datasetMap(projectOf(nr), d)), nil
}

func (p *Provider) DeleteDataset(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetId is required")
	}
	if err := p.store.DeleteDataset(ctx, projectOf(nr), datasetID); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{}), nil
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
	tables, err := p.store.ListTables(ctx, projectOf(nr), datasetID)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(tables, func(t bqstore.Table) string { return t.TableID }, pagingParams(nr.Params))
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, p.tableMap(projectOf(nr), t))
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
	t, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID)
	if err != nil {
		return nil, mapErr(err)
	}
	body := bodyMap(nr)
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
	if err := p.store.UpdateTable(ctx, projectOf(nr), datasetID, t); err != nil {
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
	return provider.OK(map[string]any{}), nil
}

// --- Tabledata ---

func (p *Provider) InsertAll(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	body := bodyMap(nr)
	rawRows, _ := body["rows"].([]any)
	rows := make([]bqstore.Row, 0, len(rawRows))
	for _, rr := range rawRows {
		rowObj, _ := rr.(map[string]any)
		jsonObj := mapValue(rowObj, "json")
		if jsonObj == nil {
			jsonObj = map[string]any{}
		}
		data, _ := json.Marshal(jsonObj)
		rows = append(rows, bqstore.Row{Data: data})
	}
	if err := p.store.InsertRows(ctx, projectOf(nr), datasetID, tableID, rows); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{
		"kind":         kindPrefix + "tableDataInsertAllResponse",
		"insertErrors": []any{},
	}), nil
}

func schemaFieldNames(schema json.RawMessage) []string {
	if len(schema) == 0 {
		return nil
	}
	var s struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	if json.Unmarshal(schema, &s) != nil {
		return nil
	}
	names := make([]string, 0, len(s.Fields))
	for _, f := range s.Fields {
		names = append(names, f.Name)
	}
	return names
}

func rowToTableRow(data json.RawMessage, fields []string) map[string]any {
	m := map[string]any{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &m)
	}
	cells := make([]any, 0, len(fields))
	if len(fields) > 0 {
		for _, f := range fields {
			cells = append(cells, map[string]any{"v": m[f]})
		}
	} else {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			cells = append(cells, map[string]any{"v": m[k]})
		}
	}
	return map[string]any{"f": cells}
}

func (p *Provider) ListRows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	t, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID)
	if err != nil {
		return nil, mapErr(err)
	}
	fields := schemaFieldNames(t.Schema)
	rows, err := p.store.ListRows(ctx, projectOf(nr), datasetID, tableID)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(rows, func(r bqstore.Row) string { return fmt.Sprintf("%020d", r.Seq) }, pagingParams(nr.Params))
	items := make([]any, 0, len(page))
	for _, r := range page {
		items = append(items, rowToTableRow(r.Data, fields))
	}
	resp := map[string]any{
		"kind":      kindPrefix + "tableDataList",
		"rows":      items,
		"totalRows": strconv.FormatInt(int64(len(rows)), 10),
	}
	if next != "" {
		resp["pageToken"] = next
	}
	return provider.OK(resp), nil
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
	jobs, err := p.store.ListJobs(ctx, projectOf(nr))
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(jobs, func(j bqstore.Job) string { return j.JobID }, pagingParams(nr.Params))
	items := make([]any, 0, len(page))
	for _, j := range page {
		items = append(items, p.jobMap(projectOf(nr), j))
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
	jobID := newID()
	now := clock.Now().UTC()
	jobCfg := map[string]any{
		"jobReference":  map[string]any{"projectId": projectOf(nr), "jobId": jobID},
		"configuration": map[string]any{"query": body},
	}
	if loc := strValue(body, "location"); loc != "" {
		jobCfg["jobReference"].(map[string]any)["location"] = loc
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
	ref := map[string]any{"projectId": projectOf(nr), "jobId": jobID}
	if loc := strValue(body, "location"); loc != "" {
		ref["location"] = loc
	}
	return provider.OK(map[string]any{
		"kind":         kindPrefix + "queryResponse",
		"jobComplete":  true,
		"jobReference": ref,
		"schema":       nil,
		"rows":         []any{},
		"totalRows":    "0",
	}), nil
}

func (p *Provider) GetQueryResults(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobID := strParam(nr, "jobId")
	if jobID == "" {
		return nil, invalidArgument("jobId is required")
	}
	if _, err := p.store.GetJob(ctx, projectOf(nr), jobID); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(map[string]any{
		"kind":        kindPrefix + "getQueryResultsResponse",
		"jobComplete": true,
		"jobReference": map[string]any{
			"projectId": projectOf(nr),
			"jobId":     jobID,
		},
		"schema":    nil,
		"rows":      []any{},
		"totalRows": "0",
	}), nil
}

func (p *Provider) GetServiceAccount(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"kind":  kindPrefix + "getServiceAccountResponse",
		"email": fmt.Sprintf("bq-%s@gcp-sa-bigquery.iam.gserviceaccount.com", projectOf(nr)),
	}), nil
}
