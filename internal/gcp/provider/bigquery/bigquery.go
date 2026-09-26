// Package bigquery implements the BigQuery v2 provider
// (bigquery.googleapis.com/bigquery/v2). BigQuery is metadata-not-engine:
// datasets/tables/jobs are logical records only, jobs.query never evaluates
// SQL (it stores the query and reports jobComplete=true with empty results),
// and tabledata.insertAll stores streamed rows that tabledata.list reads back.
// tabledata.list honors startIndex as an offset into the row set but does not
// echo it in the response (startIndex is a request-only parameter in the
// Discovery TableDataList schema). routines/models/rowAccessPolicies are
// deferred and route to an explicit Unimplemented (501) rather than the
// codec's 404.
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
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/identity"
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
		"BigQuery.Routines":          p.Routines,
		"BigQuery.Models":            p.Models,
		"BigQuery.RowAccessPolicies": p.RowAccessPolicies,
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
	out["status"] = map[string]any{"state": "DONE"}
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
	return provider.OK(withMaxTimeTravel(p.datasetMap(projectOf(nr), d))), nil
}

func (p *Provider) ListDatasets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasets, err := p.store.ListDatasets(ctx, projectOf(nr))
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(datasets, func(d bqstore.Dataset) string { return d.DatasetID }, pagingParams(nr.Params))
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
	tables, err := p.store.ListTables(ctx, projectOf(nr), datasetID)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(tables, func(t bqstore.Table) string { return t.TableID }, pagingParams(nr.Params))
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
	Fields []schemaField `json:"fields"`
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
	startIndex, err := startIndexParam(nr)
	if err != nil {
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
	page, next := paging.Page(rows, func(r bqstore.Row) string { return fmt.Sprintf("%020d", r.Seq) }, pagingParams(nr.Params))
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

// startIndexParam parses the tabledata.list startIndex query parameter,
// defaulting to 0. A non-numeric or negative value is InvalidArgument.
func startIndexParam(nr *model.NormalizedRequest) (int, error) {
	raw := strParam(nr, "startIndex")
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, invalidArgument("startIndex must be a non-negative integer")
	}
	return n, nil
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

// emptyTableSchema is the result schema for a query the emulator answers
// without a SQL engine: a TableSchema with no fields. Discovery types
// queryResponse/getQueryResultsResponse "schema" as a TableSchema object, so
// returning JSON null is a wire-contract violation (surfaced by
// tests/gcpconformance against the vendored BigQuery Discovery snapshot).
func emptyTableSchema() map[string]any {
	return map[string]any{"fields": []any{}}
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
		"schema":       emptyTableSchema(),
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
		"schema":    emptyTableSchema(),
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
