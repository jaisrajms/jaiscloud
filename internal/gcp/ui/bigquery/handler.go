package bigqueryui

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves BigQuery UI API requests by calling the BigQuery provider.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// ─── mapping helpers ─────────────────────────────────────────────────────────

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func mapAt(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	return v
}

// stringMapAt extracts a string-valued map, dropping any non-string entries.
// The provider renders labels as a typed map[string]string, so both that and a
// generic map[string]any are accepted.
func stringMapAt(m map[string]any, key string) map[string]string {
	switch raw := m[key].(type) {
	case map[string]string:
		if len(raw) == 0 {
			return nil
		}
		return raw
	case map[string]any:
		out := make(map[string]string, len(raw))
		for k, v := range raw {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}
	return nil
}

// stringMapToAny converts a string map into the map[string]any the provider's
// body readers expect (they type-assert on map[string]any).
func stringMapToAny(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// pageParams forwards the console's list query parameters onto the provider
// request. BigQuery names its page size maxResults; the provider also accepts
// pageSize, so either is forwarded.
func pageParams(r *http.Request, params map[string]any) {
	for _, key := range []string{"pageSize", "maxResults", "pageToken", "startIndex"} {
		if v := r.URL.Query().Get(key); v != "" {
			params[key] = v
		}
	}
}

// segment returns the decoded, single-segment value of a URL path parameter.
// A decoded '/' is rejected: dataset, table and job ids are single segments.
func segment(r *http.Request, key string) (string, bool) {
	v := uihelper.PathParam(r, key)
	if v == "" || strings.Contains(v, "/") {
		return "", false
	}
	return v, true
}

func datasetFromMap(m map[string]any) Dataset {
	ref := mapAt(m, "datasetReference")
	return Dataset{
		ID:           str(m, "id"),
		DatasetID:    str(ref, "datasetId"),
		ProjectID:    str(ref, "projectId"),
		Location:     str(m, "location"),
		FriendlyName: str(m, "friendlyName"),
		Labels:       stringMapAt(m, "labels"),
	}
}

func tableFromMap(m map[string]any) Table {
	ref := mapAt(m, "tableReference")
	return Table{
		ID:           str(m, "id"),
		DatasetID:    str(ref, "datasetId"),
		TableID:      str(ref, "tableId"),
		Type:         str(m, "type"),
		FriendlyName: str(m, "friendlyName"),
		CreationTime: str(m, "creationTime"),
	}
}

func jobFromMap(m map[string]any) Job {
	ref := mapAt(m, "jobReference")
	status := mapAt(m, "status")
	cfg := mapAt(m, "configuration")
	query := mapAt(cfg, "query")
	statementType := str(query, "statementType")
	if statementType == "" {
		statementType = str(mapAt(mapAt(m, "statistics"), "query"), "statementType")
	}
	return Job{
		ID:            str(m, "id"),
		JobID:         str(ref, "jobId"),
		State:         str(status, "state"),
		StatementType: statementType,
		Query:         str(query, "query"),
	}
}

// ─── Datasets ────────────────────────────────────────────────────────────────

// GET /datasets
func (h *Handler) ListDatasets(w http.ResponseWriter, r *http.Request) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.ListDatasets", "global", account)
	pageParams(r, nr.Params)

	resp, err := h.provider.ListDatasets(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["datasets"])
	datasets := make([]Dataset, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			datasets = append(datasets, datasetFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListDatasetsResponse{
		Datasets:      datasets,
		Total:         len(datasets),
		NextPageToken: str(resp.Data, "nextPageToken"),
	})
}

// POST /datasets
func (h *Handler) CreateDataset(w http.ResponseWriter, r *http.Request) {
	var req CreateDatasetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.DatasetID == "" {
		uihelper.UIError(w, "BadRequest", "datasetId is required", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.CreateDataset", "global", account)
	body := map[string]any{
		"datasetReference": map[string]any{"projectId": account, "datasetId": req.DatasetID},
	}
	if req.Location != "" {
		body["location"] = req.Location
	}
	if req.FriendlyName != "" {
		body["friendlyName"] = req.FriendlyName
	}
	if req.Description != "" {
		body["description"] = req.Description
	}
	if len(req.Labels) > 0 {
		body["labels"] = stringMapToAny(req.Labels)
	}
	nr.Params["body"] = body

	resp, err := h.provider.CreateDataset(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// GET /datasets/{dataset}
func (h *Handler) GetDataset(w http.ResponseWriter, r *http.Request) {
	dataset, ok := h.targetDataset(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.GetDataset", "global", h.account(r))
	nr.Params["datasetId"] = dataset

	resp, err := h.provider.GetDataset(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /datasets/{dataset}
func (h *Handler) DeleteDataset(w http.ResponseWriter, r *http.Request) {
	dataset, ok := h.targetDataset(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.DeleteDataset", "global", h.account(r))
	nr.Params["datasetId"] = dataset

	if _, err := h.provider.DeleteDataset(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Tables ──────────────────────────────────────────────────────────────────

// GET /datasets/{dataset}/tables
func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	dataset, ok := h.targetDataset(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.ListTables", "global", h.account(r))
	nr.Params["datasetId"] = dataset
	pageParams(r, nr.Params)

	resp, err := h.provider.ListTables(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["tables"])
	tables := make([]Table, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			tables = append(tables, tableFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListTablesResponse{
		Tables:        tables,
		Total:         len(tables),
		NextPageToken: str(resp.Data, "nextPageToken"),
	})
}

// POST /datasets/{dataset}/tables
func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	dataset, ok := h.targetDataset(w, r)
	if !ok {
		return
	}
	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.TableID == "" {
		uihelper.UIError(w, "BadRequest", "tableId is required", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.CreateTable", "global", account)
	body := map[string]any{
		"tableReference": map[string]any{
			"projectId": account,
			"datasetId": dataset,
			"tableId":   req.TableID,
		},
	}
	if req.Schema != nil {
		body["schema"] = req.Schema
	}
	if req.FriendlyName != "" {
		body["friendlyName"] = req.FriendlyName
	}
	if req.Description != "" {
		body["description"] = req.Description
	}
	nr.Params["datasetId"] = dataset
	nr.Params["body"] = body

	resp, err := h.provider.CreateTable(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// GET /datasets/{dataset}/tables/{table}
func (h *Handler) GetTable(w http.ResponseWriter, r *http.Request) {
	dataset, table, ok := h.targetTable(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.GetTable", "global", h.account(r))
	nr.Params["datasetId"] = dataset
	nr.Params["tableId"] = table

	resp, err := h.provider.GetTable(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /datasets/{dataset}/tables/{table}
func (h *Handler) DeleteTable(w http.ResponseWriter, r *http.Request) {
	dataset, table, ok := h.targetTable(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.DeleteTable", "global", h.account(r))
	nr.Params["datasetId"] = dataset
	nr.Params["tableId"] = table

	if _, err := h.provider.DeleteTable(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /datasets/{dataset}/tables/{table}/rows
func (h *Handler) ListRows(w http.ResponseWriter, r *http.Request) {
	dataset, table, ok := h.targetTable(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.ListRows", "global", h.account(r))
	nr.Params["datasetId"] = dataset
	nr.Params["tableId"] = table
	pageParams(r, nr.Params)

	resp, err := h.provider.ListRows(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["rows"])
	rows := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			rows = append(rows, m)
		}
	}
	uihelper.WriteJSON(w, ListRowsResponse{
		Rows:          rows,
		TotalRows:     str(resp.Data, "totalRows"),
		NextPageToken: str(resp.Data, "pageToken"),
	})
}

// ─── Jobs ────────────────────────────────────────────────────────────────────

// GET /jobs
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.ListJobs", "global", h.account(r))
	pageParams(r, nr.Params)

	resp, err := h.provider.ListJobs(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["jobs"])
	jobs := make([]Job, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			jobs = append(jobs, jobFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListJobsResponse{
		Jobs:          jobs,
		Total:         len(jobs),
		NextPageToken: str(resp.Data, "nextPageToken"),
	})
}

// GET /jobs/{job}
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	job, ok := h.targetJob(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.GetJob", "global", h.account(r))
	nr.Params["jobId"] = job

	resp, err := h.provider.GetJob(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /jobs/{job}
func (h *Handler) DeleteJob(w http.ResponseWriter, r *http.Request) {
	job, ok := h.targetJob(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.DeleteJob", "global", h.account(r))
	nr.Params["jobId"] = job

	if _, err := h.provider.DeleteJob(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /jobs/{job}/cancel
func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	job, ok := h.targetJob(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.CancelJob", "global", h.account(r))
	nr.Params["jobId"] = job

	resp, err := h.provider.CancelJob(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── Query ───────────────────────────────────────────────────────────────────

// POST /query  body: { query, defaultDataset?, dryRun?, useLegacySql?, location? }
func (h *Handler) RunQuery(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		uihelper.UIError(w, "BadRequest", "query is required", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.Query", "global", account)
	nr.Params["body"] = queryBody(req, account)

	resp, err := h.provider.Query(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// queryBody translates the console's query request into the jobs.query body the
// provider reads. A defaultDataset without a projectId is scoped to the
// console's current project.
func queryBody(req QueryRequest, account string) map[string]any {
	body := map[string]any{"query": req.Query}
	if req.DefaultDataset != nil && req.DefaultDataset.DatasetID != "" {
		project := req.DefaultDataset.ProjectID
		if project == "" {
			project = account
		}
		body["defaultDataset"] = map[string]any{
			"projectId": project,
			"datasetId": req.DefaultDataset.DatasetID,
		}
	}
	if req.DryRun {
		body["dryRun"] = true
	}
	if req.UseLegacySQL {
		body["useLegacySql"] = true
	}
	if req.Location != "" {
		body["location"] = req.Location
	}
	return body
}

// POST /datasets/{dataset}/tables/{table}/rows
// body: { rows: [{ insertId?, json }], skipInvalidRows?, ignoreUnknownValues? }
func (h *Handler) InsertRows(w http.ResponseWriter, r *http.Request) {
	dataset, table, ok := h.targetTable(w, r)
	if !ok {
		return
	}
	var req InsertRowsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if len(req.Rows) == 0 {
		uihelper.UIError(w, "BadRequest", "rows is required", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "bigquery", "BigQuery.InsertAll", "global", h.account(r))
	nr.Params["datasetId"] = dataset
	nr.Params["tableId"] = table
	nr.Params["body"] = insertBody(req)

	resp, err := h.provider.InsertAll(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// insertBody renders the console's rows as the tabledata.insertAll body the
// provider's body readers expect: a []any of {json, insertId?} entries.
func insertBody(req InsertRowsRequest) map[string]any {
	rows := make([]any, 0, len(req.Rows))
	for _, row := range req.Rows {
		jsonRow := row.JSON
		if jsonRow == nil {
			jsonRow = map[string]any{}
		}
		entry := map[string]any{"json": jsonRow}
		if row.InsertID != "" {
			entry["insertId"] = row.InsertID
		}
		rows = append(rows, entry)
	}
	body := map[string]any{"rows": rows}
	if req.SkipInvalidRows {
		body["skipInvalidRows"] = true
	}
	if req.IgnoreUnknownValues {
		body["ignoreUnknownValues"] = true
	}
	return body
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func (h *Handler) targetDataset(w http.ResponseWriter, r *http.Request) (string, bool) {
	dataset, ok := segment(r, "dataset")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid dataset", http.StatusBadRequest)
		return "", false
	}
	return dataset, true
}

func (h *Handler) targetTable(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	dataset, ok := h.targetDataset(w, r)
	if !ok {
		return "", "", false
	}
	table, ok := segment(r, "table")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid table", http.StatusBadRequest)
		return "", "", false
	}
	return dataset, table, true
}

func (h *Handler) targetJob(w http.ResponseWriter, r *http.Request) (string, bool) {
	job, ok := segment(r, "job")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid job", http.StatusBadRequest)
		return "", false
	}
	return job, true
}
