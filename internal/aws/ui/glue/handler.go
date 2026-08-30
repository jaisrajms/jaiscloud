package glueui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves Glue UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /databases?nextToken=...
func (h *Handler) ListDatabases(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "GetDatabases", region, account)
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["NextToken"] = tok
	}

	resp, err := h.provider.GetDatabases(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawDBs, _ := resp.Data["DatabaseList"].([]any)
	items := make([]Database, 0, len(rawDBs))
	for _, raw := range rawDBs {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapDatabase(m))
		}
	}
	nextToken, _ := resp.Data["NextToken"].(string)

	uihelper.WriteJSON(w, ListDatabasesResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /databases  body: { "name": "...", "description": "...", "locationUri": "..." }
func (h *Handler) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		LocationURI string `json:"locationUri"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "CreateDatabase", region, account)
	dbInput := map[string]any{"Name": req.Name}
	if req.Description != "" {
		dbInput["Description"] = req.Description
	}
	if req.LocationURI != "" {
		dbInput["LocationUri"] = req.LocationURI
	}
	nr.Params["DatabaseInput"] = dbInput

	if _, err := h.provider.CreateDatabase(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, map[string]any{"name": req.Name})
}

// DELETE /databases/{name}
func (h *Handler) DeleteDatabase(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "DeleteDatabase", region, account)
	nr.Params["Name"] = name

	if _, err := h.provider.DeleteDatabase(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /databases/{db}/tables?nextToken=...
func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	db := chi.URLParam(r, "db")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "GetTables", region, account)
	nr.Params["DatabaseName"] = db
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["NextToken"] = tok
	}

	resp, err := h.provider.GetTables(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawTables, _ := resp.Data["TableList"].([]any)
	items := make([]Table, 0, len(rawTables))
	for _, raw := range rawTables {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapTable(m, db))
		}
	}
	nextToken, _ := resp.Data["NextToken"].(string)

	uihelper.WriteJSON(w, ListTablesResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /databases/{db}/tables  body: { "name": "...", "description": "...", "location": "...", "storageType": "..." }
func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	db := chi.URLParam(r, "db")

	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Location    string `json:"location"`
		StorageType string `json:"storageType"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "CreateTable", region, account)
	nr.Params["DatabaseName"] = db
	tableInput := map[string]any{"Name": req.Name}
	if req.Description != "" {
		tableInput["Description"] = req.Description
	}
	if req.Location != "" || req.StorageType != "" {
		sd := map[string]any{}
		if req.Location != "" {
			sd["Location"] = req.Location
		}
		if req.StorageType != "" {
			sd["InputFormat"] = req.StorageType
		}
		tableInput["StorageDescriptor"] = sd
	}
	nr.Params["TableInput"] = tableInput

	if _, err := h.provider.CreateTable(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, map[string]any{"name": req.Name, "databaseName": db})
}

// DELETE /databases/{db}/tables/{name}
func (h *Handler) DeleteTable(w http.ResponseWriter, r *http.Request) {
	db := chi.URLParam(r, "db")
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "DeleteTable", region, account)
	nr.Params["DatabaseName"] = db
	nr.Params["Name"] = name

	if _, err := h.provider.DeleteTable(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /jobs?nextToken=...
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "GetJobs", region, account)
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["NextToken"] = tok
	}

	resp, err := h.provider.GetJobs(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawJobs, _ := resp.Data["Jobs"].([]any)
	items := make([]Job, 0, len(rawJobs))
	for _, raw := range rawJobs {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapJob(m))
		}
	}
	nextToken, _ := resp.Data["NextToken"].(string)

	uihelper.WriteJSON(w, ListJobsResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /jobs  body: { "name": "...", "role": "...", "command": "..." }
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Role    string `json:"role"`
		Command string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "CreateJob", region, account)
	nr.Params["Name"] = req.Name
	if req.Role != "" {
		nr.Params["Role"] = req.Role
	}
	if req.Command != "" {
		nr.Params["Command"] = map[string]any{"Name": req.Command}
	}

	if _, err := h.provider.CreateJob(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, map[string]any{"name": req.Name})
}

// DELETE /jobs/{name}
func (h *Handler) DeleteJob(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "DeleteJob", region, account)
	nr.Params["JobName"] = name

	if _, err := h.provider.DeleteJob(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /jobs/{name}/runs
func (h *Handler) StartJobRun(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "StartJobRun", region, account)
	nr.Params["JobName"] = name

	resp, err := h.provider.StartJobRun(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /jobs/{name}/runs?nextToken=...
func (h *Handler) ListJobRuns(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "GetJobRuns", region, account)
	nr.Params["JobName"] = name
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["NextToken"] = tok
	}

	resp, err := h.provider.GetJobRuns(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawRuns, _ := resp.Data["JobRuns"].([]any)
	items := make([]JobRun, 0, len(rawRuns))
	for _, raw := range rawRuns {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapJobRun(m))
		}
	}
	nextToken, _ := resp.Data["NextToken"].(string)

	uihelper.WriteJSON(w, ListJobRunsResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// GET /crawlers?nextToken=...
func (h *Handler) ListCrawlers(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "GetCrawlers", region, account)
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["NextToken"] = tok
	}

	resp, err := h.provider.GetCrawlers(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawCrawlers, _ := resp.Data["Crawlers"].([]any)
	items := make([]Crawler, 0, len(rawCrawlers))
	for _, raw := range rawCrawlers {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapCrawler(m))
		}
	}
	nextToken, _ := resp.Data["NextToken"].(string)

	uihelper.WriteJSON(w, ListCrawlersResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /crawlers  body: { "name": "...", "role": "...", "databaseName": "...", "s3Targets": ["..."] }
func (h *Handler) CreateCrawler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name         string   `json:"name"`
		Role         string   `json:"role"`
		DatabaseName string   `json:"databaseName"`
		S3Targets    []string `json:"s3Targets"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "CreateCrawler", region, account)
	nr.Params["Name"] = req.Name
	if req.Role != "" {
		nr.Params["Role"] = req.Role
	}
	if req.DatabaseName != "" {
		nr.Params["DatabaseName"] = req.DatabaseName
	}
	if len(req.S3Targets) > 0 {
		targets := make([]any, len(req.S3Targets))
		for i, p := range req.S3Targets {
			targets[i] = map[string]any{"Path": p}
		}
		nr.Params["Targets"] = map[string]any{"S3Targets": targets}
	}

	if _, err := h.provider.CreateCrawler(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, map[string]any{"name": req.Name})
}

// DELETE /crawlers/{name}
func (h *Handler) DeleteCrawler(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "DeleteCrawler", region, account)
	nr.Params["Name"] = name

	if _, err := h.provider.DeleteCrawler(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /crawlers/{name}/start
func (h *Handler) StartCrawler(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "glue", "StartCrawler", region, account)
	nr.Params["Name"] = name

	if _, err := h.provider.StartCrawler(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapDatabase(m map[string]any) Database {
	return Database{
		Name:        strAny(m, "Name"),
		Description: strAny(m, "Description"),
		LocationURI: strAny(m, "LocationUri"),
	}
}

func mapTable(m map[string]any, dbName string) Table {
	t := Table{
		Name:         strAny(m, "Name"),
		DatabaseName: dbName,
		Description:  strAny(m, "Description"),
	}
	if sd, ok := m["StorageDescriptor"].(map[string]any); ok {
		t.Location = strAny(sd, "Location")
		t.StorageType = strAny(sd, "InputFormat")
	}
	return t
}

func mapJob(m map[string]any) Job {
	j := Job{
		Name: strAny(m, "Name"),
		Role: strAny(m, "Role"),
	}
	if cmd, ok := m["Command"].(map[string]any); ok {
		j.Command = strAny(cmd, "Name")
	}
	return j
}

func mapJobRun(m map[string]any) JobRun {
	jr := JobRun{
		ID:      strAny(m, "Id"),
		JobName: strAny(m, "JobName"),
		State:   strAny(m, "JobRunState"),
	}
	if v, ok := m["StartedOn"].(string); ok {
		jr.StartedOn = v
	}
	if v, ok := m["CompletedOn"].(string); ok {
		jr.CompletedOn = v
	}
	return jr
}

func mapCrawler(m map[string]any) Crawler {
	return Crawler{
		Name:  strAny(m, "Name"),
		Role:  strAny(m, "Role"),
		State: strAny(m, "State"),
	}
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
