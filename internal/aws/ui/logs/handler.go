package logsui

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
)

// Handler serves CloudWatch Logs UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /groups?pageSize=50&nextToken=...&prefix=...
func (h *Handler) ListLogGroups(w http.ResponseWriter, r *http.Request) {
	pageSize := uihelper.PageSizeFrom(r, 50, 50) // CWLogs max is 50
	pageToken := uihelper.PageTokenFrom(r)
	prefix := r.URL.Query().Get("prefix")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "logs", "DescribeLogGroups", region, account)
	nr.Params["limit"] = pageSize
	if pageToken != "" {
		nr.Params["nextToken"] = pageToken
	}
	if prefix != "" {
		nr.Params["logGroupNamePrefix"] = prefix
	}

	resp, err := h.provider.DescribeLogGroups(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawGroups, _ := resp.Data["logGroups"].([]any)
	nextTok, _ := resp.Data["nextToken"].(string)

	groups := make([]LogGroup, 0, len(rawGroups))
	for _, raw := range rawGroups {
		if m, ok := raw.(map[string]any); ok {
			groups = append(groups, mapLogGroup(m))
		}
	}

	uihelper.WriteJSON(w, map[string]any{
		"items":     groups,
		"nextToken": nextTok,
	})
}

// POST /groups  body: { "logGroupName": "..." }
func (h *Handler) CreateLogGroup(w http.ResponseWriter, r *http.Request) {
	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	name := body["logGroupName"]
	if name == "" {
		uihelper.UIError(w, "BadRequest", "logGroupName is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "logs", "CreateLogGroup", region, account)
	nr.Params["logGroupName"] = name

	if _, err := h.provider.CreateLogGroup(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func decodeName(r *http.Request, key string) string {
	v := chi.URLParam(r, key)
	if d, err := url.PathUnescape(v); err == nil {
		return d
	}
	return v
}

// DELETE /groups/{name}
func (h *Handler) DeleteLogGroup(w http.ResponseWriter, r *http.Request) {
	name := decodeName(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "logs", "DeleteLogGroup", region, account)
	nr.Params["logGroupName"] = name

	if _, err := h.provider.DeleteLogGroup(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// PUT /groups/{name}/retention  body: { "retentionDays": 30 }
func (h *Handler) SetRetention(w http.ResponseWriter, r *http.Request) {
	name := decodeName(r, "name")

	var body map[string]int
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "logs", "PutRetentionPolicy", region, account)
	nr.Params["logGroupName"] = name
	nr.Params["retentionInDays"] = body["retentionDays"]

	if _, err := h.provider.PutRetentionPolicy(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /groups/{name}/streams?pageSize=50&nextToken=...
func (h *Handler) ListLogStreams(w http.ResponseWriter, r *http.Request) {
	name := decodeName(r, "name")
	pageSize := uihelper.PageSizeFrom(r, 50, 50)
	pageToken := uihelper.PageTokenFrom(r)
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "logs", "DescribeLogStreams", region, account)
	nr.Params["logGroupName"] = name
	nr.Params["limit"] = pageSize
	if pageToken != "" {
		nr.Params["nextToken"] = pageToken
	}

	resp, err := h.provider.DescribeLogStreams(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawStreams, _ := resp.Data["logStreams"].([]any)
	nextTok, _ := resp.Data["nextToken"].(string)

	streams := make([]LogStream, 0, len(rawStreams))
	for _, raw := range rawStreams {
		if m, ok := raw.(map[string]any); ok {
			streams = append(streams, mapLogStream(m))
		}
	}

	uihelper.WriteJSON(w, map[string]any{
		"items":     streams,
		"nextToken": nextTok,
	})
}

// GET /groups/{name}/streams/{stream}/events?startTime=...&endTime=...&nextToken=...&limit=200
func (h *Handler) GetLogEvents(w http.ResponseWriter, r *http.Request) {
	groupName := decodeName(r, "name")
	streamName := decodeName(r, "stream")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	now := clock.Now()
	startTime := now.Add(-15 * time.Minute).UnixMilli()
	endTime := now.UnixMilli()

	if s := r.URL.Query().Get("startTime"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			startTime = n
		}
	}
	if s := r.URL.Query().Get("endTime"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			endTime = n
		}
	}

	limit := 10000
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 10000 {
			limit = n
		}
	}

	nr := uihelper.NR(r.Context(), h.cfg, "logs", "GetLogEvents", region, account)
	nr.Params["logGroupName"] = groupName
	nr.Params["logStreamName"] = streamName
	nr.Params["startTime"] = int(startTime)
	nr.Params["endTime"] = int(endTime)
	nr.Params["limit"] = limit
	if tok := uihelper.PageTokenFrom(r); tok != "" {
		nr.Params["nextToken"] = tok
	}

	resp, err := h.provider.GetLogEvents(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawEvents, _ := resp.Data["events"].([]any)
	nextFwd, _ := resp.Data["nextForwardToken"].(string)
	nextBwd, _ := resp.Data["nextBackwardToken"].(string)
	truncated := len(rawEvents) >= 10000

	events := make([]LogEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		if m, ok := raw.(map[string]any); ok {
			events = append(events, mapLogEvent(m))
		}
	}

	uihelper.WriteJSON(w, LogEventsResponse{
		Events:            events,
		NextForwardToken:  nextFwd,
		NextBackwardToken: nextBwd,
		Truncated:         truncated,
	})
}

// POST /groups/{name}/filter  body: FilterLogEventsRequest
func (h *Handler) FilterLogEvents(w http.ResponseWriter, r *http.Request) {
	groupName := decodeName(r, "name")

	var req FilterLogEventsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.LogGroupName == "" {
		req.LogGroupName = groupName
	}
	if req.Limit <= 0 || req.Limit > 200 {
		req.Limit = 200
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "logs", "FilterLogEvents", region, account)
	nr.Params["logGroupName"] = req.LogGroupName
	nr.Params["filterPattern"] = req.FilterPattern
	nr.Params["limit"] = req.Limit
	if req.StartTime != nil {
		nr.Params["startTime"] = int(*req.StartTime)
	}
	if req.EndTime != nil {
		nr.Params["endTime"] = int(*req.EndTime)
	}
	if req.NextToken != "" {
		nr.Params["nextToken"] = req.NextToken
	}

	resp, err := h.provider.FilterLogEvents(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawEvents, _ := resp.Data["events"].([]any)
	nextTok, _ := resp.Data["nextToken"].(string)

	events := make([]LogEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		if m, ok := raw.(map[string]any); ok {
			events = append(events, mapLogEvent(m))
		}
	}

	uihelper.WriteJSON(w, map[string]any{
		"events":    events,
		"nextToken": nextTok,
	})
}

// POST /queries  body: StartQueryRequest
func (h *Handler) StartQuery(w http.ResponseWriter, r *http.Request) {
	var req StartQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueryString == "" {
		uihelper.UIError(w, "BadRequest", "queryString is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "logs", "StartQuery", region, account)
	nr.Params["queryString"] = req.QueryString
	if req.StartTime > 0 {
		nr.Params["startTime"] = req.StartTime
	}
	if req.EndTime > 0 {
		nr.Params["endTime"] = req.EndTime
	}
	if len(req.LogGroupNames) > 0 {
		names := make([]any, len(req.LogGroupNames))
		for i, n := range req.LogGroupNames {
			names[i] = n
		}
		nr.Params["logGroupNames"] = names
	} else if req.LogGroupName != "" {
		nr.Params["logGroupName"] = req.LogGroupName
	}

	resp, err := h.provider.StartQuery(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, map[string]any{"queryId": resp.Data["queryId"]})
}

// GET /queries/{id}
func (h *Handler) GetQueryResults(w http.ResponseWriter, r *http.Request) {
	queryID := chi.URLParam(r, "id")
	if queryID == "" {
		uihelper.UIError(w, "BadRequest", "query id is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "logs", "GetQueryResults", region, account)
	nr.Params["queryId"] = queryID

	resp, err := h.provider.GetQueryResults(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	status, _ := resp.Data["status"].(string)
	rawResults, _ := resp.Data["results"].([][]map[string]string)
	rawStats, _ := resp.Data["statistics"].(map[string]any)

	stats := map[string]float64{}
	for k, v := range rawStats {
		if f, ok := v.(float64); ok {
			stats[k] = f
		}
	}
	if rawResults == nil {
		rawResults = [][]map[string]string{}
	}

	uihelper.WriteJSON(w, QueryResult{
		QueryID:    queryID,
		Status:     status,
		Results:    rawResults,
		Statistics: stats,
	})
}

// DELETE /queries/{id}
func (h *Handler) StopQuery(w http.ResponseWriter, r *http.Request) {
	queryID := chi.URLParam(r, "id")
	if queryID == "" {
		uihelper.UIError(w, "BadRequest", "query id is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "logs", "StopQuery", region, account)
	nr.Params["queryId"] = queryID

	if _, err := h.provider.StopQuery(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapLogGroup(m map[string]any) LogGroup {
	g := LogGroup{
		Name:        strVal(m, "logGroupName"),
		ARN:         strVal(m, "arn"),
		StoredBytes: int64Val(m, "storedBytes"),
		CreatedAt:   int64Val(m, "creationTime"),
	}
	if v, ok := m["retentionInDays"]; ok {
		switch n := v.(type) {
		case float64:
			days := int(n)
			g.RetentionDays = &days
		}
	}
	return g
}

func mapLogStream(m map[string]any) LogStream {
	s := LogStream{
		Name:                strVal(m, "logStreamName"),
		ARN:                 strVal(m, "arn"),
		UploadSequenceToken: strVal(m, "uploadSequenceToken"),
	}
	if v := int64Val(m, "firstEventTimestamp"); v > 0 {
		s.FirstEventAt = &v
	}
	if v := int64Val(m, "lastEventTimestamp"); v > 0 {
		s.LastEventAt = &v
	}
	return s
}

func mapLogEvent(m map[string]any) LogEvent {
	return LogEvent{
		Timestamp:     int64Val(m, "timestamp"),
		Message:       strVal(m, "message"),
		IngestionTime: int64Val(m, "ingestionTime"),
	}
}

func strVal(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func int64Val(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	}
	return 0
}
