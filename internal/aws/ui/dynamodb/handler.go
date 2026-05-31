package dynamodbui

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves DynamoDB UI API requests by calling the DynamoDB provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /tables
func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	pageToken := r.URL.Query().Get("nextToken")
	limit := 100
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			limit = n
		}
	}

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "ListTables", region, account)
	nr.Params["Limit"] = limit
	if pageToken != "" {
		nr.Params["ExclusiveStartTableName"] = pageToken
	}

	resp, err := h.provider.ListTables(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	names, _ := resp.Data["TableNames"].([]string)
	lastTable, _ := resp.Data["LastEvaluatedTableName"].(string)

	// Describe each table to get status/count (acceptable at dev-tool scale).
	summaries := make([]TableSummary, 0, len(names))
	for _, name := range names {
		s := h.describeTable(r, name, region, account)
		summaries = append(summaries, s)
	}

	uihelper.WriteJSON(w, ListTablesResponse{
		Items:         summaries,
		LastTableName: lastTable,
		Total:         len(summaries),
	})
}

// POST /tables
func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.TableName == "" {
		uihelper.UIError(w, "BadRequest", "tableName is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "CreateTable", region, account)
	nr.Params["TableName"] = req.TableName

	if req.BillingMode == "" {
		req.BillingMode = "PAY_PER_REQUEST"
	}
	nr.Params["BillingMode"] = req.BillingMode

	// KeySchema
	ks := make([]any, len(req.KeySchema))
	for i, k := range req.KeySchema {
		ks[i] = map[string]any{"AttributeName": k.AttributeName, "KeyType": k.KeyType}
	}
	nr.Params["KeySchema"] = ks

	// AttributeDefinitions
	ads := make([]any, len(req.AttributeDefinitions))
	for i, a := range req.AttributeDefinitions {
		ads[i] = map[string]any{"AttributeName": a.AttributeName, "AttributeType": a.AttributeType}
	}
	nr.Params["AttributeDefinitions"] = ads

	if req.BillingMode == "PROVISIONED" {
		rc := req.ReadCapacity
		wc := req.WriteCapacity
		if rc == 0 {
			rc = 5
		}
		if wc == 0 {
			wc = 5
		}
		nr.Params["ProvisionedThroughput"] = map[string]any{
			"ReadCapacityUnits":  rc,
			"WriteCapacityUnits": wc,
		}
	}

	resp, err := h.provider.CreateTable(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /tables/{table}
func (h *Handler) GetTable(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "DescribeTable", region, account)
	nr.Params["TableName"] = table

	resp, err := h.provider.DescribeTable(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /tables/{table}
func (h *Handler) DeleteTable(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "DeleteTable", region, account)
	nr.Params["TableName"] = table

	if _, err := h.provider.DeleteTable(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /tables/{table}/scan?limit=50&nextToken=...
func (h *Handler) ScanTable(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	limit := 50
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "Scan", region, account)
	nr.Params["TableName"] = table
	nr.Params["Limit"] = limit

	if lek := r.URL.Query().Get("nextToken"); lek != "" {
		var key map[string]any
		if json.Unmarshal([]byte(lek), &key) == nil {
			nr.Params["ExclusiveStartKey"] = key
		}
	}

	resp, err := h.provider.Scan(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	items, _ := resp.Data["Items"].([]map[string]any)
	if items == nil {
		items = []map[string]any{}
	}
	count, _ := resp.Data["Count"].(int)
	scanned, _ := resp.Data["ScannedCount"].(int)
	lek, _ := resp.Data["LastEvaluatedKey"].(map[string]any)

	uihelper.WriteJSON(w, ScanResponse{
		Items:            items,
		Count:            count,
		ScannedCount:     scanned,
		LastEvaluatedKey: lek,
	})
}

// POST /tables/{table}/query
func (h *Handler) QueryTable(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "Query", region, account)
	nr.Params["TableName"] = table
	nr.Params["KeyConditionExpression"] = req.KeyConditionExpression
	if req.FilterExpression != "" {
		nr.Params["FilterExpression"] = req.FilterExpression
	}
	if req.ExpressionAttributeNames != nil {
		nr.Params["ExpressionAttributeNames"] = req.ExpressionAttributeNames
	}
	if req.ExpressionAttributeValues != nil {
		nr.Params["ExpressionAttributeValues"] = req.ExpressionAttributeValues
	}
	if req.IndexName != "" {
		nr.Params["IndexName"] = req.IndexName
	}
	if req.Limit > 0 {
		nr.Params["Limit"] = req.Limit
	}
	if req.ScanIndexForward != nil {
		nr.Params["ScanIndexForward"] = *req.ScanIndexForward
	}
	if req.ExclusiveStartKey != nil {
		nr.Params["ExclusiveStartKey"] = req.ExclusiveStartKey
	}

	resp, err := h.provider.Query(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /tables/{table}/items
func (h *Handler) PutItem(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	var req PutItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "PutItem", region, account)
	nr.Params["TableName"] = table
	nr.Params["Item"] = req.Item

	if _, err := h.provider.PutItem(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	uihelper.WriteJSON(w, map[string]string{"status": "ok"})
}

// POST /tables/{table}/items/get
func (h *Handler) GetItem(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	var req GetItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "GetItem", region, account)
	nr.Params["TableName"] = table
	nr.Params["Key"] = req.Key

	resp, err := h.provider.GetItem(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /tables/{table}/items  body: { "key": { ... } }
func (h *Handler) DeleteItem(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	var req DeleteItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "DeleteItem", region, account)
	nr.Params["TableName"] = table
	nr.Params["Key"] = req.Key

	if _, err := h.provider.DeleteItem(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// describeTable calls DescribeTable and assembles a TableSummary.
func (h *Handler) describeTable(r *http.Request, table, region, account string) TableSummary {
	nr := uihelper.NR(r.Context(), h.cfg, "dynamodb", "DescribeTable", region, account)
	nr.Params["TableName"] = table

	resp, err := h.provider.DescribeTable(r.Context(), nr)
	if err != nil {
		return TableSummary{Name: table, Status: "UNKNOWN"}
	}

	td, _ := resp.Data["Table"].(map[string]any)
	if td == nil {
		return TableSummary{Name: table, Status: "UNKNOWN"}
	}

	s := TableSummary{
		Name:   table,
		Status: strAny(td, "TableStatus"),
		ARN:    strAny(td, "TableArn"),
	}
	if s.Status == "" {
		s.Status = "ACTIVE"
	}
	if ic, ok := td["ItemCount"]; ok {
		switch v := ic.(type) {
		case float64:
			s.ItemCount = int64(v)
		case int64:
			s.ItemCount = v
		case int:
			s.ItemCount = int64(v)
		}
	}
	if sb, ok := td["TableSizeBytes"]; ok {
		switch v := sb.(type) {
		case float64:
			s.SizeBytes = int64(v)
		case int64:
			s.SizeBytes = v
		}
	}
	if bs, ok := td["BillingModeSummary"].(map[string]any); ok {
		s.BillingMode = strAny(bs, "BillingMode")
	}
	if s.BillingMode == "" {
		s.BillingMode = "PAY_PER_REQUEST"
	}
	if ct, ok := td["CreationDateTime"]; ok {
		switch v := ct.(type) {
		case string:
			s.CreatedAt = v
		case float64:
			s.CreatedAt = strconv.FormatFloat(v, 'f', 0, 64)
		}
	}
	return s
}

func strAny(m map[string]any, key string) string {
	switch v := m[key].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return ""
}
