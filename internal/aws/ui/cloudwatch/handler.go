package cloudwatchui

import (
	"encoding/json"
	"fmt"
	"net/http"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves CloudWatch UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /metrics?namespace=...&metricName=...&nextToken=...
func (h *Handler) ListMetrics(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "ListMetrics", region, account)
	if ns := r.URL.Query().Get("namespace"); ns != "" {
		nr.Params["Namespace"] = ns
	}
	if mn := r.URL.Query().Get("metricName"); mn != "" {
		nr.Params["MetricName"] = mn
	}
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["NextToken"] = tok
	}

	resp, err := h.provider.ListMetrics(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawMetrics, _ := resp.Data["Metrics"].([]any)
	nextToken, _ := resp.Data["NextToken"].(string)

	items := make([]Metric, 0, len(rawMetrics))
	for _, raw := range rawMetrics {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, Metric{
				Namespace:  strAny(m, "Namespace"),
				MetricName: strAny(m, "MetricName"),
			})
		}
	}

	uihelper.WriteJSON(w, ListMetricsResponse{Items: items, NextToken: nextToken, Total: len(items)})
}

// GET /metrics/statistics?namespace=...&metricName=...&startTime=...&endTime=...&period=...&statistics=Sum,Average
func (h *Handler) GetMetricStatistics(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ns := q.Get("namespace")
	metricName := q.Get("metricName")
	if ns == "" || metricName == "" {
		uihelper.UIError(w, "BadRequest", "namespace and metricName are required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "GetMetricStatistics", region, account)
	nr.Params["Namespace"] = ns
	nr.Params["MetricName"] = metricName
	if s := q.Get("startTime"); s != "" {
		nr.Params["StartTime"] = s
	}
	if s := q.Get("endTime"); s != "" {
		nr.Params["EndTime"] = s
	}
	period := 60
	if s := q.Get("period"); s != "" {
		if n := parseInt(s); n > 0 {
			period = n
		}
	}
	nr.Params["Period"] = float64(period)

	stats := q["statistics"]
	if len(stats) == 0 {
		stats = []string{"Sum", "Average", "Minimum", "Maximum", "SampleCount"}
	}
	for i, s := range stats {
		nr.Params[fmt.Sprintf("Statistics.member.%d", i+1)] = s
	}

	resp, err := h.provider.GetMetricStatistics(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	label, _ := resp.Data["Label"].(string)
	rawDPs, _ := resp.Data["Datapoints"].([]any)

	dps := make([]Datapoint, 0, len(rawDPs))
	for _, raw := range rawDPs {
		if m, ok := raw.(map[string]any); ok {
			dp := Datapoint{
				Timestamp: strAny(m, "Timestamp"),
				Unit:      strAny(m, "Unit"),
			}
			if v, ok := m["Sum"].(float64); ok {
				dp.Sum = v
			}
			if v, ok := m["Average"].(float64); ok {
				dp.Average = v
			}
			if v, ok := m["Minimum"].(float64); ok {
				dp.Minimum = v
			}
			if v, ok := m["Maximum"].(float64); ok {
				dp.Maximum = v
			}
			if v, ok := m["SampleCount"].(float64); ok {
				dp.SampleCount = v
			}
			dps = append(dps, dp)
		}
	}

	uihelper.WriteJSON(w, MetricStatisticsResponse{Label: label, Datapoints: dps})
}

// GET /alarms?stateValue=...&alarmNamePrefix=...&nextToken=...
func (h *Handler) ListAlarms(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "DescribeAlarms", region, account)
	if sv := r.URL.Query().Get("stateValue"); sv != "" {
		nr.Params["StateValue"] = sv
	}
	if pfx := r.URL.Query().Get("alarmNamePrefix"); pfx != "" {
		nr.Params["AlarmNamePrefix"] = pfx
	}
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["NextToken"] = tok
	}

	resp, err := h.provider.DescribeAlarms(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawAlarms, _ := resp.Data["MetricAlarms"].([]any)
	nextToken, _ := resp.Data["NextToken"].(string)

	alarms := make([]Alarm, 0, len(rawAlarms))
	for _, raw := range rawAlarms {
		if m, ok := raw.(map[string]any); ok {
			alarms = append(alarms, mapAlarm(m))
		}
	}

	uihelper.WriteJSON(w, ListAlarmsResponse{Items: alarms, NextToken: nextToken, Total: len(alarms)})
}

// POST /alarms  body: PutAlarmRequest
func (h *Handler) PutAlarm(w http.ResponseWriter, r *http.Request) {
	var req PutAlarmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.AlarmName == "" {
		uihelper.UIError(w, "BadRequest", "alarmName is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "PutMetricAlarm", region, account)
	nr.Params["AlarmName"] = req.AlarmName
	if req.AlarmDescription != "" {
		nr.Params["AlarmDescription"] = req.AlarmDescription
	}
	if req.Namespace != "" {
		nr.Params["Namespace"] = req.Namespace
	}
	if req.MetricName != "" {
		nr.Params["MetricName"] = req.MetricName
	}
	if req.Statistic != "" {
		nr.Params["Statistic"] = req.Statistic
	}
	if req.Period > 0 {
		nr.Params["Period"] = float64(req.Period)
	}
	if req.Threshold != 0 {
		nr.Params["Threshold"] = req.Threshold
	}
	if req.ComparisonOperator != "" {
		nr.Params["ComparisonOperator"] = req.ComparisonOperator
	}
	if req.EvaluationPeriods > 0 {
		nr.Params["EvaluationPeriods"] = float64(req.EvaluationPeriods)
	}
	nr.Params["ActionsEnabled"] = req.ActionsEnabled

	if _, err := h.provider.PutMetricAlarm(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// DELETE /alarms?alarmName=...
func (h *Handler) DeleteAlarm(w http.ResponseWriter, r *http.Request) {
	alarmName := r.URL.Query().Get("alarmName")
	if alarmName == "" {
		uihelper.UIError(w, "BadRequest", "alarmName is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "DeleteAlarms", region, account)
	nr.Params["AlarmNames.member.1"] = alarmName

	if _, err := h.provider.DeleteAlarms(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /alarms/state  body: { "alarmName": "...", "stateValue": "...", "stateReason": "..." }
func (h *Handler) SetAlarmState(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AlarmName   string `json:"alarmName"`
		StateValue  string `json:"stateValue"`
		StateReason string `json:"stateReason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.AlarmName == "" || req.StateValue == "" {
		uihelper.UIError(w, "BadRequest", "alarmName and stateValue are required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "SetAlarmState", region, account)
	nr.Params["AlarmName"] = req.AlarmName
	nr.Params["StateValue"] = req.StateValue
	nr.Params["StateReason"] = req.StateReason

	if _, err := h.provider.SetAlarmState(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /alarms/enable  body: { "alarmNames": [...] }
func (h *Handler) EnableAlarmActions(w http.ResponseWriter, r *http.Request) {
	h.setAlarmActionsEnabled(w, r, true)
}

// POST /alarms/disable  body: { "alarmNames": [...] }
func (h *Handler) DisableAlarmActions(w http.ResponseWriter, r *http.Request) {
	h.setAlarmActionsEnabled(w, r, false)
}

func (h *Handler) setAlarmActionsEnabled(w http.ResponseWriter, r *http.Request, enable bool) {
	var req struct {
		AlarmNames []string `json:"alarmNames"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.AlarmNames) == 0 {
		uihelper.UIError(w, "BadRequest", "alarmNames is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	action := "EnableAlarmActions"
	if !enable {
		action = "DisableAlarmActions"
	}

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", action, region, account)
	for i, n := range req.AlarmNames {
		nr.Params[fmt.Sprintf("AlarmNames.member.%d", i+1)] = n
	}

	var err error
	if enable {
		_, err = h.provider.EnableAlarmActions(r.Context(), nr)
	} else {
		_, err = h.provider.DisableAlarmActions(r.Context(), nr)
	}
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /dashboards
func (h *Handler) ListDashboards(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "ListDashboards", region, account)
	resp, err := h.provider.ListDashboards(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawEntries, _ := resp.Data["DashboardEntries"].([]any)
	items := make([]Dashboard, 0, len(rawEntries))
	for _, raw := range rawEntries {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, Dashboard{
				DashboardName: strAny(m, "DashboardName"),
				DashboardARN:  strAny(m, "DashboardArn"),
				LastModified:  strAny(m, "LastModified"),
			})
		}
	}

	uihelper.WriteJSON(w, ListDashboardsResponse{Items: items, Total: len(items)})
}

// GET /dashboards/detail?name=...
func (h *Handler) GetDashboard(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "GetDashboard", region, account)
	nr.Params["DashboardName"] = name

	resp, err := h.provider.GetDashboard(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	uihelper.WriteJSON(w, Dashboard{
		DashboardName: strAny(resp.Data, "DashboardName"),
		DashboardARN:  strAny(resp.Data, "DashboardArn"),
		DashboardBody: strAny(resp.Data, "DashboardBody"),
	})
}

// PUT /dashboards  body: { "dashboardName": "...", "dashboardBody": "..." }
func (h *Handler) PutDashboard(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DashboardName string `json:"dashboardName"`
		DashboardBody string `json:"dashboardBody"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.DashboardName == "" {
		uihelper.UIError(w, "BadRequest", "dashboardName is required", http.StatusBadRequest)
		return
	}
	if req.DashboardBody == "" {
		req.DashboardBody = "{}"
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "PutDashboard", region, account)
	nr.Params["DashboardName"] = req.DashboardName
	nr.Params["DashboardBody"] = req.DashboardBody

	if _, err := h.provider.PutDashboard(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// DELETE /dashboards?name=...
func (h *Handler) DeleteDashboard(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "DeleteDashboards", region, account)
	nr.Params["DashboardNames.member.1"] = name

	if _, err := h.provider.DeleteDashboards(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapAlarm(m map[string]any) Alarm {
	a := Alarm{
		AlarmName:          strAny(m, "AlarmName"),
		AlarmARN:           strAny(m, "AlarmArn"),
		AlarmDescription:   strAny(m, "AlarmDescription"),
		Namespace:          strAny(m, "Namespace"),
		MetricName:         strAny(m, "MetricName"),
		Statistic:          strAny(m, "Statistic"),
		ComparisonOperator: strAny(m, "ComparisonOperator"),
		StateValue:         strAny(m, "StateValue"),
		StateReason:        strAny(m, "StateReason"),
		UpdatedAt:          strAny(m, "StateUpdatedTimestamp"),
	}
	if v, ok := m["Period"].(float64); ok {
		a.Period = int(v)
	}
	if v, ok := m["Threshold"].(float64); ok {
		a.Threshold = v
	}
	if v, ok := m["EvaluationPeriods"].(float64); ok {
		a.EvaluationPeriods = int(v)
	}
	if v, ok := m["ActionsEnabled"].(bool); ok {
		a.ActionsEnabled = v
	}
	return a
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func parseInt(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}
