package cloudwatchui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

type mockCWProvider struct {
	listMetricsResp         *model.ProviderResponse
	listMetricsErr          error
	getMetricStatsResp      *model.ProviderResponse
	getMetricStatsErr       error
	putAlarmResp            *model.ProviderResponse
	putAlarmErr             error
	describeAlarmsResp      *model.ProviderResponse
	describeAlarmsErr       error
	deleteAlarmsErr         error
	setAlarmStateErr        error
	enableActionsErr        error
	disableActionsErr       error
	listDashboardsResp      *model.ProviderResponse
	listDashboardsErr       error
	getDashboardResp        *model.ProviderResponse
	getDashboardErr         error
	putDashboardErr         error
	deleteDashboardsErr     error

	lastNR      *model.NormalizedRequest
	putAlarmNR  *model.NormalizedRequest
	deletedNR   *model.NormalizedRequest
	setStateNR  *model.NormalizedRequest
	enableNR    *model.NormalizedRequest
	disableNR   *model.NormalizedRequest
	putDashNR   *model.NormalizedRequest
	delDashNR   *model.NormalizedRequest
}

func (m *mockCWProvider) ListMetrics(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listMetricsResp != nil {
		return m.listMetricsResp, m.listMetricsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Metrics": []any{}}}, m.listMetricsErr
}
func (m *mockCWProvider) GetMetricStatistics(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getMetricStatsResp != nil {
		return m.getMetricStatsResp, m.getMetricStatsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Label": "test", "Datapoints": []any{}}}, m.getMetricStatsErr
}
func (m *mockCWProvider) PutMetricAlarm(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.putAlarmNR = nr
	m.lastNR = nr
	if m.putAlarmResp != nil {
		return m.putAlarmResp, m.putAlarmErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.putAlarmErr
}
func (m *mockCWProvider) DescribeAlarms(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.describeAlarmsResp != nil {
		return m.describeAlarmsResp, m.describeAlarmsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"MetricAlarms": []any{}}}, m.describeAlarmsErr
}
func (m *mockCWProvider) DeleteAlarms(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.deletedNR = nr
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteAlarmsErr
}
func (m *mockCWProvider) SetAlarmState(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.setStateNR = nr
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.setAlarmStateErr
}
func (m *mockCWProvider) EnableAlarmActions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.enableNR = nr
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.enableActionsErr
}
func (m *mockCWProvider) DisableAlarmActions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.disableNR = nr
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.disableActionsErr
}
func (m *mockCWProvider) PutDashboard(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.putDashNR = nr
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.putDashboardErr
}
func (m *mockCWProvider) GetDashboard(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getDashboardResp != nil {
		return m.getDashboardResp, m.getDashboardErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"DashboardName": "d", "DashboardBody": "{}"}}, m.getDashboardErr
}
func (m *mockCWProvider) ListDashboards(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listDashboardsResp != nil {
		return m.listDashboardsResp, m.listDashboardsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"DashboardEntries": []any{}}}, m.listDashboardsErr
}
func (m *mockCWProvider) DeleteDashboards(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.delDashNR = nr
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteDashboardsErr
}

func testCWCfg() *config.Config {
	return &config.Config{Port: 4566, UIPort: 4567, Region: "us-east-1", AccountID: "000000000000", Clock: clock.RealClock{}}
}

func testCWHandler(p *mockCWProvider) *Handler {
	return NewHandler(p, testCWCfg())
}

func TestCWListMetrics_Returns200(t *testing.T) {
	p := &mockCWProvider{
		listMetricsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Metrics": []any{
				map[string]any{"Namespace": "AWS/EC2", "MetricName": "CPUUtilization"},
			},
		}},
	}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ListMetrics(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListMetricsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Namespace != "AWS/EC2" {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestCWListMetrics_ForwardsNamespaceFilter(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/metrics?namespace=AWS/Lambda", nil)
	w := httptest.NewRecorder()
	h.ListMetrics(w, req)

	if p.lastNR.Params["Namespace"] != "AWS/Lambda" {
		t.Fatalf("expected Namespace=AWS/Lambda, got %v", p.lastNR.Params["Namespace"])
	}
}

func TestCWGetMetricStatistics_MissingParams_Returns400(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/metrics/statistics?namespace=AWS/EC2", nil)
	w := httptest.NewRecorder()
	h.GetMetricStatistics(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCWGetMetricStatistics_SetsPeriodAndStats(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/metrics/statistics?namespace=AWS/EC2&metricName=CPUUtilization&period=300", nil)
	w := httptest.NewRecorder()
	h.GetMetricStatistics(w, req)

	if p.lastNR.Params["Period"] != float64(300) {
		t.Fatalf("expected Period=300, got %v", p.lastNR.Params["Period"])
	}
	if p.lastNR.Params["Statistics.member.1"] == nil {
		t.Fatal("expected Statistics.member.1 to be set")
	}
}

func TestCWListAlarms_Returns200(t *testing.T) {
	p := &mockCWProvider{
		describeAlarmsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"MetricAlarms": []any{
				map[string]any{"AlarmName": "my-alarm", "StateValue": "OK"},
			},
		}},
	}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/alarms", nil)
	w := httptest.NewRecorder()
	h.ListAlarms(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListAlarmsResponse
	json.Unmarshal(w.Body.Bytes(), &resp) //nolint:errcheck
	if len(resp.Items) != 1 || resp.Items[0].AlarmName != "my-alarm" {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestCWListAlarms_ForwardsStateFilter(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/alarms?stateValue=ALARM", nil)
	w := httptest.NewRecorder()
	h.ListAlarms(w, req)

	if p.lastNR.Params["StateValue"] != "ALARM" {
		t.Fatalf("expected StateValue=ALARM, got %v", p.lastNR.Params["StateValue"])
	}
}

func TestCWPutAlarm_Returns201(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]any{
		"alarmName":          "cpu-high",
		"namespace":          "AWS/EC2",
		"metricName":         "CPUUtilization",
		"statistic":          "Average",
		"threshold":          80.0,
		"comparisonOperator": "GreaterThanThreshold",
		"evaluationPeriods":  2,
	})
	req := httptest.NewRequest(http.MethodPost, "/alarms", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutAlarm(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if p.putAlarmNR.Params["AlarmName"] != "cpu-high" {
		t.Fatalf("expected AlarmName=cpu-high, got %v", p.putAlarmNR.Params["AlarmName"])
	}
	if p.putAlarmNR.Params["Namespace"] != "AWS/EC2" {
		t.Fatalf("expected Namespace=AWS/EC2, got %v", p.putAlarmNR.Params["Namespace"])
	}
}

func TestCWPutAlarm_MissingAlarmName_Returns400(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]any{"namespace": "AWS/EC2"})
	req := httptest.NewRequest(http.MethodPost, "/alarms", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutAlarm(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCWDeleteAlarm_Returns204(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/alarms?alarmName=cpu-high", nil)
	w := httptest.NewRecorder()
	h.DeleteAlarm(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.deletedNR.Params["AlarmNames.member.1"] != "cpu-high" {
		t.Fatalf("expected AlarmNames.member.1=cpu-high, got %v", p.deletedNR.Params["AlarmNames.member.1"])
	}
}

func TestCWDeleteAlarm_MissingName_Returns400(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/alarms", nil)
	w := httptest.NewRecorder()
	h.DeleteAlarm(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCWSetAlarmState_Returns204(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]string{"alarmName": "cpu-high", "stateValue": "OK", "stateReason": "manual"})
	req := httptest.NewRequest(http.MethodPost, "/alarms/state", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.SetAlarmState(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.setStateNR.Params["AlarmName"] != "cpu-high" {
		t.Fatalf("expected AlarmName=cpu-high, got %v", p.setStateNR.Params["AlarmName"])
	}
	if p.setStateNR.Params["StateValue"] != "OK" {
		t.Fatalf("expected StateValue=OK, got %v", p.setStateNR.Params["StateValue"])
	}
}

func TestCWSetAlarmState_MissingFields_Returns400(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]string{"alarmName": "cpu-high"})
	req := httptest.NewRequest(http.MethodPost, "/alarms/state", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.SetAlarmState(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCWEnableAlarmActions_SetsMemberParams(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]any{"alarmNames": []string{"alarm-a", "alarm-b"}})
	req := httptest.NewRequest(http.MethodPost, "/alarms/enable", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.EnableAlarmActions(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.enableNR.Params["AlarmNames.member.1"] != "alarm-a" {
		t.Fatalf("expected AlarmNames.member.1=alarm-a, got %v", p.enableNR.Params["AlarmNames.member.1"])
	}
	if p.enableNR.Params["AlarmNames.member.2"] != "alarm-b" {
		t.Fatalf("expected AlarmNames.member.2=alarm-b, got %v", p.enableNR.Params["AlarmNames.member.2"])
	}
}

func TestCWDisableAlarmActions_Returns204(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]any{"alarmNames": []string{"alarm-a"}})
	req := httptest.NewRequest(http.MethodPost, "/alarms/disable", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.DisableAlarmActions(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.disableNR.Params["AlarmNames.member.1"] != "alarm-a" {
		t.Fatalf("expected AlarmNames.member.1=alarm-a, got %v", p.disableNR.Params["AlarmNames.member.1"])
	}
}

func TestCWListDashboards_Returns200(t *testing.T) {
	p := &mockCWProvider{
		listDashboardsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"DashboardEntries": []any{
				map[string]any{"DashboardName": "my-dashboard", "DashboardArn": "arn:aws:cloudwatch::000000000000:dashboard/my-dashboard"},
			},
		}},
	}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/dashboards", nil)
	w := httptest.NewRecorder()
	h.ListDashboards(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListDashboardsResponse
	json.Unmarshal(w.Body.Bytes(), &resp) //nolint:errcheck
	if len(resp.Items) != 1 || resp.Items[0].DashboardName != "my-dashboard" {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestCWPutDashboard_Returns201(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]string{"dashboardName": "test-dash", "dashboardBody": `{"widgets":[]}`})
	req := httptest.NewRequest(http.MethodPut, "/dashboards", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutDashboard(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if p.putDashNR.Params["DashboardName"] != "test-dash" {
		t.Fatalf("expected DashboardName=test-dash, got %v", p.putDashNR.Params["DashboardName"])
	}
}

func TestCWPutDashboard_DefaultsBodyToEmptyObject(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]string{"dashboardName": "d"})
	req := httptest.NewRequest(http.MethodPut, "/dashboards", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutDashboard(w, req)

	if p.putDashNR.Params["DashboardBody"] != "{}" {
		t.Fatalf("expected DashboardBody={}, got %v", p.putDashNR.Params["DashboardBody"])
	}
}

func TestCWPutDashboard_MissingName_Returns400(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]string{"dashboardBody": "{}"})
	req := httptest.NewRequest(http.MethodPut, "/dashboards", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutDashboard(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCWDeleteDashboard_Returns204(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/dashboards?name=my-dashboard", nil)
	w := httptest.NewRecorder()
	h.DeleteDashboard(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.delDashNR.Params["DashboardNames.member.1"] != "my-dashboard" {
		t.Fatalf("expected DashboardNames.member.1=my-dashboard, got %v", p.delDashNR.Params["DashboardNames.member.1"])
	}
}

func TestCWNR_ClockIsNonNil(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	body, _ := json.Marshal(map[string]any{"alarmName": "a", "alarmNames": []string{"a"}})
	req := httptest.NewRequest(http.MethodPost, "/alarms/enable", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.EnableAlarmActions(w, req)

	if p.enableNR.Clock == nil {
		t.Fatal("expected nr.Clock to be non-nil")
	}
}

func TestCWNR_PortIsWirePort(t *testing.T) {
	p := &mockCWProvider{}
	h := testCWHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ListMetrics(w, req)

	if p.lastNR.Port != testCWCfg().Port {
		t.Fatalf("expected Port=%d, got %d", testCWCfg().Port, p.lastNR.Port)
	}
}
