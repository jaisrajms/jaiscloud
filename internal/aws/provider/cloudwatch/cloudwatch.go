package cloudwatch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Provider handles CloudWatch metric + alarm RPCs.
// Metric data is stored in an in-memory ring (bounded) per (namespace, metric, dims).
// Alarms are persisted via store.ResourceStore so they survive restarts in persistent mode.
type Provider struct {
	resources store.ResourceStore
	bus       *events.EventBus

	mu      sync.Mutex
	metrics map[string]*metricRing // key: ringKey(ns, name, dims)

	snsPublisher  SNSPublisher
	lambdaInvoker LambdaInvoker
}

const ringSize = 256

type metricRing struct {
	account   string
	namespace string
	name      string
	points    []datapoint
	idx       int
}

type datapoint struct {
	Timestamp time.Time
	Value     float64
	Unit      string
	Dims      map[string]string
}

func New(resources store.ResourceStore, bus *events.EventBus) *Provider {
	return &Provider{
		resources: resources,
		bus:       bus,
		metrics:   make(map[string]*metricRing),
	}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"CloudWatch.PutMetricData":           p.PutMetricData,
		"CloudWatch.GetMetricStatistics":     p.GetMetricStatistics,
		"CloudWatch.GetMetricData":           p.GetMetricData,
		"CloudWatch.ListMetrics":             p.ListMetrics,
		"CloudWatch.PutMetricAlarm":          p.PutMetricAlarm,
		"CloudWatch.DescribeAlarms":          p.DescribeAlarms,
		"CloudWatch.DescribeAlarmsForMetric": p.DescribeAlarmsForMetric,
		"CloudWatch.DeleteAlarms":            p.DeleteAlarms,
		"CloudWatch.SetAlarmState":           p.SetAlarmState,
		"CloudWatch.GetDashboard":            p.GetDashboard,
		"CloudWatch.ListDashboards":          p.ListDashboards,
		"CloudWatch.PutDashboard":            p.PutDashboard,
		"CloudWatch.DeleteDashboards":        p.DeleteDashboards,
		"CloudWatch.EnableAlarmActions":      p.EnableAlarmActions,
		"CloudWatch.DisableAlarmActions":     p.DisableAlarmActions,
		"CloudWatch.TagResource":             p.TagResource,
		"CloudWatch.UntagResource":           p.UntagResource,
		"CloudWatch.ListTagsForResource":     p.ListTagsForResource,
		// Composite alarms (13.8)
		"CloudWatch.PutCompositeAlarm":    p.PutCompositeAlarm,
		"CloudWatch.DescribeAlarmHistory": p.DescribeAlarmHistory,
		// Anomaly detectors (13.8)
		"CloudWatch.PutAnomalyDetector":       p.PutAnomalyDetector,
		"CloudWatch.DescribeAnomalyDetectors": p.DescribeAnomalyDetectors,
		"CloudWatch.DeleteAnomalyDetector":    p.DeleteAnomalyDetector,
		// Metric streams (13.9)
		"CloudWatch.PutMetricStream":    p.PutMetricStream,
		"CloudWatch.GetMetricStream":    p.GetMetricStream,
		"CloudWatch.DeleteMetricStream": p.DeleteMetricStream,
		"CloudWatch.ListMetricStreams":  p.ListMetricStreams,
		"CloudWatch.StartMetricStreams": p.StartMetricStreams,
		"CloudWatch.StopMetricStreams":  p.StopMetricStreams,
	}
}

// PutMetricData stores datapoints in the in-memory ring keyed by
// (namespace, metricName, sorted-dimensions) so different dimension sets for
// the same metric name get separate rings.
func (p *Provider) PutMetricData(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns, _ := nr.Params["Namespace"].(string)
	if ns == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "Namespace is required", HTTPStatus: 400}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 1; ; i++ {
		prefix := "MetricData.member." + strconv.Itoa(i) + "."
		name, ok := nr.Params[prefix+"MetricName"].(string)
		if !ok || name == "" {
			break
		}
		value, _ := toFloat(nr.Params[prefix+"Value"])
		unit, _ := nr.Params[prefix+"Unit"].(string)
		ts := parseTimestamp(nr.Params[prefix+"Timestamp"])
		dims := extractDimensions(nr.Params, prefix)

		key := nr.AccountID + "\x00" + ringKey(ns, name, dims)
		ring, exists := p.metrics[key]
		if !exists {
			ring = &metricRing{account: nr.AccountID, namespace: ns, name: name, points: make([]datapoint, ringSize)}
			p.metrics[key] = ring
		}
		ring.points[ring.idx] = datapoint{Timestamp: ts, Value: value, Unit: unit, Dims: dims}
		ring.idx = (ring.idx + 1) % ringSize
	}
	return provider.OK(map[string]any{"__action__": "PutMetricData"}), nil
}

func (p *Provider) GetMetricStatistics(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns, _ := nr.Params["Namespace"].(string)
	metricName, _ := nr.Params["MetricName"].(string)
	dims := extractDimensions(nr.Params, "")
	startTime := parseTimestamp(nr.Params["StartTime"])
	endTime := parseTimestamp(nr.Params["EndTime"])
	period := 60
	if pv, ok := nr.Params["Period"].(float64); ok {
		period = int(pv)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	key := nr.AccountID + "\x00" + ringKey(ns, metricName, dims)
	ring, ok := p.metrics[key]
	if !ok {
		return provider.OK(map[string]any{"Label": metricName, "Datapoints": []any{}}), nil
	}

	type bucket struct{ sum, min, max, count float64 }
	buckets := make(map[int64]*bucket)
	for _, dp := range ring.points {
		if dp.Timestamp.IsZero() {
			continue
		}
		if dp.Timestamp.Before(startTime) || dp.Timestamp.After(endTime) {
			continue
		}
		t := (dp.Timestamp.Unix() / int64(period)) * int64(period)
		b := buckets[t]
		if b == nil {
			b = &bucket{min: dp.Value, max: dp.Value}
			buckets[t] = b
		}
		b.sum += dp.Value
		b.count++
		if dp.Value < b.min {
			b.min = dp.Value
		}
		if dp.Value > b.max {
			b.max = dp.Value
		}
	}

	var stats []string
	for i := 1; ; i++ {
		s, ok := nr.Params["Statistics.member."+strconv.Itoa(i)].(string)
		if !ok || s == "" {
			break
		}
		stats = append(stats, s)
	}

	datapoints := make([]any, 0, len(buckets))
	for t, b := range buckets {
		dp := map[string]any{"Timestamp": time.Unix(t, 0).UTC().Format(time.RFC3339)}
		for _, s := range stats {
			switch s {
			case "Sum":
				dp["Sum"] = b.sum
			case "Average":
				dp["Average"] = b.sum / b.count
			case "Minimum":
				dp["Minimum"] = b.min
			case "Maximum":
				dp["Maximum"] = b.max
			case "SampleCount":
				dp["SampleCount"] = b.count
			}
		}
		datapoints = append(datapoints, dp)
	}
	sort.Slice(datapoints, func(i, j int) bool {
		ti, _ := datapoints[i].(map[string]any)["Timestamp"].(string)
		tj, _ := datapoints[j].(map[string]any)["Timestamp"].(string)
		return ti < tj
	})
	return provider.OK(map[string]any{"Label": metricName, "Datapoints": datapoints}), nil
}

func (p *Provider) GetMetricData(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	results := make([]any, 0)
	for i := 1; ; i++ {
		prefix := "MetricDataQueries.member." + strconv.Itoa(i) + "."
		id, ok := nr.Params[prefix+"Id"].(string)
		if !ok || id == "" {
			break
		}
		ns, _ := nr.Params[prefix+"MetricStat.Metric.Namespace"].(string)
		metricName, _ := nr.Params[prefix+"MetricStat.Metric.MetricName"].(string)

		var values []float64
		var timestamps []string
		if ns != "" && metricName != "" {
			dims := extractDimensions(nr.Params, prefix+"MetricStat.Metric.")
			key := nr.AccountID + "\x00" + ringKey(ns, metricName, dims)
			if ring, ok := p.metrics[key]; ok {
				for _, dp := range ring.points {
					if !dp.Timestamp.IsZero() {
						values = append(values, dp.Value)
						timestamps = append(timestamps, dp.Timestamp.UTC().Format(time.RFC3339))
					}
				}
			}
		}
		if values == nil {
			values = []float64{}
		}
		if timestamps == nil {
			timestamps = []string{}
		}
		results = append(results, map[string]any{
			"Id":         id,
			"Label":      metricName,
			"Values":     values,
			"Timestamps": timestamps,
			"StatusCode": "Complete",
		})
	}
	return provider.OK(map[string]any{"MetricDataResults": results, "Messages": []any{}}), nil
}

func (p *Provider) ListMetrics(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	nsFilter, _ := nr.Params["Namespace"].(string)
	nameFilter, _ := nr.Params["MetricName"].(string)
	all := make([]any, 0, len(p.metrics))
	for _, r := range p.metrics {
		if r.account != nr.AccountID {
			continue
		}
		if nsFilter != "" && r.namespace != nsFilter {
			continue
		}
		if nameFilter != "" && r.name != nameFilter {
			continue
		}
		all = append(all, map[string]any{"Namespace": r.namespace, "MetricName": r.name})
	}
	maxResults := 100
	token, _ := nr.Params["NextToken"].(string)
	page, next, err := pagination.Paginate(all, maxResults, token, "ListMetrics")
	if err != nil {
		return nil, model.NewProviderError("InvalidParameterValue", err.Error(), 400)
	}
	data := map[string]any{"__action__": "ListMetrics", "Metrics": page}
	if next != "" {
		data["NextToken"] = next
	}
	return provider.OK(data), nil
}

func (p *Provider) PutMetricAlarm(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["AlarmName"].(string)
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "AlarmName is required", HTTPStatus: 400}
	}
	// Inject AlarmArn and default StateValue before persisting.
	alarmData := make(map[string]any, len(nr.Params)+2)
	for k, v := range nr.Params {
		alarmData[k] = v
	}
	alarmData["AlarmArn"] = nr.ResourceID("cloudwatch-alarm", name)
	if _, hasState := alarmData["StateValue"]; !hasState {
		alarmData["StateValue"] = "INSUFFICIENT_DATA"
	}
	data, err := json.Marshal(alarmData)
	if err != nil {
		return nil, err
	}
	entry := store.ResourceEntry{Type: "cloudwatch_alarm", ID: name, Data: data}
	if err := p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry); err != nil {
		slog.Error("cloudwatch: failed to persist alarm",
			"alarm", name, "err", err)
	}
	now := clock.Now()
	p.writeAlarmHistory(ctx, nr.AccountID, nr.Region, name, "MetricAlarm", "ConfigurationUpdate",
		"Alarm created or updated",
		map[string]any{"version": "1.0", "updatedAlarm": nr.Params},
		now)
	return provider.OK(map[string]any{"__action__": "PutMetricAlarm"}), nil
}

func (p *Provider) DescribeAlarms(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, "cloudwatch_alarm", "")
	if err != nil {
		return nil, err
	}

	// Build AlarmNames set for exact-name filter.
	alarmNames := map[string]bool{}
	for i := 1; ; i++ {
		key := fmt.Sprintf("AlarmNames.member.%d", i)
		name, ok := nr.Params[key].(string)
		if !ok || name == "" {
			break
		}
		alarmNames[name] = true
	}
	prefix, _ := nr.Params["AlarmNamePrefix"].(string)
	stateFilter, _ := nr.Params["StateValue"].(string)

	out := make([]any, 0, len(entries))
	for _, e := range entries {
		var params map[string]any
		if err := json.Unmarshal(e.Data, &params); err != nil {
			slog.Warn("cloudwatch: failed to unmarshal alarm",
				"pkg", "cloudwatch", "op", "DescribeAlarms", "alarm", e.ID, "err", err)
			continue
		}
		name, _ := params["AlarmName"].(string)
		if len(alarmNames) > 0 && !alarmNames[name] {
			continue
		}
		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}
		if stateFilter != "" {
			state, _ := params["StateValue"].(string)
			if state != stateFilter {
				continue
			}
		}
		out = append(out, params)
	}
	maxResults := 100
	if v, ok := nr.Params["MaxRecords"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["NextToken"].(string)
	page, next, pgErr := pagination.Paginate(out, maxResults, token, "DescribeAlarms")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValue", pgErr.Error(), 400)
	}
	respData := map[string]any{"__action__": "DescribeAlarms", "MetricAlarms": page}
	if next != "" {
		respData["NextToken"] = next
	}
	return provider.OK(respData), nil
}

func (p *Provider) DescribeAlarmsForMetric(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{"__action__": "DescribeAlarmsForMetric", "MetricAlarms": []any{}}), nil
}

func (p *Provider) DeleteAlarms(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	for i := 1; ; i++ {
		key := "AlarmNames.member." + strconv.Itoa(i)
		name, ok := nr.Params[key].(string)
		if !ok || name == "" {
			break
		}
		if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, "cloudwatch_alarm", name); err != nil {
			slog.Error("cloudwatch: failed to delete alarm",
				"pkg", "cloudwatch", "op", "DeleteAlarms", "alarm", name, "err", err)
		}
	}
	return provider.OK(map[string]any{"__action__": "DeleteAlarms"}), nil
}

func (p *Provider) SetAlarmState(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["AlarmName"].(string)
	stateValue, _ := nr.Params["StateValue"].(string)
	stateReason, _ := nr.Params["StateReason"].(string)
	if name == "" || stateValue == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "AlarmName and StateValue are required", HTTPStatus: 400}
	}
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_alarm", name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFoundException", Message: "Alarm not found: " + name, HTTPStatus: 404}
	}
	var params map[string]any
	if err := json.Unmarshal(e.Data, &params); err != nil {
		return nil, err
	}
	oldState, _ := params["StateValue"].(string)
	params["StateValue"] = stateValue
	params["StateReason"] = stateReason
	now := clock.Now()
	params["StateUpdatedTimestamp"] = now.Format(time.RFC3339)
	data, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: "cloudwatch_alarm", ID: name, Data: data}); err != nil {
		slog.Error("cloudwatch: failed to persist alarm state", "alarm", name, "err", err)
	}
	// Record history for explicit state changes via SetAlarmState.
	p.writeAlarmHistory(ctx, nr.AccountID, nr.Region, name, "MetricAlarm", "StateUpdate",
		fmt.Sprintf("Alarm updated from %s to %s", oldState, stateValue),
		map[string]any{
			"version":  "1.0",
			"oldState": map[string]any{"stateValue": oldState},
			"newState": map[string]any{"stateValue": stateValue, "stateReason": stateReason},
		},
		now)
	go p.fireAlarmActions(context.WithoutCancel(ctx), params, stateValue)
	return provider.OK(map[string]any{"__action__": "SetAlarmState"}), nil
}

// ─── Alarm actions ────────────────────────────────────────────────────────────

func (p *Provider) EnableAlarmActions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setAlarmActionsEnabled(ctx, nr, true)
}

func (p *Provider) DisableAlarmActions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setAlarmActionsEnabled(ctx, nr, false)
}

func (p *Provider) setAlarmActionsEnabled(ctx context.Context, nr *model.NormalizedRequest, enabled bool) (*model.ProviderResponse, error) {
	for i := 1; ; i++ {
		name, ok := nr.Params["AlarmNames.member."+strconv.Itoa(i)].(string)
		if !ok || name == "" {
			break
		}
		e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_alarm", name)
		if err != nil {
			continue
		}
		var params map[string]any
		if json.Unmarshal(e.Data, &params) != nil {
			continue
		}
		params["ActionsEnabled"] = enabled
		data, _ := json.Marshal(params)
		_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: "cloudwatch_alarm", ID: name, Data: data})
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Dashboards ───────────────────────────────────────────────────────────────

type dashboardEntry struct {
	DashboardName string `json:"DashboardName"`
	DashboardBody string `json:"DashboardBody"`
	DashboardArn  string `json:"DashboardArn"`
	LastModified  string `json:"LastModified"`
}

func (p *Provider) PutDashboard(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["DashboardName"].(string)
	body, _ := nr.Params["DashboardBody"].(string)
	d := dashboardEntry{
		DashboardName: name,
		DashboardBody: body,
		DashboardArn:  nr.ResourceID("cloudwatch-dashboard", name),
		LastModified:  clock.Now().Format(time.RFC3339),
	}
	data, _ := json.Marshal(d)
	entry := store.ResourceEntry{Type: "cloudwatch_dashboard", ID: name, Data: data}
	_ = p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry)
	return provider.OK(map[string]any{"DashboardValidationMessages": []any{}}), nil
}

func (p *Provider) GetDashboard(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["DashboardName"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_dashboard", name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFound", Message: "Dashboard not found: " + name, HTTPStatus: 404}
	}
	var d dashboardEntry
	if err := json.Unmarshal(e.Data, &d); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"DashboardName": d.DashboardName,
		"DashboardBody": d.DashboardBody,
		"DashboardArn":  d.DashboardArn,
	}), nil
}

func (p *Provider) ListDashboards(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "cloudwatch_dashboard", "")
	out := make([]any, 0, len(entries))
	for _, e := range entries {
		var d dashboardEntry
		if json.Unmarshal(e.Data, &d) == nil {
			out = append(out, map[string]any{
				"DashboardName": d.DashboardName,
				"DashboardArn":  d.DashboardArn,
				"LastModified":  d.LastModified,
			})
		}
	}
	return provider.OK(map[string]any{"DashboardEntries": out}), nil
}

func (p *Provider) DeleteDashboards(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	for i := 1; ; i++ {
		name, ok := nr.Params["DashboardNames.member."+strconv.Itoa(i)].(string)
		if !ok || name == "" {
			break
		}
		_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, "cloudwatch_dashboard", name)
	}
	return provider.OK(map[string]any{}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *Provider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["ResourceARN"].(string)
	tags := make(map[string]string)
	if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_tags", arn); err == nil {
		_ = json.Unmarshal(e.Data, &tags)
	}
	for i := 1; ; i++ {
		k, kok := nr.Params["Tags.member."+strconv.Itoa(i)+".Key"].(string)
		v, _ := nr.Params["Tags.member."+strconv.Itoa(i)+".Value"].(string)
		if !kok || k == "" {
			break
		}
		tags[k] = v
	}
	data, _ := json.Marshal(tags)
	entry := store.ResourceEntry{Type: "cloudwatch_tags", ID: arn, Data: data}
	_ = p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["ResourceARN"].(string)
	tags := make(map[string]string)
	if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_tags", arn); err == nil {
		_ = json.Unmarshal(e.Data, &tags)
	}
	for i := 1; ; i++ {
		k, ok := nr.Params["TagKeys.member."+strconv.Itoa(i)].(string)
		if !ok || k == "" {
			break
		}
		delete(tags, k)
	}
	data, _ := json.Marshal(tags)
	_ = p.resources.Update(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: "cloudwatch_tags", ID: arn, Data: data})
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn, _ := nr.Params["ResourceARN"].(string)
	tags := make(map[string]string)
	if e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_tags", arn); err == nil {
		_ = json.Unmarshal(e.Data, &tags)
	}
	out := make([]any, 0, len(tags))
	for k, v := range tags {
		out = append(out, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"Tags": out}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func ringKey(ns, name string, dims map[string]string) string {
	keys := make([]string, 0, len(dims))
	for k := range dims {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	h.Write([]byte(ns + "\x00" + name + "\x00"))
	for _, k := range keys {
		h.Write([]byte(k + "=" + dims[k] + "\x00"))
	}
	return ns + "/" + name + "/" + hex.EncodeToString(h.Sum(nil))[:16]
}

func extractDimensions(params map[string]any, prefix string) map[string]string {
	out := map[string]string{}
	for i := 1; ; i++ {
		base := prefix + "Dimensions.member." + strconv.Itoa(i) + "."
		n, nok := params[base+"Name"].(string)
		v, vok := params[base+"Value"].(string)
		if !nok || !vok || n == "" {
			break
		}
		out[n] = v
	}
	return out
}

func parseTimestamp(raw any) time.Time {
	switch v := raw.(type) {
	case string:
		if v == "" {
			return clock.Now()
		}
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.UTC()
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return time.Unix(int64(f), 0).UTC()
		}
	case float64:
		return time.Unix(int64(v), 0).UTC()
	}
	return clock.Now()
}

func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	}
	return 0, false
}

// ─── Composite Alarms (13.8) ──────────────────────────────────────────────────

type compositeAlarm struct {
	AlarmName               string    `json:"AlarmName"`
	AlarmRule               string    `json:"AlarmRule"`
	AlarmActions            []string  `json:"AlarmActions"`
	OKActions               []string  `json:"OKActions"`
	InsufficientDataActions []string  `json:"InsufficientDataActions"`
	ActionsEnabled          bool      `json:"ActionsEnabled"`
	State                   string    `json:"State"`
	Description             string    `json:"Description"`
	ARN                     string    `json:"ARN"`
	CreationDate            time.Time `json:"CreationDate"`
}

func (p *Provider) PutCompositeAlarm(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["AlarmName"].(string)
	if name == "" {
		return nil, &model.ProviderError{Code: "ValidationError", Message: "AlarmName is required", HTTPStatus: 400}
	}
	rule, _ := nr.Params["AlarmRule"].(string)
	alarm := compositeAlarm{
		AlarmName:      name,
		AlarmRule:      rule,
		ActionsEnabled: true,
		State:          "OK",
		ARN:            nr.ResourceID("cloudwatch-alarm", name),
		CreationDate:   clock.Now(),
	}
	if v, ok := nr.Params["ActionsEnabled"].(bool); ok {
		alarm.ActionsEnabled = v
	}
	if v, ok := nr.Params["AlarmDescription"].(string); ok {
		alarm.Description = v
	}
	alarm.AlarmActions = strSlice(nr.Params, "AlarmActions")
	alarm.OKActions = strSlice(nr.Params, "OKActions")
	alarm.InsufficientDataActions = strSlice(nr.Params, "InsufficientDataActions")
	data, _ := json.Marshal(alarm)
	entry := store.ResourceEntry{Type: "cloudwatch_composite_alarm", ID: name, Data: data}
	_ = p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeAlarmHistory(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	alarmName, _ := nr.Params["AlarmName"].(string)
	histItemType, _ := nr.Params["HistoryItemType"].(string)

	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, "cloudwatch_alarm_history", "")
	if err != nil {
		return nil, err
	}

	type historyItem struct {
		AlarmName       string `json:"AlarmName"`
		AlarmType       string `json:"AlarmType"`
		HistoryItemType string `json:"HistoryItemType"`
		HistorySummary  string `json:"HistorySummary"`
		HistoryData     string `json:"HistoryData"`
		Timestamp       string `json:"Timestamp"`
	}

	items := make([]historyItem, 0, len(entries))
	for _, e := range entries {
		var h historyItem
		if err := json.Unmarshal(e.Data, &h); err != nil {
			continue
		}
		if alarmName != "" && h.AlarmName != alarmName {
			continue
		}
		if histItemType != "" && h.HistoryItemType != histItemType {
			continue
		}
		items = append(items, h)
	}

	// Sort descending by Timestamp.
	sort.Slice(items, func(i, j int) bool { return items[i].Timestamp > items[j].Timestamp })

	out := make([]any, len(items))
	for i, h := range items {
		out[i] = map[string]any{
			"AlarmName":       h.AlarmName,
			"AlarmType":       h.AlarmType,
			"HistoryItemType": h.HistoryItemType,
			"HistorySummary":  h.HistorySummary,
			"HistoryData":     h.HistoryData,
			"Timestamp":       h.Timestamp,
		}
	}
	return provider.OK(map[string]any{"AlarmHistoryItems": out}), nil
}

// writeAlarmHistory persists a single alarm history record.
func (p *Provider) writeAlarmHistory(ctx context.Context, account, region, alarmName, alarmType, itemType, summary string, dataObj map[string]any, ts time.Time) {
	histData, _ := json.Marshal(dataObj)
	record := map[string]any{
		"AlarmName":       alarmName,
		"AlarmType":       alarmType,
		"HistoryItemType": itemType,
		"HistorySummary":  summary,
		"HistoryData":     string(histData),
		"Timestamp":       ts.Format(time.RFC3339Nano),
	}
	data, _ := json.Marshal(record)
	id := fmt.Sprintf("%s::%d", alarmName, ts.UnixNano())
	_ = p.resources.Create(ctx, account, region, store.ResourceEntry{Type: "cloudwatch_alarm_history", ID: id, Data: data})
}

func (p *Provider) Reset(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.metrics = make(map[string]*metricRing)
}

// ─── Anomaly Detectors (13.8) ─────────────────────────────────────────────────

type anomalyDetector struct {
	Namespace     string            `json:"Namespace"`
	MetricName    string            `json:"MetricName"`
	Dimensions    map[string]string `json:"Dimensions"`
	Stat          string            `json:"Stat"`
	Configuration map[string]any    `json:"Configuration"`
	StateValue    string            `json:"StateValue"`
}

func anomalyKey(ns, metric string, dims map[string]string) string {
	return ringKey(ns, metric, dims)
}

func (p *Provider) PutAnomalyDetector(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns, _ := nr.Params["Namespace"].(string)
	metric, _ := nr.Params["MetricName"].(string)
	stat, _ := nr.Params["Stat"].(string)
	dims := extractDimensions(nr.Params, "")
	cfg, _ := nr.Params["Configuration"].(map[string]any)
	det := anomalyDetector{
		Namespace:     ns,
		MetricName:    metric,
		Dimensions:    dims,
		Stat:          stat,
		Configuration: cfg,
		StateValue:    "TRAINED_INSUFFICIENT_DATA",
	}
	data, _ := json.Marshal(det)
	id := anomalyKey(ns, metric, dims)
	entry := store.ResourceEntry{Type: "cloudwatch_anomaly_detector", ID: id, Data: data}
	_ = p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) DescribeAnomalyDetectors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "cloudwatch_anomaly_detector", "")
	nsFilter, _ := nr.Params["Namespace"].(string)
	metricFilter, _ := nr.Params["MetricName"].(string)
	out := []map[string]any{}
	for _, e := range entries {
		var det anomalyDetector
		json.Unmarshal(e.Data, &det)
		if nsFilter != "" && det.Namespace != nsFilter {
			continue
		}
		if metricFilter != "" && det.MetricName != metricFilter {
			continue
		}
		dims := make([]map[string]any, 0, len(det.Dimensions))
		for k, v := range det.Dimensions {
			dims = append(dims, map[string]any{"Name": k, "Value": v})
		}
		out = append(out, map[string]any{
			"Namespace":     det.Namespace,
			"MetricName":    det.MetricName,
			"Dimensions":    dims,
			"Stat":          det.Stat,
			"Configuration": det.Configuration,
			"StateValue":    det.StateValue,
		})
	}
	return provider.OK(map[string]any{"AnomalyDetectors": out}), nil
}

func (p *Provider) DeleteAnomalyDetector(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ns, _ := nr.Params["Namespace"].(string)
	metric, _ := nr.Params["MetricName"].(string)
	dims := extractDimensions(nr.Params, "")
	id := anomalyKey(ns, metric, dims)
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_anomaly_detector", id); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFound", Message: "Anomaly detector not found", HTTPStatus: 400}
	}
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, "cloudwatch_anomaly_detector", id)
	return provider.OK(map[string]any{}), nil
}

// ─── Metric Streams (13.9) ────────────────────────────────────────────────────

type metricStream struct {
	Name           string           `json:"Name"`
	ARN            string           `json:"ARN"`
	State          string           `json:"State"`
	FirehoseARN    string           `json:"FirehoseArn"`
	RoleARN        string           `json:"RoleArn"`
	OutputFormat   string           `json:"OutputFormat"`
	IncludeFilters []map[string]any `json:"IncludeFilters"`
	ExcludeFilters []map[string]any `json:"ExcludeFilters"`
	CreationDate   time.Time        `json:"CreationDate"`
	LastUpdateDate time.Time        `json:"LastUpdateDate"`
}

func (p *Provider) PutMetricStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	if name == "" {
		return nil, &model.ProviderError{Code: "InvalidParameterValue", Message: "Name is required", HTTPStatus: 400}
	}
	now := clock.Now()
	ms := metricStream{
		Name:           name,
		ARN:            nr.ResourceID("cloudwatch-metric-stream", name),
		State:          "running",
		FirehoseARN:    strParam2(nr.Params, "FirehoseArn"),
		RoleARN:        strParam2(nr.Params, "RoleArn"),
		OutputFormat:   strParam2(nr.Params, "OutputFormat"),
		CreationDate:   now,
		LastUpdateDate: now,
	}
	if v, ok := nr.Params["IncludeFilters"].([]any); ok {
		for _, f := range v {
			if m, ok := f.(map[string]any); ok {
				ms.IncludeFilters = append(ms.IncludeFilters, m)
			}
		}
	}
	if v, ok := nr.Params["ExcludeFilters"].([]any); ok {
		for _, f := range v {
			if m, ok := f.(map[string]any); ok {
				ms.ExcludeFilters = append(ms.ExcludeFilters, m)
			}
		}
	}
	data, _ := json.Marshal(ms)
	entry := store.ResourceEntry{Type: "cloudwatch_metric_stream", ID: name, Data: data}
	_ = p.resources.Upsert(ctx, nr.AccountID, nr.Region, entry)
	return provider.OK(map[string]any{"Arn": ms.ARN}), nil
}

func (p *Provider) GetMetricStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	e, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_metric_stream", name)
	if err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFound", Message: "Metric stream not found: " + name, HTTPStatus: 400}
	}
	var ms metricStream
	json.Unmarshal(e.Data, &ms)
	return provider.OK(map[string]any{
		"Name":           ms.Name,
		"Arn":            ms.ARN,
		"State":          ms.State,
		"FirehoseArn":    ms.FirehoseARN,
		"RoleArn":        ms.RoleARN,
		"OutputFormat":   ms.OutputFormat,
		"IncludeFilters": ms.IncludeFilters,
		"ExcludeFilters": ms.ExcludeFilters,
		"CreationDate":   ms.CreationDate.Unix(),
		"LastUpdateDate": ms.LastUpdateDate.Unix(),
	}), nil
}

func (p *Provider) DeleteMetricStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["Name"].(string)
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "cloudwatch_metric_stream", name); err != nil {
		return nil, &model.ProviderError{Code: "ResourceNotFound", Message: "Metric stream not found: " + name, HTTPStatus: 400}
	}
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, "cloudwatch_metric_stream", name)
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ListMetricStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "cloudwatch_metric_stream", "")
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var ms metricStream
		json.Unmarshal(e.Data, &ms)
		out = append(out, map[string]any{
			"Name":           ms.Name,
			"Arn":            ms.ARN,
			"State":          ms.State,
			"FirehoseArn":    ms.FirehoseARN,
			"OutputFormat":   ms.OutputFormat,
			"CreationDate":   ms.CreationDate.Unix(),
			"LastUpdateDate": ms.LastUpdateDate.Unix(),
		})
	}
	return provider.OK(map[string]any{"Entries": out}), nil
}

func (p *Provider) setMetricStreamState(ctx context.Context, account, region string, names []string, state string) error {
	for _, name := range names {
		e, err := p.resources.Get(ctx, account, region, "cloudwatch_metric_stream", name)
		if err != nil {
			return &model.ProviderError{Code: "ResourceNotFound", Message: "Metric stream not found: " + name, HTTPStatus: 400}
		}
		var ms metricStream
		json.Unmarshal(e.Data, &ms)
		ms.State = state
		ms.LastUpdateDate = clock.Now()
		data, _ := json.Marshal(ms)
		_ = p.resources.Update(ctx, account, region, store.ResourceEntry{Type: "cloudwatch_metric_stream", ID: name, Data: data})
	}
	return nil
}

func (p *Provider) StartMetricStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	names := strSlice(nr.Params, "Names")
	if err := p.setMetricStreamState(ctx, nr.AccountID, nr.Region, names, "running"); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) StopMetricStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	names := strSlice(nr.Params, "Names")
	if err := p.setMetricStreamState(ctx, nr.AccountID, nr.Region, names, "stopped"); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// strSlice extracts a []string from params[key] (handles []any and []string).
func strSlice(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	switch val := v.(type) {
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return val
	}
	return nil
}

// strParam2 is a string extractor for cloudwatch (avoids redeclaration conflicts with other files).
func strParam2(params map[string]any, key string) string {
	s, _ := params[key].(string)
	return s
}
