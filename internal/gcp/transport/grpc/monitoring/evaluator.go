package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/encoding/protojson"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"

	"github.com/google/uuid"
)

// Publisher publishes a notification payload to a Pub/Sub topic identified by
// its full resource name (projects/{p}/topics/{t}). It is the evaluator's only
// side-effecting dependency and is injected so the engine is unit-testable;
// main.go wires an adapter over the emulator's Pub/Sub message store.
type Publisher interface {
	Publish(ctx context.Context, topic string, data []byte) error
}

// Evaluator is the background alert-policy evaluator. Every tick it lists the
// alert policies of every known project, evaluates each enabled policy's
// condition_threshold and condition_absent conditions, combines them with the
// policy's Combiner, and reconciles incidents: a transition to firing opens an
// incident and delivers notifications; a transition to not-firing closes the
// open incident and delivers a resolution notification.
type Evaluator struct {
	store     monitoringstore.Store
	publisher Publisher
	projects  func() []string
	tick      time.Duration
	log       *slog.Logger
}

// Option configures an Evaluator.
type Option func(*Evaluator)

// WithTick overrides the evaluation interval (default 30s, matching the AWS
// CloudWatch evaluator).
func WithTick(d time.Duration) Option {
	return func(e *Evaluator) {
		if d > 0 {
			e.tick = d
		}
	}
}

// WithProjects injects the project list the evaluator scans. When unset the
// evaluator derives projects from the store (ListProjects).
func WithProjects(f func() []string) Option {
	return func(e *Evaluator) { e.projects = f }
}

// WithLogger overrides the logger (default slog.Default).
func WithLogger(l *slog.Logger) Option {
	return func(e *Evaluator) {
		if l != nil {
			e.log = l
		}
	}
}

// NewEvaluator returns an Evaluator over the monitoring store. pub may be nil
// (pubsub delivery is then skipped and recorded as such).
func NewEvaluator(store monitoringstore.Store, pub Publisher, opts ...Option) *Evaluator {
	e := &Evaluator{
		store:     store,
		publisher: pub,
		tick:      30 * time.Second,
		log:       slog.Default(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// errIncidentNotOpen aborts an atomic close when the incident is already
// closed (a concurrent evaluator won the race); callers treat it as "no
// transition", not an error.
var errIncidentNotOpen = errors.New("incident not open")

// Run blocks until ctx is cancelled, evaluating every tick.
func (e *Evaluator) Run(ctx context.Context) {
	t := time.NewTicker(e.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.EvaluateAll(ctx)
		}
	}
}

// EvaluateAll evaluates every enabled alert policy in every known project once.
// It is the synchronous entry point used by tests and by Run's ticker.
func (e *Evaluator) EvaluateAll(ctx context.Context) {
	projects, err := e.listProjects(ctx)
	if err != nil {
		e.log.Warn("monitoring evaluator: list projects failed", "err", err)
		return
	}
	for _, project := range projects {
		policies, err := e.store.ListAlertPolicies(ctx, project)
		if err != nil {
			e.log.Warn("monitoring evaluator: list policies failed", "project", project, "err", err)
			continue
		}
		for _, p := range policies {
			if p.Enabled != nil && !*p.Enabled {
				continue
			}
			e.evaluatePolicy(ctx, project, p)
		}
	}
}

func (e *Evaluator) listProjects(ctx context.Context) ([]string, error) {
	if e.projects != nil {
		return e.projects(), nil
	}
	return e.store.ListProjects(ctx)
}

func (e *Evaluator) evaluatePolicy(ctx context.Context, project string, p monitoringstore.AlertPolicy) {
	firing, conditionName, reason := e.evaluateConditions(ctx, project, p)
	e.reconcile(ctx, project, p, firing, conditionName, reason)
}

// evaluateConditions combines the policy's conditions. AND and
// AND_WITH_MATCHING_RESOURCE require every condition to fire; OR requires at
// least one. An empty condition list never fires.
func (e *Evaluator) evaluateConditions(ctx context.Context, project string, p monitoringstore.AlertPolicy) (bool, string, string) {
	if len(p.Conditions) == 0 {
		return false, "", "no conditions"
	}
	useOr := monitoringpb.AlertPolicy_ConditionCombinerType(p.Combiner) == monitoringpb.AlertPolicy_OR

	firingName := ""
	firingReason := ""
	fired := false
	allFire := true
	for _, raw := range p.Conditions {
		cond := &monitoringpb.AlertPolicy_Condition{}
		if err := protojson.Unmarshal(raw, cond); err != nil {
			e.log.Warn("monitoring evaluator: unparseable condition", "project", project, "policy", p.ID, "err", err)
			allFire = false
			continue
		}
		firing, reason := e.evaluateCondition(ctx, project, p, cond)
		if firing {
			if !fired {
				firingName = cond.GetDisplayName()
				firingReason = reason
				fired = true
			}
			continue
		}
		allFire = false
		if !useOr {
			return false, "", "condition not firing: " + cond.GetDisplayName()
		}
	}
	if useOr {
		if fired {
			return true, firingName, firingReason
		}
		return false, "", "no condition firing"
	}
	if allFire {
		return true, firingName, firingReason
	}
	return false, "", "not all conditions firing"
}

// evaluateCondition evaluates a single condition. condition_threshold and
// condition_absent are evaluated; every other condition type is logged and
// treated as not firing.
func (e *Evaluator) evaluateCondition(ctx context.Context, project string, p monitoringstore.AlertPolicy, cond *monitoringpb.AlertPolicy_Condition) (bool, string) {
	if mt := cond.GetConditionThreshold(); mt != nil {
		return e.evaluateThreshold(ctx, project, p, mt)
	}
	if ma := cond.GetConditionAbsent(); ma != nil {
		return e.evaluateAbsent(ctx, project, p, ma)
	}
	e.log.Warn("monitoring evaluator: condition type not evaluated",
		"project", project, "policy", p.ID, "condition", cond.GetDisplayName())
	return false, "unsupported condition type"
}

// matchingSeries lists the project's time series that satisfy the shared
// equality filter subset. Series with no points are treated as not yet seen
// (real Cloud Monitoring rejects a point-less write, so they carry no
// measurement for either condition type). A store read error is returned so
// callers can distinguish it from "no matching series".
func (e *Evaluator) matchingSeries(ctx context.Context, project, filter string) ([]monitoringstore.TimeSeries, error) {
	f := compileMetricFilter(filter)
	all, err := e.store.ListTimeSeries(ctx, project)
	if err != nil {
		return nil, err
	}
	matching := make([]monitoringstore.TimeSeries, 0, len(all))
	for _, ts := range all {
		if len(ts.Points) == 0 {
			continue
		}
		if f.match(ts) {
			matching = append(matching, ts)
		}
	}
	return matching, nil
}

// evaluateThreshold implements the emulator's condition_threshold semantics.
//
// Filter subset: equality clauses joined by " AND " on metric.type,
// resource.type, metric.label.{key}, and resource.label.{key}. Unknown clauses
// are ignored.
//
// The matching series' latest point in each alignment window is taken as the
// per-series value; a cross-series reducer then collapses them (default: mean
// across series when no reducer is set). trigger count/percent applies only
// when no reducer is set (per-series violation counting); with a reducer the
// single reduced value is compared directly. duration widens the evaluation to
// consecutive alignment windows that must all satisfy the comparison.
func (e *Evaluator) evaluateThreshold(ctx context.Context, project string, p monitoringstore.AlertPolicy, mt *monitoringpb.AlertPolicy_Condition_MetricThreshold) (bool, string) {
	matching, err := e.matchingSeries(ctx, project, mt.GetFilter())
	if err != nil {
		e.log.Warn("monitoring evaluator: list time series failed", "project", project, "policy", p.ID, "err", err)
		return false, "time series read failed"
	}
	if len(matching) == 0 {
		return false, "no matching time series"
	}

	alignment := 60 * time.Second
	reducer := monitoringpb.Aggregation_REDUCE_NONE
	if aggs := mt.GetAggregations(); len(aggs) > 0 {
		if ap := aggs[0].GetAlignmentPeriod(); ap != nil && ap.AsDuration() > 0 {
			alignment = ap.AsDuration()
		}
		reducer = aggs[0].GetCrossSeriesReducer()
	}
	var duration time.Duration
	if d := mt.GetDuration(); d != nil {
		duration = d.AsDuration()
	}
	triggerCount := int32(0)
	triggerPercent := float64(0)
	if t := mt.GetTrigger(); t != nil {
		triggerCount = t.GetCount()
		triggerPercent = t.GetPercent()
	}
	cmp := mt.GetComparison()
	threshold := mt.GetThresholdValue()

	now := clock.Now()
	if duration <= alignment {
		return e.evalWindow(matching, now.Add(-alignment), now, reducer, cmp, threshold, triggerCount, triggerPercent)
	}

	// The threshold must hold across the whole duration: every consecutive
	// alignment window covering it must fire.
	slots := int((duration + alignment - 1) / alignment)
	for i := 0; i < slots; i++ {
		end := now.Add(-time.Duration(i) * alignment)
		start := end.Add(-alignment)
		firing, reason := e.evalWindow(matching, start, end, reducer, cmp, threshold, triggerCount, triggerPercent)
		if !firing {
			return false, fmt.Sprintf("threshold not held for duration (%s)", reason)
		}
	}
	return true, fmt.Sprintf("threshold held for %s", duration)
}

// evaluateAbsent implements the emulator's condition_absent semantics.
//
// Filter subset: the same equality clauses as condition_threshold (metric.type,
// resource.type, metric.label.{key}, resource.label.{key}); unknown clauses are
// ignored.
//
// A matching time series is "absent" when it has no data point in the
// (now-duration, now] window. Real Cloud Monitoring requires at least one
// successful measurement before a metric-absence condition can be met, so a
// filter that matches no series never fires. `trigger` selects how many absent
// series are required (count or percent); when unset, any absent series fires
// (the real "Any time series violates" default).
//
// The aggregations' perSeriesAligner / crossSeriesReducer / groupByFields are
// not applied to absence — the duration window and per-series trigger govern
// (documented limitation).
func (e *Evaluator) evaluateAbsent(ctx context.Context, project string, p monitoringstore.AlertPolicy, ma *monitoringpb.AlertPolicy_Condition_MetricAbsence) (bool, string) {
	matching, err := e.matchingSeries(ctx, project, ma.GetFilter())
	if err != nil {
		e.log.Warn("monitoring evaluator: list time series failed", "project", project, "policy", p.ID, "err", err)
		return false, "time series read failed"
	}
	if len(matching) == 0 {
		// Never seen: real Cloud Monitoring does not meet a metric-absence
		// condition until the metric has produced at least one measurement.
		return false, "no matching time series"
	}

	var duration time.Duration
	if d := ma.GetDuration(); d != nil {
		duration = d.AsDuration()
	}
	if duration <= 0 {
		e.log.Warn("monitoring evaluator: condition_absent without a positive duration",
			"project", project, "policy", p.ID, "filter", ma.GetFilter())
		return false, "condition_absent without a duration"
	}

	now := clock.Now()
	start := now.Add(-duration)
	absent := 0
	for _, ts := range matching {
		if !seriesHasPointInWindow(ts, start, now) {
			absent++
		}
	}

	if t := ma.GetTrigger(); t != nil {
		if t.GetCount() > 0 {
			return absent >= int(t.GetCount()), fmt.Sprintf("%d/%d series absent (trigger count %d)", absent, len(matching), t.GetCount())
		}
		if t.GetPercent() > 0 {
			pct := float64(absent) / float64(len(matching)) * 100
			return pct >= t.GetPercent(), fmt.Sprintf("%.1f%% series absent (trigger percent %.1f%%)", pct, t.GetPercent())
		}
	}
	return absent > 0, fmt.Sprintf("%d/%d series absent for %s", absent, len(matching), duration)
}

// seriesHasPointInWindow reports whether a series has any data point whose
// end time falls within (start, end]. Unlike seriesLatestInWindow it does not
// interpret the point value, so a string- or distribution-valued point still
// counts as data for metric-absence conditions.
func seriesHasPointInWindow(ts monitoringstore.TimeSeries, start, end time.Time) bool {
	for _, p := range ts.Points {
		if p.EndTime.IsZero() {
			continue
		}
		if p.EndTime.After(start) && !p.EndTime.After(end) {
			return true
		}
	}
	return false
}

// evalWindow evaluates one alignment window. start is exclusive, end
// inclusive. It returns (firing, reason); a window with no data does not fire.
func (e *Evaluator) evalWindow(series []monitoringstore.TimeSeries, start, end time.Time, reducer monitoringpb.Aggregation_Reducer, cmp monitoringpb.ComparisonType, threshold float64, triggerCount int32, triggerPercent float64) (bool, string) {
	vals := make([]float64, 0, len(series))
	for _, ts := range series {
		if v, ok := seriesLatestInWindow(ts, start, end); ok {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return false, "insufficient data"
	}

	if reducer != monitoringpb.Aggregation_REDUCE_NONE {
		v, _ := reduceValues(reducer, vals)
		return compareThreshold(v, cmp, threshold), fmt.Sprintf("reduced value %g %s threshold %g", v, comparisonSymbol(cmp), threshold)
	}

	// No reducer: honor an explicit trigger as per-series violation counting;
	// otherwise default to the mean across series.
	if triggerCount > 0 || triggerPercent > 0 {
		violating := 0
		for _, v := range vals {
			if compareThreshold(v, cmp, threshold) {
				violating++
			}
		}
		switch {
		case triggerCount > 0:
			return violating >= int(triggerCount), fmt.Sprintf("%d/%d series violating (trigger count %d)", violating, len(vals), triggerCount)
		case triggerPercent > 0:
			pct := float64(violating) / float64(len(vals)) * 100
			return pct >= triggerPercent, fmt.Sprintf("%.1f%% series violating (trigger percent %.1f%%)", pct, triggerPercent)
		}
	}

	v, _ := reduceValues(monitoringpb.Aggregation_REDUCE_MEAN, vals)
	return compareThreshold(v, cmp, threshold), fmt.Sprintf("mean %g %s threshold %g", v, comparisonSymbol(cmp), threshold)
}

// seriesLatestInWindow returns the latest point value within (start, end].
func seriesLatestInWindow(ts monitoringstore.TimeSeries, start, end time.Time) (float64, bool) {
	var best *monitoringstore.Point
	for i := range ts.Points {
		p := &ts.Points[i]
		if p.EndTime.IsZero() {
			continue
		}
		if p.EndTime.After(start) && !p.EndTime.After(end) {
			if best == nil || p.EndTime.After(best.EndTime) {
				best = p
			}
		}
	}
	if best == nil {
		return 0, false
	}
	return pointValue(best.Value)
}

// pointValue extracts the numeric value from a stored TypedValue, mirroring how
// the store reads int64/double/bool.
func pointValue(v monitoringstore.TypedValue) (float64, bool) {
	switch {
	case v.DoubleValue != nil:
		return *v.DoubleValue, true
	case v.Int64Value != nil:
		return float64(*v.Int64Value), true
	case v.BoolValue != nil:
		if *v.BoolValue {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// reduceValues applies a cross-series reducer. A zero-length input has no
// value. REDUCE_NONE and unknown reducers fall back to the mean.
func reduceValues(reducer monitoringpb.Aggregation_Reducer, vals []float64) (float64, bool) {
	if len(vals) == 0 {
		return 0, false
	}
	switch reducer {
	case monitoringpb.Aggregation_REDUCE_SUM:
		var s float64
		for _, v := range vals {
			s += v
		}
		return s, true
	case monitoringpb.Aggregation_REDUCE_MAX:
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m, true
	case monitoringpb.Aggregation_REDUCE_MIN:
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m, true
	case monitoringpb.Aggregation_REDUCE_COUNT:
		return float64(len(vals)), true
	default: // REDUCE_MEAN, REDUCE_NONE, anything else
		var s float64
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals)), true
	}
}

func compareThreshold(v float64, cmp monitoringpb.ComparisonType, threshold float64) bool {
	switch cmp {
	case monitoringpb.ComparisonType_COMPARISON_GT:
		return v > threshold
	case monitoringpb.ComparisonType_COMPARISON_GE:
		return v >= threshold
	case monitoringpb.ComparisonType_COMPARISON_LT:
		return v < threshold
	case monitoringpb.ComparisonType_COMPARISON_LE:
		return v <= threshold
	case monitoringpb.ComparisonType_COMPARISON_EQ:
		return v == threshold
	case monitoringpb.ComparisonType_COMPARISON_NE:
		return v != threshold
	}
	return false
}

func comparisonSymbol(cmp monitoringpb.ComparisonType) string {
	switch cmp {
	case monitoringpb.ComparisonType_COMPARISON_GT:
		return ">"
	case monitoringpb.ComparisonType_COMPARISON_GE:
		return ">="
	case monitoringpb.ComparisonType_COMPARISON_LT:
		return "<"
	case monitoringpb.ComparisonType_COMPARISON_LE:
		return "<="
	case monitoringpb.ComparisonType_COMPARISON_EQ:
		return "=="
	case monitoringpb.ComparisonType_COMPARISON_NE:
		return "!="
	}
	return "?"
}

// ─── filter subset ────────────────────────────────────────────────────────────

// metricFilter is the metric.type / resource.type / metric.label.* /
// resource.label.* equality subset of the Cloud Monitoring filter grammar that
// the evaluator supports. Unknown clauses are ignored.
type metricFilter struct {
	metricType     string
	resourceType   string
	metricLabels   map[string]string
	resourceLabels map[string]string
}

func compileMetricFilter(s string) metricFilter {
	f := metricFilter{}
	for _, clause := range strings.Split(s, " AND ") {
		key, val, ok := core.ParseEquality(strings.TrimSpace(clause))
		if !ok {
			continue
		}
		switch {
		case key == "metric.type":
			f.metricType = val
		case key == "resource.type":
			f.resourceType = val
		case strings.HasPrefix(key, "metric.label."):
			if f.metricLabels == nil {
				f.metricLabels = make(map[string]string)
			}
			f.metricLabels[strings.TrimPrefix(key, "metric.label.")] = val
		case strings.HasPrefix(key, "resource.label."):
			if f.resourceLabels == nil {
				f.resourceLabels = make(map[string]string)
			}
			f.resourceLabels[strings.TrimPrefix(key, "resource.label.")] = val
		}
	}
	return f
}

func (f metricFilter) match(ts monitoringstore.TimeSeries) bool {
	if f.metricType != "" && ts.MetricType != f.metricType {
		return false
	}
	if f.resourceType != "" && ts.ResourceType != f.resourceType {
		return false
	}
	for k, v := range f.metricLabels {
		if ts.MetricLabels[k] != v {
			return false
		}
	}
	for k, v := range f.resourceLabels {
		if ts.ResourceLabels[k] != v {
			return false
		}
	}
	return true
}

// ─── incidents + delivery ─────────────────────────────────────────────────────

func (e *Evaluator) reconcile(ctx context.Context, project string, p monitoringstore.AlertPolicy, firing bool, conditionName, reason string) {
	if firing {
		e.openIncident(ctx, project, p, conditionName, reason)
		return
	}
	e.closeIncident(ctx, project, p, reason)
}

func (e *Evaluator) openIncident(ctx context.Context, project string, p monitoringstore.AlertPolicy, conditionName, reason string) {
	if _, err := e.store.FindOpenIncident(ctx, project, p.ID); err == nil {
		return // already open
	} else if !errors.Is(err, monitoringstore.ErrIncidentNotFound) {
		e.log.Warn("monitoring evaluator: find open incident failed", "project", project, "policy", p.ID, "err", err)
		return
	}

	now := clock.Now()
	inc := monitoringstore.Incident{
		ID:            uuid.NewString(),
		ProjectID:     project,
		PolicyID:      p.ID,
		ConditionName: conditionName,
		State:         monitoringstore.IncidentOpen,
		StartedAt:     now,
		Reason:        reason,
	}
	if err := e.store.CreateIncident(ctx, inc); err != nil {
		if !errors.Is(err, monitoringstore.ErrIncidentExists) {
			e.log.Warn("monitoring evaluator: create incident failed", "project", project, "policy", p.ID, "err", err)
		}
		return
	}
	e.log.Info("monitoring: incident opened",
		"project", project, "policy", p.ID, "policyName", p.DisplayName, "incident", inc.ID, "reason", reason)

	notifs := e.deliver(ctx, project, p, inc, "OPEN", reason)
	if len(notifs) > 0 {
		_, _ = e.store.UpdateIncidentAtomic(ctx, project, inc.ID, func(cur monitoringstore.Incident) (monitoringstore.Incident, error) {
			cur.Notifications = append(cur.Notifications, notifs...)
			return cur, nil
		})
	}
}

func (e *Evaluator) closeIncident(ctx context.Context, project string, p monitoringstore.AlertPolicy, reason string) {
	open, err := e.store.FindOpenIncident(ctx, project, p.ID)
	if err != nil {
		return
	}
	now := clock.Now()
	closed, err := e.store.UpdateIncidentAtomic(ctx, project, open.ID, func(cur monitoringstore.Incident) (monitoringstore.Incident, error) {
		if cur.State != monitoringstore.IncidentOpen {
			return cur, errIncidentNotOpen
		}
		cur.State = monitoringstore.IncidentClosed
		cur.EndedAt = now
		cur.Reason = "resolved: " + reason
		return cur, nil
	})
	if err != nil {
		if !errors.Is(err, errIncidentNotOpen) {
			e.log.Warn("monitoring evaluator: close incident failed", "project", project, "policy", p.ID, "err", err)
		}
		return
	}
	e.log.Info("monitoring: incident closed",
		"project", project, "policy", p.ID, "policyName", p.DisplayName, "incident", closed.ID, "reason", closed.Reason)

	notifs := e.deliver(ctx, project, p, closed, "CLOSED", closed.Reason)
	if len(notifs) > 0 {
		_, _ = e.store.UpdateIncidentAtomic(ctx, project, closed.ID, func(cur monitoringstore.Incident) (monitoringstore.Incident, error) {
			cur.Notifications = append(cur.Notifications, notifs...)
			return cur, nil
		})
	}
}

// deliver resolves each notification channel on the policy and attempts a
// delivery. Only "pubsub" channels are actually published (to the channel's
// labels.topic via the injected Publisher); email/webhook/sms are recorded on
// the incident and slog'd but not sent, and every outcome is recorded.
func (e *Evaluator) deliver(ctx context.Context, project string, p monitoringstore.AlertPolicy, inc monitoringstore.Incident, state, reason string) []monitoringstore.IncidentNotification {
	if len(p.NotificationChannels) == 0 {
		return nil
	}
	now := clock.Now()
	out := make([]monitoringstore.IncidentNotification, 0, len(p.NotificationChannels))
	for _, name := range p.NotificationChannels {
		cp, id, ok := core.SplitNotificationChannelName(name)
		if !ok {
			out = append(out, monitoringstore.IncidentNotification{ChannelName: name, Status: "skipped", Detail: "invalid channel name", DeliveredAt: now})
			continue
		}
		ch, err := e.store.GetNotificationChannel(ctx, cp, id)
		if err != nil {
			e.log.Warn("monitoring evaluator: notification channel not found", "project", cp, "channel", id, "err", err)
			out = append(out, monitoringstore.IncidentNotification{ChannelName: name, Status: "skipped", Detail: "channel not found", DeliveredAt: now})
			continue
		}
		if ch.Enabled != nil && !*ch.Enabled {
			out = append(out, monitoringstore.IncidentNotification{ChannelName: name, ChannelType: ch.Type, Status: "skipped", Detail: "channel disabled", DeliveredAt: now})
			continue
		}

		n := monitoringstore.IncidentNotification{ChannelName: name, ChannelType: ch.Type, DeliveredAt: now}
		switch ch.Type {
		case "pubsub":
			topic := ch.Labels["topic"]
			if topic == "" {
				n.Status, n.Detail = "skipped", "pubsub channel has no labels.topic"
				break
			}
			if e.publisher == nil {
				n.Status, n.Detail = "skipped", "no pubsub publisher wired"
				break
			}
			payload := notificationPayload(project, p, inc, state, reason, now)
			if err := e.publisher.Publish(ctx, topic, payload); err != nil {
				e.log.Warn("monitoring evaluator: pubsub notification failed", "project", project, "topic", topic, "err", err)
				n.Status, n.Detail = "failed", err.Error()
				break
			}
			n.Status = "delivered"
			e.log.Info("monitoring: notification delivered",
				"project", project, "policy", p.ID, "incident", inc.ID, "topic", topic, "state", state)
		case "email", "webhook", "sms":
			n.Status, n.Detail = "recorded", "recorded but not sent by the emulator"
			e.log.Info("monitoring: notification recorded (not sent by emulator)",
				"project", project, "policy", p.ID, "incident", inc.ID, "type", ch.Type, "state", state)
		default:
			n.Status, n.Detail = "skipped", "unsupported channel type "+ch.Type
			e.log.Warn("monitoring evaluator: unsupported notification channel type", "type", ch.Type)
		}
		out = append(out, n)
	}
	return out
}

// notificationPayload renders a CloudEvents-style JSON envelope. It mirrors the
// AWS CloudWatch->SNS alarm payload as the observable emulator effect.
func notificationPayload(project string, p monitoringstore.AlertPolicy, inc monitoringstore.Incident, state, reason string, now time.Time) []byte {
	envelope := map[string]any{
		"specversion":     "1.0",
		"id":              inc.ID,
		"source":          fmt.Sprintf("//monitoring.googleapis.com/projects/%s/alertPolicies/%s", project, p.ID),
		"type":            "google.cloud.monitoring.alert.v1.Incident",
		"time":            now.Format(time.RFC3339),
		"datacontenttype": "application/json",
		"data": map[string]any{
			"state":  state,
			"reason": reason,
			"incident": map[string]any{
				"incidentId":        inc.ID,
				"policyId":          p.ID,
				"policyName":        core.AlertPolicyName(project, p.ID),
				"policyDisplayName": p.DisplayName,
				"conditionName":     inc.ConditionName,
				"state":             string(inc.State),
				"startedAt":         inc.StartedAt.Format(time.RFC3339),
			},
		},
	}
	b, err := json.Marshal(envelope)
	if err != nil {
		return []byte("{}")
	}
	return b
}
