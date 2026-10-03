// Package monitoring is the transport-neutral core of the Cloud Monitoring v3
// service (MetricService, AlertPolicyService, NotificationChannelService, and
// ServiceMonitoringService) over the shared monitoring store — the Amazon
// CloudWatch metrics+alarms analogue. It manages the metric descriptor catalog,
// the time-series data plane, the alert-policy (alarm) registry, the
// notification-channel registry, and the Service Monitoring services +
// service-level objectives registry.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/monitoring) and the REST
// transport (internal/gcp/transport/rest/monitoring) both transcode their wire
// format into this package's typed API and then call the SAME Service instance.
// That is the dual-protocol invariant: one core, one piece of state, so the
// transports cannot drift.
//
// Monitored resource descriptors are served from the canonical catalog shared
// with Cloud Logging (internal/gcp/rescatalog); a type outside it is NotFound.
// Notification channel descriptors come from a small static Monitoring-only
// catalog. The descriptor list methods honor a discovery subset of the
// Monitoring filter grammar (equality and starts_with clauses joined by AND);
// any other key, operator, or malformed clause is rejected rather than silently
// matching everything.
//
// Documented limitations: only condition_threshold and condition_absent alert
// conditions are evaluated by the background worker; ListTimeSeries supports the
// metric.type/resource.type and metric.labels.<k>/resource.labels.<k> filter
// subset (equality and starts_with) plus per-series alignment and cross-series
// reduction; SendNotificationChannelVerificationCode performs no delivery;
// ServiceLevelObjectives are stored and returned as declared (no burn-rate
// evaluation) and the ListServices/ListServiceLevelObjectives filters support an
// equality subset; the SLO "view=EXPLICIT" fold is not applied.
package monitoring

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"

	"github.com/google/uuid"
)

// Service is the transport-neutral Cloud Monitoring service over the shared
// store. All methods take an already-resolved project (the transports resolve
// it from the resource name, request project, or routing metadata).
type Service struct {
	store       monitoringstore.Store
	defaultProj string
}

// NewService returns a Monitoring core backed by the shared store. defaultProj
// is the config-default project.
func NewService(store monitoringstore.Store, defaultProj string) *Service {
	return &Service{store: store, defaultProj: defaultProj}
}

// ─── MetricService: metric descriptors ────────────────────────────────────────

func (s *Service) ListMetricDescriptors(ctx context.Context, project, filter string, pageSize int, pageToken string) ([]monitoringstore.MetricDescriptor, string, error) {
	f, err := compileDescriptorFilter(filter, metricDescriptorFilterKeys)
	if err != nil {
		return nil, "", invalidArgument("invalid filter: " + err.Error())
	}
	descriptors, err := s.store.ListMetricDescriptors(ctx, project)
	if err != nil {
		return nil, "", mapStoreError(err)
	}
	matching := make([]monitoringstore.MetricDescriptor, 0, len(descriptors))
	for _, d := range descriptors {
		if f.match(d.Type, MetricDescriptorName(project, d.Type)) {
			matching = append(matching, d)
		}
	}
	page, next := pageSlice(matching, pageSize, pageToken)
	return page, next, nil
}

func (s *Service) GetMetricDescriptor(ctx context.Context, project, typ string) (monitoringstore.MetricDescriptor, error) {
	d, err := s.store.GetMetricDescriptor(ctx, project, typ)
	if err != nil {
		return monitoringstore.MetricDescriptor{}, mapStoreError(err)
	}
	return d, nil
}

func (s *Service) CreateMetricDescriptor(ctx context.Context, project string, d monitoringstore.MetricDescriptor) (monitoringstore.MetricDescriptor, error) {
	if d.Type == "" {
		return monitoringstore.MetricDescriptor{}, invalidArgument("metric descriptor type is required")
	}
	stored, err := s.store.CreateMetricDescriptor(ctx, project, d)
	if err != nil {
		return monitoringstore.MetricDescriptor{}, mapStoreError(err)
	}
	return stored, nil
}

func (s *Service) DeleteMetricDescriptor(ctx context.Context, project, typ string) error {
	return mapStoreError(s.store.DeleteMetricDescriptor(ctx, project, typ))
}

// ─── MetricService: time series ───────────────────────────────────────────────

func (s *Service) ListTimeSeries(ctx context.Context, project, filter string, interval *TimeInterval, aggregation *Aggregation, secondaryAggregation *Aggregation, orderBy string, headersOnly bool, pageSize int, pageToken string) ([]monitoringstore.TimeSeries, string, error) {
	// order_by is documented as unsupported by ListTimeSeries (points are
	// returned most-recent-first); real Cloud Monitoring requires it blank, so a
	// non-blank value is rejected rather than silently ignored (J43).
	if strings.TrimSpace(orderBy) != "" {
		return nil, "", invalidArgument("order_by is not supported and must be left blank")
	}
	if secondaryAggregation != nil && aggregation == nil {
		return nil, "", invalidArgument("secondary_aggregation requires aggregation to be specified")
	}
	f, err := compileTSFilter(filter)
	if err != nil {
		return nil, "", invalidArgument("invalid filter: " + err.Error())
	}
	series, err := s.store.ListTimeSeries(ctx, project)
	if err != nil {
		return nil, "", mapStoreError(err)
	}
	matching := make([]monitoringstore.TimeSeries, 0, len(series))
	for _, ts := range series {
		if !f.match(ts) {
			continue
		}
		ts.Points = filterPoints(ts.Points, interval)
		if !headersOnly && len(ts.Points) == 0 {
			continue
		}
		matching = append(matching, ts)
	}
	if aggregation != nil {
		matching, err = applyAggregation(matching, aggregation)
		if err != nil {
			return nil, "", err
		}
	}
	if secondaryAggregation != nil {
		matching, err = applyAggregation(matching, secondaryAggregation)
		if err != nil {
			return nil, "", err
		}
	}
	if headersOnly {
		for i := range matching {
			matching[i].Points = nil
		}
	}
	page, next := pageSlice(matching, pageSize, pageToken)
	return page, next, nil
}

func (s *Service) CreateTimeSeries(ctx context.Context, project string, series []monitoringstore.TimeSeries) error {
	return s.writeTimeSeries(ctx, project, series, true)
}

// CreateServiceTimeSeries is the service-scoped counterpart to CreateTimeSeries.
// In real Cloud Monitoring the two differ only in the identity/permission used
// to authorize the write; the emulator has no authz plane, so this mirrors the
// CreateTimeSeries write path. Service time series target a metric descriptor
// the service already published, so this path does not auto-create one.
func (s *Service) CreateServiceTimeSeries(ctx context.Context, project string, series []monitoringstore.TimeSeries) error {
	return s.writeTimeSeries(ctx, project, series, false)
}

// writeTimeSeries stores each series, optionally auto-creating a metric
// descriptor for a previously unseen metric type. Real Cloud Monitoring creates
// a custom metric descriptor on first write, so a later get/list/delete of that
// type behaves as if it had been created explicitly.
func (s *Service) writeTimeSeries(ctx context.Context, project string, series []monitoringstore.TimeSeries, autoCreateDescriptors bool) error {
	for _, ts := range series {
		if ts.MetricType == "" {
			return invalidArgument("time series metric type is required")
		}
		if autoCreateDescriptors {
			if err := s.ensureMetricDescriptor(ctx, project, ts); err != nil {
				return err
			}
		}
		if err := s.store.CreateTimeSeries(ctx, project, ts); err != nil {
			return mapStoreError(err)
		}
	}
	return nil
}

// ensureMetricDescriptor creates a descriptor for ts.MetricType when one does
// not already exist. Real Cloud Monitoring derives the schema from the series:
// the metric kind defaults to GAUGE for a custom metric, and the value type is
// taken from the metric kind/value type when set, else from the point value.
// An existing descriptor is left untouched so writes cannot clobber an
// explicitly created schema.
func (s *Service) ensureMetricDescriptor(ctx context.Context, project string, ts monitoringstore.TimeSeries) error {
	_, err := s.store.GetMetricDescriptor(ctx, project, ts.MetricType)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, monitoringstore.ErrMetricDescriptorNotFound):
		d := monitoringstore.MetricDescriptor{
			Type:       ts.MetricType,
			MetricKind: inferMetricKind(ts),
			ValueType:  inferValueType(ts),
			Unit:       ts.Unit,
		}
		if ts.ResourceType != "" {
			d.MonitoredResourceTypes = []string{ts.ResourceType}
		}
		_, cerr := s.store.CreateMetricDescriptor(ctx, project, d)
		return mapStoreError(cerr)
	default:
		return mapStoreError(err)
	}
}

// inferMetricKind returns the series' metric kind, defaulting to GAUGE for a
// custom metric that does not specify one.
func inferMetricKind(ts monitoringstore.TimeSeries) int32 {
	if ts.MetricKind != 0 {
		return ts.MetricKind
	}
	return metricKindGauge
}

// inferValueType returns the series' value type, inferring it from the first
// typed point when unset (DOUBLE when the series carries no typed value).
func inferValueType(ts monitoringstore.TimeSeries) int32 {
	if ts.ValueType != 0 {
		return ts.ValueType
	}
	for _, p := range ts.Points {
		switch {
		case p.Value.BoolValue != nil:
			return valueTypeBool
		case p.Value.Int64Value != nil:
			return valueTypeInt64
		case p.Value.DoubleValue != nil:
			return valueTypeDouble
		case p.Value.StringValue != nil:
			return valueTypeString
		case p.Value.DistributionValue != nil:
			return valueTypeDistribution
		}
	}
	return valueTypeDouble
}

// ─── MetricService: monitored resource descriptors ────────────────────────────

func (s *Service) ListMonitoredResourceDescriptors(ctx context.Context, project, filter string, pageSize int, pageToken string) ([]MonitoredResourceDescriptor, string, error) {
	f, err := compileDescriptorFilter(filter, monitoredResourceFilterKeys)
	if err != nil {
		return nil, "", invalidArgument("invalid filter: " + err.Error())
	}
	all := MonitoredResourceDescriptors(project)
	matching := make([]MonitoredResourceDescriptor, 0, len(all))
	for _, d := range all {
		if f.match(d.Type, d.Name) {
			matching = append(matching, d)
		}
	}
	page, next := pageSlice(matching, pageSize, pageToken)
	return page, next, nil
}

func (s *Service) GetMonitoredResourceDescriptor(ctx context.Context, project, typ string) (MonitoredResourceDescriptor, error) {
	d, ok := LookupMonitoredResourceDescriptor(project, typ)
	if !ok {
		return MonitoredResourceDescriptor{}, notFound("monitored resource descriptor not found: " + typ)
	}
	return d, nil
}

// ─── AlertPolicyService ───────────────────────────────────────────────────────

func (s *Service) ListAlertPolicies(ctx context.Context, project string, pageSize int, pageToken string) ([]monitoringstore.AlertPolicy, int, string, error) {
	policies, err := s.store.ListAlertPolicies(ctx, project)
	if err != nil {
		return nil, 0, "", mapStoreError(err)
	}
	page, next := pageSlice(policies, pageSize, pageToken)
	return page, len(policies), next, nil
}

func (s *Service) GetAlertPolicy(ctx context.Context, project, id string) (monitoringstore.AlertPolicy, error) {
	p, err := s.store.GetAlertPolicy(ctx, project, id)
	if err != nil {
		return monitoringstore.AlertPolicy{}, mapStoreError(err)
	}
	return p, nil
}

func (s *Service) CreateAlertPolicy(ctx context.Context, project string, p monitoringstore.AlertPolicy) (monitoringstore.AlertPolicy, error) {
	if p.DisplayName == "" {
		return monitoringstore.AlertPolicy{}, invalidArgument("alert policy display_name is required")
	}
	if p.Enabled == nil {
		enabled := true
		p.Enabled = &enabled
	}
	p.ID = uuid.NewString()
	if err := s.store.CreateAlertPolicy(ctx, project, p); err != nil {
		return monitoringstore.AlertPolicy{}, mapStoreError(err)
	}
	return p, nil
}

// UpdateAlertPolicy merges an incoming policy into the stored one. An
// empty/nil update mask is a full replace; a non-empty mask merges field-by-field
// inside the store's locked section so a concurrent masked update touching
// different fields cannot be lost.
func (s *Service) UpdateAlertPolicy(ctx context.Context, project, id string, incoming monitoringstore.AlertPolicy, updateMask []string) (monitoringstore.AlertPolicy, error) {
	incoming.ID = id
	p, err := s.store.UpdateAlertPolicyAtomic(ctx, project, id, func(stored monitoringstore.AlertPolicy) (monitoringstore.AlertPolicy, error) {
		if len(updateMask) == 0 {
			return incoming, nil
		}
		return applyAlertPolicyMask(stored, incoming, updateMask)
	})
	if err != nil {
		return monitoringstore.AlertPolicy{}, mapStoreError(err)
	}
	return p, nil
}

func (s *Service) DeleteAlertPolicy(ctx context.Context, project, id string) error {
	return mapStoreError(s.store.DeleteAlertPolicy(ctx, project, id))
}

// applyAlertPolicyMask merges an incoming alert policy into the stored policy
// according to the field paths in updateMask. For each masked path the incoming
// value wins; every other field retains the stored value.
//
// Mask paths are normalized so both the proto (snake_case) and the REST
// FieldMask JSON (camelCase) spellings are accepted; a nested path is collapsed
// to its root field (e.g. documentation.content updates the whole documentation
// message, matching the emulator's opaque-JSON storage).
func applyAlertPolicyMask(stored, incoming monitoringstore.AlertPolicy, updateMask []string) (monitoringstore.AlertPolicy, error) {
	for _, raw := range updateMask {
		switch normalizeMaskPath(raw) {
		case "display_name":
			stored.DisplayName = incoming.DisplayName
		case "documentation":
			stored.Documentation = incoming.Documentation
		case "conditions":
			stored.Conditions = incoming.Conditions
		case "combiner":
			stored.Combiner = incoming.Combiner
		case "enabled":
			stored.Enabled = incoming.Enabled
		case "notification_channels":
			stored.NotificationChannels = incoming.NotificationChannels
		case "user_labels":
			stored.UserLabels = incoming.UserLabels
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+raw, 501)
		}
	}
	return stored, nil
}

// normalizeMaskPath converts a FieldMask path to its canonical snake_case root
// field. The gRPC FieldMask carries proto field names (snake_case); the REST
// FieldMask JSON carries lowerCamelCase names. Collapsing a nested path to its
// root lets both forms address the same field.
func normalizeMaskPath(path string) string {
	root := path
	if i := strings.IndexByte(root, '.'); i >= 0 {
		root = root[:i]
	}
	var b strings.Builder
	for i, r := range root {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r + ('a' - 'A'))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ─── NotificationChannelService ───────────────────────────────────────────────

func (s *Service) ListNotificationChannels(ctx context.Context, project string, pageSize int, pageToken string) ([]monitoringstore.NotificationChannel, int, string, error) {
	channels, err := s.store.ListNotificationChannels(ctx, project)
	if err != nil {
		return nil, 0, "", mapStoreError(err)
	}
	page, next := pageSlice(channels, pageSize, pageToken)
	return page, len(channels), next, nil
}

func (s *Service) GetNotificationChannel(ctx context.Context, project, id string) (monitoringstore.NotificationChannel, error) {
	c, err := s.store.GetNotificationChannel(ctx, project, id)
	if err != nil {
		return monitoringstore.NotificationChannel{}, mapStoreError(err)
	}
	return c, nil
}

func (s *Service) CreateNotificationChannel(ctx context.Context, project string, c monitoringstore.NotificationChannel) (monitoringstore.NotificationChannel, error) {
	if c.Type == "" {
		return monitoringstore.NotificationChannel{}, invalidArgument("notification channel type is required")
	}
	if c.Enabled == nil {
		enabled := true
		c.Enabled = &enabled
	}
	now := clock.Now()
	c.ID = uuid.NewString()
	c.CreateTime = now
	c.UpdateTime = now
	c.VerificationStatus = int32(notificationChannelVerified)
	if err := s.store.CreateNotificationChannel(ctx, project, c); err != nil {
		return monitoringstore.NotificationChannel{}, mapStoreError(err)
	}
	return c, nil
}

// UpdateNotificationChannel merges an incoming channel into the stored one. An
// empty/nil update mask is a full replacement that preserves the immutable
// create time and verification status unless explicitly masked.
func (s *Service) UpdateNotificationChannel(ctx context.Context, project, id string, incoming monitoringstore.NotificationChannel, updateMask []string) (monitoringstore.NotificationChannel, error) {
	incoming.ID = id
	incoming.UpdateTime = clock.Now()
	c, err := s.store.UpdateNotificationChannelAtomic(ctx, project, id, func(stored monitoringstore.NotificationChannel) (monitoringstore.NotificationChannel, error) {
		if len(updateMask) == 0 {
			incoming.CreateTime = stored.CreateTime
			if incoming.VerificationStatus == 0 {
				incoming.VerificationStatus = stored.VerificationStatus
			}
			return incoming, nil
		}
		return applyNotificationChannelMask(stored, incoming, updateMask)
	})
	if err != nil {
		return monitoringstore.NotificationChannel{}, mapStoreError(err)
	}
	return c, nil
}

func (s *Service) DeleteNotificationChannel(ctx context.Context, project, id string) error {
	return mapStoreError(s.store.DeleteNotificationChannel(ctx, project, id))
}

func (s *Service) ListNotificationChannelDescriptors(ctx context.Context, project string, pageSize int, pageToken string) ([]NotificationChannelDescriptor, string, error) {
	all := NotificationChannelDescriptors(project)
	page, next := pageSlice(all, pageSize, pageToken)
	return page, next, nil
}

func (s *Service) GetNotificationChannelDescriptor(ctx context.Context, project, typ string) (NotificationChannelDescriptor, error) {
	d, ok := LookupNotificationChannelDescriptor(project, typ)
	if !ok {
		return NotificationChannelDescriptor{}, notFound("notification channel descriptor not found: " + typ)
	}
	return d, nil
}

// SendNotificationChannelVerificationCode resolves the target channel and
// returns success. The emulator performs no delivery, but a missing channel is
// still NotFound so the call is not a blind accept.
func (s *Service) SendNotificationChannelVerificationCode(ctx context.Context, project, id string) error {
	if _, err := s.store.GetNotificationChannel(ctx, project, id); err != nil {
		return mapStoreError(err)
	}
	return nil
}

// GetNotificationChannelVerificationCode returns a deterministic synthetic
// verification code for an existing channel.
func (s *Service) GetNotificationChannelVerificationCode(ctx context.Context, project, id string, expire *time.Time) (string, time.Time, error) {
	c, err := s.store.GetNotificationChannel(ctx, project, id)
	if err != nil {
		return "", time.Time{}, mapStoreError(err)
	}
	expireTime := clock.Now().Add(verificationCodeTTL)
	if expire != nil {
		expireTime = *expire
	}
	return verificationCodeFor(c.ID), expireTime, nil
}

// VerifyNotificationChannel marks the channel VERIFIED and returns it.
func (s *Service) VerifyNotificationChannel(ctx context.Context, project, id string) (monitoringstore.NotificationChannel, error) {
	c, err := s.store.UpdateNotificationChannelAtomic(ctx, project, id, func(stored monitoringstore.NotificationChannel) (monitoringstore.NotificationChannel, error) {
		stored.VerificationStatus = int32(notificationChannelVerified)
		stored.UpdateTime = clock.Now()
		return stored, nil
	})
	if err != nil {
		return monitoringstore.NotificationChannel{}, mapStoreError(err)
	}
	return c, nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// applyNotificationChannelMask merges an incoming channel into the stored
// channel according to the field paths in updateMask. Paths are normalized the
// same way as applyAlertPolicyMask (camelCase or snake_case).
func applyNotificationChannelMask(stored, incoming monitoringstore.NotificationChannel, updateMask []string) (monitoringstore.NotificationChannel, error) {
	for _, raw := range updateMask {
		switch normalizeMaskPath(raw) {
		case "type":
			stored.Type = incoming.Type
		case "display_name":
			stored.DisplayName = incoming.DisplayName
		case "description":
			stored.Description = incoming.Description
		case "labels":
			stored.Labels = incoming.Labels
		case "user_labels":
			stored.UserLabels = incoming.UserLabels
		case "enabled":
			stored.Enabled = incoming.Enabled
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+raw, 501)
		}
	}
	stored.UpdateTime = incoming.UpdateTime
	return stored, nil
}

// verificationCodeTTL is the lifetime attached to a synthesized verification
// code when the request carries no expiration.
const verificationCodeTTL = time.Hour

// notificationChannelVerified is the numeric
// google.monitoring.v3.NotificationChannel.VerificationStatus VERIFIED value.
const notificationChannelVerified int32 = 2

// verificationCodeFor derives a stable, non-empty synthetic verification code
// from a channel id. It need not be cryptographically strong: the emulator
// never delivers or compares it.
func verificationCodeFor(id string) string {
	compact := strings.ReplaceAll(id, "-", "")
	if len(compact) > 10 {
		compact = compact[:10]
	}
	return "JC-" + strings.ToUpper(compact)
}

// filterPoints restricts points to those whose end time falls within the
// half-open interval (start, end]. A nil interval keeps every point.
func filterPoints(points []monitoringstore.Point, iv *TimeInterval) []monitoringstore.Point {
	if iv == nil || len(points) == 0 {
		return points
	}
	out := make([]monitoringstore.Point, 0, len(points))
	for _, p := range points {
		if !iv.End.IsZero() && !p.EndTime.IsZero() && p.EndTime.After(iv.End) {
			continue
		}
		if !iv.Start.IsZero() && !p.EndTime.IsZero() && !p.EndTime.After(iv.Start) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// sortedPoints returns a copy of points ordered reverse-chronologically (most
// recent first), the order the wire exposes.
func SortedPoints(points []monitoringstore.Point) []monitoringstore.Point {
	out := make([]monitoringstore.Point, len(points))
	copy(out, points)
	sort.Slice(out, func(i, j int) bool { return out[i].EndTime.After(out[j].EndTime) })
	return out
}
