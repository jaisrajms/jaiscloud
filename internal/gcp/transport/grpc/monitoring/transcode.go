package monitoring

import (
	"encoding/json"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// ─── metric descriptors ───────────────────────────────────────────────────────

func metricDescriptorToProto(d monitoringstore.MetricDescriptor, project string) *metricpb.MetricDescriptor {
	out := &metricpb.MetricDescriptor{
		Name:                   core.MetricDescriptorName(project, d.Type),
		Type:                   d.Type,
		MetricKind:             metricpb.MetricDescriptor_MetricKind(d.MetricKind),
		ValueType:              metricpb.MetricDescriptor_ValueType(d.ValueType),
		Unit:                   d.Unit,
		Description:            d.Description,
		DisplayName:            d.DisplayName,
		MonitoredResourceTypes: d.MonitoredResourceTypes,
	}
	for _, l := range d.Labels {
		out.Labels = append(out.Labels, &labelpb.LabelDescriptor{
			Key:         l.Key,
			ValueType:   labelpb.LabelDescriptor_ValueType(l.ValueType),
			Description: l.Description,
		})
	}
	return out
}

func metricDescriptorFromProto(p *metricpb.MetricDescriptor) monitoringstore.MetricDescriptor {
	if p == nil {
		return monitoringstore.MetricDescriptor{}
	}
	d := monitoringstore.MetricDescriptor{
		Type:                   p.GetType(),
		MetricKind:             int32(p.GetMetricKind()),
		ValueType:              int32(p.GetValueType()),
		Unit:                   p.GetUnit(),
		Description:            p.GetDescription(),
		DisplayName:            p.GetDisplayName(),
		MonitoredResourceTypes: p.GetMonitoredResourceTypes(),
	}
	for _, l := range p.GetLabels() {
		d.Labels = append(d.Labels, monitoringstore.LabelDescriptor{
			Key:         l.GetKey(),
			ValueType:   int32(l.GetValueType()),
			Description: l.GetDescription(),
		})
	}
	return d
}

// ─── time series ──────────────────────────────────────────────────────────────

func timeSeriesToProto(ts monitoringstore.TimeSeries) *monitoringpb.TimeSeries {
	out := &monitoringpb.TimeSeries{
		Metric:     &metricpb.Metric{Type: ts.MetricType, Labels: ts.MetricLabels},
		Resource:   &monitoredrespb.MonitoredResource{Type: ts.ResourceType, Labels: ts.ResourceLabels},
		MetricKind: metricpb.MetricDescriptor_MetricKind(ts.MetricKind),
		ValueType:  metricpb.MetricDescriptor_ValueType(ts.ValueType),
		Unit:       ts.Unit,
	}
	// Points are returned reverse-chronologically (most recent first).
	for _, pt := range core.SortedPoints(ts.Points) {
		out.Points = append(out.Points, pointToProto(pt))
	}
	return out
}

func timeSeriesFromProto(p *monitoringpb.TimeSeries) (monitoringstore.TimeSeries, error) {
	ts := monitoringstore.TimeSeries{}
	if p.GetMetric() != nil {
		ts.MetricType = p.GetMetric().GetType()
		ts.MetricLabels = p.GetMetric().GetLabels()
	}
	if p.GetResource() != nil {
		ts.ResourceType = p.GetResource().GetType()
		ts.ResourceLabels = p.GetResource().GetLabels()
	}
	ts.MetricKind = int32(p.GetMetricKind())
	ts.ValueType = int32(p.GetValueType())
	ts.Unit = p.GetUnit()
	for _, pt := range p.GetPoints() {
		sp, err := pointFromProto(pt)
		if err != nil {
			return ts, err
		}
		ts.Points = append(ts.Points, sp)
	}
	return ts, nil
}

func pointToProto(p monitoringstore.Point) *monitoringpb.Point {
	iv := &monitoringpb.TimeInterval{}
	if !p.EndTime.IsZero() {
		iv.EndTime = timestamppb.New(p.EndTime)
	}
	if !p.StartTime.IsZero() {
		iv.StartTime = timestamppb.New(p.StartTime)
	}
	return &monitoringpb.Point{Interval: iv, Value: typedValueToProto(p.Value)}
}

func pointFromProto(p *monitoringpb.Point) (monitoringstore.Point, error) {
	pt := monitoringstore.Point{}
	if p == nil {
		return pt, nil
	}
	if iv := p.GetInterval(); iv != nil {
		if iv.GetStartTime() != nil {
			pt.StartTime = iv.GetStartTime().AsTime()
		}
		if iv.GetEndTime() != nil {
			pt.EndTime = iv.GetEndTime().AsTime()
		}
	}
	if pt.EndTime.IsZero() {
		pt.EndTime = clock.Now()
	}
	v, err := typedValueFromProto(p.GetValue())
	if err != nil {
		return pt, err
	}
	pt.Value = v
	return pt, nil
}

func typedValueToProto(v monitoringstore.TypedValue) *monitoringpb.TypedValue {
	switch {
	case v.BoolValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: *v.BoolValue}}
	case v.Int64Value != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: *v.Int64Value}}
	case v.DoubleValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: *v.DoubleValue}}
	case v.StringValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_StringValue{StringValue: *v.StringValue}}
	case v.DistributionValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: distributionToProto(*v.DistributionValue)}}
	}
	return &monitoringpb.TypedValue{}
}

func typedValueFromProto(p *monitoringpb.TypedValue) (monitoringstore.TypedValue, error) {
	if p == nil {
		return monitoringstore.TypedValue{}, nil
	}
	switch v := p.GetValue().(type) {
	case *monitoringpb.TypedValue_BoolValue:
		return monitoringstore.TypedValue{BoolValue: &v.BoolValue}, nil
	case *monitoringpb.TypedValue_Int64Value:
		return monitoringstore.TypedValue{Int64Value: &v.Int64Value}, nil
	case *monitoringpb.TypedValue_DoubleValue:
		return monitoringstore.TypedValue{DoubleValue: &v.DoubleValue}, nil
	case *monitoringpb.TypedValue_StringValue:
		return monitoringstore.TypedValue{StringValue: &v.StringValue}, nil
	case *monitoringpb.TypedValue_DistributionValue:
		d := distributionFromProto(v.DistributionValue)
		return monitoringstore.TypedValue{DistributionValue: &d}, nil
	}
	return monitoringstore.TypedValue{}, nil
}

func distributionToProto(d monitoringstore.Distribution) *distributionpb.Distribution {
	out := &distributionpb.Distribution{
		Count:                 d.Count,
		Mean:                  d.Mean,
		SumOfSquaredDeviation: d.SumOfSquaredDeviation,
		BucketCounts:          append([]int64(nil), d.BucketCounts...),
	}
	if d.Range != nil {
		out.Range = &distributionpb.Distribution_Range{Min: d.Range.Min, Max: d.Range.Max}
	}
	if d.BucketOptions != nil {
		bo := &distributionpb.Distribution_BucketOptions{}
		switch {
		case d.BucketOptions.Linear != nil:
			bo.Options = &distributionpb.Distribution_BucketOptions_LinearBuckets{
				LinearBuckets: &distributionpb.Distribution_BucketOptions_Linear{
					NumFiniteBuckets: d.BucketOptions.Linear.NumFiniteBuckets,
					Width:            d.BucketOptions.Linear.Width,
					Offset:           d.BucketOptions.Linear.Offset,
				},
			}
		case d.BucketOptions.Exponential != nil:
			bo.Options = &distributionpb.Distribution_BucketOptions_ExponentialBuckets{
				ExponentialBuckets: &distributionpb.Distribution_BucketOptions_Exponential{
					NumFiniteBuckets: d.BucketOptions.Exponential.NumFiniteBuckets,
					GrowthFactor:     d.BucketOptions.Exponential.GrowthFactor,
					Scale:            d.BucketOptions.Exponential.Scale,
				},
			}
		case d.BucketOptions.Explicit != nil:
			bo.Options = &distributionpb.Distribution_BucketOptions_ExplicitBuckets{
				ExplicitBuckets: &distributionpb.Distribution_BucketOptions_Explicit{
					Bounds: append([]float64(nil), d.BucketOptions.Explicit.Bounds...),
				},
			}
		}
		out.BucketOptions = bo
	}
	for _, ex := range d.Exemplars {
		pe := &distributionpb.Distribution_Exemplar{Value: ex.Value}
		if !ex.Timestamp.IsZero() {
			pe.Timestamp = timestamppb.New(ex.Timestamp)
		}
		for _, raw := range ex.Attachments {
			var a anypb.Any
			if proto.Unmarshal(raw, &a) == nil {
				pe.Attachments = append(pe.Attachments, &a)
			}
		}
		out.Exemplars = append(out.Exemplars, pe)
	}
	return out
}

func distributionFromProto(p *distributionpb.Distribution) monitoringstore.Distribution {
	d := monitoringstore.Distribution{
		Count:                 p.GetCount(),
		Mean:                  p.GetMean(),
		SumOfSquaredDeviation: p.GetSumOfSquaredDeviation(),
		BucketCounts:          append([]int64(nil), p.GetBucketCounts()...),
	}
	if r := p.GetRange(); r != nil {
		d.Range = &monitoringstore.DistributionRange{Min: r.GetMin(), Max: r.GetMax()}
	}
	if bo := p.GetBucketOptions(); bo != nil {
		out := &monitoringstore.BucketOptions{}
		switch o := bo.GetOptions().(type) {
		case *distributionpb.Distribution_BucketOptions_LinearBuckets:
			out.Linear = &monitoringstore.LinearBuckets{
				NumFiniteBuckets: o.LinearBuckets.GetNumFiniteBuckets(),
				Width:            o.LinearBuckets.GetWidth(),
				Offset:           o.LinearBuckets.GetOffset(),
			}
		case *distributionpb.Distribution_BucketOptions_ExponentialBuckets:
			out.Exponential = &monitoringstore.ExponentialBuckets{
				NumFiniteBuckets: o.ExponentialBuckets.GetNumFiniteBuckets(),
				GrowthFactor:     o.ExponentialBuckets.GetGrowthFactor(),
				Scale:            o.ExponentialBuckets.GetScale(),
			}
		case *distributionpb.Distribution_BucketOptions_ExplicitBuckets:
			out.Explicit = &monitoringstore.ExplicitBuckets{
				Bounds: append([]float64(nil), o.ExplicitBuckets.GetBounds()...),
			}
		}
		d.BucketOptions = out
	}
	for _, ex := range p.GetExemplars() {
		me := monitoringstore.Exemplar{Value: ex.GetValue()}
		if ex.GetTimestamp() != nil {
			me.Timestamp = ex.GetTimestamp().AsTime()
		}
		for _, a := range ex.GetAttachments() {
			if b, err := proto.Marshal(a); err == nil {
				me.Attachments = append(me.Attachments, b)
			}
		}
		d.Exemplars = append(d.Exemplars, me)
	}
	return d
}

// ─── alert policies ───────────────────────────────────────────────────────────

func alertPolicyToProto(p monitoringstore.AlertPolicy, project string) *monitoringpb.AlertPolicy {
	out := &monitoringpb.AlertPolicy{
		Name:                 core.AlertPolicyName(project, p.ID),
		DisplayName:          p.DisplayName,
		Combiner:             monitoringpb.AlertPolicy_ConditionCombinerType(p.Combiner),
		NotificationChannels: p.NotificationChannels,
		UserLabels:           p.UserLabels,
	}
	if p.Enabled != nil {
		out.Enabled = wrapperspb.Bool(*p.Enabled)
	}
	if len(p.Documentation) > 0 && string(p.Documentation) != "null" {
		doc := &monitoringpb.AlertPolicy_Documentation{}
		if protojson.Unmarshal(p.Documentation, doc) == nil {
			out.Documentation = doc
		}
	}
	for _, c := range p.Conditions {
		cond := &monitoringpb.AlertPolicy_Condition{}
		if protojson.Unmarshal(c, cond) == nil {
			out.Conditions = append(out.Conditions, cond)
		}
	}
	return out
}

func alertPolicyFromProto(p *monitoringpb.AlertPolicy) (monitoringstore.AlertPolicy, error) {
	ap := monitoringstore.AlertPolicy{
		DisplayName:          p.GetDisplayName(),
		Combiner:             int32(p.GetCombiner()),
		NotificationChannels: p.GetNotificationChannels(),
		UserLabels:           p.GetUserLabels(),
	}
	if p.GetEnabled() != nil {
		v := p.GetEnabled().GetValue()
		ap.Enabled = &v
	}
	if p.GetDocumentation() != nil {
		b, err := protojson.Marshal(p.GetDocumentation())
		if err != nil {
			return ap, err
		}
		ap.Documentation = json.RawMessage(b)
	}
	for _, c := range p.GetConditions() {
		b, err := protojson.Marshal(c)
		if err != nil {
			return ap, err
		}
		ap.Conditions = append(ap.Conditions, json.RawMessage(b))
	}
	return ap, nil
}

// ─── notification channels ────────────────────────────────────────────────────

func notificationChannelToProto(c monitoringstore.NotificationChannel, project string) *monitoringpb.NotificationChannel {
	out := &monitoringpb.NotificationChannel{
		Name:               core.NotificationChannelName(project, c.ID),
		Type:               c.Type,
		DisplayName:        c.DisplayName,
		Description:        c.Description,
		Labels:             c.Labels,
		UserLabels:         c.UserLabels,
		VerificationStatus: monitoringpb.NotificationChannel_VerificationStatus(c.VerificationStatus),
	}
	if c.Enabled != nil {
		out.Enabled = wrapperspb.Bool(*c.Enabled)
	}
	if !c.CreateTime.IsZero() {
		out.CreationRecord = &monitoringpb.MutationRecord{MutateTime: timestamppb.New(c.CreateTime)}
	}
	if !c.UpdateTime.IsZero() {
		out.MutationRecords = []*monitoringpb.MutationRecord{{MutateTime: timestamppb.New(c.UpdateTime)}}
	}
	return out
}

func notificationChannelFromProto(p *monitoringpb.NotificationChannel) monitoringstore.NotificationChannel {
	if p == nil {
		return monitoringstore.NotificationChannel{}
	}
	c := monitoringstore.NotificationChannel{
		Type:               p.GetType(),
		DisplayName:        p.GetDisplayName(),
		Description:        p.GetDescription(),
		Labels:             p.GetLabels(),
		UserLabels:         p.GetUserLabels(),
		VerificationStatus: int32(p.GetVerificationStatus()),
	}
	if p.GetEnabled() != nil {
		v := p.GetEnabled().GetValue()
		c.Enabled = &v
	}
	return c
}

// ─── neutral descriptors → proto ──────────────────────────────────────────────

func monitoredResourceDescriptorToProto(d core.MonitoredResourceDescriptor) *monitoredrespb.MonitoredResourceDescriptor {
	labels := make([]*labelpb.LabelDescriptor, 0, len(d.Labels))
	for _, l := range d.Labels {
		labels = append(labels, &labelpb.LabelDescriptor{
			Key:         l.Key,
			ValueType:   labelValueTypeToProto(l.ValueType),
			Description: l.Description,
		})
	}
	return &monitoredrespb.MonitoredResourceDescriptor{
		Name:        d.Name,
		Type:        d.Type,
		DisplayName: d.DisplayName,
		Description: d.Description,
		Labels:      labels,
	}
}

func notificationChannelDescriptorToProto(d core.NotificationChannelDescriptor) *monitoringpb.NotificationChannelDescriptor {
	labels := make([]*labelpb.LabelDescriptor, 0, len(d.Labels))
	for _, l := range d.Labels {
		labels = append(labels, &labelpb.LabelDescriptor{
			Key:         l.Key,
			ValueType:   labelValueTypeToProto(l.ValueType),
			Description: l.Description,
		})
	}
	return &monitoringpb.NotificationChannelDescriptor{
		Name:        d.Name,
		Type:        d.Type,
		DisplayName: d.DisplayName,
		Description: d.Description,
		Labels:      labels,
	}
}

func labelValueTypeToProto(name string) labelpb.LabelDescriptor_ValueType {
	switch name {
	case "BOOL":
		return labelpb.LabelDescriptor_BOOL
	case "INT64":
		return labelpb.LabelDescriptor_INT64
	default:
		return labelpb.LabelDescriptor_STRING
	}
}
