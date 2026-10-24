// depth_test.go closes DC5 (plan_docs/gcp-depth-closure-wave-plan.md W2.3) for
// Cloud Monitoring: the three cells its record left exempt — p2 (concurrent
// metric-descriptor and time-series writes), p3 (the error envelope on the
// official-client path) and p5 (a writer scale boundary) — asserted through the
// official cloud.google.com/go/monitoring client and the shared depth-test kit
// (tests/integration/gcp/testdepth).
//
// p2 asserts the documented state under contention: distinct metric descriptors
// written concurrently all land, and concurrent writes to ONE time series append
// every point (the store serialises the append under its lock, so no point is
// lost to a read-modify-write race). The series identity is
// (metric.type, metric.labels, resource.type, resource.labels), so the writers
// share those and vary only the point value.
//
// p3 asserts the typed gRPC status the official client surfaces for the
// documented negatives: a missing descriptor and an unknown monitored-resource
// type are NOT_FOUND, and an empty metric type, a non-blank order_by (which
// ListTimeSeries documents as unsupported, J43), a malformed filter and a
// secondary_aggregation with no primary aggregation are INVALID_ARGUMENT.
//
// p5 is the writer boundary the class names: a large single-request write of
// distinct series is fully retained and readable, so the write path is not lossy
// at scale. Monitoring enforces no cardinality cap, so this is the observable
// scale property rather than a rejected-over-limit negative.
//
// The concurrency cases run under -race (the module's gate adds it); a contention
// assertion in a binary without the race detector is a failure
// (testdepth.AssertRaceEnabled), not a silent downgrade.
package sdk_monitoring_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"
	testdepth "jaiscloud-gcp-testdepth"
)

// descName renders a metric-descriptor resource name for a metric type.
func descName(typ string) string {
	return "projects/" + projectID + "/metricDescriptors/" + typ
}

// TestSDKMonitoringDepthConcurrency is monitoring p2: concurrent writes under
// -race. Distinct metric descriptors written concurrently each land, and
// concurrent writes to a single time series retain every point (the append is
// already serialised, so no update is lost).
func TestSDKMonitoringDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	mc := newMetricClient(t)
	testdepth.AssertRaceEnabled(t)

	t.Run("ConcurrentDescriptorCreatesAllLand", func(t *testing.T) {
		const writers = 8
		types := make([]string, writers)
		for i := range types {
			types[i] = "custom.googleapis.com/depth/race/" + unique(fmt.Sprintf("d%d", i))
		}

		errs := testdepth.Race(writers, func(i int) error {
			_, err := mc.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
				Name: "projects/" + projectID,
				MetricDescriptor: &metricpb.MetricDescriptor{
					Type:        types[i],
					MetricKind:  metricpb.MetricDescriptor_GAUGE,
					ValueType:   metricpb.MetricDescriptor_DOUBLE,
					Description: "depth contention",
				},
			})
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "create %d", i)
		}

		for i, typ := range types {
			got, err := mc.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{Name: descName(typ)})
			require.NoError(t, err, "get %d", i)
			require.Equal(t, typ, got.GetType())
		}
	})

	t.Run("ConcurrentSeriesWritesRetainEveryPoint", func(t *testing.T) {
		metricType := "custom.googleapis.com/depth/race/" + unique("series")
		now := time.Now().UTC().Truncate(time.Second)
		resource := &monitoredrespb.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": projectID},
		}

		// Writers share the metric+resource labels (one series) and differ only
		// in the point value, so a lost update would drop a value from the set.
		const writers = 8
		values := make([]float64, writers)
		errs := testdepth.Race(writers, func(i int) error {
			values[i] = float64(1000 + i)
			return mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
				Name: "projects/" + projectID,
				TimeSeries: []*monitoringpb.TimeSeries{{
					Metric:   &metricpb.Metric{Type: metricType},
					Resource: resource,
					Points: []*monitoringpb.Point{{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: values[i]}},
					}},
				}},
			})
		})
		for i, err := range errs {
			require.NoError(t, err, "write %d", i)
		}

		got := listSeries(t, mc, metricType, now)
		require.Len(t, got, 1, "the concurrent writers share one series")
		require.Len(t, got[0].GetPoints(), writers, "every concurrent point must be retained")
		seen := map[float64]bool{}
		for _, p := range got[0].GetPoints() {
			seen[p.GetValue().GetDoubleValue()] = true
		}
		for i, v := range values {
			require.True(t, seen[v], "point %d (%v) must survive the concurrent writes", i, v)
		}
	})
}

// listSeries queries one metric type over a wide interval and returns the
// matching time series with their points (FULL view).
func listSeries(t *testing.T, mc *monitoring.MetricClient, metricType string, now time.Time) []*monitoringpb.TimeSeries {
	t.Helper()
	it := mc.ListTimeSeries(context.Background(), &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/" + projectID,
		Filter:   fmt.Sprintf(`metric.type = %q AND resource.type = "global"`, metricType),
		Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(now.Add(-time.Hour)), EndTime: timestamppb.New(now.Add(time.Hour))},
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	var out []*monitoringpb.TimeSeries
	for {
		ts, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		out = append(out, ts)
	}
	return out
}

// TestSDKMonitoringDepthErrors is monitoring p3: the error envelope the official
// client surfaces for the documented negatives. Each spec asserts both the gRPC
// code and the google.rpc status name, so a drift in either is caught.
func TestSDKMonitoringDepthErrors(t *testing.T) {
	ctx := context.Background()
	mc := newMetricClient(t)
	now := time.Now().UTC()

	validInterval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-time.Hour)),
		EndTime:   timestamppb.New(now.Add(time.Hour)),
	}

	// listErr drives a ListTimeSeries call and returns the error the iterator
	// surfaces on the first Next.
	listErr := func(req *monitoringpb.ListTimeSeriesRequest) error {
		req.Name = "projects/" + projectID
		_, err := mc.ListTimeSeries(ctx, req).Next()
		return err
	}

	testdepth.RunNegatives(t, []testdepth.NegativeCase{
		{
			Name: "MetricDescriptorNotFound",
			Run: func() error {
				_, err := mc.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
					Name: descName("custom.googleapis.com/depth/does-not-exist"),
				})
				return err
			},
			Want: testdepth.SpecNotFound(),
		},
		{
			Name: "MonitoredResourceDescriptorNotFound",
			Run: func() error {
				_, err := mc.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{
					Name: "projects/" + projectID + "/monitoredResourceDescriptors/no.such.type",
				})
				return err
			},
			Want: testdepth.SpecNotFound(),
		},
		{
			Name: "CreateTimeSeriesEmptyMetricType",
			Run: func() error {
				return mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
					Name: "projects/" + projectID,
					TimeSeries: []*monitoringpb.TimeSeries{{
						Resource: &monitoredrespb.MonitoredResource{Type: "global"},
						Points: []*monitoringpb.Point{{
							Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
							Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1}},
						}},
					}},
				})
			},
			Want: testdepth.SpecInvalidArgument(),
		},
		{
			Name: "ListTimeSeriesOrderByUnsupported",
			Run: func() error {
				return listErr(&monitoringpb.ListTimeSeriesRequest{
					Filter:   `metric.type = "custom.googleapis.com/depth/x"`,
					Interval: validInterval,
					OrderBy:  "metric.type",
				})
			},
			Want: testdepth.SpecInvalidArgument(),
		},
		{
			Name: "ListTimeSeriesMalformedFilter",
			Run: func() error {
				return listErr(&monitoringpb.ListTimeSeriesRequest{
					Filter:   "metric.type =",
					Interval: validInterval,
				})
			},
			Want: testdepth.SpecInvalidArgument(),
		},
		{
			Name: "ListTimeSeriesSecondaryAggregationWithoutPrimary",
			Run: func() error {
				return listErr(&monitoringpb.ListTimeSeriesRequest{
					Filter:               `metric.type = "custom.googleapis.com/depth/x"`,
					Interval:             validInterval,
					SecondaryAggregation: &monitoringpb.Aggregation{},
				})
			},
			Want: testdepth.SpecInvalidArgument(),
		},
	})
}

// TestSDKMonitoringDepthWriterScale is monitoring p5: a writer boundary. A single
// CreateTimeSeries request carries many distinct series (each a distinct
// metric.labels value), and every one must be retained and readable — the write
// path neither truncates a large request nor loses a series.
func TestSDKMonitoringDepthWriterScale(t *testing.T) {
	ctx := context.Background()
	mc := newMetricClient(t)

	const series = 100
	metricType := "custom.googleapis.com/depth/scale/" + unique("many")
	now := time.Now().UTC().Truncate(time.Second)

	batch := make([]*monitoringpb.TimeSeries, 0, series)
	for i := 0; i < series; i++ {
		batch = append(batch, &monitoringpb.TimeSeries{
			Metric: &metricpb.Metric{
				Type:   metricType,
				Labels: map[string]string{"series": fmt.Sprintf("%03d", i)},
			},
			Resource: &monitoredrespb.MonitoredResource{
				Type:   "global",
				Labels: map[string]string{"project_id": projectID},
			},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: float64(i)}},
			}},
		})
	}

	require.NoError(t, mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name:       "projects/" + projectID,
		TimeSeries: batch,
	}))

	got := listSeries(t, mc, metricType, now)
	require.Len(t, got, series, "every series in the batch must be retained")

	labelsSeen := map[string]bool{}
	for _, ts := range got {
		require.Len(t, ts.GetPoints(), 1, "each series keeps its one point")
		labelsSeen[ts.GetMetric().GetLabels()["series"]] = true
	}
	for i := 0; i < series; i++ {
		require.True(t, labelsSeen[fmt.Sprintf("%03d", i)], "series %d must be listed", i)
	}
}
