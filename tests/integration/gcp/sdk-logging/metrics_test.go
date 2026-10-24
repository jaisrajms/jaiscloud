package logging_test

import (
	"context"
	"strings"
	"testing"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestLoggingMetricsCRUD drives the logs-based metric surface (MetricsServiceV2)
// through the official gRPC client: create, duplicate, get, list, update, and
// delete + NotFound. The emulator stores the metric definition and synthesizes
// the output-only descriptor; it evaluates the filter against entries but does
// not deliver a Monitoring time series, so the suite pins the definition and
// the evaluation inputs (filter + descriptor) rather than emitted data points.
func TestLoggingMetricsCRUD(t *testing.T) {
	ctx := context.Background()
	c := MetricsClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()
	id := uniqueName("metric")
	fullName := parent + "/metrics/" + id

	created, err := c.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: parent,
		Metric: &loggingpb.LogMetric{
			Name:        id,
			Description: "counts errors",
			Filter:      `severity>=ERROR`,
			MetricDescriptor: &metricpb.MetricDescriptor{
				MetricKind: metricpb.MetricDescriptor_DELTA,
				ValueType:  metricpb.MetricDescriptor_INT64,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, id, created.GetName())
	require.Equal(t, `severity>=ERROR`, created.GetFilter())
	require.NotNil(t, created.GetCreateTime())
	// The descriptor's type/name are output-only and synthesized from the id.
	require.Equal(t, "logging.googleapis.com/user/"+id, created.GetMetricDescriptor().GetType())
	require.Equal(t, parent+"/metricDescriptors/logging.googleapis.com/user/"+id, created.GetMetricDescriptor().GetName())
	require.Equal(t, "1", created.GetMetricDescriptor().GetUnit(), "the default unit is 1")

	// A duplicate create is AlreadyExists.
	_, err = c.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: parent,
		Metric: &loggingpb.LogMetric{Name: id, Filter: `severity>=INFO`},
	})
	require.Equal(t, codes.AlreadyExists, status.Code(err))

	got, err := c.GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{MetricName: fullName})
	require.NoError(t, err)
	require.Equal(t, `severity>=ERROR`, got.GetFilter())
	require.Equal(t, metricpb.MetricDescriptor_DELTA, got.GetMetricDescriptor().GetMetricKind())
	require.Equal(t, metricpb.MetricDescriptor_INT64, got.GetMetricDescriptor().GetValueType())

	require.True(t, metricListContains(ctx, c, parent, fullName), "created metric must appear in ListLogMetrics")

	// Update replaces the mutable fields; metric_kind/value_type are immutable
	// and an omitted descriptor keeps the stored kind/value type.
	updated, err := c.UpdateLogMetric(ctx, &loggingpb.UpdateLogMetricRequest{
		MetricName: fullName,
		Metric: &loggingpb.LogMetric{
			Filter:      `severity>=CRITICAL`,
			Description: "counts criticals",
		},
	})
	require.NoError(t, err)
	require.Equal(t, `severity>=CRITICAL`, updated.GetFilter())
	require.Equal(t, "counts criticals", updated.GetDescription())
	require.Equal(t, metricpb.MetricDescriptor_INT64, updated.GetMetricDescriptor().GetValueType(),
		"an omitted value_type must stay immutable rather than reset")
	require.Equal(t, created.GetCreateTime().AsTime(), updated.GetCreateTime().AsTime(), "createTime is preserved across an update")

	require.NoError(t, c.DeleteLogMetric(ctx, &loggingpb.DeleteLogMetricRequest{MetricName: fullName}))
	_, err = c.GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{MetricName: fullName})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestLoggingMetricsUpdateUpsert verifies the documented create-or-update
// semantics of logs-based metrics: an update against an absent metric creates it.
func TestLoggingMetricsUpdateUpsert(t *testing.T) {
	ctx := context.Background()
	c := MetricsClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()
	id := uniqueName("upsert")
	fullName := parent + "/metrics/" + id

	upserted, err := c.UpdateLogMetric(ctx, &loggingpb.UpdateLogMetricRequest{
		MetricName: fullName,
		Metric: &loggingpb.LogMetric{
			Filter: `severity>=WARNING`,
			MetricDescriptor: &metricpb.MetricDescriptor{
				MetricKind: metricpb.MetricDescriptor_GAUGE,
				ValueType:  metricpb.MetricDescriptor_DOUBLE,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, `severity>=WARNING`, upserted.GetFilter())
	require.Equal(t, metricpb.MetricDescriptor_GAUGE, upserted.GetMetricDescriptor().GetMetricKind())

	got, err := c.GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{MetricName: fullName})
	require.NoError(t, err)
	require.Equal(t, `severity>=WARNING`, got.GetFilter())
}

// TestLoggingMetricsSlashIDAndLabels verifies a metric id containing a slash
// (a documented name hierarchy) is percent-encoded in the resource name and
// round-trips, and that descriptor labels + label extractors are preserved.
func TestLoggingMetricsSlashIDAndLabels(t *testing.T) {
	ctx := context.Background()
	c := MetricsClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()
	id := uniqueName("nginx") + "/requests"
	fullName := parent + "/metrics/" + strings.ReplaceAll(id, "/", "%2F")

	created, err := c.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: parent,
		Metric: &loggingpb.LogMetric{
			Name:            id,
			Filter:          `resource.type="global"`,
			ValueExtractor:  "EXTRACT(jsonPayload.latency)",
			LabelExtractors: map[string]string{"code": "EXTRACT(jsonPayload.code)"},
			MetricDescriptor: &metricpb.MetricDescriptor{
				MetricKind: metricpb.MetricDescriptor_DELTA,
				ValueType:  metricpb.MetricDescriptor_DISTRIBUTION,
				Labels:     []*labelpb.LabelDescriptor{{Key: "code", ValueType: labelpb.LabelDescriptor_INT64}},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, id, created.GetName())
	require.Equal(t, "logging.googleapis.com/user/"+id, created.GetMetricDescriptor().GetType())
	require.Len(t, created.GetMetricDescriptor().GetLabels(), 1)
	require.Equal(t, "code", created.GetMetricDescriptor().GetLabels()[0].GetKey())

	got, err := c.GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{MetricName: fullName})
	require.NoError(t, err)
	require.Equal(t, id, got.GetName())
	require.Equal(t, "EXTRACT(jsonPayload.latency)", got.GetValueExtractor())
	require.Equal(t, "EXTRACT(jsonPayload.code)", got.GetLabelExtractors()["code"])
}

// TestLoggingMetricsValidation pins the metric filter/descriptor validation:
// a metric with no filter, an uncompilable filter, or a label extractor for an
// undefined label is InvalidArgument.
func TestLoggingMetricsValidation(t *testing.T) {
	ctx := context.Background()
	c := MetricsClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()

	_, err := c.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: parent,
		Metric: &loggingpb.LogMetric{Name: uniqueName("no-filter")},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a metric filter is required")

	_, err = c.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: parent,
		Metric: &loggingpb.LogMetric{Name: uniqueName("bad-filter"), Filter: "not a filter"},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "an uncompilable metric filter must be rejected")

	// A label extractor that references an undefined descriptor label is rejected.
	_, err = c.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: parent,
		Metric: &loggingpb.LogMetric{
			Name:            uniqueName("bad-label"),
			Filter:          `severity>=ERROR`,
			LabelExtractors: map[string]string{"missing": "EXTRACT(jsonPayload.x)"},
			MetricDescriptor: &metricpb.MetricDescriptor{
				MetricKind: metricpb.MetricDescriptor_DELTA,
				ValueType:  metricpb.MetricDescriptor_INT64,
			},
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a label extractor for an undefined label must be rejected")
}

func metricListContains(ctx context.Context, c *logging.MetricsClient, parent, fullName string) bool {
	it := c.ListLogMetrics(ctx, &loggingpb.ListLogMetricsRequest{Parent: parent, PageSize: 1000})
	for {
		m, err := it.Next()
		if err == iterator.Done {
			return false
		}
		if err != nil {
			return false
		}
		if parent+"/metrics/"+m.GetName() == fullName || m.GetName() == fullName {
			return true
		}
	}
}
