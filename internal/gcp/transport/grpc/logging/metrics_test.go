package logging

import (
	"context"
	"net"
	"testing"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"

	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func metricsTestClient(t *testing.T) (loggingpb.MetricsServiceV2Client, func()) {
	t.Helper()
	svc := NewMetricsService(core.NewService(loggingstore.NewMemoryStore(), "test"), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	loggingpb.RegisterMetricsServiceV2Server(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return loggingpb.NewMetricsServiceV2Client(conn), func() {
		conn.Close()
		srv.Stop()
	}
}

func TestGRPCMetricCRUD(t *testing.T) {
	client, cleanup := metricsTestClient(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: "projects/test",
		Metric: &loggingpb.LogMetric{
			Name:           "nginx/requests",
			Description:    "request count",
			Filter:         "severity>=ERROR",
			ValueExtractor: "EXTRACT(jsonPayload.latency)",
			LabelExtractors: map[string]string{
				"code": "EXTRACT(jsonPayload.code)",
			},
			MetricDescriptor: &metricpb.MetricDescriptor{
				ValueType: metricpb.MetricDescriptor_DISTRIBUTION,
				Unit:      "ms",
				Labels:    []*labelpb.LabelDescriptor{{Key: "code", ValueType: labelpb.LabelDescriptor_INT64}},
			},
			BucketOptions: &distributionpb.Distribution_BucketOptions{
				Options: &distributionpb.Distribution_BucketOptions_LinearBuckets{
					LinearBuckets: &distributionpb.Distribution_BucketOptions_Linear{NumFiniteBuckets: 3, Width: 1, Offset: 0},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateLogMetric: %v", err)
	}
	if created.GetName() != "nginx/requests" {
		t.Fatalf("name = %q", created.GetName())
	}
	if md := created.GetMetricDescriptor(); md.GetType() != "logging.googleapis.com/user/nginx/requests" {
		t.Fatalf("descriptor.type = %q", md.GetType())
	}
	if created.GetMetricDescriptor().GetValueType() != metricpb.MetricDescriptor_DISTRIBUTION {
		t.Fatalf("descriptor.valueType = %v", created.GetMetricDescriptor().GetValueType())
	}
	if created.GetBucketOptions().GetLinearBuckets().GetNumFiniteBuckets() != 3 {
		t.Fatalf("bucketOptions = %v", created.GetBucketOptions())
	}
	if created.GetCreateTime() == nil {
		t.Fatal("createTime missing")
	}

	got, err := client.GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{
		MetricName: "projects/test/metrics/nginx%2Frequests",
	})
	if err != nil {
		t.Fatalf("GetLogMetric: %v", err)
	}
	if got.GetFilter() != "severity>=ERROR" {
		t.Fatalf("filter = %q", got.GetFilter())
	}

	list, err := client.ListLogMetrics(ctx, &loggingpb.ListLogMetricsRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("ListLogMetrics: %v", err)
	}
	if len(list.GetMetrics()) != 1 {
		t.Fatalf("metrics = %v", list.GetMetrics())
	}

	// A duplicate create is AlreadyExists.
	if _, err := client.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: "projects/test",
		Metric: &loggingpb.LogMetric{Name: "nginx/requests", Filter: "severity>=INFO"},
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate create code = %v, want AlreadyExists", status.Code(err))
	}

	// A change of value_type is rejected.
	if _, err := client.UpdateLogMetric(ctx, &loggingpb.UpdateLogMetricRequest{
		MetricName: "projects/test/metrics/nginx%2Frequests",
		Metric: &loggingpb.LogMetric{
			Filter:         "severity>=WARNING",
			ValueExtractor: "EXTRACT(jsonPayload.latency)",
			LabelExtractors: map[string]string{
				"code": "EXTRACT(jsonPayload.code)",
			},
			MetricDescriptor: &metricpb.MetricDescriptor{
				ValueType: metricpb.MetricDescriptor_INT64,
				Labels:    []*labelpb.LabelDescriptor{{Key: "code", ValueType: labelpb.LabelDescriptor_INT64}},
			},
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("value_type change code = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := client.DeleteLogMetric(ctx, &loggingpb.DeleteLogMetricRequest{
		MetricName: "projects/test/metrics/nginx%2Frequests",
	}); err != nil {
		t.Fatalf("DeleteLogMetric: %v", err)
	}
	if _, err := client.GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{
		MetricName: "projects/test/metrics/nginx%2Frequests",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete code = %v, want NotFound", status.Code(err))
	}
}
