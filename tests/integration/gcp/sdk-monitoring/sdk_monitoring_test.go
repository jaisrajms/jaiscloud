// Package sdk_monitoring_test exercises the jaiscloud-gcp Cloud Monitoring
// emulator through the official cloud.google.com/go/monitoring high-level
// client over gRPC. This is the acceptance client for the Monitoring gRPC
// transport (the CloudWatch metrics+alarms analogue).
//
// Run with the binary started on the gRPC port:
//
//	./jaiscloud-gcp start --port 8090 --grpc-port 8081 --ephemeral &
//	MONITORING_EMULATOR_HOST=localhost:8081 go test ./...
//
// This is a separate Go module so the heavy cloud.google.com/go/monitoring
// dependency does not bloat the main module.
package sdk_monitoring_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const projectID = "proj"

func emulatorHost() string {
	if h := os.Getenv("MONITORING_EMULATOR_HOST"); h != "" {
		return h
	}
	return "localhost:8081"
}

func dialConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(emulatorHost(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func newMetricClient(t *testing.T) *monitoring.MetricClient {
	t.Helper()
	c, err := monitoring.NewMetricClient(context.Background(),
		option.WithGRPCConn(dialConn(t)), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func newAlertPolicyClient(t *testing.T) *monitoring.AlertPolicyClient {
	t.Helper()
	c, err := monitoring.NewAlertPolicyClient(context.Background(),
		option.WithGRPCConn(dialConn(t)), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func newServiceMonitoringClient(t *testing.T) *monitoring.ServiceMonitoringClient {
	t.Helper()
	c, err := monitoring.NewServiceMonitoringClient(context.Background(),
		option.WithGRPCConn(dialConn(t)), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func unique(prefix string) string {
	return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
}

// TestSDKMetricDescriptor exercises create + get + list + delete of a metric
// descriptor through the high-level client.
func TestSDKMetricDescriptor(t *testing.T) {
	ctx := context.Background()
	mc := newMetricClient(t)

	metricType := "custom.googleapis.com/sdk/" + unique("metric")

	created, err := mc.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: "projects/" + projectID,
		MetricDescriptor: &metricpb.MetricDescriptor{
			Type:        metricType,
			MetricKind:  metricpb.MetricDescriptor_GAUGE,
			ValueType:   metricpb.MetricDescriptor_DOUBLE,
			Unit:        "1",
			Description: "sdk test metric",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "projects/"+projectID+"/metricDescriptors/"+metricType, created.GetName())

	got, err := mc.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: "projects/" + projectID + "/metricDescriptors/" + metricType,
	})
	require.NoError(t, err)
	require.Equal(t, metricType, got.GetType())
	require.Equal(t, "sdk test metric", got.GetDescription())

	var listed []string
	it := mc.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{Name: "projects/" + projectID})
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		listed = append(listed, d.GetType())
	}
	require.Contains(t, listed, metricType)

	require.NoError(t, mc.DeleteMetricDescriptor(ctx, &monitoringpb.DeleteMetricDescriptorRequest{
		Name: "projects/" + projectID + "/metricDescriptors/" + metricType,
	}))
}

// TestSDKTimeSeries exercises create + list of time series data points.
func TestSDKTimeSeries(t *testing.T) {
	ctx := context.Background()
	mc := newMetricClient(t)

	metricType := "custom.googleapis.com/sdk/" + unique("ts")
	now := time.Now().UTC().Truncate(time.Second)

	require.NoError(t, mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/" + projectID,
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: metricType, Labels: map[string]string{"zone": "us-east1-a"}},
			Resource: &monitoredrespb.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": projectID}},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 42}},
			}},
		}},
	}))

	filter := fmt.Sprintf(`metric.type = %q AND resource.type = "global"`, metricType)
	it := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/" + projectID,
		Filter:   filter,
		Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(now.Add(-time.Minute)), EndTime: timestamppb.New(now.Add(time.Minute))},
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})

	var found int
	for {
		ts, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		require.Equal(t, metricType, ts.GetMetric().GetType())
		require.Len(t, ts.GetPoints(), 1)
		require.Equal(t, float64(42), ts.GetPoints()[0].GetValue().GetDoubleValue())
		found++
	}
	require.Equal(t, 1, found)
}

// TestSDKAlertPolicy exercises create + get + list + delete of an alert policy.
func TestSDKAlertPolicy(t *testing.T) {
	ctx := context.Background()
	ac := newAlertPolicyClient(t)

	displayName := unique("policy")
	created, err := ac.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/" + projectID,
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: displayName,
			Combiner:    monitoringpb.AlertPolicy_OR,
			Enabled:     wrapperspb.Bool(true),
			Conditions: []*monitoringpb.AlertPolicy_Condition{{
				DisplayName: "cpu high",
				Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
					Filter:         `metric.type = "custom.googleapis.com/sdk/cpu"`,
					Comparison:     monitoringpb.ComparisonType_COMPARISON_GT,
					ThresholdValue: 0.9,
				}},
			}},
		},
	})
	require.NoError(t, err)
	require.Contains(t, created.GetName(), "projects/"+projectID+"/alertPolicies/")

	got, err := ac.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: created.GetName()})
	require.NoError(t, err)
	require.Equal(t, displayName, got.GetDisplayName())
	require.Len(t, got.GetConditions(), 1)

	var listed []string
	it := ac.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{Name: "projects/" + projectID})
	for {
		p, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		listed = append(listed, p.GetName())
	}
	require.Contains(t, listed, created.GetName())

	require.NoError(t, ac.DeleteAlertPolicy(ctx, &monitoringpb.DeleteAlertPolicyRequest{Name: created.GetName()}))
}

// TestSDKServiceMonitoring exercises create + get + list + patch + delete of a
// Service and its ServiceLevelObjective through the high-level client.
func TestSDKServiceMonitoring(t *testing.T) {
	ctx := context.Background()
	sc := newServiceMonitoringClient(t)

	serviceID := unique("svc")
	createdSvc, err := sc.CreateService(ctx, &monitoringpb.CreateServiceRequest{
		Parent:    "projects/" + projectID,
		ServiceId: serviceID,
		Service: &monitoringpb.Service{
			DisplayName: "SDK Checkout",
			Identifier: &monitoringpb.Service_CloudRun_{
				CloudRun: &monitoringpb.Service_CloudRun{ServiceName: "checkout"},
			},
			UserLabels: map[string]string{"team": "payments"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "projects/"+projectID+"/services/"+serviceID, createdSvc.GetName())
	require.Equal(t, "checkout", createdSvc.GetCloudRun().GetServiceName())

	gotSvc, err := sc.GetService(ctx, &monitoringpb.GetServiceRequest{Name: createdSvc.GetName()})
	require.NoError(t, err)
	require.Equal(t, "SDK Checkout", gotSvc.GetDisplayName())

	// A masked patch updates the display name and preserves the identifier.
	updatedSvc, err := sc.UpdateService(ctx, &monitoringpb.UpdateServiceRequest{
		Service:    &monitoringpb.Service{Name: createdSvc.GetName(), DisplayName: "SDK Checkout v2"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	require.NoError(t, err)
	require.Equal(t, "SDK Checkout v2", updatedSvc.GetDisplayName())
	require.Equal(t, "checkout", updatedSvc.GetCloudRun().GetServiceName())

	sloID := unique("slo")
	createdSLO, err := sc.CreateServiceLevelObjective(ctx, &monitoringpb.CreateServiceLevelObjectiveRequest{
		Parent:                  createdSvc.GetName(),
		ServiceLevelObjectiveId: sloID,
		ServiceLevelObjective: &monitoringpb.ServiceLevelObjective{
			DisplayName: "Availability",
			Goal:        0.99,
			Period:      &monitoringpb.ServiceLevelObjective_RollingPeriod{RollingPeriod: durationpb.New(30 * 24 * time.Hour)},
			ServiceLevelIndicator: &monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_BasicSli{
					BasicSli: &monitoringpb.BasicSli{
						SliCriteria: &monitoringpb.BasicSli_Availability{Availability: &monitoringpb.BasicSli_AvailabilityCriteria{}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, createdSvc.GetName()+"/serviceLevelObjectives/"+sloID, createdSLO.GetName())
	require.Equal(t, 0.99, createdSLO.GetGoal())
	require.Equal(t, 30*24*time.Hour, createdSLO.GetRollingPeriod().AsDuration())
	require.NotNil(t, createdSLO.GetServiceLevelIndicator().GetBasicSli().GetAvailability())

	var listed []string
	it := sc.ListServiceLevelObjectives(ctx, &monitoringpb.ListServiceLevelObjectivesRequest{Parent: createdSvc.GetName()})
	for {
		slo, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		listed = append(listed, slo.GetName())
	}
	require.Contains(t, listed, createdSLO.GetName())

	require.NoError(t, sc.DeleteServiceLevelObjective(ctx, &monitoringpb.DeleteServiceLevelObjectiveRequest{Name: createdSLO.GetName()}))
	require.NoError(t, sc.DeleteService(ctx, &monitoringpb.DeleteServiceRequest{Name: createdSvc.GetName()}))
}
