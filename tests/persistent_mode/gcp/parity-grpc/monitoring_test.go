//go:build gcp_persistence

package paritygrpc_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/option"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// metricClient builds an official Cloud Monitoring metric client over the
// emulator's gRPC listener and returns a close func for both the client and its
// connection.
func (d *driver) metricClient(ctx context.Context) (*monitoring.MetricClient, func(), error) {
	conn, err := dialGRPC(d.target("MONITORING_EMULATOR_HOST"))
	if err != nil {
		return nil, nil, fmt.Errorf("monitoring dial: %w", err)
	}
	c, err := monitoring.NewMetricClient(ctx, option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("monitoring client: %w", err)
	}
	return c, func() { c.Close(); conn.Close() }, nil
}

// seedMonitoring creates a metric descriptor and returns checks that read it
// back after a restart and assert its absence after a reset.
func seedMonitoring(d *driver, suffix string) (func() error, func() error, func() error, error) {
	project := projectID()
	metricType := "custom.googleapis.com/parity/" + suffix
	name := fmt.Sprintf("projects/%s/metricDescriptors/%s", project, metricType)
	const description = "parity persistence probe"

	// get returns the descriptor and a nil error only when it is present.
	get := func() (*metricpb.MetricDescriptor, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, closeFn, err := d.metricClient(ctx)
		if err != nil {
			return nil, err
		}
		defer closeFn()
		return c.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{Name: name})
	}

	create := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, closeFn, err := d.metricClient(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		_, err = c.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
			Name: "projects/" + project,
			MetricDescriptor: &metricpb.MetricDescriptor{
				Type:        metricType,
				MetricKind:  metricpb.MetricDescriptor_GAUGE,
				ValueType:   metricpb.MetricDescriptor_DOUBLE,
				Unit:        "1",
				Description: description,
			},
		})
		if err != nil {
			return fmt.Errorf("monitoring create: %w", err)
		}
		return nil
	}

	seeded := func() error {
		got, err := get()
		if err != nil {
			return fmt.Errorf("monitoring get after seed: %w", err)
		}
		if got.GetType() != metricType || got.GetDescription() != description {
			return fmt.Errorf("monitoring descriptor mismatch after seed: type=%q description=%q", got.GetType(), got.GetDescription())
		}
		return nil
	}
	survived := func() error {
		got, err := get()
		if err != nil {
			return fmt.Errorf("monitoring get after restart: %w", err)
		}
		if got.GetType() != metricType || got.GetDescription() != description {
			return fmt.Errorf("monitoring descriptor mismatch after restart: type=%q description=%q", got.GetType(), got.GetDescription())
		}
		return nil
	}
	cleared := func() error {
		got, err := get()
		if err == nil {
			return fmt.Errorf("metric descriptor still present after reset: %s", got.GetName())
		}
		if status.Code(err) != codes.NotFound {
			return fmt.Errorf("monitoring get after reset: got %v, want NotFound", errors.Unwrap(err))
		}
		return nil
	}

	if err := create(); err != nil {
		return nil, nil, nil, err
	}
	if err := seeded(); err != nil {
		return nil, nil, nil, err
	}
	return seeded, survived, cleared, nil
}
