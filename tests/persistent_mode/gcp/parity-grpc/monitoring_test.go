//go:build gcp_persistence

package paritygrpc_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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

// uptimeCheckClient builds an official Cloud Monitoring uptime-check client over
// the emulator's gRPC listener and returns a close func for both the client and
// its connection.
func (d *driver) uptimeCheckClient(ctx context.Context) (*monitoring.UptimeCheckClient, func(), error) {
	conn, err := dialGRPC(d.target("MONITORING_EMULATOR_HOST"))
	if err != nil {
		return nil, nil, fmt.Errorf("monitoring dial: %w", err)
	}
	c, err := monitoring.NewUptimeCheckClient(ctx, option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("uptime check client: %w", err)
	}
	return c, func() { c.Close(); conn.Close() }, nil
}

// seedMonitoring creates a metric descriptor and a declarative uptime check
// configuration, and returns checks that read both back after a restart and
// assert their absence after a reset.
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

	// Uptime checks are the declarative control-plane half of UptimeCheckService:
	// the configuration round-trips through --dsn like any other resource.
	uptimeGet := func() (*monitoringpb.UptimeCheckConfig, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, closeFn, err := d.uptimeCheckClient(ctx)
		if err != nil {
			return nil, err
		}
		defer closeFn()
		it := c.ListUptimeCheckConfigs(ctx, &monitoringpb.ListUptimeCheckConfigsRequest{Parent: "projects/" + project})
		for {
			cfg, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, err
			}
			if cfg.GetDisplayName() == uptimeDisplayName {
				return cfg, nil
			}
		}
		return nil, fmt.Errorf("uptime check %q not listed", uptimeDisplayName)
	}
	createUptime := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, closeFn, err := d.uptimeCheckClient(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		_, err = c.CreateUptimeCheckConfig(ctx, &monitoringpb.CreateUptimeCheckConfigRequest{
			Parent: "projects/" + project,
			UptimeCheckConfig: &monitoringpb.UptimeCheckConfig{
				DisplayName: uptimeDisplayName,
				Resource: &monitoringpb.UptimeCheckConfig_MonitoredResource{
					MonitoredResource: &monitoredrespb.MonitoredResource{
						Type:   "uptime_url",
						Labels: map[string]string{"host": "parity.example.com"},
					},
				},
				CheckRequestType: &monitoringpb.UptimeCheckConfig_HttpCheck_{
					HttpCheck: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/healthz"},
				},
				Timeout: durationpb.New(10 * time.Second),
				Period:  durationpb.New(60 * time.Second),
			},
		})
		if err != nil {
			return fmt.Errorf("uptime check create: %w", err)
		}
		return nil
	}
	childSeeded := func() error {
		cfg, err := uptimeGet()
		if err != nil {
			return fmt.Errorf("uptime check after seed: %w", err)
		}
		if cfg.GetHttpCheck().GetPath() != "/healthz" {
			return fmt.Errorf("uptime check httpCheck after seed = %+v, want /healthz", cfg.GetHttpCheck())
		}
		return nil
	}
	childSurvived := func() error {
		if _, err := uptimeGet(); err != nil {
			return fmt.Errorf("uptime check after restart: %w", err)
		}
		return nil
	}
	childCleared := func() error {
		if _, err := uptimeGet(); err == nil {
			return fmt.Errorf("uptime check %q still present after reset", uptimeDisplayName)
		}
		return nil
	}

	if err := create(); err != nil {
		return nil, nil, nil, err
	}
	if err := createUptime(); err != nil {
		return nil, nil, nil, err
	}
	all := func(checks ...func() error) func() error {
		return func() error {
			for _, check := range checks {
				if err := check(); err != nil {
					return err
				}
			}
			return nil
		}
	}
	if err := all(seeded, childSeeded)(); err != nil {
		return nil, nil, nil, err
	}
	return all(seeded, childSeeded), all(survived, childSurvived), all(cleared, childCleared), nil
}

// uptimeDisplayName names the declarative uptime check the monitoring probe
// seeds; it is unique per run like every other probe resource.
const uptimeDisplayName = "parity-uptime-check"
