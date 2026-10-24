//go:build gcp_persistence

package paritygrpc_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// loggingWrite writes one entry through the official LoggingServiceV2 client.
//
// The RPC is issued directly rather than through the high-level Logger: that
// client appends a one-time instrumentation entry and sets PartialSuccess,
// which this emulator intentionally rejects as Unimplemented, so the first
// high-level write in a process always fails. WriteLogEntries is the same wire
// call the high-level client makes, minus the unsupported flag.
func (d *driver) loggingWrite(ctx context.Context, logName, payload string) error {
	conn, err := dialGRPC(d.target("LOGGING_EMULATOR_HOST"))
	if err != nil {
		return fmt.Errorf("logging dial: %w", err)
	}
	defer conn.Close()
	client, err := logging.NewClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		return fmt.Errorf("logging client: %w", err)
	}
	defer client.Close()
	_, err = client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{{
			LogName: logName,
			Payload: &loggingpb.LogEntry_TextPayload{TextPayload: payload},
		}},
	})
	if err != nil {
		return fmt.Errorf("logging write: %w", err)
	}
	return nil
}

// loggingCount reads entries back through the official logadmin client, which
// is the read side that exposes Client.Entries.
func (d *driver) loggingCount(ctx context.Context, logName string) (int, error) {
	conn, err := dialGRPC(d.target("LOGGING_EMULATOR_HOST"))
	if err != nil {
		return 0, fmt.Errorf("logadmin dial: %w", err)
	}
	defer conn.Close()
	client, err := logadmin.NewClient(ctx, projectID(), option.WithGRPCConn(conn))
	if err != nil {
		return 0, fmt.Errorf("logadmin client: %w", err)
	}
	defer client.Close()

	it := client.Entries(ctx, logadmin.Filter(fmt.Sprintf("logName=%q", logName)))
	n := 0
	for {
		if _, err := it.Next(); err != nil {
			if err == iterator.Done {
				break
			}
			return 0, fmt.Errorf("logadmin next: %w", err)
		}
		n++
	}
	return n, nil
}

// loggingCreateMetric creates one logs-based metric through the official
// MetricsServiceV2 client.
func (d *driver) loggingCreateMetric(ctx context.Context, metricName string) error {
	conn, err := dialGRPC(d.target("LOGGING_EMULATOR_HOST"))
	if err != nil {
		return fmt.Errorf("logging metrics dial: %w", err)
	}
	defer conn.Close()
	client, err := logging.NewMetricsClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		return fmt.Errorf("logging metrics client: %w", err)
	}
	defer client.Close()
	_, err = client.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: fmt.Sprintf("projects/%s", projectID()),
		Metric: &loggingpb.LogMetric{
			Name:   metricName,
			Filter: "severity>=ERROR",
		},
	})
	if err != nil {
		return fmt.Errorf("create log metric: %w", err)
	}
	return nil
}

// loggingMetricPresent reports whether the named logs-based metric is readable
// (found=true) or absent (found=false, NotFound).
func (d *driver) loggingMetricPresent(ctx context.Context, metricName string) (bool, error) {
	conn, err := dialGRPC(d.target("LOGGING_EMULATOR_HOST"))
	if err != nil {
		return false, fmt.Errorf("logging metrics dial: %w", err)
	}
	defer conn.Close()
	client, err := logging.NewMetricsClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		return false, fmt.Errorf("logging metrics client: %w", err)
	}
	defer client.Close()
	_, err = client.GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{
		MetricName: fmt.Sprintf("projects/%s/metrics/%s", projectID(), metricName),
	})
	switch {
	case err == nil:
		return true, nil
	case status.Code(err) == codes.NotFound:
		return false, nil
	default:
		return false, fmt.Errorf("get log metric: %w", err)
	}
}

// seedLogging writes a log entry, a logs-based metric, and an Admin v2 log
// bucket, and returns checks that read them back after a restart and assert
// their absence after a reset.
func seedLogging(d *driver, suffix string) (func() error, func() error, func() error, error) {
	logName := fmt.Sprintf("projects/%s/logs/%s", projectID(), "parity-log-"+suffix)
	metricName := "parity-metric-" + suffix
	bucketID := "parity-bucket-" + suffix
	payload := "parity-seed-" + suffix

	seeded := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n, err := d.loggingCount(ctx, logName)
		if err != nil {
			return fmt.Errorf("logging read after seed: %w", err)
		}
		if n == 0 {
			return errors.New("log entry not visible immediately after write")
		}
		present, err := d.loggingMetricPresent(ctx, metricName)
		if err != nil {
			return fmt.Errorf("metric read after seed: %w", err)
		}
		if !present {
			return errors.New("log metric not visible immediately after create")
		}
		bucket, err := d.loggingBucketPresent(ctx, bucketID)
		if err != nil {
			return fmt.Errorf("bucket read after seed: %w", err)
		}
		if !bucket {
			return errors.New("log bucket not visible immediately after create")
		}
		return nil
	}
	survived := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n, err := d.loggingCount(ctx, logName)
		if err != nil {
			return fmt.Errorf("logging read after restart: %w", err)
		}
		if n == 0 {
			return errors.New("log entry not present after restart")
		}
		present, err := d.loggingMetricPresent(ctx, metricName)
		if err != nil {
			return fmt.Errorf("metric read after restart: %w", err)
		}
		if !present {
			return errors.New("log metric not present after restart")
		}
		bucket, err := d.loggingBucketPresent(ctx, bucketID)
		if err != nil {
			return fmt.Errorf("bucket read after restart: %w", err)
		}
		if !bucket {
			return errors.New("log bucket not present after restart")
		}
		return nil
	}
	cleared := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		n, err := d.loggingCount(ctx, logName)
		if err != nil {
			return fmt.Errorf("logging read after reset: %w", err)
		}
		if n != 0 {
			return fmt.Errorf("log entries still present after reset: %d", n)
		}
		present, err := d.loggingMetricPresent(ctx, metricName)
		if err != nil {
			return fmt.Errorf("metric read after reset: %w", err)
		}
		if present {
			return errors.New("log metric still present after reset")
		}
		bucket, err := d.loggingBucketPresent(ctx, bucketID)
		if err != nil {
			return fmt.Errorf("bucket read after reset: %w", err)
		}
		if bucket {
			return errors.New("log bucket still present after reset")
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := d.loggingWrite(ctx, logName, payload); err != nil {
		return nil, nil, nil, err
	}
	if err := d.loggingCreateMetric(ctx, metricName); err != nil {
		return nil, nil, nil, err
	}
	if err := d.loggingCreateBucket(ctx, bucketID); err != nil {
		return nil, nil, nil, err
	}
	if err := seeded(); err != nil {
		return nil, nil, nil, err
	}
	return seeded, survived, cleared, nil
}

// loggingCreateBucket creates one Admin v2 log bucket through the official
// ConfigServiceV2 client.
func (d *driver) loggingCreateBucket(ctx context.Context, bucketID string) error {
	conn, err := dialGRPC(d.target("LOGGING_EMULATOR_HOST"))
	if err != nil {
		return fmt.Errorf("logging config dial: %w", err)
	}
	defer conn.Close()
	client, err := logging.NewConfigClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		return fmt.Errorf("logging config client: %w", err)
	}
	defer client.Close()
	_, err = client.CreateBucket(ctx, &loggingpb.CreateBucketRequest{
		Parent:   fmt.Sprintf("projects/%s/locations/global", projectID()),
		BucketId: bucketID,
		Bucket:   &loggingpb.LogBucket{Description: "parity"},
	})
	if err != nil {
		return fmt.Errorf("create log bucket: %w", err)
	}
	return nil
}

// loggingBucketPresent reports whether the named log bucket is readable
// (present=true) or absent (present=false, NotFound).
func (d *driver) loggingBucketPresent(ctx context.Context, bucketID string) (bool, error) {
	conn, err := dialGRPC(d.target("LOGGING_EMULATOR_HOST"))
	if err != nil {
		return false, fmt.Errorf("logging config dial: %w", err)
	}
	defer conn.Close()
	client, err := logging.NewConfigClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		return false, fmt.Errorf("logging config client: %w", err)
	}
	defer client.Close()
	_, err = client.GetBucket(ctx, &loggingpb.GetBucketRequest{
		Name: fmt.Sprintf("projects/%s/locations/global/buckets/%s", projectID(), bucketID),
	})
	switch {
	case err == nil:
		return true, nil
	case status.Code(err) == codes.NotFound:
		return false, nil
	default:
		return false, fmt.Errorf("get log bucket: %w", err)
	}
}
