package grpcconformance

import (
	"context"
	"fmt"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// loggingChecks covers the Cloud Logging v2 surface
// (google.logging.v2.LoggingServiceV2) via the official generated
// cloud.google.com/go/logging/apiv2 client.
//
// The high-level cloud.google.com/go/logging client buffers Logger.Log and
// flushes WriteLogEntries asynchronously, which would make a check
// nondeterministic; the generated client issues each RPC synchronously (its
// list RPCs are client-side synchronous iterators), so every probe below is
// deterministic.
//
// The probes share one run-unique log: WriteLogEntries leaves the entry in place
// for ListLogEntries and ListLogs, and DeleteLog removes it last. Names are
// run-unique via cfg.ResourceName, so a long-lived emulator never sees
// cross-run collisions.
//
// TailLogEntries (bidirectional streaming) is intentionally not covered: the
// emulator serves a bounded, store-polling approximation whose delivery latency
// is derived from buffer_window, so there is no deterministic, non-flaky
// assertion available (a timing-dependent probe would either flake or have to
// treat a timeout as success). It stays unverified and therefore `limited` in
// the fidelity matrix.
func loggingChecks() []Check {
	return []Check{
		{Service: "logging", RPC: "WriteLogEntries", Method: "WriteLogEntries", KeyField: "entry round-trips (logName/textPayload)", Run: checkLoggingWrite},
		{Service: "logging", RPC: "ListLogEntries", Method: "ListLogEntries", KeyField: "entries[].logName/textPayload", Run: checkLoggingListEntries},
		{Service: "logging", RPC: "ListLogs", Method: "ListLogs", KeyField: "logNames[]", Run: checkLoggingListLogs},
		{Service: "logging", RPC: "ListMonitoredResourceDescriptors", Method: "ListMonitoredResourceDescriptors", KeyField: "resourceDescriptors[].type/displayName/labels", Run: checkLoggingListDescriptors},
		{Service: "logging", RPC: "DeleteLog", Method: "DeleteLog", KeyField: "log absent from ListLogs after delete", Run: checkLoggingDelete},
	}
}

// loggingConfigChecks covers the Cloud Logging v2 config plane
// (google.logging.v2.ConfigServiceV2) for sinks and exclusions through the
// official generated logging ConfigClient. The probes run create -> get ->
// list -> update -> delete and share one run-unique sink and exclusion, so the
// sequence is self-contained and idempotent.
func loggingConfigChecks() []Check {
	return []Check{
		{Service: "logging", RPC: "CreateSink", KeyField: "sink name/destination/writerIdentity round-trip", Run: checkLoggingCreateSink},
		{Service: "logging", RPC: "GetSink", KeyField: "sink by full resource name", Run: checkLoggingGetSink},
		{Service: "logging", RPC: "ListSinks", KeyField: "sinks[] contains the created sink", Run: checkLoggingListSinks},
		{Service: "logging", RPC: "UpdateSink", KeyField: "masked filter update preserves destination", Run: checkLoggingUpdateSink},
		{Service: "logging", RPC: "DeleteSink", KeyField: "sink absent from ListSinks after delete", Run: checkLoggingDeleteSink},
		{Service: "logging", RPC: "CreateExclusion", KeyField: "exclusion filter round-trip", Run: checkLoggingCreateExclusion},
		{Service: "logging", RPC: "GetExclusion", KeyField: "exclusion by full resource name", Run: checkLoggingGetExclusion},
		{Service: "logging", RPC: "ListExclusions", KeyField: "exclusions[] contains the created exclusion", Run: checkLoggingListExclusions},
		{Service: "logging", RPC: "UpdateExclusion", KeyField: "masked disabled update", Run: checkLoggingUpdateExclusion},
		{Service: "logging", RPC: "DeleteExclusion", KeyField: "exclusion absent from ListExclusions after delete", Run: checkLoggingDeleteExclusion},
	}
}

// loggingMetricsChecks covers the Cloud Logging v2 logs-based metric plane
// (google.logging.v2.MetricsServiceV2) through the official generated logging
// MetricsClient. The probes run create -> get -> list -> update -> delete and
// share one run-unique metric, so the sequence is self-contained and idempotent.
func loggingMetricsChecks() []Check {
	return []Check{
		{Service: "logging", RPC: "CreateLogMetric", KeyField: "metric descriptor type/valueType round-trip", Run: checkLoggingCreateMetric},
		{Service: "logging", RPC: "GetLogMetric", KeyField: "metric by full resource name", Run: checkLoggingGetMetric},
		{Service: "logging", RPC: "ListLogMetrics", KeyField: "metrics[] contains the created metric", Run: checkLoggingListMetrics},
		{Service: "logging", RPC: "UpdateLogMetric", KeyField: "filter update preserves descriptor type", Run: checkLoggingUpdateMetric},
		{Service: "logging", RPC: "UpdateLogMetric (create)", Method: "UpdateLogMetric", KeyField: "update on an absent metric creates it", Run: checkLoggingUpsertMetric},
		{Service: "logging", RPC: "DeleteLogMetric", KeyField: "metric absent from ListLogMetrics after delete", Run: checkLoggingDeleteMetric},
	}
}

// newLoggingMetricsClient dials the emulator and returns the official generated
// Logging metrics client.
func newLoggingMetricsClient(ctx context.Context, cfg Config) (*logging.MetricsClient, error) {
	return logging.NewMetricsClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func loggingMetricID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-metric") }

func loggingMetricName(cfg Config) string {
	return fmt.Sprintf("%s/metrics/%s", loggingParent(cfg), loggingMetricID(cfg))
}

func checkLoggingCreateMetric(ctx context.Context, cfg Config) error {
	client, err := newLoggingMetricsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new metrics client: %w", err)
	}
	defer client.Close()

	m, err := client.CreateLogMetric(ctx, &loggingpb.CreateLogMetricRequest{
		Parent: loggingParent(cfg),
		Metric: &loggingpb.LogMetric{
			Name:            loggingMetricID(cfg),
			Description:     "conformance",
			Filter:          "severity>=ERROR",
			LabelExtractors: map[string]string{"code": "EXTRACT(jsonPayload.code)"},
			MetricDescriptor: &metricpb.MetricDescriptor{
				ValueType: metricpb.MetricDescriptor_INT64,
				Labels:    []*labelpb.LabelDescriptor{{Key: "code", ValueType: labelpb.LabelDescriptor_INT64}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateLogMetric: %w", err)
	}
	if m.GetName() != loggingMetricID(cfg) {
		return fmt.Errorf("metric name = %q, want %q", m.GetName(), loggingMetricID(cfg))
	}
	if want := "logging.googleapis.com/user/" + loggingMetricID(cfg); m.GetMetricDescriptor().GetType() != want {
		return fmt.Errorf("metric descriptor type = %q, want %q", m.GetMetricDescriptor().GetType(), want)
	}
	if m.GetMetricDescriptor().GetValueType() != metricpb.MetricDescriptor_INT64 {
		return fmt.Errorf("metric descriptor valueType = %v, want INT64", m.GetMetricDescriptor().GetValueType())
	}
	if m.GetCreateTime() == nil {
		return fmt.Errorf("metric createTime is unset")
	}
	return nil
}

func checkLoggingGetMetric(ctx context.Context, cfg Config) error {
	client, err := newLoggingMetricsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new metrics client: %w", err)
	}
	defer client.Close()

	m, err := client.GetLogMetric(ctx, &loggingpb.GetLogMetricRequest{MetricName: loggingMetricName(cfg)})
	if err != nil {
		return fmt.Errorf("GetLogMetric: %w", err)
	}
	if m.GetFilter() != "severity>=ERROR" {
		return fmt.Errorf("metric filter = %q, want %q", m.GetFilter(), "severity>=ERROR")
	}
	return nil
}

func checkLoggingListMetrics(ctx context.Context, cfg Config) error {
	client, err := newLoggingMetricsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new metrics client: %w", err)
	}
	defer client.Close()

	it := client.ListLogMetrics(ctx, &loggingpb.ListLogMetricsRequest{Parent: loggingParent(cfg)})
	for {
		m, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("created metric %q not listed", loggingMetricID(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListLogMetrics: %w", err)
		}
		if m.GetName() == loggingMetricID(cfg) {
			return nil
		}
	}
}

func checkLoggingUpdateMetric(ctx context.Context, cfg Config) error {
	client, err := newLoggingMetricsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new metrics client: %w", err)
	}
	defer client.Close()

	updated, err := client.UpdateLogMetric(ctx, &loggingpb.UpdateLogMetricRequest{
		MetricName: loggingMetricName(cfg),
		Metric: &loggingpb.LogMetric{
			Filter:          "severity>=WARNING",
			LabelExtractors: map[string]string{"code": "EXTRACT(jsonPayload.code)"},
			MetricDescriptor: &metricpb.MetricDescriptor{
				ValueType: metricpb.MetricDescriptor_INT64,
				Labels:    []*labelpb.LabelDescriptor{{Key: "code", ValueType: labelpb.LabelDescriptor_INT64}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateLogMetric: %w", err)
	}
	if updated.GetFilter() != "severity>=WARNING" {
		return fmt.Errorf("updated filter = %q, want %q", updated.GetFilter(), "severity>=WARNING")
	}
	if want := "logging.googleapis.com/user/" + loggingMetricID(cfg); updated.GetMetricDescriptor().GetType() != want {
		return fmt.Errorf("update changed descriptor type: %q, want %q", updated.GetMetricDescriptor().GetType(), want)
	}
	return nil
}

// checkLoggingUpsertMetric verifies the API's create-or-update contract: an
// UpdateLogMetric against a metric that does not exist creates it.
func checkLoggingUpsertMetric(ctx context.Context, cfg Config) error {
	client, err := newLoggingMetricsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new metrics client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-metric-upsert")
	created, err := client.UpdateLogMetric(ctx, &loggingpb.UpdateLogMetricRequest{
		MetricName: fmt.Sprintf("%s/metrics/%s", loggingParent(cfg), id),
		Metric:     &loggingpb.LogMetric{Filter: "severity>=ERROR"},
	})
	if err != nil {
		return fmt.Errorf("UpdateLogMetric (create): %w", err)
	}
	if created.GetName() != id {
		return fmt.Errorf("upserted metric name = %q, want %q", created.GetName(), id)
	}
	if created.GetFilter() != "severity>=ERROR" {
		return fmt.Errorf("upserted metric filter = %q, want %q", created.GetFilter(), "severity>=ERROR")
	}
	return nil
}

func checkLoggingDeleteMetric(ctx context.Context, cfg Config) error {
	client, err := newLoggingMetricsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new metrics client: %w", err)
	}
	defer client.Close()

	if err := client.DeleteLogMetric(ctx, &loggingpb.DeleteLogMetricRequest{MetricName: loggingMetricName(cfg)}); err != nil {
		return fmt.Errorf("DeleteLogMetric: %w", err)
	}
	it := client.ListLogMetrics(ctx, &loggingpb.ListLogMetricsRequest{Parent: loggingParent(cfg)})
	for {
		m, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ListLogMetrics after DeleteLogMetric: %w", err)
		}
		if m.GetName() == loggingMetricID(cfg) {
			return fmt.Errorf("ListLogMetrics still lists %q after DeleteLogMetric", loggingMetricID(cfg))
		}
	}
}

// newLoggingConfigClient dials the emulator and returns the official generated
// Logging config client.
func newLoggingConfigClient(ctx context.Context, cfg Config) (*logging.ConfigClient, error) {
	return logging.NewConfigClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func loggingSinkID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-sink") }

func loggingSinkDestination(cfg Config) string {
	return "storage.googleapis.com/" + cfg.ResourceName("gcpc-grpc-sink-bucket")
}

func loggingSinkName(cfg Config) string {
	return fmt.Sprintf("%s/sinks/%s", loggingParent(cfg), loggingSinkID(cfg))
}

func loggingExclusionID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-exclusion") }

func loggingExclusionName(cfg Config) string {
	return fmt.Sprintf("%s/exclusions/%s", loggingParent(cfg), loggingExclusionID(cfg))
}

func checkLoggingCreateSink(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	sink, err := client.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: loggingParent(cfg),
		Sink: &loggingpb.LogSink{
			Name:        loggingSinkID(cfg),
			Destination: loggingSinkDestination(cfg),
			Filter:      "severity>=WARNING",
			Description: "conformance",
		},
		UniqueWriterIdentity: true,
	})
	if err != nil {
		return fmt.Errorf("CreateSink: %w", err)
	}
	if sink.GetName() != loggingSinkID(cfg) {
		return fmt.Errorf("sink name = %q, want %q", sink.GetName(), loggingSinkID(cfg))
	}
	if sink.GetDestination() != loggingSinkDestination(cfg) {
		return fmt.Errorf("sink destination = %q, want %q", sink.GetDestination(), loggingSinkDestination(cfg))
	}
	if sink.GetWriterIdentity() == "" {
		return fmt.Errorf("sink writerIdentity is empty")
	}
	return nil
}

func checkLoggingGetSink(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	sink, err := client.GetSink(ctx, &loggingpb.GetSinkRequest{SinkName: loggingSinkName(cfg)})
	if err != nil {
		return fmt.Errorf("GetSink: %w", err)
	}
	if sink.GetFilter() != "severity>=WARNING" {
		return fmt.Errorf("sink filter = %q, want %q", sink.GetFilter(), "severity>=WARNING")
	}
	return nil
}

func checkLoggingListSinks(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	it := client.ListSinks(ctx, &loggingpb.ListSinksRequest{Parent: loggingParent(cfg)})
	for {
		sink, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("created sink %q not listed", loggingSinkID(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListSinks: %w", err)
		}
		if sink.GetName() == loggingSinkID(cfg) {
			return nil
		}
	}
}

func checkLoggingUpdateSink(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	updated, err := client.UpdateSink(ctx, &loggingpb.UpdateSinkRequest{
		SinkName:   loggingSinkName(cfg),
		Sink:       &loggingpb.LogSink{Filter: "severity>=ERROR"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"filter"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateSink: %w", err)
	}
	if updated.GetFilter() != "severity>=ERROR" {
		return fmt.Errorf("updated filter = %q, want %q", updated.GetFilter(), "severity>=ERROR")
	}
	if updated.GetDestination() != loggingSinkDestination(cfg) {
		return fmt.Errorf("masked update cleared destination: %q", updated.GetDestination())
	}
	return nil
}

func checkLoggingDeleteSink(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	if err := client.DeleteSink(ctx, &loggingpb.DeleteSinkRequest{SinkName: loggingSinkName(cfg)}); err != nil {
		return fmt.Errorf("DeleteSink: %w", err)
	}
	it := client.ListSinks(ctx, &loggingpb.ListSinksRequest{Parent: loggingParent(cfg)})
	for {
		sink, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ListSinks after DeleteSink: %w", err)
		}
		if sink.GetName() == loggingSinkID(cfg) {
			return fmt.Errorf("ListSinks still lists %q after DeleteSink", loggingSinkID(cfg))
		}
	}
}

func checkLoggingCreateExclusion(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	e, err := client.CreateExclusion(ctx, &loggingpb.CreateExclusionRequest{
		Parent: loggingParent(cfg),
		Exclusion: &loggingpb.LogExclusion{
			Name:        loggingExclusionID(cfg),
			Filter:      "severity<DEBUG",
			Description: "conformance",
		},
	})
	if err != nil {
		return fmt.Errorf("CreateExclusion: %w", err)
	}
	if e.GetName() != loggingExclusionID(cfg) {
		return fmt.Errorf("exclusion name = %q, want %q", e.GetName(), loggingExclusionID(cfg))
	}
	if e.GetFilter() != "severity<DEBUG" {
		return fmt.Errorf("exclusion filter = %q, want %q", e.GetFilter(), "severity<DEBUG")
	}
	return nil
}

func checkLoggingGetExclusion(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	e, err := client.GetExclusion(ctx, &loggingpb.GetExclusionRequest{Name: loggingExclusionName(cfg)})
	if err != nil {
		return fmt.Errorf("GetExclusion: %w", err)
	}
	if e.GetFilter() != "severity<DEBUG" {
		return fmt.Errorf("exclusion filter = %q, want %q", e.GetFilter(), "severity<DEBUG")
	}
	return nil
}

func checkLoggingListExclusions(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	it := client.ListExclusions(ctx, &loggingpb.ListExclusionsRequest{Parent: loggingParent(cfg)})
	for {
		e, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("created exclusion %q not listed", loggingExclusionID(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListExclusions: %w", err)
		}
		if e.GetName() == loggingExclusionID(cfg) {
			return nil
		}
	}
}

func checkLoggingUpdateExclusion(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	updated, err := client.UpdateExclusion(ctx, &loggingpb.UpdateExclusionRequest{
		Name:       loggingExclusionName(cfg),
		Exclusion:  &loggingpb.LogExclusion{Disabled: true},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"disabled"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateExclusion: %w", err)
	}
	if !updated.GetDisabled() {
		return fmt.Errorf("updated exclusion disabled = false, want true")
	}
	if updated.GetFilter() != "severity<DEBUG" {
		return fmt.Errorf("masked update cleared filter: %q", updated.GetFilter())
	}
	return nil
}

func checkLoggingDeleteExclusion(ctx context.Context, cfg Config) error {
	client, err := newLoggingConfigClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new config client: %w", err)
	}
	defer client.Close()

	if err := client.DeleteExclusion(ctx, &loggingpb.DeleteExclusionRequest{Name: loggingExclusionName(cfg)}); err != nil {
		return fmt.Errorf("DeleteExclusion: %w", err)
	}
	it := client.ListExclusions(ctx, &loggingpb.ListExclusionsRequest{Parent: loggingParent(cfg)})
	for {
		e, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ListExclusions after DeleteExclusion: %w", err)
		}
		if e.GetName() == loggingExclusionID(cfg) {
			return fmt.Errorf("ListExclusions still lists %q after DeleteExclusion", loggingExclusionID(cfg))
		}
	}
}

// newLoggingClient dials the emulator and returns the official generated
// Logging client. It mirrors the KMS/Secret Manager probes: an explicit
// insecure endpoint with authentication disabled.
func newLoggingClient(ctx context.Context, cfg Config) (*logging.Client, error) {
	return logging.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func loggingParent(cfg Config) string { return "projects/" + cfg.Project }

func loggingLogID(cfg Config) string { return cfg.ResourceName("gcpc-grpc-log") }

func loggingLogName(cfg Config) string {
	return fmt.Sprintf("%s/logs/%s", loggingParent(cfg), loggingLogID(cfg))
}

// loggingEntryText is the run-unique text payload shared by the write and the
// reads that round-trip it.
func loggingEntryText(cfg Config) string { return cfg.ResourceName("gcpc-grpc-entry") }

// collectLogEntries drains a ListLogEntries iterator, failing on the first page
// error.
func collectLogEntries(it *logging.LogEntryIterator) ([]*loggingpb.LogEntry, error) {
	var entries []*loggingpb.LogEntry
	for {
		e, err := it.Next()
		if err == iterator.Done {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
}

// Check 1: WriteLogEntries must accept a run-unique entry and make it
// immediately readable back through ListLogEntries (a synchronous round-trip,
// so the probe does not depend on any async flush).
func checkLoggingWrite(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	resp, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		LogName: loggingLogName(cfg),
		Resource: &monitoredres.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": cfg.Project},
		},
		Entries: []*loggingpb.LogEntry{{
			LogName:  loggingLogName(cfg),
			Severity: ltype.LogSeverity_INFO,
			Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: loggingEntryText(cfg)},
		}},
	})
	if err != nil {
		return fmt.Errorf("WriteLogEntries: %w", err)
	}
	if resp == nil {
		return fmt.Errorf("WriteLogEntries returned a nil response")
	}

	it := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{loggingParent(cfg)},
		Filter:        fmt.Sprintf("logName=%q", loggingLogName(cfg)),
	})
	entries, err := collectLogEntries(it)
	if err != nil {
		return fmt.Errorf("round-trip ListLogEntries: %w", err)
	}
	if len(entries) != 1 {
		return fmt.Errorf("round-trip ListLogEntries returned %d entries, want 1", len(entries))
	}
	if got := entries[0].GetLogName(); got != loggingLogName(cfg) {
		return fmt.Errorf("round-tripped logName = %q, want %q", got, loggingLogName(cfg))
	}
	if got := entries[0].GetTextPayload(); got != loggingEntryText(cfg) {
		return fmt.Errorf("round-tripped textPayload = %q, want %q", got, loggingEntryText(cfg))
	}
	return nil
}

// Check 2: ListLogEntries filtered by the run-unique log name must return
// exactly the entry written by check 1, with its log name and payload intact.
func checkLoggingListEntries(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{loggingParent(cfg)},
		Filter:        fmt.Sprintf("logName=%q", loggingLogName(cfg)),
	})
	entries, err := collectLogEntries(it)
	if err != nil {
		return fmt.Errorf("ListLogEntries: %w", err)
	}
	if len(entries) != 1 {
		return fmt.Errorf("ListLogEntries returned %d entries, want 1", len(entries))
	}
	e := entries[0]
	if e.GetLogName() != loggingLogName(cfg) {
		return fmt.Errorf("ListLogEntries logName = %q, want %q", e.GetLogName(), loggingLogName(cfg))
	}
	if e.GetTextPayload() != loggingEntryText(cfg) {
		return fmt.Errorf("ListLogEntries textPayload = %q, want %q", e.GetTextPayload(), loggingEntryText(cfg))
	}
	if e.GetSeverity() != ltype.LogSeverity_INFO {
		return fmt.Errorf("ListLogEntries severity = %v, want INFO", e.GetSeverity())
	}
	return nil
}

// Check 3: ListLogs under the project parent must include the run-unique log
// name created by check 1.
func checkLoggingListLogs(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: loggingParent(cfg)})
	for {
		name, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListLogs did not include %q", loggingLogName(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListLogs: %w", err)
		}
		if name == loggingLogName(cfg) {
			return nil
		}
	}
}

// Check 4: ListMonitoredResourceDescriptors must return a non-empty catalog
// with the expected shape: every descriptor carries a type, display name and
// label set, and (unlike Cloud Monitoring) leaves the resource name unset.
func checkLoggingListDescriptors(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.ListMonitoredResourceDescriptors(ctx, &loggingpb.ListMonitoredResourceDescriptorsRequest{})
	count := 0
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListMonitoredResourceDescriptors: %w", err)
		}
		count++
		if d.GetType() == "" {
			return fmt.Errorf("descriptor %d has an empty type", count)
		}
		if d.GetDisplayName() == "" {
			return fmt.Errorf("descriptor %q has an empty display_name", d.GetType())
		}
		if len(d.GetLabels()) == 0 {
			return fmt.Errorf("descriptor %q has no labels", d.GetType())
		}
		if d.GetName() != "" {
			return fmt.Errorf("Logging descriptor %q unexpectedly set name %q", d.GetType(), d.GetName())
		}
	}
	if count == 0 {
		return fmt.Errorf("ListMonitoredResourceDescriptors returned no descriptors")
	}
	return nil
}

// Check 5: DeleteLog must succeed, and a follow-up ListLogs must no longer
// list the log. Runs last because it removes the shared run-unique log.
func checkLoggingDelete(ctx context.Context, cfg Config) error {
	client, err := newLoggingClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := client.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: loggingLogName(cfg)}); err != nil {
		return fmt.Errorf("DeleteLog: %w", err)
	}

	it := client.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: loggingParent(cfg)})
	for {
		name, err := it.Next()
		if err == iterator.Done {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ListLogs after DeleteLog: %w", err)
		}
		if name == loggingLogName(cfg) {
			return fmt.Errorf("ListLogs still lists %q after DeleteLog", loggingLogName(cfg))
		}
	}
}
