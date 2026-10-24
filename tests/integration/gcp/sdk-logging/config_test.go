package logging_test

import (
	"context"
	"fmt"
	"testing"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestLoggingSinksCRUD drives the Cloud Logging v2 ConfigServiceV2 sink surface
// through the official gRPC client: create, duplicate-create, get, list,
// masked update (merge), and delete + NotFound. Sinks are the routing config
// the write path evaluates; because the emulator performs no delivery, this
// suite pins the stored routing configuration rather than an export side effect.
func TestLoggingSinksCRUD(t *testing.T) {
	ctx := context.Background()
	c := ConfigClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()
	id := uniqueName("sink")
	fullName := parent + "/sinks/" + id

	created, err := c.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink: &loggingpb.LogSink{
			Name:        id,
			Destination: "storage.googleapis.com/my-export-bucket",
			Filter:      `severity>=ERROR`,
			Description: "errors go to GCS",
		},
	})
	require.NoError(t, err)
	// Compliant: the Logging v2 proto defines LogSink.name as the client-assigned
	// sink identifier ("my-syslog-errors-to-pubsub"), so the short id is the
	// correct value here (the logadmin client maps Sink.ID <-> LogSink.name).
	//
	// Known deferred fidelity gap (explicitly not accepted design): the
	// Discovery's output-only `resourceName` (the full "projects/{p}/sinks/{id}"
	// path) is emitted by the emulator's REST transport, but it is not a field of
	// the Logging v2 gRPC proto (loggingpb.LogSink has no resource_name in any
	// published revision), so the gRPC surface cannot carry it. A gRPC client
	// therefore has no way to observe resourceName; do not read this as gRPC/REST
	// parity.
	require.Equal(t, id, created.GetName())
	require.Equal(t, "storage.googleapis.com/my-export-bucket", created.GetDestination())
	require.Equal(t, `severity>=ERROR`, created.GetFilter())
	require.NotEmpty(t, created.GetWriterIdentity(), "a synthesized writer identity must be returned")
	require.NotNil(t, created.GetCreateTime())

	// A duplicate create is AlreadyExists.
	_, err = c.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink:   &loggingpb.LogSink{Name: id, Destination: "storage.googleapis.com/other"},
	})
	require.Equal(t, codes.AlreadyExists, status.Code(err))

	got, err := c.GetSink(ctx, &loggingpb.GetSinkRequest{SinkName: fullName})
	require.NoError(t, err)
	require.Equal(t, created.GetFilter(), got.GetFilter())
	require.Equal(t, created.GetWriterIdentity(), got.GetWriterIdentity())

	// ListSinks contains the created sink.
	require.True(t, sinkListContains(ctx, c, parent, fullName), "created sink must appear in ListSinks")

	// A masked update merges only the named fields: the filter changes, the
	// destination and writer identity are preserved.
	updated, err := c.UpdateSink(ctx, &loggingpb.UpdateSinkRequest{
		SinkName: fullName,
		Sink: &loggingpb.LogSink{
			Filter:      `severity>=WARNING`,
			Description: "retuned",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"filter", "description"}},
	})
	require.NoError(t, err)
	require.Equal(t, `severity>=WARNING`, updated.GetFilter())
	require.Equal(t, "retuned", updated.GetDescription())
	require.Equal(t, "storage.googleapis.com/my-export-bucket", updated.GetDestination(),
		"an unmasked destination must be preserved")
	require.Equal(t, created.GetWriterIdentity(), updated.GetWriterIdentity(),
		"writer identity is output-only and must survive an update")

	require.NoError(t, c.DeleteSink(ctx, &loggingpb.DeleteSinkRequest{SinkName: fullName}))
	_, err = c.GetSink(ctx, &loggingpb.GetSinkRequest{SinkName: fullName})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestLoggingSinkWriterIdentity verifies the three documented writer-identity
// modes: the shared Google-managed identity by default, a per-sink unique
// identity when requested, and an explicit custom identity when supplied.
func TestLoggingSinkWriterIdentity(t *testing.T) {
	ctx := context.Background()
	c := ConfigClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()

	shared, err := c.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink:   &loggingpb.LogSink{Name: uniqueName("sink-shared"), Destination: "storage.googleapis.com/b"},
	})
	require.NoError(t, err)
	require.Contains(t, shared.GetWriterIdentity(), "gcp-sa-logging.iam.gserviceaccount.com")

	unique, err := c.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent:               parent,
		Sink:                 &loggingpb.LogSink{Name: uniqueName("sink-unique"), Destination: "storage.googleapis.com/b"},
		UniqueWriterIdentity: true,
	})
	require.NoError(t, err)
	require.Contains(t, unique.GetWriterIdentity(), "gcp-sa-logging.iam.gserviceaccount.com")
	// The unique identity is scoped to the sink, so it differs from the shared one.
	require.NotEqual(t, shared.GetWriterIdentity(), unique.GetWriterIdentity())
	require.Contains(t, unique.GetWriterIdentity(), unique.GetName())
}

// TestLoggingSinkFilterValidation pins the routing-evaluation contract: the
// write path compiles each sink's filter with the shared filter engine, so a
// sink whose filter cannot be compiled is rejected at config time
// (InvalidArgument) rather than silently routing nothing. A missing destination
// and an unsupported output version format are likewise InvalidArgument.
func TestLoggingSinkFilterValidation(t *testing.T) {
	ctx := context.Background()
	c := ConfigClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()

	// Unsupported filter field (the filter grammar only knows logName,
	// severity, timestamp, resource.type, textPayload).
	_, err := c.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink:   &loggingpb.LogSink{Name: uniqueName("sink-bad-filter"), Destination: "storage.googleapis.com/b", Filter: "bogusField=1"},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "an uncompilable sink filter must be rejected")

	// Missing destination.
	_, err = c.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink:   &loggingpb.LogSink{Name: uniqueName("sink-no-dest")},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// Unsupported output version format.
	_, err = c.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink: &loggingpb.LogSink{
			Name: uniqueName("sink-bad-format"), Destination: "storage.googleapis.com/b",
			OutputVersionFormat: loggingpb.LogSink_VersionFormat(99),
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// TestLoggingExclusionsCRUD drives the resource-level exclusion surface:
// create, duplicate, get, list, a required-mask update, and delete + NotFound.
// An exclusion with an empty or uncompilable filter is InvalidArgument.
func TestLoggingExclusionsCRUD(t *testing.T) {
	ctx := context.Background()
	c := ConfigClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()
	id := uniqueName("excl")
	fullName := parent + "/exclusions/" + id

	created, err := c.CreateExclusion(ctx, &loggingpb.CreateExclusionRequest{
		Parent: parent,
		Exclusion: &loggingpb.LogExclusion{
			Name:        id,
			Description: "drop noise",
			Filter:      `textPayload:"healthz"`,
		},
	})
	require.NoError(t, err)
	require.Equal(t, id, created.GetName())
	require.Equal(t, `textPayload:"healthz"`, created.GetFilter())
	require.False(t, created.GetDisabled())
	require.NotNil(t, created.GetCreateTime())

	_, err = c.CreateExclusion(ctx, &loggingpb.CreateExclusionRequest{
		Parent:    parent,
		Exclusion: &loggingpb.LogExclusion{Name: id, Filter: `severity>=ERROR`},
	})
	require.Equal(t, codes.AlreadyExists, status.Code(err))

	got, err := c.GetExclusion(ctx, &loggingpb.GetExclusionRequest{Name: fullName})
	require.NoError(t, err)
	require.Equal(t, "drop noise", got.GetDescription())

	require.True(t, exclusionListContains(ctx, c, parent, fullName), "created exclusion must appear in ListExclusions")

	// UpdateExclusionRequest.update_mask is REQUIRED: an empty mask is rejected.
	_, err = c.UpdateExclusion(ctx, &loggingpb.UpdateExclusionRequest{
		Name:      fullName,
		Exclusion: &loggingpb.LogExclusion{Description: "no mask"},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	updated, err := c.UpdateExclusion(ctx, &loggingpb.UpdateExclusionRequest{
		Name:      fullName,
		Exclusion: &loggingpb.LogExclusion{Description: "drop more noise", Disabled: true},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"description", "disabled"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "drop more noise", updated.GetDescription())
	require.True(t, updated.GetDisabled())
	require.Equal(t, `textPayload:"healthz"`, updated.GetFilter(), "an unmasked filter must be preserved")

	require.NoError(t, c.DeleteExclusion(ctx, &loggingpb.DeleteExclusionRequest{Name: fullName}))
	_, err = c.GetExclusion(ctx, &loggingpb.GetExclusionRequest{Name: fullName})
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestLoggingExclusionFilterValidation pins the routing-evaluation contract for
// exclusions: an empty or uncompilable filter is InvalidArgument at config time.
func TestLoggingExclusionFilterValidation(t *testing.T) {
	ctx := context.Background()
	c := ConfigClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()

	_, err := c.CreateExclusion(ctx, &loggingpb.CreateExclusionRequest{
		Parent:    parent,
		Exclusion: &loggingpb.LogExclusion{Name: uniqueName("excl-empty")},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a missing exclusion filter must be rejected")

	_, err = c.CreateExclusion(ctx, &loggingpb.CreateExclusionRequest{
		Parent:    parent,
		Exclusion: &loggingpb.LogExclusion{Name: uniqueName("excl-bad"), Filter: "not a filter"},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "an uncompilable exclusion filter must be rejected")
}

// TestLoggingSinkRoutingNotDelivered pins the documented routing contract: the
// write path evaluates each sink/exclusion filter against the written entry but
// performs no delivery (there is no export destination). The observable
// behavior is that a matching write succeeds and the entry remains fully
// queryable through entries.list — routing is evaluated, never consumed.
func TestLoggingSinkRoutingNotDelivered(t *testing.T) {
	ctx := context.Background()
	lc := LoggingClient(ctx)
	defer lc.Close()
	cc := ConfigClient(ctx)
	defer cc.Close()

	project := ProjectID()
	parent := "projects/" + project
	logName := parent + "/logs/" + uniqueName("routing")
	t.Cleanup(func() { _ = lc.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: logName}) })

	_, err := cc.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent,
		Sink: &loggingpb.LogSink{
			Name:        uniqueName("sink-route"),
			Destination: "storage.googleapis.com/export-bucket",
			Filter:      `severity>=ERROR`,
		},
	})
	require.NoError(t, err)

	// A matching write succeeds (routing was evaluated) and does not error even
	// though nothing is delivered.
	_, err = lc.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{{
			LogName:  logName,
			Resource: &mrpb.MonitoredResource{Type: "global"},
			Severity: ltype.LogSeverity_ERROR,
			Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: "routed but not delivered"},
		}},
	})
	require.NoError(t, err)

	// The entry is still queryable: nothing was consumed or exported.
	it := lc.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{parent},
		Filter:        fmt.Sprintf("logName=%q", logName),
	})
	got, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, "routed but not delivered", got.GetTextPayload())
}

func sinkListContains(ctx context.Context, c *logging.ConfigClient, parent, fullName string) bool {
	it := c.ListSinks(ctx, &loggingpb.ListSinksRequest{Parent: parent, PageSize: 1000})
	for {
		s, err := it.Next()
		if err == iterator.Done {
			return false
		}
		if err != nil {
			return false
		}
		if parent+"/sinks/"+s.GetName() == fullName || s.GetName() == fullName {
			return true
		}
	}
}

func exclusionListContains(ctx context.Context, c *logging.ConfigClient, parent, fullName string) bool {
	it := c.ListExclusions(ctx, &loggingpb.ListExclusionsRequest{Parent: parent, PageSize: 1000})
	for {
		e, err := it.Next()
		if err == iterator.Done {
			return false
		}
		if err != nil {
			return false
		}
		if parent+"/exclusions/"+e.GetName() == fullName || e.GetName() == fullName {
			return true
		}
	}
}

// TestLoggingConfigListNonPositivePageSize closes DC21 on the official gRPC
// client: the config/admin lists whose Discovery documents say "Non-positive
// values are ignored" (sinks, exclusions, buckets, views) must answer the
// default page for PageSize=-1 instead of INVALID_ARGUMENT. links.list is the
// one config list without that documented exception, so it keeps AIP-158's
// rejection.
func TestLoggingConfigListNonPositivePageSize(t *testing.T) {
	ctx := context.Background()
	c := ConfigClient(ctx)
	defer c.Close()

	parent := "projects/" + ProjectID()
	location := parent + "/locations/global"
	bucketID := uniqueName("list-page-size")
	bucket := location + "/buckets/" + bucketID

	_, err := c.CreateBucket(ctx, &loggingpb.CreateBucketRequest{
		Parent: location, BucketId: bucketID, Bucket: &loggingpb.LogBucket{Description: "d"},
	})
	require.NoError(t, err)
	_, err = c.CreateView(ctx, &loggingpb.CreateViewRequest{
		Parent: bucket, ViewId: "v1", View: &loggingpb.LogView{Filter: "severity>=ERROR"},
	})
	require.NoError(t, err)

	sinkID := uniqueName("page-size-sink")
	_, err = c.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: parent, Sink: &loggingpb.LogSink{Name: sinkID, Destination: "storage.googleapis.com/bucket"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = c.DeleteSink(ctx, &loggingpb.DeleteSinkRequest{SinkName: parent + "/sinks/" + sinkID})
	})

	exclusionID := uniqueName("page-size-excl")
	_, err = c.CreateExclusion(ctx, &loggingpb.CreateExclusionRequest{
		Parent: parent, Exclusion: &loggingpb.LogExclusion{Name: exclusionID, Filter: "severity<DEBUG"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = c.DeleteExclusion(ctx, &loggingpb.DeleteExclusionRequest{Name: parent + "/exclusions/" + exclusionID})
	})

	// Every ignore-the-non-positive list must return its default page with the
	// seeded resource, not an empty page and not an error.
	sink, err := c.ListSinks(ctx, &loggingpb.ListSinksRequest{Parent: parent, PageSize: -1}).Next()
	require.NoError(t, err)
	require.NotNil(t, sink)

	exclusion, err := c.ListExclusions(ctx, &loggingpb.ListExclusionsRequest{Parent: parent, PageSize: -1}).Next()
	require.NoError(t, err)
	require.NotNil(t, exclusion)

	gotBucket, err := c.ListBuckets(ctx, &loggingpb.ListBucketsRequest{Parent: location, PageSize: -1}).Next()
	require.NoError(t, err)
	require.Equal(t, bucket, gotBucket.GetName())

	view, err := c.ListViews(ctx, &loggingpb.ListViewsRequest{Parent: bucket, PageSize: -1}).Next()
	require.NoError(t, err)
	require.Equal(t, bucket+"/views/v1", view.GetName())

	if _, err := c.ListLinks(ctx, &loggingpb.ListLinksRequest{Parent: bucket, PageSize: -1}).Next(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("links PageSize=-1 err = %v, want InvalidArgument", err)
	}
}
