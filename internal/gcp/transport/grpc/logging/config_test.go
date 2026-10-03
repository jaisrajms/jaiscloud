package logging

import (
	"context"
	"net"
	"testing"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func configTestClient(t *testing.T) (loggingpb.ConfigServiceV2Client, func()) {
	t.Helper()
	svc := NewConfigService(core.NewService(loggingstore.NewMemoryStore(), "test"), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	loggingpb.RegisterConfigServiceV2Server(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return loggingpb.NewConfigServiceV2Client(conn), func() {
		conn.Close()
		srv.Stop()
	}
}

func TestGRPCConfigSinkCRUD(t *testing.T) {
	client, cleanup := configTestClient(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink: &loggingpb.LogSink{
			Name:        "grpc-sink",
			Destination: "storage.googleapis.com/bucket",
			Filter:      "severity>=WARNING",
		},
		UniqueWriterIdentity: true,
	})
	if err != nil {
		t.Fatalf("CreateSink: %v", err)
	}
	if created.GetName() != "grpc-sink" || created.GetWriterIdentity() == "" {
		t.Fatalf("created = %+v", created)
	}

	if _, err := client.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink:   &loggingpb.LogSink{Name: "grpc-sink", Destination: "d"},
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate CreateSink = %v, want AlreadyExists", err)
	}

	got, err := client.GetSink(ctx, &loggingpb.GetSinkRequest{SinkName: "projects/test/sinks/grpc-sink"})
	if err != nil {
		t.Fatalf("GetSink: %v", err)
	}
	if got.GetDestination() != "storage.googleapis.com/bucket" {
		t.Fatalf("destination = %q", got.GetDestination())
	}

	list, err := client.ListSinks(ctx, &loggingpb.ListSinksRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("ListSinks: %v", err)
	}
	if len(list.GetSinks()) != 1 || list.GetSinks()[0].GetName() != "grpc-sink" {
		t.Fatalf("ListSinks = %+v", list.GetSinks())
	}

	updated, err := client.UpdateSink(ctx, &loggingpb.UpdateSinkRequest{
		SinkName:   "projects/test/sinks/grpc-sink",
		Sink:       &loggingpb.LogSink{Filter: "severity>=ERROR"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"filter"}},
	})
	if err != nil {
		t.Fatalf("UpdateSink: %v", err)
	}
	if updated.GetFilter() != "severity>=ERROR" || updated.GetDestination() != "storage.googleapis.com/bucket" {
		t.Fatalf("updated = %+v", updated)
	}

	// A masked update that clears the required destination fails validation.
	if _, err := client.UpdateSink(ctx, &loggingpb.UpdateSinkRequest{
		SinkName:   "projects/test/sinks/grpc-sink",
		Sink:       &loggingpb.LogSink{},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"destination"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("masked empty destination = %v, want InvalidArgument", err)
	}

	if _, err := client.DeleteSink(ctx, &loggingpb.DeleteSinkRequest{SinkName: "projects/test/sinks/grpc-sink"}); err != nil {
		t.Fatalf("DeleteSink: %v", err)
	}
	if _, err := client.GetSink(ctx, &loggingpb.GetSinkRequest{SinkName: "projects/test/sinks/grpc-sink"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSink after delete = %v, want NotFound", err)
	}
}

func TestGRPCConfigExclusionCRUD(t *testing.T) {
	client, cleanup := configTestClient(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateExclusion(ctx, &loggingpb.CreateExclusionRequest{
		Parent:    "projects/test",
		Exclusion: &loggingpb.LogExclusion{Name: "grpc-excl", Filter: "severity<DEBUG"},
	})
	if err != nil {
		t.Fatalf("CreateExclusion: %v", err)
	}
	if created.GetName() != "grpc-excl" {
		t.Fatalf("created = %+v", created)
	}

	got, err := client.GetExclusion(ctx, &loggingpb.GetExclusionRequest{Name: "projects/test/exclusions/grpc-excl"})
	if err != nil {
		t.Fatalf("GetExclusion: %v", err)
	}
	if got.GetFilter() != "severity<DEBUG" {
		t.Fatalf("filter = %q", got.GetFilter())
	}

	updated, err := client.UpdateExclusion(ctx, &loggingpb.UpdateExclusionRequest{
		Name:       "projects/test/exclusions/grpc-excl",
		Exclusion:  &loggingpb.LogExclusion{Disabled: true},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"disabled"}},
	})
	if err != nil {
		t.Fatalf("UpdateExclusion: %v", err)
	}
	if !updated.GetDisabled() {
		t.Fatal("disabled not applied")
	}

	if _, err := client.DeleteExclusion(ctx, &loggingpb.DeleteExclusionRequest{Name: "projects/test/exclusions/grpc-excl"}); err != nil {
		t.Fatalf("DeleteExclusion: %v", err)
	}
	if _, err := client.GetExclusion(ctx, &loggingpb.GetExclusionRequest{Name: "projects/test/exclusions/grpc-excl"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetExclusion after delete = %v, want NotFound", err)
	}
}

func TestGRPCConfigInvalidRequests(t *testing.T) {
	client, cleanup := configTestClient(t)
	defer cleanup()
	ctx := context.Background()

	// Missing destination is InvalidArgument.
	if _, err := client.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink:   &loggingpb.LogSink{Name: "s"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateSink missing destination = %v, want InvalidArgument", err)
	}

	// An unsupported update_mask path fails loud.
	if _, err := client.CreateSink(ctx, &loggingpb.CreateSinkRequest{
		Parent: "projects/test",
		Sink:   &loggingpb.LogSink{Name: "s2", Destination: "d"},
	}); err != nil {
		t.Fatalf("CreateSink: %v", err)
	}
	if _, err := client.UpdateSink(ctx, &loggingpb.UpdateSinkRequest{
		SinkName:   "projects/test/sinks/s2",
		Sink:       &loggingpb.LogSink{},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"createTime"}},
	}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unsupported mask = %v, want Unimplemented", err)
	}

	// Unknown sink is NotFound.
	if _, err := client.GetSink(ctx, &loggingpb.GetSinkRequest{SinkName: "projects/test/sinks/nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSink missing = %v, want NotFound", err)
	}
}
