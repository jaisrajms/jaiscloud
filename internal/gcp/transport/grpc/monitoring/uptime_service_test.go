package monitoring

import (
	"context"
	"net"
	"testing"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/monitoring"
	"jaiscloud/internal/store"
)

// testUptimeCheckServer starts the UptimeCheckService over an in-memory shared
// resource store and returns a client plus a cleanup func.
func testUptimeCheckServer(t *testing.T) (monitoringpb.UptimeCheckServiceClient, func()) {
	t.Helper()
	svc := NewService(core.NewService(nil, "test", core.WithResources(store.NewMemoryResourceStore())), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	monitoringpb.RegisterUptimeCheckServiceServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return monitoringpb.NewUptimeCheckServiceClient(conn), func() { conn.Close(); srv.Stop() }
}

func testUptimeCheckConfig(displayName string) *monitoringpb.UptimeCheckConfig {
	return &monitoringpb.UptimeCheckConfig{
		DisplayName: displayName,
		Resource: &monitoringpb.UptimeCheckConfig_MonitoredResource{
			MonitoredResource: &monitoredrespb.MonitoredResource{
				Type:   "uptime_url",
				Labels: map[string]string{"host": "shop.example.com"},
			},
		},
		CheckRequestType: &monitoringpb.UptimeCheckConfig_HttpCheck_{
			HttpCheck: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/healthz", UseSsl: true},
		},
		Timeout:         durationpb.New(10_000_000_000),
		Period:          durationpb.New(300_000_000_000),
		SelectedRegions: []monitoringpb.UptimeCheckRegion{monitoringpb.UptimeCheckRegion_USA},
	}
}

// TestGRPCUptimeCheckCRUD drives the UptimeCheckService through the official
// client: create/get/list/update/delete round-trip the declaration.
func TestGRPCUptimeCheckCRUD(t *testing.T) {
	uc, cleanup := testUptimeCheckServer(t)
	defer cleanup()
	ctx := context.Background()

	created, err := uc.CreateUptimeCheckConfig(ctx, &monitoringpb.CreateUptimeCheckConfigRequest{
		Parent:            "projects/test",
		UptimeCheckConfig: testUptimeCheckConfig("checkout"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetName() == "" || created.GetDisplayName() != "checkout" {
		t.Fatalf("created = %+v, want a name and display name checkout", created)
	}
	if got := created.GetTimeout().AsDuration().Seconds(); got != 10 {
		t.Fatalf("timeout = %vs, want 10s", got)
	}
	httpCheck := created.GetHttpCheck()
	if httpCheck.GetPath() != "/healthz" || !httpCheck.GetUseSsl() {
		t.Fatalf("httpCheck = %+v, want the declared path/useSsl", httpCheck)
	}
	if len(created.GetSelectedRegions()) != 1 || created.GetSelectedRegions()[0] != monitoringpb.UptimeCheckRegion_USA {
		t.Fatalf("selectedRegions = %v, want USA", created.GetSelectedRegions())
	}

	got, err := uc.GetUptimeCheckConfig(ctx, &monitoringpb.GetUptimeCheckConfigRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetDisplayName() != "checkout" {
		t.Fatalf("get = %+v, want the created config", got)
	}

	list, err := uc.ListUptimeCheckConfigs(ctx, &monitoringpb.ListUptimeCheckConfigsRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetUptimeCheckConfigs()) != 1 {
		t.Fatalf("list = %d configs, want 1", len(list.GetUptimeCheckConfigs()))
	}

	// A masked update touches only the named field.
	updated, err := uc.UpdateUptimeCheckConfig(ctx, &monitoringpb.UpdateUptimeCheckConfigRequest{
		UptimeCheckConfig: &monitoringpb.UptimeCheckConfig{Name: created.GetName(), DisplayName: "checkout-v2"},
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.GetDisplayName() != "checkout-v2" {
		t.Fatalf("updated display name = %q, want checkout-v2", updated.GetDisplayName())
	}
	if updated.GetHttpCheck().GetPath() != "/healthz" {
		t.Fatalf("updated httpCheck = %+v, want the unmasked path preserved", updated.GetHttpCheck())
	}

	if _, err := uc.DeleteUptimeCheckConfig(ctx, &monitoringpb.DeleteUptimeCheckConfigRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = uc.GetUptimeCheckConfig(ctx, &monitoringpb.GetUptimeCheckConfigRequest{Name: created.GetName()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete code = %v, want NotFound", status.Code(err))
	}
}

// TestGRPCUptimeCheckErrors pins the status codes both transports share.
func TestGRPCUptimeCheckErrors(t *testing.T) {
	uc, cleanup := testUptimeCheckServer(t)
	defer cleanup()
	ctx := context.Background()

	// Missing display name is INVALID_ARGUMENT (the field is Required).
	_, err := uc.CreateUptimeCheckConfig(ctx, &monitoringpb.CreateUptimeCheckConfigRequest{
		Parent:            "projects/test",
		UptimeCheckConfig: &monitoringpb.UptimeCheckConfig{Timeout: durationpb.New(10_000_000_000)},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("create without display name code = %v, want InvalidArgument", status.Code(err))
	}

	// An unsupported update_mask path is a client error.
	created, err := uc.CreateUptimeCheckConfig(ctx, &monitoringpb.CreateUptimeCheckConfigRequest{
		Parent:            "projects/test",
		UptimeCheckConfig: testUptimeCheckConfig("masked"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = uc.UpdateUptimeCheckConfig(ctx, &monitoringpb.UpdateUptimeCheckConfigRequest{
		UptimeCheckConfig: &monitoringpb.UptimeCheckConfig{Name: created.GetName(), DisplayName: "x"},
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"nonsense"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown mask path code = %v, want InvalidArgument", status.Code(err))
	}

	// The probe-derived checker address list is UNIMPLEMENTED, not fabricated.
	_, err = uc.ListUptimeCheckIps(ctx, &monitoringpb.ListUptimeCheckIpsRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("list uptime check ips code = %v, want Unimplemented", status.Code(err))
	}

	// A name outside the Discovery pattern is INVALID_ARGUMENT.
	_, err = uc.GetUptimeCheckConfig(ctx, &monitoringpb.GetUptimeCheckConfigRequest{Name: "projects/test/services/x"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad name code = %v, want InvalidArgument", status.Code(err))
	}
}
