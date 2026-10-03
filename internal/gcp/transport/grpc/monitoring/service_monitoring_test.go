package monitoring

import (
	"context"
	"net"
	"testing"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func testServiceMonitoringServer(t *testing.T) (monitoringpb.ServiceMonitoringServiceClient, func()) {
	t.Helper()
	svc := NewService(core.NewService(monitoringstore.NewMemoryStore(), "test"), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	monitoringpb.RegisterServiceMonitoringServiceServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return monitoringpb.NewServiceMonitoringServiceClient(conn),
		func() { conn.Close(); srv.Stop() }
}

func TestGRPCServiceMonitoringCRUD(t *testing.T) {
	client, cleanup := testServiceMonitoringServer(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateService(ctx, &monitoringpb.CreateServiceRequest{
		Parent:    "projects/test",
		ServiceId: "checkout",
		Service: &monitoringpb.Service{
			DisplayName: "Checkout",
			Identifier: &monitoringpb.Service_CloudRun_{
				CloudRun: &monitoringpb.Service_CloudRun{ServiceName: "checkout"},
			},
			UserLabels: map[string]string{"team": "payments"},
		},
	})
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	if created.GetName() != "projects/test/services/checkout" || created.GetDisplayName() != "Checkout" {
		t.Fatalf("created = %+v", created)
	}
	if created.GetCloudRun().GetServiceName() != "checkout" {
		t.Fatalf("created identifier = %+v", created.GetIdentifier())
	}

	got, err := client.GetService(ctx, &monitoringpb.GetServiceRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	if got.GetDisplayName() != "Checkout" || got.GetCloudRun().GetServiceName() != "checkout" {
		t.Fatalf("get = %+v", got)
	}

	// A basic service is identified by the separate basicService field, not the
	// identifier oneof.
	basic, err := client.CreateService(ctx, &monitoringpb.CreateServiceRequest{
		Parent:    "projects/test",
		ServiceId: "basic-svc",
		Service: &monitoringpb.Service{
			BasicService: &monitoringpb.Service_BasicService{ServiceType: "CLOUD_RUN"},
		},
	})
	if err != nil {
		t.Fatalf("create basic service: %v", err)
	}
	if basic.GetBasicService().GetServiceType() != "CLOUD_RUN" || basic.GetIdentifier() != nil {
		t.Fatalf("basic service = %+v", basic)
	}

	list, err := client.ListServices(ctx, &monitoringpb.ListServicesRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("list services: %v", err)
	}
	if len(list.GetServices()) != 2 {
		t.Fatalf("list = %+v, want 2", list)
	}

	// A masked update changes only displayName.
	updated, err := client.UpdateService(ctx, &monitoringpb.UpdateServiceRequest{
		Service:    &monitoringpb.Service{Name: created.GetName(), DisplayName: "Checkout v2"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	if err != nil {
		t.Fatalf("update service: %v", err)
	}
	if updated.GetDisplayName() != "Checkout v2" || updated.GetCloudRun().GetServiceName() != "checkout" {
		t.Fatalf("updated = %+v", updated)
	}

	createdSLO, err := client.CreateServiceLevelObjective(ctx, &monitoringpb.CreateServiceLevelObjectiveRequest{
		Parent:                  created.GetName(),
		ServiceLevelObjectiveId: "avail",
		ServiceLevelObjective: &monitoringpb.ServiceLevelObjective{
			DisplayName: "Availability",
			Goal:        0.99,
			Period: &monitoringpb.ServiceLevelObjective_RollingPeriod{
				RollingPeriod: durationpb.New(30 * 24 * time.Hour),
			},
			ServiceLevelIndicator: &monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_BasicSli{
					BasicSli: &monitoringpb.BasicSli{
						SliCriteria: &monitoringpb.BasicSli_Availability{
							Availability: &monitoringpb.BasicSli_AvailabilityCriteria{},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("create SLO: %v", err)
	}
	wantName := "projects/test/services/checkout/serviceLevelObjectives/avail"
	if createdSLO.GetName() != wantName || createdSLO.GetGoal() != 0.99 {
		t.Fatalf("created SLO = %+v", createdSLO)
	}
	if createdSLO.GetRollingPeriod().AsDuration() != 30*24*time.Hour {
		t.Fatalf("rolling period = %v", createdSLO.GetRollingPeriod().AsDuration())
	}
	if createdSLO.GetServiceLevelIndicator().GetBasicSli().GetAvailability() == nil {
		t.Fatalf("SLI not round-tripped: %+v", createdSLO.GetServiceLevelIndicator())
	}

	if _, err := client.GetServiceLevelObjective(ctx, &monitoringpb.GetServiceLevelObjectiveRequest{
		Name: wantName,
		View: monitoringpb.ServiceLevelObjective_FULL,
	}); err != nil {
		t.Fatalf("get SLO: %v", err)
	}

	sloList, err := client.ListServiceLevelObjectives(ctx, &monitoringpb.ListServiceLevelObjectivesRequest{Parent: created.GetName()})
	if err != nil {
		t.Fatalf("list SLOs: %v", err)
	}
	if len(sloList.GetServiceLevelObjectives()) != 1 {
		t.Fatalf("list SLOs = %+v", sloList)
	}

	if _, err := client.UpdateServiceLevelObjective(ctx, &monitoringpb.UpdateServiceLevelObjectiveRequest{
		ServiceLevelObjective: &monitoringpb.ServiceLevelObjective{Name: wantName, Goal: 0.95},
		UpdateMask:            &fieldmaskpb.FieldMask{Paths: []string{"goal"}},
	}); err != nil {
		t.Fatalf("update SLO: %v", err)
	}

	// A goal outside (0, 1] is InvalidArgument.
	_, err = client.CreateServiceLevelObjective(ctx, &monitoringpb.CreateServiceLevelObjectiveRequest{
		Parent: created.GetName(),
		ServiceLevelObjective: &monitoringpb.ServiceLevelObjective{
			Goal: 2,
			Period: &monitoringpb.ServiceLevelObjective_RollingPeriod{
				RollingPeriod: durationpb.New(time.Hour),
			},
			ServiceLevelIndicator: &monitoringpb.ServiceLevelIndicator{
				Type: &monitoringpb.ServiceLevelIndicator_BasicSli{
					BasicSli: &monitoringpb.BasicSli{SliCriteria: &monitoringpb.BasicSli_Availability{Availability: &monitoringpb.BasicSli_AvailabilityCriteria{}}},
				},
			},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("goal=2 err = %v, want InvalidArgument", err)
	}

	if _, err := client.DeleteServiceLevelObjective(ctx, &monitoringpb.DeleteServiceLevelObjectiveRequest{Name: wantName}); err != nil {
		t.Fatalf("delete SLO: %v", err)
	}
	if _, err := client.DeleteService(ctx, &monitoringpb.DeleteServiceRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("delete service: %v", err)
	}
	if _, err := client.GetService(ctx, &monitoringpb.GetServiceRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete = %v, want NotFound", err)
	}
}
