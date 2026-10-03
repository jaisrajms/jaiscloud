package serviceusage

import (
	"context"
	"strings"
	"testing"

	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "jaiscloud/internal/gcp/service/serviceusage"
	"jaiscloud/internal/store"
)

func newService() *Service {
	return NewService(core.NewService(store.NewMemoryResourceStore()), "default-proj")
}

const serviceName = "projects/proj/services/run.googleapis.com"

func TestEnableGetListProto(t *testing.T) {
	ctx := context.Background()
	s := newService()

	op, err := s.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: serviceName})
	if err != nil {
		t.Fatalf("EnableService: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("operation not done: %v", op)
	}
	if got := op.GetName(); !strings.HasPrefix(got, "operations/") {
		t.Fatalf("operation name = %q, want operations/...", got)
	}
	enabled := &serviceusagepb.EnableServiceResponse{}
	if err := op.GetResponse().UnmarshalTo(enabled); err != nil {
		t.Fatalf("unmarshal EnableServiceResponse: %v", err)
	}
	meta := &serviceusagepb.OperationMetadata{}
	if err := op.GetMetadata().UnmarshalTo(meta); err != nil {
		t.Fatalf("unmarshal OperationMetadata: %v", err)
	}
	if len(meta.GetResourceNames()) != 1 || meta.GetResourceNames()[0] != serviceName {
		t.Fatalf("operation metadata resourceNames = %v, want [%s]", meta.GetResourceNames(), serviceName)
	}
	if enabled.GetService().GetState() != serviceusagepb.State_ENABLED {
		t.Fatalf("enable state = %v, want ENABLED", enabled.GetService().GetState())
	}
	if enabled.GetService().GetName() != serviceName {
		t.Fatalf("enable name = %q, want %q", enabled.GetService().GetName(), serviceName)
	}
	if enabled.GetService().GetConfig().GetName() != "run.googleapis.com" {
		t.Fatalf("enable config name = %q", enabled.GetService().GetConfig().GetName())
	}

	got, err := s.GetService(ctx, &serviceusagepb.GetServiceRequest{Name: serviceName})
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if got.GetState() != serviceusagepb.State_ENABLED || got.GetParent() != "projects/proj" {
		t.Fatalf("GetService = %+v", got)
	}

	list, err := s.ListServices(ctx, &serviceusagepb.ListServicesRequest{Parent: "projects/proj"})
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}
	if len(list.GetServices()) != 1 || list.GetServices()[0].GetName() != serviceName {
		t.Fatalf("ListServices = %+v", list.GetServices())
	}

	// state:ENABLED includes it; state:DISABLED excludes it.
	enabledList, err := s.ListServices(ctx, &serviceusagepb.ListServicesRequest{Parent: "projects/proj", Filter: "state:ENABLED"})
	if err != nil {
		t.Fatalf("ListServices enabled: %v", err)
	}
	if len(enabledList.GetServices()) != 1 {
		t.Fatalf("enabled list = %d, want 1", len(enabledList.GetServices()))
	}
	disabledList, err := s.ListServices(ctx, &serviceusagepb.ListServicesRequest{Parent: "projects/proj", Filter: "state:DISABLED"})
	if err != nil {
		t.Fatalf("ListServices disabled: %v", err)
	}
	if len(disabledList.GetServices()) != 0 {
		t.Fatalf("disabled list = %d, want 0", len(disabledList.GetServices()))
	}
}

func TestDisableProto(t *testing.T) {
	ctx := context.Background()
	s := newService()

	// Disabling a never-enabled service is FAILED_PRECONDITION.
	_, err := s.DisableService(ctx, &serviceusagepb.DisableServiceRequest{Name: serviceName})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DisableService not-enabled code = %v, want FailedPrecondition", status.Code(err))
	}

	if _, err := s.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: serviceName}); err != nil {
		t.Fatalf("EnableService: %v", err)
	}
	op, err := s.DisableService(ctx, &serviceusagepb.DisableServiceRequest{Name: serviceName})
	if err != nil {
		t.Fatalf("DisableService: %v", err)
	}
	disabled := &serviceusagepb.DisableServiceResponse{}
	if err := op.GetResponse().UnmarshalTo(disabled); err != nil {
		t.Fatalf("unmarshal DisableServiceResponse: %v", err)
	}
	if disabled.GetService().GetState() != serviceusagepb.State_DISABLED {
		t.Fatalf("disable state = %v, want DISABLED", disabled.GetService().GetState())
	}
}

func TestBatchEnableProto(t *testing.T) {
	ctx := context.Background()
	s := newService()

	op, err := s.BatchEnableServices(ctx, &serviceusagepb.BatchEnableServicesRequest{
		Parent:     "projects/proj",
		ServiceIds: []string{"a.googleapis.com", "b.googleapis.com"},
	})
	if err != nil {
		t.Fatalf("BatchEnableServices: %v", err)
	}
	resp := &serviceusagepb.BatchEnableServicesResponse{}
	if err := op.GetResponse().UnmarshalTo(resp); err != nil {
		t.Fatalf("unmarshal BatchEnableServicesResponse: %v", err)
	}
	if len(resp.GetServices()) != 2 {
		t.Fatalf("batch services = %d, want 2", len(resp.GetServices()))
	}
	for _, svc := range resp.GetServices() {
		if svc.GetState() != serviceusagepb.State_ENABLED {
			t.Fatalf("batch state = %v, want ENABLED", svc.GetState())
		}
	}

	// Empty batch is INVALID_ARGUMENT.
	_, err = s.BatchEnableServices(ctx, &serviceusagepb.BatchEnableServicesRequest{Parent: "projects/proj"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty batch code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestInvalidFilterAndMissingName(t *testing.T) {
	ctx := context.Background()
	s := newService()

	_, err := s.ListServices(ctx, &serviceusagepb.ListServicesRequest{Parent: "projects/proj", Filter: "state:RUNNING"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid filter code = %v, want InvalidArgument", status.Code(err))
	}

	// An empty service name has no service id: INVALID_ARGUMENT.
	_, err = s.GetService(ctx, &serviceusagepb.GetServiceRequest{Name: "projects/proj"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing service code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestBatchGetServicesUnimplemented(t *testing.T) {
	s := newService()
	_, err := s.BatchGetServices(context.Background(), &serviceusagepb.BatchGetServicesRequest{Parent: "projects/proj"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("BatchGetServices code = %v, want Unimplemented", status.Code(err))
	}
}

func TestProjectFallsBackToDefault(t *testing.T) {
	ctx := context.Background()
	s := newService()

	// A parent with no project segment falls back to the configured default.
	if _, err := s.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: "services/x.googleapis.com"}); err != nil {
		t.Fatalf("EnableService: %v", err)
	}
	got, err := s.GetService(ctx, &serviceusagepb.GetServiceRequest{Name: "services/x.googleapis.com"})
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if got.GetName() != "projects/default-proj/services/x.googleapis.com" {
		t.Fatalf("fallback name = %q", got.GetName())
	}
}
