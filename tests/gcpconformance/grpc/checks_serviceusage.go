package grpcconformance

import (
	"context"
	"fmt"

	serviceusage "cloud.google.com/go/serviceusage/apiv1"
	serviceusagepb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// serviceUsageChecks covers the Service Usage v1 surface
// (google.api.serviceusage.v1.ServiceUsage) via the official generated
// cloud.google.com/go/serviceusage/apiv1 client: GetService, ListServices, and
// the EnableService / DisableService / BatchEnableServices long-running
// operations (the emulator completes them inline, so the client's Wait observes
// the response without polling).
//
// Every probe is self-contained and run-unique (cfg.ResourceName), so a
// long-lived emulator never sees cross-run collisions.
func serviceUsageChecks() []Check {
	return []Check{
		{Service: "serviceusage", RPC: "EnableService", Method: "EnableService", KeyField: "LRO done + state ENABLED", Run: checkSUEnableService},
		{Service: "serviceusage", RPC: "GetService", Method: "GetService", KeyField: "name/state/config.name round-trip", Run: checkSUGetService},
		{Service: "serviceusage", RPC: "ListServices", Method: "ListServices", KeyField: "enabled service present", Run: checkSUListServices},
		{Service: "serviceusage", RPC: "BatchEnableServices", Method: "BatchEnableServices", KeyField: "LRO done + both services ENABLED", Run: checkSUBatchEnableServices},
		{Service: "serviceusage", RPC: "DisableService", Method: "DisableService", KeyField: "LRO done + state DISABLED", Run: checkSUDisableService},
	}
}

// newServiceUsageClient dials the emulator and returns the official generated
// Service Usage client.
func newServiceUsageClient(ctx context.Context, cfg Config) (*serviceusage.Client, error) {
	return serviceusage.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// serviceUsageName is the run-unique full resource name of a probe service.
func serviceUsageName(cfg Config, prefix string) string {
	return fmt.Sprintf("projects/%s/services/%s.googleapis.com", cfg.Project, cfg.ResourceName(prefix))
}

// serviceUsageID is the run-unique bare service id (a DNS name) that
// BatchEnableServices' service_ids field expects.
func serviceUsageID(cfg Config, prefix string) string {
	return cfg.ResourceName(prefix) + ".googleapis.com"
}

// Check 1: EnableService returns a done LRO whose response carries the service
// in the ENABLED state.
func checkSUEnableService(ctx context.Context, cfg Config) error {
	client, err := newServiceUsageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := serviceUsageName(cfg, "gcpc-grpc-su-en")
	op, err := client.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: name})
	if err != nil {
		return fmt.Errorf("EnableService: %w", err)
	}
	resp, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("EnableService Wait: %w", err)
	}
	if resp.GetService().GetState() != serviceusagepb.State_ENABLED {
		return fmt.Errorf("EnableService state = %v, want ENABLED", resp.GetService().GetState())
	}
	if resp.GetService().GetName() != name {
		return fmt.Errorf("EnableService name = %q, want %q", resp.GetService().GetName(), name)
	}
	return nil
}

// Check 2: GetService returns the enabled service with its config name.
func checkSUGetService(ctx context.Context, cfg Config) error {
	client, err := newServiceUsageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := serviceUsageName(cfg, "gcpc-grpc-su-get")
	if _, err := client.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: name}); err != nil {
		return fmt.Errorf("EnableService: %w", err)
	}
	got, err := client.GetService(ctx, &serviceusagepb.GetServiceRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetService: %w", err)
	}
	if got.GetState() != serviceusagepb.State_ENABLED {
		return fmt.Errorf("GetService state = %v, want ENABLED", got.GetState())
	}
	if got.GetConfig().GetName() != cfg.ResourceName("gcpc-grpc-su-get")+".googleapis.com" {
		return fmt.Errorf("GetService config.name = %q", got.GetConfig().GetName())
	}
	return nil
}

// Check 3: ListServices includes the enabled service.
func checkSUListServices(ctx context.Context, cfg Config) error {
	client, err := newServiceUsageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := serviceUsageName(cfg, "gcpc-grpc-su-list")
	if _, err := client.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: name}); err != nil {
		return fmt.Errorf("EnableService: %w", err)
	}

	it := client.ListServices(ctx, &serviceusagepb.ListServicesRequest{
		Parent: fmt.Sprintf("projects/%s", cfg.Project),
		Filter: "state:ENABLED",
	})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListServices did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListServices: %w", err)
		}
		if got.GetName() == name {
			if got.GetState() != serviceusagepb.State_ENABLED {
				return fmt.Errorf("ListServices state = %v, want ENABLED", got.GetState())
			}
			return nil
		}
	}
}

// Check 4: BatchEnableServices returns a done LRO with both services enabled.
func checkSUBatchEnableServices(ctx context.Context, cfg Config) error {
	client, err := newServiceUsageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	firstID := serviceUsageID(cfg, "gcpc-grpc-su-batch-a")
	secondID := serviceUsageID(cfg, "gcpc-grpc-su-batch-b")
	op, err := client.BatchEnableServices(ctx, &serviceusagepb.BatchEnableServicesRequest{
		Parent:     fmt.Sprintf("projects/%s", cfg.Project),
		ServiceIds: []string{firstID, secondID},
	})
	if err != nil {
		return fmt.Errorf("BatchEnableServices: %w", err)
	}
	resp, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("BatchEnableServices Wait: %w", err)
	}
	if len(resp.GetServices()) != 2 {
		return fmt.Errorf("BatchEnableServices returned %d services, want 2", len(resp.GetServices()))
	}
	wantNames := map[string]bool{
		fmt.Sprintf("projects/%s/services/%s", cfg.Project, firstID):  false,
		fmt.Sprintf("projects/%s/services/%s", cfg.Project, secondID): false,
	}
	for _, svc := range resp.GetServices() {
		if svc.GetState() != serviceusagepb.State_ENABLED {
			return fmt.Errorf("BatchEnableServices state = %v, want ENABLED", svc.GetState())
		}
		if _, ok := wantNames[svc.GetName()]; !ok {
			return fmt.Errorf("BatchEnableServices unexpected name %q", svc.GetName())
		}
		wantNames[svc.GetName()] = true
	}
	for name, seen := range wantNames {
		if !seen {
			return fmt.Errorf("BatchEnableServices did not return %q", name)
		}
	}
	return nil
}

// Check 5: DisableService returns a done LRO with the service DISABLED.
func checkSUDisableService(ctx context.Context, cfg Config) error {
	client, err := newServiceUsageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := serviceUsageName(cfg, "gcpc-grpc-su-dis")
	if _, err := client.EnableService(ctx, &serviceusagepb.EnableServiceRequest{Name: name}); err != nil {
		return fmt.Errorf("EnableService: %w", err)
	}
	op, err := client.DisableService(ctx, &serviceusagepb.DisableServiceRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DisableService: %w", err)
	}
	resp, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("DisableService Wait: %w", err)
	}
	if resp.GetService().GetState() != serviceusagepb.State_DISABLED {
		return fmt.Errorf("DisableService state = %v, want DISABLED", resp.GetService().GetState())
	}
	return nil
}
