package grpcconformance

import (
	"context"
	"fmt"

	longrunning "cloud.google.com/go/longrunning/autogen"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// operationsChecks covers the google.longrunning.Operations surface
// (google.longrunning.Operations) via the official generated longrunning
// client.
//
// The emulator's Operations service is a synchronous stub: every operation the
// emulator would return is already terminal, so it persists no operation
// registry. These probes therefore assert the documented stub contract — a
// done=true operation echoing the requested name, an empty well-formed list,
// and successful no-op delete/cancel — rather than real-GCP async semantics
// (which would require an in-flight operation to poll or wait on). Names are
// run-unique via cfg.ResourceName, so a long-lived emulator never sees cross-run
// collisions.
func operationsChecks() []Check {
	return []Check{
		{Service: "operations", RPC: "GetOperation", Method: "GetOperation", KeyField: "name round-trips with done=true", Run: checkOperationsGet},
		{Service: "operations", RPC: "ListOperations", Method: "ListOperations", KeyField: "well-formed empty list", Run: checkOperationsList},
		{Service: "operations", RPC: "DeleteOperation", Method: "DeleteOperation", KeyField: "success", Run: checkOperationsDelete},
		{Service: "operations", RPC: "CancelOperation", Method: "CancelOperation", KeyField: "success", Run: checkOperationsCancel},
		{Service: "operations", RPC: "WaitOperation", Method: "WaitOperation", KeyField: "name round-trips with done=true", Run: checkOperationsWait},
	}
}

// newOperationsClient dials the emulator and returns the official generated
// longrunning client. It mirrors the KMS/Secret Manager/Logging probes: an
// explicit insecure endpoint with authentication disabled.
func newOperationsClient(ctx context.Context, cfg Config) (*longrunning.OperationsClient, error) {
	return longrunning.NewOperationsClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// operationsParent is the project/location parent ListOperations is scoped to.
func operationsParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/global", cfg.Project)
}

// operationsName is a run-unique, well-formed operation resource name.
func operationsName(cfg Config) string {
	return fmt.Sprintf("%s/operations/%s", operationsParent(cfg), cfg.ResourceName("gcpc-grpc-operation"))
}

// Check 1: GetOperation must echo the requested name as a terminal operation.
func checkOperationsGet(ctx context.Context, cfg Config) error {
	client, err := newOperationsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := operationsName(cfg)
	op, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetOperation: %w", err)
	}
	if got := op.GetName(); got != name {
		return fmt.Errorf("GetOperation name = %q, want %q", got, name)
	}
	if !op.GetDone() {
		return fmt.Errorf("GetOperation done = false, want true")
	}
	return nil
}

// Check 2: ListOperations must succeed against the project/location parent and
// return a well-formed (empty) page, since the stub persists no operations.
func checkOperationsList(ctx context.Context, cfg Config) error {
	client, err := newOperationsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	it := client.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: operationsParent(cfg)})
	count := 0
	for {
		_, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListOperations: %w", err)
		}
		count++
	}
	if count != 0 {
		return fmt.Errorf("ListOperations returned %d operations, want 0", count)
	}
	return nil
}

// Check 3: DeleteOperation is a no-op that must succeed.
func checkOperationsDelete(ctx context.Context, cfg Config) error {
	client, err := newOperationsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := client.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: operationsName(cfg)}); err != nil {
		return fmt.Errorf("DeleteOperation: %w", err)
	}
	return nil
}

// Check 4: CancelOperation is a no-op that must succeed.
func checkOperationsCancel(ctx context.Context, cfg Config) error {
	client, err := newOperationsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := client.CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: operationsName(cfg)}); err != nil {
		return fmt.Errorf("CancelOperation: %w", err)
	}
	return nil
}

// Check 5: WaitOperation must return the requested name already done, without
// blocking (the synchronous stub has no in-flight operation to wait on).
func checkOperationsWait(ctx context.Context, cfg Config) error {
	client, err := newOperationsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := operationsName(cfg)
	op, err := client.WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: name})
	if err != nil {
		return fmt.Errorf("WaitOperation: %w", err)
	}
	if got := op.GetName(); got != name {
		return fmt.Errorf("WaitOperation name = %q, want %q", got, name)
	}
	if !op.GetDone() {
		return fmt.Errorf("WaitOperation done = false, want true")
	}
	return nil
}
