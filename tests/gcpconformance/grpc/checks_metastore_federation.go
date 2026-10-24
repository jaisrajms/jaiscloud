package grpcconformance

import (
	"context"
	"fmt"

	iam "cloud.google.com/go/iam/apiv1"
	metastore "cloud.google.com/go/metastore/apiv1"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// metastoreFederationChecks covers the separate
// google.cloud.metastore.v1.DataprocMetastoreFederation gRPC service (federation
// CRUD) via the official generated client. The federation IAM mixin is served
// through the standalone google.iam.v1.IAMPolicy service, so those probes live
// in checks_metastore_iam.go.
func metastoreFederationChecks() []Check {
	return []Check{
		{Service: "metastore", RPC: "CreateFederation", Method: "CreateFederation", KeyField: "LRO done + ACTIVE federation", Run: checkMSCreateFederation},
		{Service: "metastore", RPC: "GetFederation", Method: "GetFederation", KeyField: "name/state round-trip", Run: checkMSGetFederation},
		{Service: "metastore", RPC: "ListFederations", Method: "ListFederations", KeyField: "created federation present", Run: checkMSListFederations},
		{Service: "metastore", RPC: "UpdateFederation", Method: "UpdateFederation", KeyField: "labels updated via LRO", Run: checkMSUpdateFederation},
		{Service: "metastore", RPC: "DeleteFederation", Method: "DeleteFederation", KeyField: "LRO done + NotFound after", Run: checkMSDeleteFederation},
	}
}

// newMetastoreFederationClient dials the emulator and returns the official
// DataprocMetastoreFederation client.
func newMetastoreFederationClient(ctx context.Context, cfg Config) (*metastore.DataprocMetastoreFederationClient, error) {
	return metastore.NewDataprocMetastoreFederationClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func metastoreFederationName(cfg Config, id string) string {
	return metastoreParent(cfg) + "/federations/" + id
}

// ensureMetastoreFederation creates the run-unique probe federation, tolerating
// AlreadyExists so repeated probes are idempotent.
func ensureMetastoreFederation(ctx context.Context, client *metastore.DataprocMetastoreFederationClient, cfg Config) (string, error) {
	id := cfg.ResourceName("gcpc-grpc-msfed")
	op, err := client.CreateFederation(ctx, &metastorepb.CreateFederationRequest{
		Parent:       metastoreParent(cfg),
		FederationId: id,
		Federation:   &metastorepb.Federation{Labels: map[string]string{"probe": "gcpc"}},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return metastoreFederationName(cfg, id), nil
		}
		return "", fmt.Errorf("create federation: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return "", fmt.Errorf("wait federation: %w", err)
	}
	return metastoreFederationName(cfg, id), nil
}

// Check: CreateFederation returns a done operation whose response is the ACTIVE
// federation.
func checkMSCreateFederation(ctx context.Context, cfg Config) error {
	client, err := newMetastoreFederationClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-msfed-create")
	wantName := metastoreFederationName(cfg, id)
	op, err := client.CreateFederation(ctx, &metastorepb.CreateFederationRequest{
		Parent:       metastoreParent(cfg),
		FederationId: id,
		Federation:   &metastorepb.Federation{Version: "3.1.2", Labels: map[string]string{"probe": "gcpc"}},
	})
	if err != nil {
		return fmt.Errorf("CreateFederation: %w", err)
	}
	if !op.Done() {
		return fmt.Errorf("CreateFederation operation not done")
	}
	fed, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if fed.GetState() != metastorepb.Federation_ACTIVE {
		return fmt.Errorf("federation state = %v, want ACTIVE", fed.GetState())
	}
	if fed.GetName() != wantName {
		return fmt.Errorf("federation name = %q, want %q", fed.GetName(), wantName)
	}
	if fed.GetEndpointUri() == "" {
		return fmt.Errorf("federation endpointUri is empty")
	}
	if fed.GetVersion() != "3.1.2" || fed.GetLabels()["probe"] != "gcpc" {
		return fmt.Errorf("federation fields = %+v", fed)
	}
	return nil
}

// Check: GetFederation round-trips.
func checkMSGetFederation(ctx context.Context, cfg Config) error {
	client, err := newMetastoreFederationClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureMetastoreFederation(ctx, client, cfg)
	if err != nil {
		return err
	}
	got, err := client.GetFederation(ctx, &metastorepb.GetFederationRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetFederation: %w", err)
	}
	if got.GetName() != name || got.GetState() != metastorepb.Federation_ACTIVE {
		return fmt.Errorf("GetFederation = %+v", got)
	}
	return nil
}

// Check: ListFederations includes the probe federation.
func checkMSListFederations(ctx context.Context, cfg Config) error {
	client, err := newMetastoreFederationClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureMetastoreFederation(ctx, client, cfg)
	if err != nil {
		return err
	}
	it := client.ListFederations(ctx, &metastorepb.ListFederationsRequest{Parent: metastoreParent(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListFederations did not include %q", name)
		}
		if err != nil {
			return fmt.Errorf("ListFederations: %w", err)
		}
		if got.GetName() == name {
			return nil
		}
	}
}

// Check: UpdateFederation applies labels through an LRO.
func checkMSUpdateFederation(ctx context.Context, cfg Config) error {
	client, err := newMetastoreFederationClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureMetastoreFederation(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.UpdateFederation(ctx, &metastorepb.UpdateFederationRequest{
		Federation: &metastorepb.Federation{Name: name, Labels: map[string]string{"updated": "yes"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateFederation: %w", err)
	}
	fed, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if fed.GetLabels()["updated"] != "yes" {
		return fmt.Errorf("federation labels = %v", fed.GetLabels())
	}
	return nil
}

// Check: DeleteFederation completes and the federation is gone.
func checkMSDeleteFederation(ctx context.Context, cfg Config) error {
	client, err := newMetastoreFederationClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	name, err := ensureMetastoreFederation(ctx, client, cfg)
	if err != nil {
		return err
	}
	op, err := client.DeleteFederation(ctx, &metastorepb.DeleteFederationRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteFederation: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("operation Wait: %w", err)
	}
	if _, err := client.GetFederation(ctx, &metastorepb.GetFederationRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetFederation after delete = %v, want NotFound", err)
	}
	return nil
}

// newMetastoreIAMPolicyClient dials the standalone google.iam.v1.IAMPolicy
// service.
func newMetastoreIAMPolicyClient(ctx context.Context, cfg Config) (*iam.IamPolicyClient, error) {
	return iam.NewIamPolicyClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}
