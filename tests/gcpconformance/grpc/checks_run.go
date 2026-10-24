package grpcconformance

import (
	"context"
	"fmt"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	run "cloud.google.com/go/run/apiv2"
	runpb "cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// runLocation is a canonical Cloud Run location; the emulator does not validate
// the location list.
const runLocation = "us-central1"

// runChecks covers the Cloud Run Admin v2 surface
// (google.cloud.run.v2.Services + google.cloud.run.v2.Revisions) via the
// official generated cloud.google.com/go/run/apiv2 client (gRPC transport):
// service create/get/list/update/delete, service IAM, revision list/get, and
// DeleteRevision (both the retired-revision success and the serving-revision
// FailedPrecondition). Every probe is self-contained and run-unique
// (cfg.ResourceName).
func runChecks() []Check {
	return []Check{
		{Service: "run", RPC: "CreateService", Method: "CreateService", KeyField: "done operation response = Service (Ready/uri/latestReadyRevision)", Run: checkRunCreateService},
		{Service: "run", RPC: "GetService", Method: "GetService", KeyField: "name/uri round-trip", Run: checkRunGetService},
		{Service: "run", RPC: "ListServices", Method: "ListServices", KeyField: "created service present", Run: checkRunListServices},
		{Service: "run", RPC: "UpdateService", Method: "UpdateService", KeyField: "template change mints the next revision", Run: checkRunUpdateService},
		{Service: "run", RPC: "GetRevision", Method: "GetRevision", KeyField: "service + containers[0].image round-trip", Run: checkRunGetRevision},
		{Service: "run", RPC: "ListRevisions", Method: "ListRevisions", KeyField: "first revision present", Run: checkRunListRevisions},
		{Service: "run", RPC: "SetIamPolicy", Method: "SetIamPolicy", KeyField: "binding round-trip", Run: checkRunSetIamPolicy},
		{Service: "run", RPC: "GetIamPolicy", Method: "GetIamPolicy", KeyField: "policy round-trip", Run: checkRunGetIamPolicy},
		{Service: "run", RPC: "TestIamPermissions", Method: "TestIamPermissions", KeyField: "requested permissions echoed", Run: checkRunTestIamPermissions},
		{Service: "run", RPC: "DeleteRevision (retired)", Method: "DeleteRevision", KeyField: "retired revision deleted; NotFound after", Run: checkRunDeleteRetiredRevision},
		{Service: "run", RPC: "DeleteRevision (serving)", Method: "DeleteRevision", KeyField: "serving revision rejected FailedPrecondition", Run: checkRunDeleteServingRevision},
		{Service: "run", RPC: "DeleteService", Method: "DeleteService", KeyField: "done operation; NotFound after", Run: checkRunDeleteService},
		{Service: "run", RPC: "CreateService (validateOnly)", Method: "CreateService", KeyField: "dry-run previews; not persisted", Run: checkRunValidateOnlyCreate},
		{Service: "run", RPC: "DeleteService (etag)", Method: "DeleteService", KeyField: "stale etag rejected Aborted", Run: checkRunDeleteServiceEtag},
		{Service: "run", RPC: "UpdateService (allowMissing)", Method: "UpdateService", KeyField: "upserts missing service", Run: checkRunAllowMissingUpdate},
	}
}

func newRunServicesClient(ctx context.Context, cfg Config) (*run.ServicesClient, error) {
	return run.NewServicesClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func newRunRevisionsClient(ctx context.Context, cfg Config) (*run.RevisionsClient, error) {
	return run.NewRevisionsClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func runParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, runLocation)
}

func runServiceName(cfg Config, id string) string {
	return runParent(cfg) + "/services/" + id
}

func runRevisionName(cfg Config, serviceID, revisionID string) string {
	return runServiceName(cfg, serviceID) + "/revisions/" + revisionID
}

// createRunServiceRequest builds the canonical create request for a probe.
func createRunServiceRequest(cfg Config, id string) *runpb.CreateServiceRequest {
	return &runpb.CreateServiceRequest{
		Parent:    runParent(cfg),
		ServiceId: id,
		Service: &runpb.Service{
			Template: &runpb.RevisionTemplate{
				Containers: []*runpb.Container{{Image: "nginx:latest", Ports: []*runpb.ContainerPort{{ContainerPort: 80}}}},
			},
		},
	}
}

// createRunService creates a service and returns its ready Service (the create
// operation is completed inline by the emulator).
func createRunService(ctx context.Context, client *run.ServicesClient, cfg Config, id string) (*runpb.Service, error) {
	op, err := client.CreateService(ctx, createRunServiceRequest(cfg, id))
	if err != nil {
		return nil, fmt.Errorf("CreateService: %w", err)
	}
	svc, err := op.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("CreateService.Wait: %w", err)
	}
	return svc, nil
}

// deleteRunService best-effort removes a probe's service.
func deleteRunService(ctx context.Context, client *run.ServicesClient, cfg Config, id string) {
	if op, err := client.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: runServiceName(cfg, id)}); err == nil {
		_, _ = op.Wait(ctx)
	}
}

// Check 1: CreateService returns a done operation whose response is a Ready
// Service with a derived uri, a latest-ready revision and 100% traffic.
func checkRunCreateService(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-create")
	svc, err := createRunService(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	if svc.GetName() != runServiceName(cfg, id) {
		return fmt.Errorf("CreateService name = %q", svc.GetName())
	}
	if svc.GetUri() == "" {
		return fmt.Errorf("CreateService uri is empty")
	}
	if svc.GetLatestReadyRevision() == "" {
		return fmt.Errorf("CreateService latestReadyRevision is empty")
	}
	if c := svc.GetTerminalCondition(); c.GetType() != "Ready" || c.GetState() != runpb.Condition_CONDITION_SUCCEEDED {
		return fmt.Errorf("CreateService terminalCondition = %v/%v", c.GetType(), c.GetState())
	}
	if len(svc.GetTrafficStatuses()) == 0 || svc.GetTrafficStatuses()[0].GetPercent() != 100 {
		return fmt.Errorf("CreateService trafficStatuses = %v", svc.GetTrafficStatuses())
	}
	return nil
}

// Check 2: GetService round-trips the created name/uri.
func checkRunGetService(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-get")
	created, err := createRunService(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	got, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: runServiceName(cfg, id)})
	if err != nil {
		return fmt.Errorf("GetService: %w", err)
	}
	if got.GetName() != created.GetName() || got.GetUri() != created.GetUri() {
		return fmt.Errorf("GetService = %q/%q, want %q/%q", got.GetName(), got.GetUri(), created.GetName(), created.GetUri())
	}
	return nil
}

// Check 3: ListServices includes the created service.
func checkRunListServices(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-list")
	if _, err := createRunService(ctx, client, cfg, id); err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	it := client.ListServices(ctx, &runpb.ListServicesRequest{Parent: runParent(cfg)})
	for {
		svc, err := it.Next()
		if err != nil {
			return fmt.Errorf("ListServices: %w", err)
		}
		if svc == nil {
			break
		}
		if svc.GetName() == runServiceName(cfg, id) {
			return nil
		}
	}
	return fmt.Errorf("ListServices did not include %q", id)
}

// Check 4: a template-changing UpdateService mints the next revision.
func checkRunUpdateService(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-update")
	created, err := createRunService(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	if rev := created.GetLatestReadyRevision(); rev == "" {
		return fmt.Errorf("CreateService latestReadyRevision is empty")
	}
	op, err := client.UpdateService(ctx, &runpb.UpdateServiceRequest{
		Service: &runpb.Service{
			Name: runServiceName(cfg, id),
			Template: &runpb.RevisionTemplate{
				Containers: []*runpb.Container{{Image: "nginx:1.27", Ports: []*runpb.ContainerPort{{ContainerPort: 80}}}},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"template"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateService: %w", err)
	}
	updated, err := op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("UpdateService.Wait: %w", err)
	}
	if updated.GetLatestReadyRevision() == created.GetLatestReadyRevision() {
		return fmt.Errorf("UpdateService did not mint a new revision: %q", updated.GetLatestReadyRevision())
	}
	return nil
}

// Check 5: GetRevision echoes the service and the template image.
func checkRunGetRevision(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	revClient, err := newRunRevisionsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new revisions client: %w", err)
	}
	defer revClient.Close()

	id := cfg.ResourceName("gcpc-run-getrev")
	created, err := createRunService(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	rev, err := revClient.GetRevision(ctx, &runpb.GetRevisionRequest{Name: created.GetLatestReadyRevision()})
	if err != nil {
		return fmt.Errorf("GetRevision: %w", err)
	}
	if rev.GetService() != runServiceName(cfg, id) {
		return fmt.Errorf("GetRevision service = %q", rev.GetService())
	}
	if len(rev.GetContainers()) == 0 || rev.GetContainers()[0].GetImage() != "nginx:latest" {
		return fmt.Errorf("GetRevision containers = %v", rev.GetContainers())
	}
	return nil
}

// Check 6: ListRevisions returns the created revision.
func checkRunListRevisions(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	revClient, err := newRunRevisionsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new revisions client: %w", err)
	}
	defer revClient.Close()

	id := cfg.ResourceName("gcpc-run-listrev")
	created, err := createRunService(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	it := revClient.ListRevisions(ctx, &runpb.ListRevisionsRequest{Parent: runServiceName(cfg, id)})
	for {
		rev, err := it.Next()
		if err != nil {
			return fmt.Errorf("ListRevisions: %w", err)
		}
		if rev == nil {
			break
		}
		if rev.GetName() == created.GetLatestReadyRevision() {
			return nil
		}
	}
	return fmt.Errorf("ListRevisions did not include %q", created.GetLatestReadyRevision())
}

// Check 7: SetIamPolicy stores a binding.
func checkRunSetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-setiam")
	if _, err := createRunService(ctx, client, cfg, id); err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	pol, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: runServiceName(cfg, id),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/run.invoker", Members: []string{"allUsers"}}}},
	})
	if err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != "roles/run.invoker" {
		return fmt.Errorf("SetIamPolicy bindings = %v", pol.GetBindings())
	}
	return nil
}

// Check 8: GetIamPolicy reflects the stored binding.
func checkRunGetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-getiam")
	if _, err := createRunService(ctx, client, cfg, id); err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	if _, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: runServiceName(cfg, id),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/run.invoker", Members: []string{"allUsers"}}}},
	}); err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: runServiceName(cfg, id)})
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetMembers()[0] != "allUsers" {
		return fmt.Errorf("GetIamPolicy bindings = %v", pol.GetBindings())
	}
	return nil
}

// Check 9: TestIamPermissions echoes the requested permissions.
func checkRunTestIamPermissions(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-testiam")
	if _, err := createRunService(ctx, client, cfg, id); err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    runServiceName(cfg, id),
		Permissions: []string{"run.services.get", "run.services.delete"},
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions: %w", err)
	}
	if len(resp.GetPermissions()) != 2 {
		return fmt.Errorf("TestIamPermissions = %v", resp.GetPermissions())
	}
	return nil
}

// Check 10: a retired revision can be deleted; the deleted name is NotFound
// afterward. The template-changing update retires the first revision.
func checkRunDeleteRetiredRevision(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	revClient, err := newRunRevisionsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new revisions client: %w", err)
	}
	defer revClient.Close()

	id := cfg.ResourceName("gcpc-run-delrev")
	created, err := createRunService(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	retired := created.GetLatestReadyRevision()
	// Mint a successor so `retired` is no longer serving.
	op, err := client.UpdateService(ctx, &runpb.UpdateServiceRequest{
		Service: &runpb.Service{
			Name: runServiceName(cfg, id),
			Template: &runpb.RevisionTemplate{
				Containers: []*runpb.Container{{Image: "nginx:1.27"}},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"template"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateService: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("UpdateService.Wait: %w", err)
	}
	delOp, err := revClient.DeleteRevision(ctx, &runpb.DeleteRevisionRequest{Name: retired})
	if err != nil {
		return fmt.Errorf("DeleteRevision: %w", err)
	}
	del, err := delOp.Wait(ctx)
	if err != nil {
		return fmt.Errorf("DeleteRevision.Wait: %w", err)
	}
	if del.GetName() != retired {
		return fmt.Errorf("DeleteRevision response = %q, want %q", del.GetName(), retired)
	}
	if _, err := revClient.GetRevision(ctx, &runpb.GetRevisionRequest{Name: retired}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetRevision after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}

// Check 11: deleting the revision currently serving traffic is rejected with
// FailedPrecondition (the proto: only retired revisions can be deleted).
func checkRunDeleteServingRevision(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	revClient, err := newRunRevisionsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new revisions client: %w", err)
	}
	defer revClient.Close()

	id := cfg.ResourceName("gcpc-run-delcur")
	created, err := createRunService(ctx, client, cfg, id)
	if err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	_, err = revClient.DeleteRevision(ctx, &runpb.DeleteRevisionRequest{Name: created.GetLatestReadyRevision()})
	if status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("DeleteRevision serving code = %v, want FailedPrecondition", status.Code(err))
	}
	return nil
}

// Check 12: DeleteService completes and the service is NotFound afterward.
func checkRunDeleteService(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-delete")
	if _, err := createRunService(ctx, client, cfg, id); err != nil {
		return err
	}
	op, err := client.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: runServiceName(cfg, id)})
	if err != nil {
		return fmt.Errorf("DeleteService: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("DeleteService.Wait: %w", err)
	}
	if _, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: runServiceName(cfg, id)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetService after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}

// Check 13: a validateOnly CreateService previews the service but persists
// nothing (a following GetService is NotFound).
func checkRunValidateOnlyCreate(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-dryrun")
	req := createRunServiceRequest(cfg, id)
	req.ValidateOnly = true
	op, err := client.CreateService(ctx, req)
	if err != nil {
		return fmt.Errorf("validateOnly CreateService: %w", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("validateOnly CreateService.Wait: %w", err)
	}
	if _, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: runServiceName(cfg, id)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("validateOnly create persisted the service (GetService code %v)", status.Code(err))
	}
	return nil
}

// Check 14: a stale etag on DeleteService is Aborted and leaves the service.
func checkRunDeleteServiceEtag(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-deletag")
	if _, err := createRunService(ctx, client, cfg, id); err != nil {
		return err
	}
	defer deleteRunService(ctx, client, cfg, id)
	_, err = client.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: runServiceName(cfg, id), Etag: "stale-token"})
	if status.Code(err) != codes.Aborted {
		return fmt.Errorf("stale-etag DeleteService code = %v, want Aborted", status.Code(err))
	}
	if _, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: runServiceName(cfg, id)}); err != nil {
		return fmt.Errorf("stale-etag delete removed the service: %w", err)
	}
	return nil
}

// Check 15: an UpdateService with allowMissing=true upserts a missing service.
func checkRunAllowMissingUpdate(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-run-allowmissing")
	if _, err := client.UpdateService(ctx, &runpb.UpdateServiceRequest{
		Service: &runpb.Service{
			Name:     runServiceName(cfg, id),
			Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: "nginx:latest"}}},
		},
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{"template"}},
		AllowMissing: true,
	}); err != nil {
		return fmt.Errorf("allowMissing UpdateService: %w", err)
	}
	defer deleteRunService(ctx, client, cfg, id)
	if _, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: runServiceName(cfg, id)}); err != nil {
		return fmt.Errorf("allowMissing update did not create the service: %w", err)
	}
	return nil
}
