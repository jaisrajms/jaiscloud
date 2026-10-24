package grpcconformance

import (
	"context"
	"fmt"

	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	longrunning "cloud.google.com/go/longrunning/autogen"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	metastorepb "cloud.google.com/go/metastore/apiv1/metastorepb"
	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
)

// managedKafkaParent is the project/location parent the Managed Kafka probes use.
func managedKafkaParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, managedKafkaLocation)
}

// checkMetastoreOperationsEndpoint verifies the shared
// google.longrunning.Operations service is endpoint-scoped, as real GCP serves
// it per service host: a client addressed at the Metastore endpoint resolves and
// lists the operation a Metastore create persisted, while the Managed Kafka
// endpoint (which shares the location parent) does not list it.
func checkMetastoreOperationsEndpoint(ctx context.Context, cfg Config) error {
	client, err := newMetastoreClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("metastore client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateService(ctx, &metastorepb.CreateServiceRequest{
		Parent:    metastoreParent(cfg),
		ServiceId: cfg.ResourceName("gcpc-grpc-ms-ops"),
		Service:   &metastorepb.Service{Labels: map[string]string{"probe": "gcpc-ops"}},
	})
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	opName := op.Name()

	own, err := newEndpointOperationsClient(ctx, cfg, "metastore.localhost")
	if err != nil {
		return fmt.Errorf("metastore operations client: %w", err)
	}
	defer own.Close()
	got, err := own.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: opName})
	if err != nil {
		return fmt.Errorf("GetOperation at the metastore endpoint: %w", err)
	}
	if got.GetName() != opName {
		return fmt.Errorf("GetOperation name = %q, want %q", got.GetName(), opName)
	}
	if err := operationsListContains(ctx, own, metastoreParent(cfg), opName); err != nil {
		return err
	}

	sibling, err := newEndpointOperationsClient(ctx, cfg, "managedkafka.localhost")
	if err != nil {
		return fmt.Errorf("sibling operations client: %w", err)
	}
	defer sibling.Close()
	return operationsListExcludes(ctx, sibling, metastoreParent(cfg), opName)
}

// checkManagedKafkaOperationsEndpoint mirrors checkMetastoreOperationsEndpoint
// for Managed Kafka: its endpoint resolves and lists the operation a cluster
// create persisted, and the Metastore endpoint does not list it.
func checkManagedKafkaOperationsEndpoint(ctx context.Context, cfg Config) error {
	client, err := newManagedKafkaClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("managedkafka client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    managedKafkaParent(cfg),
		ClusterId: cfg.ResourceName("gcpc-grpc-mk-ops"),
		Cluster: &managedkafkapb.Cluster{
			Labels:         map[string]string{"probe": "gcpc-ops"},
			CapacityConfig: &managedkafkapb.CapacityConfig{VcpuCount: 3, MemoryBytes: 3221225472},
		},
	})
	if err != nil {
		return fmt.Errorf("create cluster: %w", err)
	}
	opName := op.Name()

	own, err := newEndpointOperationsClient(ctx, cfg, "managedkafka.localhost")
	if err != nil {
		return fmt.Errorf("managedkafka operations client: %w", err)
	}
	defer own.Close()
	got, err := own.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: opName})
	if err != nil {
		return fmt.Errorf("GetOperation at the managedkafka endpoint: %w", err)
	}
	if got.GetName() != opName {
		return fmt.Errorf("GetOperation name = %q, want %q", got.GetName(), opName)
	}
	if err := operationsListContains(ctx, own, managedKafkaParent(cfg), opName); err != nil {
		return err
	}

	sibling, err := newEndpointOperationsClient(ctx, cfg, "metastore.localhost")
	if err != nil {
		return fmt.Errorf("sibling operations client: %w", err)
	}
	defer sibling.Close()
	return operationsListExcludes(ctx, sibling, managedKafkaParent(cfg), opName)
}

// checkRunOperationsEndpoint verifies google.longrunning.Operations is
// endpoint-scoped for Cloud Run: the operation a service create persisted is
// resolved and listed at the run endpoint, and the Cloud Functions endpoint
// (sharing the location parent) does not list it.
func checkRunOperationsEndpoint(ctx context.Context, cfg Config) error {
	client, err := newRunServicesClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("run client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateService(ctx, createRunServiceRequest(cfg, cfg.ResourceName("gcpc-grpc-run-ops")))
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	opName := op.Name()

	own, err := newEndpointOperationsClient(ctx, cfg, "run.localhost")
	if err != nil {
		return fmt.Errorf("run operations client: %w", err)
	}
	defer own.Close()
	if err := operationsEndpointResolvesAndLists(ctx, own, opName, runParent(cfg)); err != nil {
		return err
	}

	sibling, err := newEndpointOperationsClient(ctx, cfg, "cloudfunctions.localhost")
	if err != nil {
		return fmt.Errorf("sibling operations client: %w", err)
	}
	defer sibling.Close()
	return operationsListExcludes(ctx, sibling, runParent(cfg), opName)
}

// checkFunctionsOperationsEndpoint mirrors checkRunOperationsEndpoint for Cloud
// Functions v2.
func checkFunctionsOperationsEndpoint(ctx context.Context, cfg Config) error {
	client, err := newFunctionsV2Client(ctx, cfg)
	if err != nil {
		return fmt.Errorf("functions v2 client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateFunction(ctx, &apiv2functionspb.CreateFunctionRequest{
		Parent:     functionsParent(cfg),
		FunctionId: cfg.ResourceName("gcpc-grpc-fn-ops"),
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
		},
	})
	if err != nil {
		return fmt.Errorf("create function: %w", err)
	}
	opName := op.Name()

	own, err := newEndpointOperationsClient(ctx, cfg, "cloudfunctions.localhost")
	if err != nil {
		return fmt.Errorf("functions operations client: %w", err)
	}
	defer own.Close()
	if err := operationsEndpointResolvesAndLists(ctx, own, opName, functionsParent(cfg)); err != nil {
		return err
	}

	sibling, err := newEndpointOperationsClient(ctx, cfg, "run.localhost")
	if err != nil {
		return fmt.Errorf("sibling operations client: %w", err)
	}
	defer sibling.Close()
	return operationsListExcludes(ctx, sibling, functionsParent(cfg), opName)
}

// checkWorkflowsOperationsEndpoint verifies google.longrunning.Operations is
// endpoint-scoped for Cloud Workflows and serves its operations list/delete
// surface: the operation a workflow create persisted is resolved and listed at
// the workflows endpoint, a sibling endpoint (run) does not list it, and
// deleting it at the workflows endpoint removes it from the page.
func checkWorkflowsOperationsEndpoint(ctx context.Context, cfg Config) error {
	client, err := newWorkflowsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("workflows client: %w", err)
	}
	defer client.Close()

	op, err := client.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
		Parent:     workflowParent(cfg),
		WorkflowId: cfg.ResourceName("gcpc-grpc-wf-ops"),
		Workflow: &workflowspb.Workflow{
			SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: workflowSource},
		},
	})
	if err != nil {
		return fmt.Errorf("create workflow: %w", err)
	}
	opName := op.Name()

	own, err := newEndpointOperationsClient(ctx, cfg, "workflows.localhost")
	if err != nil {
		return fmt.Errorf("workflows operations client: %w", err)
	}
	defer own.Close()
	if err := operationsEndpointResolvesAndLists(ctx, own, opName, workflowParent(cfg)); err != nil {
		return err
	}

	sibling, err := newEndpointOperationsClient(ctx, cfg, "run.localhost")
	if err != nil {
		return fmt.Errorf("sibling operations client: %w", err)
	}
	defer sibling.Close()
	if err := operationsListExcludes(ctx, sibling, workflowParent(cfg), opName); err != nil {
		return err
	}

	// operations.delete (declared by the workflows Discovery) removes it.
	if err := own.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: opName}); err != nil {
		return fmt.Errorf("DeleteOperation at the workflows endpoint: %w", err)
	}
	return operationsListExcludes(ctx, own, workflowParent(cfg), opName)
}

// operationsEndpointResolvesAndLists asserts the endpoint's Get resolves opName
// and its List page for parent includes it.
func operationsEndpointResolvesAndLists(ctx context.Context, client *longrunning.OperationsClient, opName, parent string) error {
	got, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: opName})
	if err != nil {
		return fmt.Errorf("GetOperation: %w", err)
	}
	if got.GetName() != opName {
		return fmt.Errorf("GetOperation name = %q, want %q", got.GetName(), opName)
	}
	return operationsListContains(ctx, client, parent, opName)
}
