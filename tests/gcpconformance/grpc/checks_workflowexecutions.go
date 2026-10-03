package grpcconformance

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	executions "cloud.google.com/go/workflows/executions/apiv1"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// workflowExecutionsChecks covers the Cloud Workflow Executions v1 surface
// (google.cloud.workflows.executions.v1.Executions) via the official generated
// cloud.google.com/go/workflows/executions/apiv1 client.
//
// An execution cannot exist without an owning workflow, and the Workflows
// management API is REST-only in the emulator (it is not part of this phase's
// gRPC surface), so each probe first ensures a run-unique workflow exists
// through the emulator's REST management endpoint (cfg.RESTEndpoint) and then
// exercises the gRPC execution RPCs against it. That mirrors the real
// deployment: a workflow is deployed over the management API, then run over the
// executions API.
//
// The probes are self-contained — each creates its own execution — so they do
// not depend on execution order. Names are run-unique via cfg.ResourceName, so a
// long-lived emulator never sees cross-run collisions.
func workflowExecutionsChecks() []Check {
	return []Check{
		{Service: "workflowexecutions", RPC: "CreateExecution", Method: "CreateExecution", KeyField: "state=SUCCEEDED + result=42", Run: checkWorkflowExecCreate},
		{Service: "workflowexecutions", RPC: "GetExecution", Method: "GetExecution", KeyField: "name/state/result round-trip", Run: checkWorkflowExecGet},
		{Service: "workflowexecutions", RPC: "ListExecutions", Method: "ListExecutions", KeyField: "created execution present in list", Run: checkWorkflowExecList},
		{Service: "workflowexecutions", RPC: "CancelExecution", Method: "CancelExecution", KeyField: "terminal execution returned unchanged", Run: checkWorkflowExecCancel},
	}
}

// workflowExecLocation is the region the probe workflow is deployed to.
const workflowExecLocation = "us-central1"

// workflowExecSource is a deterministic a+b workflow whose execution result is
// the JSON number 42.
const workflowExecSource = "main:\n  params: [a, b]\n  steps:\n    - init:\n        assign:\n          - sum: ${a + b}\n    - done:\n        return: ${sum}\n"

// workflowExecArgument is the run-unique execution argument (a+b=42).
const workflowExecArgument = `{"a": 20, "b": 22}`

// newWorkflowExecutionsClient dials the emulator and returns the official
// generated executions client. It mirrors the other gRPC probes: an explicit
// insecure endpoint with authentication disabled.
func newWorkflowExecutionsClient(ctx context.Context, cfg Config) (*executions.Client, error) {
	return executions.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// workflowExecWorkflowName is the run-unique workflow all probes share.
func workflowExecWorkflowName(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s/workflows/%s", cfg.Project, workflowExecLocation, cfg.ResourceName("gcpc-grpc-wf"))
}

// ensureWorkflow deploys the run-unique probe workflow over the emulator's REST
// management endpoint, treating an already-exists (409) as success so repeated
// probes in the same run are idempotent. It returns the workflow's full name.
func ensureWorkflow(ctx context.Context, cfg Config) (string, error) {
	name := workflowExecWorkflowName(cfg)
	url := fmt.Sprintf("%s/v1/projects/%s/locations/%s/workflows?workflowId=%s",
		strings.TrimRight(cfg.RESTEndpoint, "/"), cfg.Project, workflowExecLocation, cfg.ResourceName("gcpc-grpc-wf"))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(fmt.Sprintf(`{"sourceContents":%q}`, workflowExecSource)))
	if err != nil {
		return "", fmt.Errorf("build create-workflow request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("create workflow: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return "", fmt.Errorf("create workflow: HTTP %d", resp.StatusCode)
	}
	return name, nil
}

// createExecution runs the probe workflow once over gRPC and returns the
// terminal execution.
func createExecution(ctx context.Context, cfg Config) (*executions.Client, *executionspb.Execution, error) {
	parent, err := ensureWorkflow(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	client, err := newWorkflowExecutionsClient(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("new client: %w", err)
	}
	exec, err := client.CreateExecution(ctx, &executionspb.CreateExecutionRequest{
		Parent:    parent,
		Execution: &executionspb.Execution{Argument: workflowExecArgument},
	})
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("CreateExecution: %w", err)
	}
	return client, exec, nil
}

// Check 1: CreateExecution must run the workflow synchronously and return the
// SUCCEEDED execution with the computed result and revision.
func checkWorkflowExecCreate(ctx context.Context, cfg Config) error {
	client, exec, err := createExecution(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	if exec.GetState() != executionspb.Execution_SUCCEEDED {
		return fmt.Errorf("CreateExecution state = %v, want SUCCEEDED", exec.GetState())
	}
	if exec.GetResult() != "42" {
		return fmt.Errorf("CreateExecution result = %q, want %q", exec.GetResult(), "42")
	}
	if exec.GetWorkflowRevisionId() == "" {
		return fmt.Errorf("CreateExecution workflowRevisionId is empty")
	}
	if want := workflowExecWorkflowName(cfg) + "/executions/"; !strings.HasPrefix(exec.GetName(), want) {
		return fmt.Errorf("CreateExecution name = %q, want prefix %q", exec.GetName(), want)
	}
	return nil
}

// Check 2: GetExecution must return the created execution with the FULL view
// default (state + result intact).
func checkWorkflowExecGet(ctx context.Context, cfg Config) error {
	client, exec, err := createExecution(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	got, err := client.GetExecution(ctx, &executionspb.GetExecutionRequest{Name: exec.GetName()})
	if err != nil {
		return fmt.Errorf("GetExecution: %w", err)
	}
	if got.GetName() != exec.GetName() {
		return fmt.Errorf("GetExecution name = %q, want %q", got.GetName(), exec.GetName())
	}
	if got.GetState() != executionspb.Execution_SUCCEEDED {
		return fmt.Errorf("GetExecution state = %v, want SUCCEEDED", got.GetState())
	}
	if got.GetResult() != "42" {
		return fmt.Errorf("GetExecution result = %q, want %q", got.GetResult(), "42")
	}
	return nil
}

// Check 3: ListExecutions must include the execution created by this probe.
func checkWorkflowExecList(ctx context.Context, cfg Config) error {
	client, exec, err := createExecution(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	it := client.ListExecutions(ctx, &executionspb.ListExecutionsRequest{Parent: workflowExecWorkflowName(cfg)})
	for {
		got, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListExecutions did not include %q", exec.GetName())
		}
		if err != nil {
			return fmt.Errorf("ListExecutions: %w", err)
		}
		if got.GetName() == exec.GetName() {
			if got.GetState() != executionspb.Execution_SUCCEEDED {
				return fmt.Errorf("ListExecutions state = %v, want SUCCEEDED", got.GetState())
			}
			return nil
		}
	}
}

// Check 4: CancelExecution on a terminal execution is a no-op that returns the
// execution unchanged.
func checkWorkflowExecCancel(ctx context.Context, cfg Config) error {
	client, exec, err := createExecution(ctx, cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	cancelled, err := client.CancelExecution(ctx, &executionspb.CancelExecutionRequest{Name: exec.GetName()})
	if err != nil {
		return fmt.Errorf("CancelExecution: %w", err)
	}
	if cancelled.GetState() != executionspb.Execution_SUCCEEDED {
		return fmt.Errorf("CancelExecution state = %v, want SUCCEEDED (terminal no-op)", cancelled.GetState())
	}
	return nil
}
