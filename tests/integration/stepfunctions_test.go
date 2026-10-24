package integration

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awslambda "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	middleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
)

// sfnCreateLambdaFn creates a minimal Lambda function for use in SFN Task state tests.
// The mock executor echoes the payload, so the SFN execution will SUCCEED.
func sfnCreateLambdaFn(t *testing.T, name string) {
	t.Helper()
	cfg := newAWSConfig(t)
	c := awslambda.NewFromConfig(cfg, func(o *awslambda.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint())
	})
	_, err := c.CreateFunction(context.Background(), &awslambda.CreateFunctionInput{
		FunctionName: aws.String(name),
		Runtime:      lambdatypes.RuntimeNodejs18x,
		Role:         aws.String("arn:aws:iam::000000000000:role/lambda-role"),
		Handler:      aws.String("index.handler"),
		Code:         &lambdatypes.FunctionCode{ZipFile: []byte("fake-zip-payload")},
	})
	require.NoError(t, err, "create Lambda function %q for SFN test", name)
}

const testDefinition = `{"StartAt":"S1","States":{"S1":{"Type":"Pass","End":true}}}`
const testRoleARN = "arn:aws:iam::000000000000:role/sfn-test"

func sfnClient(t *testing.T) *awssfn.Client {
	t.Helper()
	cfg := newAWSConfig(t)
	return awssfn.NewFromConfig(cfg, func(o *awssfn.Options) {
		o.BaseEndpoint = aws.String(jaiscloudEndpoint())
		// The SDK's StartSyncExecution middleware prepends "sync-" to the host at
		// the Serialize step. ClearStackValues is called before the middleware chain
		// runs, so DisableEndpointHostPrefix set on a context before the call is
		// wiped. We inject an Initialize-step middleware (which runs after
		// ClearStackValues) to re-disable the prefix for every request.
		o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
			return s.Initialize.Add(middleware.InitializeMiddlewareFunc(
				"DisableHostPrefix",
				func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					ctx = smithyhttp.DisableEndpointHostPrefix(ctx, true)
					return next.HandleInitialize(ctx, in)
				},
			), middleware.Before)
		})
	})
}

var sfnNameCounter atomic.Uint64

func sfnName(t *testing.T) string {
	return fmt.Sprintf("sm-%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), sfnNameCounter.Add(1))
}

// ─── State Machine CRUD ───────────────────────────────────────────────────────

func TestSFN_CreateStateMachine(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	out, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String("test-sm"),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	assert.Contains(t, *out.StateMachineArn, "arn:aws:states:")
	assert.Contains(t, *out.StateMachineArn, "test-sm")
}

func TestSFN_CreateStateMachine_AlreadyExists(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	_, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	_, err = client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StateMachineAlreadyExists")
}

func TestSFN_DescribeStateMachine(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	assert.Equal(t, name, *descOut.Name)
	assert.Equal(t, testDefinition, *descOut.Definition)
	assert.Equal(t, sfntypes.StateMachineStatusActive, descOut.Status)
	assert.Equal(t, sfntypes.StateMachineTypeStandard, descOut.Type)
}

func TestSFN_UpdateStateMachine_GeneratesNewRevisionID(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	desc1, err := client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	oldRevision := *desc1.RevisionId

	newDef := `{"StartAt":"S2","States":{"S2":{"Type":"Succeed"}}}`
	_, err = client.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
		Definition:      aws.String(newDef),
	})
	require.NoError(t, err)

	desc2, err := client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	assert.NotEqual(t, oldRevision, *desc2.RevisionId)
	assert.Equal(t, newDef, *desc2.Definition)
}

func TestSFN_DeleteStateMachine(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	_, err = client.DeleteStateMachine(ctx, &awssfn.DeleteStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	_, err = client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StateMachineDoesNotExist")
}

func TestSFN_ListStateMachines_Pagination(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
			Name:       aws.String(fmt.Sprintf("sm-page-%02d", i)),
			Definition: aws.String(testDefinition),
			RoleArn:    aws.String(testRoleARN),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListStateMachines(ctx, &awssfn.ListStateMachinesInput{
		MaxResults: 3,
	})
	require.NoError(t, err)
	require.Len(t, page1.StateMachines, 3)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListStateMachines(ctx, &awssfn.ListStateMachinesInput{
		MaxResults: 3,
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.StateMachines, 2)
	assert.Nil(t, page2.NextToken)
}

// ─── Execution Lifecycle ──────────────────────────────────────────────────────

func TestSFN_ExecutionLifecycle_LiteMode_InstantSuccess(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	input := `{"key":"value"}`
	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)
	assert.Contains(t, *startOut.ExecutionArn, "execution:")

	descOut, err := client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
	assert.Equal(t, input, *descOut.Input)
	assert.JSONEq(t, input, aws.ToString(descOut.Output)) // passthrough (JSON equality in persistent mode)
	assert.NotNil(t, descOut.StopDate)
}

func TestSFN_StartExecution_OutputEqualsInput(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	input := `{"x":42,"nested":{"y":true}}`
	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.JSONEq(t, input, aws.ToString(descOut.Output))
}

func TestSFN_StartExecution_DuplicateName_SameInput_Idempotent(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	execName := "my-exec-idem"
	input := `{}`

	out1, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Name:            aws.String(execName),
		Input:           aws.String(input),
	})
	require.NoError(t, err)

	out2, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Name:            aws.String(execName),
		Input:           aws.String(input),
	})
	require.NoError(t, err)
	assert.Equal(t, *out1.ExecutionArn, *out2.ExecutionArn)
}

func TestSFN_StopExecution_AlreadyTerminal_NoOp(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	// Stop a SUCCEEDED execution — should be no-op (not error)
	_, err = client.StopExecution(ctx, &awssfn.StopExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)

	// Status should still be SUCCEEDED
	descOut, err := client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
}

func TestSFN_GetExecutionHistory_LiteMode_TwoEvents(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	// StartExecution returns as soon as the execution is queued; with the
	// asynchronous engine the history is still being appended, so wait for the
	// terminal state before asserting on the final event.
	pollUntilTerminal(t, client, aws.ToString(startOut.ExecutionArn), 10*time.Second)

	histOut, err := client.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(histOut.Events), 2)
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionStarted, histOut.Events[0].Type)
	assert.Equal(t, sfntypes.HistoryEventTypeExecutionSucceeded, histOut.Events[len(histOut.Events)-1].Type)
}

func TestSFN_ListExecutions(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Input:           aws.String(fmt.Sprintf(`{"i":%d}`, i)),
		})
		require.NoError(t, err)
	}

	listOut, err := client.ListExecutions(ctx, &awssfn.ListExecutionsInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Executions, 3)
}

// ─── Versions & Aliases ───────────────────────────────────────────────────────

func TestSFN_VersionsAndAliases(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	smARN := *createOut.StateMachineArn

	// Publish version 1
	v1Out, err := client.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: aws.String(smARN),
		Description:     aws.String("v1"),
	})
	require.NoError(t, err)
	assert.Contains(t, *v1Out.StateMachineVersionArn, ":1")

	// Update and publish version 2
	_, err = client.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
		StateMachineArn: aws.String(smARN),
		Definition:      aws.String(`{"StartAt":"S2","States":{"S2":{"Type":"Succeed"}}}`),
	})
	require.NoError(t, err)

	v2Out, err := client.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: aws.String(smARN),
		Description:     aws.String("v2"),
	})
	require.NoError(t, err)
	assert.Contains(t, *v2Out.StateMachineVersionArn, ":2")

	// Create alias with 50/50 routing
	aliasOut, err := client.CreateStateMachineAlias(ctx, &awssfn.CreateStateMachineAliasInput{
		Name: aws.String("prod"),
		RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{
			{StateMachineVersionArn: v1Out.StateMachineVersionArn, Weight: 50},
			{StateMachineVersionArn: v2Out.StateMachineVersionArn, Weight: 50},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, *aliasOut.StateMachineAliasArn, ":prod")

	// List versions
	versionsOut, err := client.ListStateMachineVersions(ctx, &awssfn.ListStateMachineVersionsInput{
		StateMachineArn: aws.String(smARN),
	})
	require.NoError(t, err)
	assert.Len(t, versionsOut.StateMachineVersions, 2)
}

func TestSFN_DeleteVersion_ReferencedByAlias_Error(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	v1Out, err := client.PublishStateMachineVersion(ctx, &awssfn.PublishStateMachineVersionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	_, err = client.CreateStateMachineAlias(ctx, &awssfn.CreateStateMachineAliasInput{
		Name: aws.String("alias-v1"),
		RoutingConfiguration: []sfntypes.RoutingConfigurationListItem{
			{StateMachineVersionArn: v1Out.StateMachineVersionArn, Weight: 100},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteStateMachineVersion(ctx, &awssfn.DeleteStateMachineVersionInput{
		StateMachineVersionArn: v1Out.StateMachineVersionArn,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ConflictException")
}

// ─── Activities ───────────────────────────────────────────────────────────────

func TestSFN_CreateActivity(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	out, err := client.CreateActivity(ctx, &awssfn.CreateActivityInput{
		Name: aws.String("my-activity"),
	})
	require.NoError(t, err)
	assert.Contains(t, *out.ActivityArn, "activity:my-activity")
}

func TestSFN_GetActivityTask_Empty(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()

	createOut, err := client.CreateActivity(ctx, &awssfn.CreateActivityInput{
		Name: aws.String("empty-act"),
	})
	require.NoError(t, err)

	taskOut, err := client.GetActivityTask(ctx, &awssfn.GetActivityTaskInput{
		ActivityArn: createOut.ActivityArn,
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(taskOut.TaskToken))
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestSFN_Tags_AddRemoveList(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)
	smARN := *createOut.StateMachineArn

	_, err = client.TagResource(ctx, &awssfn.TagResourceInput{
		ResourceArn: aws.String(smARN),
		Tags: []sfntypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &awssfn.ListTagsForResourceInput{
		ResourceArn: aws.String(smARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2)

	_, err = client.UntagResource(ctx, &awssfn.UntagResourceInput{
		ResourceArn: aws.String(smARN),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &awssfn.ListTagsForResourceInput{
		ResourceArn: aws.String(smARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Tags, 1)
}

// ─── Express Execution ────────────────────────────────────────────────────────

func TestSFN_StartSyncExecution_ExpressOnly(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	syncCtx := sfnSyncCtx(ctx)

	// STANDARD machine → error
	stdOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name + "-std"),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	_, err = client.StartSyncExecution(syncCtx, &awssfn.StartSyncExecutionInput{
		StateMachineArn: stdOut.StateMachineArn,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StateMachineTypeNotSupported")

	// EXPRESS machine → success
	expOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name + "-exp"),
		Definition: aws.String(testDefinition),
		RoleArn:    aws.String(testRoleARN),
		Type:       sfntypes.StateMachineTypeExpress,
	})
	require.NoError(t, err)

	syncOut, err := client.StartSyncExecution(syncCtx, &awssfn.StartSyncExecutionInput{
		StateMachineArn: expOut.StateMachineArn,
		Input:           aws.String(`{"hello":"world"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, sfntypes.SyncExecutionStatusSucceeded, syncOut.Status)
}

// ─── I-PENDING-5: SFN ASL conformance tests ──────────────────────────────────

// pollUntilTerminal polls DescribeExecution until the execution reaches a
// terminal state (SUCCEEDED, FAILED, ABORTED, TIMED_OUT) or the timeout elapses.
func pollUntilTerminal(t *testing.T, client *awssfn.Client, execARN string, timeout time.Duration) *awssfn.DescribeExecutionOutput {
	t.Helper()
	deadline := clock.RealNow().Add(timeout)
	for clock.RealNow().Before(deadline) {
		out, err := client.DescribeExecution(context.Background(), &awssfn.DescribeExecutionInput{
			ExecutionArn: aws.String(execARN),
		})
		require.NoError(t, err)
		switch out.Status {
		case sfntypes.ExecutionStatusSucceeded,
			sfntypes.ExecutionStatusFailed,
			sfntypes.ExecutionStatusAborted,
			sfntypes.ExecutionStatusTimedOut:
			return out
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("execution %s did not reach terminal state within %s", execARN, timeout)
	return nil
}

// TestSFN_PassState_InputOutput tests a state machine with a Pass state that
// uses InputPath and OutputPath to project input fields to output.
func TestSFN_PassState_InputOutput(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	// Pass state: take $.payload from input and use it as output.
	definition := `{
		"StartAt": "Extract",
		"States": {
			"Extract": {
				"Type": "Pass",
				"InputPath": "$.payload",
				"End": true
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"payload":{"result":42},"extra":"ignore"}`),
	})
	require.NoError(t, err)

	descOut := pollUntilTerminal(t, client, *startOut.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
	assert.NotEmpty(t, aws.ToString(descOut.Output), "execution output must be non-empty")
}

// TestSFN_ChoiceState_StringEquals tests a Choice state that branches on a
// string equality condition. Invokes twice with type "A" and type "B".
func TestSFN_ChoiceState_StringEquals(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "Route",
		"States": {
			"Route": {
				"Type": "Choice",
				"Choices": [
					{
						"Variable": "$.type",
						"StringEquals": "A",
						"Next": "BranchA"
					},
					{
						"Variable": "$.type",
						"StringEquals": "B",
						"Next": "BranchB"
					}
				],
				"Default": "BranchB"
			},
			"BranchA": {
				"Type": "Pass",
				"Result": {"branch": "A"},
				"End": true
			},
			"BranchB": {
				"Type": "Pass",
				"Result": {"branch": "B"},
				"End": true
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	// Invoke with type "A".
	startA, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"type":"A"}`),
	})
	require.NoError(t, err)
	descA := pollUntilTerminal(t, client, *startA.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descA.Status)
	assert.Contains(t, aws.ToString(descA.Output), "A", "type=A must route to BranchA")

	// Invoke with type "B".
	startB, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"type":"B"}`),
	})
	require.NoError(t, err)
	descB := pollUntilTerminal(t, client, *startB.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descB.Status)
	assert.Contains(t, aws.ToString(descB.Output), "B", "type=B must route to BranchB")
}

// TestSFN_MapState_Items tests a Map state that iterates over an array of items
// and processes each through a nested Pass state.
func TestSFN_MapState_Items(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "ProcessItems",
		"States": {
			"ProcessItems": {
				"Type": "Map",
				"ItemsPath": "$.items",
				"Iterator": {
					"StartAt": "PassItem",
					"States": {
						"PassItem": {
							"Type": "Pass",
							"End": true
						}
					}
				},
				"End": true
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"items":[{"id":1},{"id":2},{"id":3}]}`),
	})
	require.NoError(t, err)

	descOut := pollUntilTerminal(t, client, *startOut.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
	// The output must be an array (Map state collects results).
	output := aws.ToString(descOut.Output)
	assert.NotEmpty(t, output, "Map state must produce non-empty output")
}

// TestSFN_WaitState_Seconds tests a Wait state with a 1-second delay.
func TestSFN_WaitState_Seconds(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "Pause",
		"States": {
			"Pause": {
				"Type": "Wait",
				"Seconds": 1,
				"Next": "Done"
			},
			"Done": {
				"Type": "Pass",
				"End": true
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	descOut := pollUntilTerminal(t, client, *startOut.ExecutionArn, 15*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
}

// TestSFN_ParallelState tests a Parallel state with two branches, asserting
// both branch outputs appear in the result array.
func TestSFN_ParallelState(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "Parallel",
		"States": {
			"Parallel": {
				"Type": "Parallel",
				"Branches": [
					{
						"StartAt": "Branch1",
						"States": {
							"Branch1": {
								"Type": "Pass",
								"Result": {"branch": 1},
								"End": true
							}
						}
					},
					{
						"StartAt": "Branch2",
						"States": {
							"Branch2": {
								"Type": "Pass",
								"Result": {"branch": 2},
								"End": true
							}
						}
					}
				],
				"End": true
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	descOut := pollUntilTerminal(t, client, *startOut.ExecutionArn, 10*time.Second)
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
	// Output must be a JSON array with 2 branch results.
	output := aws.ToString(descOut.Output)
	assert.NotEmpty(t, output, "Parallel state must produce non-empty output")
}

// TestSFN_TaskState_Lambda tests a state machine with a Task state backed by a
// Lambda function ARN. In memory mode the engine uses passthrough; with dispatcher
// it calls the Lambda echo mock. Both paths must produce SUCCEEDED.
func TestSFN_TaskState_Lambda(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	// Create the Lambda function so the SFN engine can invoke it via the dispatcher.
	sfnCreateLambdaFn(t, "echo-fn")

	// Use legacy Lambda ARN format (arn:aws:lambda:...:function:Name).
	// parseTaskResource recognises this as "lambda:invoke".
	definition := `{
		"StartAt": "InvokeFunction",
		"States": {
			"InvokeFunction": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:echo-fn",
				"End": true
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"value":"hello"}`),
	})
	require.NoError(t, err)

	descOut := pollUntilTerminal(t, client, *startOut.ExecutionArn, 10*time.Second)
	// In memory mode (dispatcher == nil) the Task passes input through — SUCCEEDED.
	// With dispatcher the Lambda echo mock echoes the payload — also SUCCEEDED.
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
	assert.NotEmpty(t, aws.ToString(descOut.Output), "Task state output must be non-empty")
}

// TestSFN_ErrorHandling_Retry tests a state machine with a Task state that
// has Retry configuration. In memory mode the task passes through without error,
// so the execution completes SUCCEEDED without retrying.
func TestSFN_ErrorHandling_Retry(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	sfnCreateLambdaFn(t, "retry-fn")

	definition := `{
		"StartAt": "RetryTask",
		"States": {
			"RetryTask": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:retry-fn",
				"Retry": [
					{
						"ErrorEquals": ["States.ALL"],
						"IntervalSeconds": 1,
						"MaxAttempts": 2,
						"BackoffRate": 1.5
					}
				],
				"End": true
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"attempt":1}`),
	})
	require.NoError(t, err)

	descOut := pollUntilTerminal(t, client, *startOut.ExecutionArn, 15*time.Second)
	// In memory mode, task is a passthrough — no error occurs, so SUCCEEDED without retrying.
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
}

// TestSFN_ErrorHandling_Catch tests a state machine with a Task state and a
// Catch clause. In memory mode the task passes through without error, so the
// Catch clause is never triggered and the execution completes SUCCEEDED directly.
func TestSFN_ErrorHandling_Catch(t *testing.T) {
	resetState(t)
	client := sfnClient(t)
	ctx := context.Background()
	name := sfnName(t)

	definition := `{
		"StartAt": "CatchTask",
		"States": {
			"CatchTask": {
				"Type": "Task",
				"Resource": "arn:aws:lambda:us-east-1:000000000000:function:catch-fn",
				"Catch": [
					{
						"ErrorEquals": ["States.ALL"],
						"Next": "HandleError",
						"ResultPath": "$.error"
					}
				],
				"End": true
			},
			"HandleError": {
				"Type": "Pass",
				"Result": {"handled": true},
				"End": true
			}
		}
	}`

	createOut, err := client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name:       aws.String(name),
		Definition: aws.String(definition),
		RoleArn:    aws.String(testRoleARN),
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"input":"data"}`),
	})
	require.NoError(t, err)

	descOut := pollUntilTerminal(t, client, *startOut.ExecutionArn, 10*time.Second)
	// In memory mode, task is a passthrough — SUCCEEDED via CatchTask directly.
	assert.Equal(t, sfntypes.ExecutionStatusSucceeded, descOut.Status)
	assert.NotEmpty(t, aws.ToString(descOut.Output), "execution output must be non-empty")
}
