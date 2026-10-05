package lambda_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/aws/provider/lambda"
	"jaiscloud/internal/clock"
	lambdaexec "jaiscloud/internal/executor/awslambda"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func newRequest(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		AccountID: "000000000000",
		Region:    "us-east-1",
		Params:    params,
		Clock:     clock.RealClock{},
		ResourceID: func(_, name string) string {
			return "arn:aws:lambda:us-east-1:000000000000:function:" + name
		},
	}
}

// createFn registers a function in the store and returns the provider.
func createFn(t *testing.T, p *lambda.FunctionProvider, name string, timeout int) {
	t.Helper()
	nr := newRequest(map[string]any{
		"FunctionName": name,
		"Runtime":      "provided",
		"Role":         "arn:aws:iam::000000000000:role/test",
		"Handler":      "main",
		"Timeout":      float64(timeout),
	})
	_, err := p.CreateFunction(context.Background(), nr)
	require.NoError(t, err)
}

// blockingExecutor blocks until the context is cancelled, then returns the ctx error.
type blockingExecutor struct{}

func (b *blockingExecutor) Invoke(ctx context.Context, _ lambdaexec.InvokeRequest) (lambdaexec.InvokeResult, error) {
	<-ctx.Done()
	return lambdaexec.InvokeResult{}, ctx.Err()
}
func (b *blockingExecutor) DeleteFunction(_ context.Context, _ string) {}
func (b *blockingExecutor) Reset(_ context.Context)                    {}
func (b *blockingExecutor) Close() error                               { return nil }

// ─── Timeout validation ───────────────────────────────────────────────────────

func TestCreateFunction_TimeoutBoundaries(t *testing.T) {
	cases := []struct {
		timeout int
		wantErr bool
	}{
		{0, true},
		{-1, true},
		{901, true},
		{1, false},
		{900, false},
		{3, false},
	}
	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &lambdaexec.MockExecutor{}, lambdaexec.LambdaConfig{})
			nr := newRequest(map[string]any{
				"FunctionName": "fn",
				"Runtime":      "provided",
				"Role":         "r",
				"Handler":      "h",
				"Timeout":      float64(tc.timeout),
			})
			_, err := p.CreateFunction(context.Background(), nr)
			if tc.wantErr {
				require.Error(t, err)
				var pe *model.ProviderError
				assert.ErrorAs(t, err, &pe)
				assert.Equal(t, "InvalidParameterValueException", pe.Code)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUpdateFunctionConfiguration_TimeoutValidation(t *testing.T) {
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &lambdaexec.MockExecutor{}, lambdaexec.LambdaConfig{})
	createFn(t, p, "fn", 3)

	nr := newRequest(map[string]any{
		"_function_name": "fn",
		"Timeout":        float64(0),
	})
	_, err := p.UpdateFunctionConfiguration(context.Background(), nr)
	require.Error(t, err)
	var pe *model.ProviderError
	assert.ErrorAs(t, err, &pe)
	assert.Equal(t, "InvalidParameterValueException", pe.Code)
}

// ─── Timeout envelope ─────────────────────────────────────────────────────────

func TestInvokeFunction_TimeoutReturnsAWSEnvelope(t *testing.T) {
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &blockingExecutor{}, lambdaexec.LambdaConfig{})
	createFn(t, p, "slow", 1) // 1-second timeout

	nr := newRequest(map[string]any{
		"_function_name":   "slow",
		"_invocation_type": "RequestResponse",
		"_payload":         []byte(`{}`),
	})
	resp, err := p.InvokeFunction(context.Background(), nr)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 200, resp.HTTPStatus)
	assert.Equal(t, "Unhandled", resp.Data["_function_error"])

	var body map[string]any
	require.NoError(t, json.Unmarshal(resp.Data["_payload"].([]byte), &body))
	assert.Equal(t, "Runtime.TimeoutError", body["errorType"])
	assert.Contains(t, body["errorMessage"].(string), "Task timed out after 1.00 seconds")
}

// ─── Concurrency limit ────────────────────────────────────────────────────────

func TestInvokeFunction_ConcurrencyLimitThrottles(t *testing.T) {
	cfg := lambdaexec.LambdaConfig{ConcurrencyLimit: 1}
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &blockingExecutor{}, cfg)
	createFn(t, p, "fn", 10)

	// First invocation runs (blocks in background).
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	done := make(chan struct{})
	go func() {
		defer close(done)
		nr := newRequest(map[string]any{"_function_name": "fn", "_payload": []byte(`{}`)})
		p.InvokeFunction(ctx1, nr) //nolint:errcheck
	}()

	// Give the goroutine time to acquire the slot.
	time.Sleep(50 * time.Millisecond)

	// Second invocation should be throttled.
	nr2 := newRequest(map[string]any{"_function_name": "fn", "_payload": []byte(`{}`)})
	_, err := p.InvokeFunction(context.Background(), nr2)
	require.Error(t, err)
	var pe *model.ProviderError
	assert.ErrorAs(t, err, &pe)
	assert.Equal(t, "TooManyRequestsException", pe.Code)
	assert.Equal(t, 429, pe.HTTPStatus)

	cancel1()
	<-done
}

func TestInvokeFunction_ZeroConcurrencyIsUnlimited(t *testing.T) {
	cfg := lambdaexec.LambdaConfig{ConcurrencyLimit: 0}
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &lambdaexec.MockExecutor{}, cfg)
	createFn(t, p, "fn", 3)

	for i := 0; i < 20; i++ {
		nr := newRequest(map[string]any{"_function_name": "fn", "_payload": []byte(`{}`)})
		_, err := p.InvokeFunction(context.Background(), nr)
		require.NoError(t, err)
	}
}

// ─── Payload size gates ───────────────────────────────────────────────────────

func TestInvokeFunction_SyncPayloadTooLarge(t *testing.T) {
	cfg := lambdaexec.LambdaConfig{SyncPayloadMax: 100}
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &lambdaexec.MockExecutor{}, cfg)
	createFn(t, p, "fn", 3)

	nr := newRequest(map[string]any{
		"_function_name": "fn",
		"_payload":       bytes.Repeat([]byte("x"), 101),
	})
	_, err := p.InvokeFunction(context.Background(), nr)
	require.Error(t, err)
	var pe *model.ProviderError
	assert.ErrorAs(t, err, &pe)
	assert.Equal(t, "RequestEntityTooLargeException", pe.Code)
	assert.Equal(t, 413, pe.HTTPStatus)
}

func TestInvokeFunction_AsyncPayloadTooLarge(t *testing.T) {
	cfg := lambdaexec.LambdaConfig{AsyncPayloadMax: 50}
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &lambdaexec.MockExecutor{}, cfg)
	createFn(t, p, "fn", 3)

	nr := newRequest(map[string]any{
		"_function_name":   "fn",
		"_invocation_type": "Event",
		"_payload":         bytes.Repeat([]byte("x"), 51),
	})
	_, err := p.InvokeFunction(context.Background(), nr)
	require.Error(t, err)
	var pe *model.ProviderError
	assert.ErrorAs(t, err, &pe)
	assert.Equal(t, "RequestEntityTooLargeException", pe.Code)
}

func TestInvokeFunction_AsyncWithinLimit_Returns202(t *testing.T) {
	cfg := lambdaexec.LambdaConfig{AsyncPayloadMax: 100}
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &lambdaexec.MockExecutor{}, cfg)
	createFn(t, p, "fn", 3)

	nr := newRequest(map[string]any{
		"_function_name":   "fn",
		"_invocation_type": "Event",
		"_payload":         bytes.Repeat([]byte("x"), 100),
	})
	resp, err := p.InvokeFunction(context.Background(), nr)
	require.NoError(t, err)
	assert.Equal(t, 202, resp.HTTPStatus)
}

func TestInvokeFunction_ResponseTooLarge_ReturnsEnvelope(t *testing.T) {
	cfg := lambdaexec.LambdaConfig{ResponsePayloadMax: 10}
	// MockExecutor echoes the payload; send a response that exceeds the cap.
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &lambdaexec.MockExecutor{}, cfg)
	createFn(t, p, "fn", 3)

	nr := newRequest(map[string]any{
		"_function_name": "fn",
		"_payload":       bytes.Repeat([]byte("x"), 11),
	})
	resp, err := p.InvokeFunction(context.Background(), nr)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.HTTPStatus)
	assert.Equal(t, "Unhandled", resp.Data["_function_error"])

	var body map[string]any
	require.NoError(t, json.Unmarshal(resp.Data["_payload"].([]byte), &body))
	assert.Equal(t, "Function.ResponseSizeTooLarge", body["errorType"])
	assert.True(t, strings.Contains(body["errorMessage"].(string), "exceeded maximum"))
}

func TestInvokeFunction_ZeroResponseMax_NoLimit(t *testing.T) {
	cfg := lambdaexec.LambdaConfig{ResponsePayloadMax: 0}
	p := lambda.NewWithLimits(store.NewMemoryResourceStore(), &lambdaexec.MockExecutor{}, cfg)
	createFn(t, p, "fn", 3)

	bigPayload := bytes.Repeat([]byte("x"), 10*1024*1024)
	nr := newRequest(map[string]any{
		"_function_name": "fn",
		"_payload":       bigPayload,
	})
	resp, err := p.InvokeFunction(context.Background(), nr)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.HTTPStatus)
	assert.Nil(t, resp.Data["_function_error"])
}
