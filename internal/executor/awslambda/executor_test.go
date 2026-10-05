package awslambda_test

import (
	"context"
	"testing"

	lambdaexec "jaiscloud/internal/executor/awslambda"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── MockExecutor ─────────────────────────────────────────────────────────────

func TestMockExecutor_EchoesPayload(t *testing.T) {
	exec := &lambdaexec.MockExecutor{}
	payload := []byte(`{"key":"value"}`)
	got, err := exec.Invoke(context.Background(), lambdaexec.InvokeRequest{
		FunctionName: "my-fn",
		Payload:      payload,
	})
	require.NoError(t, err)
	assert.Equal(t, payload, got.Payload)
}

func TestMockExecutor_NilPayload(t *testing.T) {
	exec := &lambdaexec.MockExecutor{}
	got, err := exec.Invoke(context.Background(), lambdaexec.InvokeRequest{FunctionName: "fn"})
	require.NoError(t, err)
	assert.Nil(t, got.Payload)
}

func TestMockExecutor_Close(t *testing.T) {
	exec := &lambdaexec.MockExecutor{}
	assert.NoError(t, exec.Close())
}

// ─── Config / ImageForRuntime ─────────────────────────────────────────────────

func TestImageForRuntime_KnownRuntime(t *testing.T) {
	cases := []struct {
		runtime string
		want    string
	}{
		{"python3.12", "public.ecr.aws/lambda/python:3.12"},
		{"nodejs20.x", "public.ecr.aws/lambda/nodejs:20"},
		{"java21", "public.ecr.aws/lambda/java:21"},
		{"go1.x", "public.ecr.aws/lambda/provided:al2"},
		{"provided.al2", "public.ecr.aws/lambda/provided:al2"},
	}
	for _, tc := range cases {
		t.Run(tc.runtime, func(t *testing.T) {
			img := lambdaexec.ImageForRuntime(
				lambdaexec.InvokeRequest{Runtime: tc.runtime},
				lambdaexec.LambdaConfig{},
			)
			assert.Equal(t, tc.want, img)
		})
	}
}

func TestImageForRuntime_UnknownRuntime_FallsBack(t *testing.T) {
	img := lambdaexec.ImageForRuntime(
		lambdaexec.InvokeRequest{Runtime: "cobol1.x"},
		lambdaexec.LambdaConfig{},
	)
	assert.Equal(t, "public.ecr.aws/lambda/provided:al2", img)
}

func TestImageForRuntime_PerRequestImageOverrides(t *testing.T) {
	img := lambdaexec.ImageForRuntime(
		lambdaexec.InvokeRequest{Runtime: "python3.12", Image: "my-custom:latest"},
		lambdaexec.LambdaConfig{DefaultImage: "global-override:1"},
	)
	assert.Equal(t, "my-custom:latest", img)
}

func TestImageForRuntime_DefaultImageOverridesRuntime(t *testing.T) {
	img := lambdaexec.ImageForRuntime(
		lambdaexec.InvokeRequest{Runtime: "python3.12"},
		lambdaexec.LambdaConfig{DefaultImage: "global-override:1"},
	)
	assert.Equal(t, "global-override:1", img)
}

// ─── NewExecutor factory ──────────────────────────────────────────────────────

func TestNewExecutor_DefaultIsMock(t *testing.T) {
	exec := lambdaexec.NewExecutor(lambdaexec.LambdaConfig{Mode: "mock"})
	require.NotNil(t, exec)
	payload := []byte(`"hello"`)
	got, err := exec.Invoke(context.Background(), lambdaexec.InvokeRequest{Payload: payload})
	require.NoError(t, err)
	assert.Equal(t, payload, got.Payload)
	exec.Close()
}

func TestNewExecutor_EmptyModeIsMock(t *testing.T) {
	exec := lambdaexec.NewExecutor(lambdaexec.LambdaConfig{})
	require.NotNil(t, exec)
	exec.Close()
}
