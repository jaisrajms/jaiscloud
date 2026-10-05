package awslambda

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLambdaProfile_ReproducesContract locks the seam extraction: the profile
// must reproduce the previous invocation path/method, mount dirs, workload
// naming, entrypoint args and image mapping.
func TestLambdaProfile_ReproducesContract(t *testing.T) {
	p := LambdaProfile{}
	req := InvokeRequest{FunctionName: "fn", Handler: "app.handler", Payload: []byte(`{"x":1}`)}

	assert.Equal(t, "lambda", p.Name())
	assert.Equal(t, 8080, p.InvocationPort())
	assert.Equal(t, "/var/task", p.CodeMountDir())
	assert.Equal(t, "/opt", p.LayerMountDir())
	assert.Equal(t, "jaiscloud-lambda", p.WorkloadLabel())
	assert.Equal(t, "jc-lambda-abcdef01-", p.WorkloadNamePrefix("abcdef0123456789"))
	assert.Equal(t, "jc-lambda-", p.WorkloadNamePrefix(""))
	assert.Equal(t, []string{"app.handler"}, p.ContainerArgs(req))
	assert.Nil(t, p.ContainerArgs(InvokeRequest{FunctionName: "fn"}))

	inv := p.Invocation(req)
	assert.Equal(t, "POST", inv.Method)
	assert.Equal(t, "/2015-03-31/functions/function/invocations", inv.Path)
	assert.Equal(t, "application/json", inv.Header["Content-Type"])
	assert.Equal(t, req.Payload, inv.Body)

	if _, err := p.DecodeResponse(200, []byte("ok")); err != nil {
		t.Fatalf("200 decode errored: %v", err)
	}
	if _, err := p.DecodeResponse(500, nil); err == nil {
		t.Fatal("500 decode must error")
	}
}

// TestLambdaProfile_ImageMapping asserts the profile delegates to the Lambda
// image table with the documented precedence.
func TestLambdaProfile_ImageMapping(t *testing.T) {
	p := LambdaProfile{}
	got := p.ImageForRuntime(LambdaConfig{}, InvokeRequest{Runtime: "python3.12"})
	assert.Equal(t, "public.ecr.aws/lambda/python:3.12", got)

	got = p.ImageForRuntime(LambdaConfig{}, InvokeRequest{Runtime: "python3.12", Image: "custom:1"})
	assert.Equal(t, "custom:1", got)

	got = p.ImageForRuntime(LambdaConfig{DefaultImage: "default:1"}, InvokeRequest{Runtime: "python3.12"})
	assert.Equal(t, "default:1", got)

	got = p.ImageForRuntime(LambdaConfig{}, InvokeRequest{Runtime: "cobol1.x"})
	assert.Equal(t, "public.ecr.aws/lambda/provided:al2", got)

	env := p.RuntimeEnv(LambdaConfig{Region: "eu-west-1"}, InvokeRequest{FunctionName: "fn"})
	require.NotEmpty(t, env)
}
