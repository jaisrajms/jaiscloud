package container

import "context"

// MockExecutor echoes the request payload as the response.
// Used in memory mode and as the default when no executor mode is configured.
type MockExecutor struct{}

func (e *MockExecutor) Invoke(_ context.Context, req Request) (Result, error) {
	return Result{Payload: req.Payload}, nil
}

func (e *MockExecutor) DeleteFunction(_ context.Context, _ string) {}
func (e *MockExecutor) Reset(_ context.Context)                    {}
func (e *MockExecutor) Close() error                               { return nil }
