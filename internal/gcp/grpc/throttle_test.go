package grpc

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/gcp/throttle"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func retryInfoOf(t *testing.T, err error) *errdetails.RetryInfo {
	t.Helper()
	st := status.Convert(err)
	for _, d := range st.Details() {
		if r, ok := d.(*errdetails.RetryInfo); ok {
			return r
		}
	}
	return nil
}

func TestThrottleUnaryInterceptorInjects(t *testing.T) {
	inj := throttle.New(throttle.Config{
		Fault: true, FailFirst: 1,
		Status: 429, RetryDelay: 2 * time.Second,
	})
	interceptor := throttleUnaryInterceptor(inj)
	info := &grpc.UnaryServerInfo{FullMethod: "/google.storage.v2.Storage/GetObject"}

	called := false
	handler := func(context.Context, any) (any, error) { called = true; return "ok", nil }

	_, err := interceptor(context.Background(), nil, info, handler)
	if called {
		t.Fatal("handler must not run for an injected failure")
	}
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("code: got %v want ResourceExhausted", got)
	}
	ri := retryInfoOf(t, err)
	if ri == nil {
		t.Fatalf("missing RetryInfo detail")
	}
	if got := ri.GetRetryDelay().AsDuration(); got != 2*time.Second {
		t.Fatalf("retry delay: got %v want 2s", got)
	}

	// The fault fails only the first match, so the next call reaches the handler.
	if _, err := interceptor(context.Background(), nil, info, handler); err != nil {
		t.Fatalf("second call must pass: %v", err)
	}
	if !called {
		t.Fatal("handler must run once the fault is spent")
	}
}

func TestThrottleUnaryInterceptorUnavailable(t *testing.T) {
	inj := throttle.New(throttle.Config{Fault: true, FailFirst: 1, Status: 503, RetryDelay: time.Second})
	interceptor := throttleUnaryInterceptor(inj)
	info := &grpc.UnaryServerInfo{FullMethod: "/google.pubsub.v1.Publisher/Publish"}
	_, err := interceptor(context.Background(), nil, info, func(context.Context, any) (any, error) { return nil, nil })
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("code: got %v want Unavailable", got)
	}
}

func TestThrottleUnaryInterceptorSkipsHealth(t *testing.T) {
	inj := throttle.New(throttle.Config{Fault: true, FailCount: 100})
	interceptor := throttleUnaryInterceptor(inj)
	info := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	if _, err := interceptor(context.Background(), nil, info, func(context.Context, any) (any, error) { return "ok", nil }); err != nil {
		t.Fatalf("health must not be throttled: %v", err)
	}
}

func TestThrottleStreamInterceptorInjects(t *testing.T) {
	inj := throttle.New(throttle.Config{Fault: true, FailFirst: 1, Status: 429})
	interceptor := throttleStreamInterceptor(inj)
	info := &grpc.StreamServerInfo{FullMethod: "/google.pubsub.v1.Subscriber/StreamingPull"}

	err := interceptor(nil, nil, info, func(any, grpc.ServerStream) error { return nil })
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("code: got %v want ResourceExhausted", got)
	}
}

func TestThrottleServerOptions(t *testing.T) {
	// The interceptors are installed even for a disabled injector so a runtime
	// arm (POST /_jaiscloud/throttle) takes effect without a restart.
	if opts := ThrottleServerOptions(throttle.New(throttle.Config{})); len(opts) != 2 {
		t.Fatalf("a present injector must install unary+stream options, got %d", len(opts))
	}
	if opts := ThrottleServerOptions(nil); opts != nil {
		t.Fatalf("a nil injector must install no interceptors, got %d options", len(opts))
	}
}

// TestThrottleRuntimeArm proves the installed interceptor honours an injector
// armed after startup: it allows while disabled, then injects once armed.
func TestThrottleRuntimeArm(t *testing.T) {
	inj := throttle.New(throttle.Config{})
	interceptor := throttleUnaryInterceptor(inj)
	info := &grpc.UnaryServerInfo{FullMethod: "/google.storage.v2.Storage/GetObject"}
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	if _, err := interceptor(context.Background(), nil, info, handler); err != nil {
		t.Fatalf("disabled injector must allow: %v", err)
	}

	if _, err := inj.SetThrottleConfig([]byte(`{"mode":"fault","failFirst":1,"status":429}`)); err != nil {
		t.Fatalf("arm: %v", err)
	}
	if _, err := interceptor(context.Background(), nil, info, handler); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("armed injector must inject: %v", err)
	}
}
