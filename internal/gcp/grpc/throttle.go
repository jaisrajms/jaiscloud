package grpc

import (
	"context"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/throttle"
	"jaiscloud/internal/model"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ThrottleServerOptions returns the gRPC interceptors that apply the opt-in
// throttle/quota injector to every registered service. The interceptors are
// installed whenever an injector is present — even one that starts disabled —
// so POST /_jaiscloud/throttle can arm it at runtime without a restart; a
// disabled injector makes each interceptor a single cheap no-op call.
func ThrottleServerOptions(inj *throttle.Injector) []grpc.ServerOption {
	if inj == nil {
		return nil
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(throttleUnaryInterceptor(inj)),
		grpc.ChainStreamInterceptor(throttleStreamInterceptor(inj)),
	}
}

// throttleUnaryInterceptor refuses a unary call before its handler runs when the
// injector matches the method.
func throttleUnaryInterceptor(inj *throttle.Injector) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if pe := inj.CheckMethod(info.FullMethod); pe != nil {
			return nil, throttleError(pe)
		}
		return handler(ctx, req)
	}
}

// throttleStreamInterceptor is the stream counterpart: it injects at call start
// (mid-stream injection is a documented out-of-scope deferral, THR3).
func throttleStreamInterceptor(inj *throttle.Injector) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if pe := inj.CheckMethod(info.FullMethod); pe != nil {
			return throttleError(pe)
		}
		return handler(srv, ss)
	}
}

// throttleError maps an injected ProviderError to a gRPC status with the
// matching code and a google.rpc.RetryInfo detail, consistent with the REST
// error path via gcperr.
func throttleError(pe *model.ProviderError) error {
	name, _ := gcperr.Resolve(pe)
	code, ok := gcperr.GRPCCodeForStatus(name)
	if !ok {
		code = codes.ResourceExhausted
	}
	st := status.New(code, pe.Message)
	if pe.RetryAfter > 0 {
		if withDetails, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(pe.RetryAfter)}); err == nil {
			st = withDetails
		}
	}
	return st.Err()
}
