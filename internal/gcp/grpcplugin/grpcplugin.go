// Package grpcplugin lets plugins override GCP gRPC calls.
//
// The gRPC adapters call their service cores directly and never pass through
// provider.Registry, so a plugin that overrides a registry key would not see
// them. This package closes that gap with a server interceptor:
//
//   - A gRPC method is mapped to the registry key of its REST twin by running the
//     method's google.api.http rule through the REST adapter, so one plugin key
//     (for example PubSub.TopicCreate) covers both transports.
//   - A call whose key no plugin touches goes straight to the normal handler,
//     unchanged.
//   - A covered call is dispatched through a shadow registry carrying the same
//     plugin links, with the real gRPC handler as the innermost "built-in". The
//     plugin sees the protojson form of the RPC messages (Request.Transport is
//     "grpc"); the answer is converted back into the RPC's response message.
//
// Streaming RPCs get one call at stream open (see stream).
package grpcplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/gcperr"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Host is the part of the plugin host this package needs.
type Host interface {
	Covers(key string) bool
	CoveredKeys() []string
	Attach(reg *provider.Registry)
}

// Config wires the interceptor to the running emulator.
type Config struct {
	Host    Host
	Adapter adapter.CloudAdapter // the same GCP adapter the REST gateway uses
	Clock   clock.Clock
	Port    int
	Region  string // config default, as the gateway passes to EnrichRequest
	Project string // config default project
}

// Interceptor routes covered gRPC calls through the plugin chain.
type Interceptor struct {
	cfg    Config
	rs     *resolver
	shadow *provider.Registry
}

// New builds the interceptor and its shadow registry. Call it after the plugin
// host has loaded; host may be nil, in which case ServerOptions is empty.
func New(cfg Config) *Interceptor {
	i := &Interceptor{cfg: cfg, shadow: provider.NewRegistry()}
	i.rs = &resolver{keyOf: i.keyOf}
	routes := map[string]provider.HandlerFunc{}
	for _, k := range cfg.Host.CoveredKeys() {
		routes[k] = delegate
	}
	i.shadow.RegisterAll(routes)
	cfg.Host.Attach(i.shadow)
	return i
}

// ServerOptions returns the interceptors to install on the gRPC server.
func (i *Interceptor) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(i.unary),
		grpc.ChainStreamInterceptor(i.stream),
	}
}

// Route returns the registry key a gRPC method maps to, for tests and docs.
func (i *Interceptor) Route(fullMethod string) (key string, ok bool) {
	if rt := i.rs.route(fullMethod); rt != nil {
		if rt.dynamic {
			return "(by resource)", true
		}
		return rt.key, true
	}
	return "", false
}

// keyOf runs a verb and path through the GCP adapter's detector and returns
// the registry key it dispatches to.
func (i *Interceptor) keyOf(host, verb, path string) (string, bool) {
	nr, _, err := i.decode(context.Background(), host, verb, path, nil)
	if err != nil || nr.Service == "" {
		return "", false
	}
	return i.cfg.Adapter.ServiceToProvider(nr.Service) + "." + nr.Action, true
}

func (i *Interceptor) decode(ctx context.Context, host, verb, path string, body []byte) (*model.NormalizedRequest, *http.Request, error) {
	if len(body) == 0 {
		body = []byte("{}")
	}
	r, err := http.NewRequestWithContext(ctx, verb, "http://localhost"+path, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	if host != "" {
		r.Host = host // some services are told apart by the Host, as a real client sends it
	}
	r.Header.Set("Content-Type", "application/json")
	nr, _, err := i.cfg.Adapter.DetectAndDecode(r, body)
	if err != nil {
		return nil, nil, err
	}
	return nr, r, nil
}

// nextFn is the real gRPC handler, carried in the context to the shadow
// registry's innermost handler.
type nextKey struct{}

func delegate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return ctx.Value(nextKey{}).(provider.HandlerFunc)(ctx, nr)
}

// request builds the NormalizedRequest a plugin chain sees for msg.
func (i *Interceptor) request(ctx context.Context, rt *route, msg proto.Message) (*model.NormalizedRequest, error) {
	body, err := protojson.Marshal(msg)
	if err != nil {
		return nil, err
	}
	var params map[string]any
	if err := json.Unmarshal(body, &params); err != nil {
		return nil, err
	}
	path, ok := render(rt.template, msg.ProtoReflect())
	var nr *model.NormalizedRequest
	var r *http.Request
	if ok {
		nr, r, err = i.decodeVersions(ctx, rt, path, body)
	}
	if !ok || err != nil {
		// The request names no valid resource; the plugin still sees the call, on
		// the default project, and the built-in answers the validation error.
		nr, r, err = i.decodeVersions(ctx, rt, mustRender(rt.template), nil)
		if err != nil {
			return nil, err
		}
		region, _, key := i.cfg.Adapter.EnrichRequest(r, i.cfg.Region, i.cfg.Project)
		i.inject(nr, region, i.cfg.Project, key)
	} else {
		region, account, key := i.cfg.Adapter.EnrichRequest(r, i.cfg.Region, i.cfg.Project)
		i.inject(nr, region, account, key)
	}
	nr.Params = params
	nr.Transport = "grpc"
	return nr, nil
}

// decodeVersions decodes path at the API versions the REST adapters use.
func (i *Interceptor) decodeVersions(ctx context.Context, rt *route, path string, body []byte) (*model.NormalizedRequest, *http.Request, error) {
	var lastErr error
	for _, p := range versions(path) {
		nr, r, err := i.decode(ctx, rt.host, rt.verb, p, body)
		if err == nil {
			return nr, r, nil
		}
		lastErr = err
	}
	return nil, nil, lastErr
}

func mustRender(tmpl string) string { p, _ := render(tmpl, nil); return p }

func (i *Interceptor) inject(nr *model.NormalizedRequest, region, account, accessKey string) {
	nr.Clock = i.cfg.Clock
	nr.Region, nr.AccountID, nr.AccessKey = region, account, accessKey
	nr.Port = i.cfg.Port
	nr.Cloud = i.cfg.Adapter.Cloud()
	nr.ResourceID = i.cfg.Adapter.ResourceIDFor(region, account)
}

// dynamicKey resolves the key of a generic IAM RPC from the resource it names.
func (i *Interceptor) dynamicKey(ctx context.Context, rt *route, msg proto.Message) string {
	path, ok := render(rt.template, msg.ProtoReflect())
	if !ok {
		return ""
	}
	for _, p := range versions(path) {
		if nr, _, err := i.decode(ctx, rt.host, rt.verb, p, nil); err == nil && nr.Service != "" {
			return i.cfg.Adapter.ServiceToProvider(nr.Service) + "." + nr.Action
		}
	}
	return ""
}

// ---- unary -----------------------------------------------------------------

func (i *Interceptor) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	rt := i.rs.route(info.FullMethod)
	msg, isMsg := req.(proto.Message)
	if rt == nil || !isMsg {
		return handler(ctx, req)
	}
	key := rt.key
	if rt.dynamic {
		if key = i.dynamicKey(ctx, rt, msg); key == "" {
			return handler(ctx, req)
		}
	}
	if !i.cfg.Host.Covers(key) {
		return handler(ctx, req)
	}
	nr, err := i.request(ctx, rt, msg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "plugin dispatch: %v", err)
	}
	orig := canonical(nr.Params)

	next := func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		call := msg
		if canonical(nr.Params) != orig { // a before hook patched the request
			patched := rt.in.New().Interface()
			if err := fromMap(nr.Params, patched); err != nil {
				return nil, model.NewProviderError("PluginProtocolError",
					"a plugin's patched request is not a valid "+string(rt.in.Descriptor().Name())+": "+err.Error(), 502)
			}
			call = patched
		}
		resp, err := handler(ctx, call)
		if err != nil {
			return nil, &handlerError{orig: err, perr: providerError(err)}
		}
		out, ok := resp.(proto.Message)
		if !ok {
			return &model.ProviderResponse{HTTPStatus: http.StatusOK, Data: map[string]any{}}, nil
		}
		data, err := toMap(out)
		if err != nil {
			return nil, err
		}
		return &model.ProviderResponse{HTTPStatus: http.StatusOK, Data: data}, nil
	}

	pr, err := i.shadow.Dispatch(context.WithValue(ctx, nextKey{}, provider.HandlerFunc(next)), key, nr)
	if err != nil {
		var he *handlerError
		if errors.As(err, &he) {
			return nil, he.orig // the built-in's own status, details intact
		}
		return nil, grpcutil.GRPCStatus(err)
	}
	out := rt.out.New().Interface()
	if err := fromMap(pr.Data, out); err != nil {
		return nil, status.Errorf(codes.Internal,
			"plugin answer for %s is not a valid %s: %v", key, rt.out.Descriptor().Name(), err)
	}
	return out, nil
}

// handlerError carries the built-in's gRPC error through the plugin chain: hooks
// see perr, and if nothing replaces the outcome the client gets orig back.
type handlerError struct {
	orig error
	perr *model.ProviderError
}

func (e *handlerError) Error() string { return e.orig.Error() }
func (e *handlerError) Unwrap() error { return e.perr }

// providerError shows a gRPC status to plugins in the form they know.
func providerError(err error) *model.ProviderError {
	st, _ := status.FromError(err)
	name := statusName(st.Code())
	pe := model.NewProviderError(name, st.Message(), gcperr.HTTPForStatus(name))
	pe.Status = name
	return pe
}

func statusName(c codes.Code) string {
	switch c {
	case codes.Canceled:
		return gcperr.Cancelled
	case codes.InvalidArgument:
		return gcperr.InvalidArgument
	case codes.DeadlineExceeded:
		return gcperr.DeadlineExceeded
	case codes.NotFound:
		return gcperr.NotFound
	case codes.AlreadyExists:
		return gcperr.AlreadyExists
	case codes.PermissionDenied:
		return gcperr.PermissionDenied
	case codes.ResourceExhausted:
		return gcperr.ResourceExhausted
	case codes.FailedPrecondition:
		return gcperr.FailedPrecondition
	case codes.Aborted:
		return gcperr.Aborted
	case codes.OutOfRange:
		return gcperr.OutOfRange
	case codes.Unimplemented:
		return gcperr.Unimplemented
	case codes.Internal:
		return gcperr.Internal
	case codes.Unavailable:
		return gcperr.Unavailable
	case codes.DataLoss:
		return gcperr.DataLoss
	case codes.Unauthenticated:
		return gcperr.Unauthenticated
	}
	return gcperr.Unknown
}

// ---- streaming ---------------------------------------------------------------

// stream gives a plugin one call when a covered streaming RPC opens. Before may
// reject the stream; After runs at once with an empty answer (the stream has
// not produced anything yet); Replace cannot produce a stream and is refused.
// For a server-streaming RPC the single request message is read first and shown
// to the plugin (and a before patch is applied to it); for client and
// bidirectional streams there is no request message yet, so Params is empty.
func (i *Interceptor) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	rt := i.rs.route(info.FullMethod)
	if rt == nil || !i.cfg.Host.Covers(rt.key) {
		return handler(srv, ss)
	}
	ctx := ss.Context()
	var first proto.Message
	if !info.IsClientStream {
		first = rt.in.New().Interface()
		if err := ss.RecvMsg(first); err != nil {
			return err
		}
	}
	var nr *model.NormalizedRequest
	var err error
	if first != nil {
		nr, err = i.request(ctx, rt, first)
	} else {
		nr, err = i.request(ctx, rt, rt.in.New().Interface())
	}
	if err != nil {
		return status.Errorf(codes.Internal, "plugin dispatch: %v", err)
	}
	orig := canonical(nr.Params)
	opened := &model.ProviderResponse{HTTPStatus: http.StatusOK, Data: map[string]any{}}
	next := func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) { return opened, nil }

	pr, err := i.shadow.Dispatch(context.WithValue(ctx, nextKey{}, provider.HandlerFunc(next)), rt.key, nr)
	if err != nil {
		return grpcutil.GRPCStatus(err)
	}
	if pr != opened {
		return status.Errorf(codes.Unimplemented,
			"a plugin replaced %s, but a streaming RPC cannot be replaced by a plugin (only checked when the stream opens)", rt.key)
	}
	if first != nil {
		if canonical(nr.Params) != orig {
			patched := rt.in.New().Interface()
			if err := fromMap(nr.Params, patched); err != nil {
				return status.Errorf(codes.Internal, "a plugin's patched request is not a valid %s: %v", rt.in.Descriptor().Name(), err)
			}
			first = patched
		}
		ss = &replay{ServerStream: ss, first: first}
	}
	return handler(srv, ss)
}

// replay hands the request message read at stream open to the handler.
type replay struct {
	grpc.ServerStream
	first proto.Message
	used  bool
}

func (r *replay) RecvMsg(m any) error {
	if !r.used {
		r.used = true
		dst, ok := m.(proto.Message)
		if !ok {
			return io.ErrUnexpectedEOF
		}
		proto.Reset(dst)
		proto.Merge(dst, r.first)
		return nil
	}
	return r.ServerStream.RecvMsg(m)
}

// ---- conversion ----------------------------------------------------------------

func toMap(m proto.Message) (map[string]any, error) {
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func fromMap(m map[string]any, dst proto.Message) error {
	if m == nil {
		m = map[string]any{}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return protojson.Unmarshal(b, dst)
}

// canonical is a stable text form of params, to tell whether a hook changed them.
func canonical(m map[string]any) string {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf("%v", m)
	}
	return string(b)
}
