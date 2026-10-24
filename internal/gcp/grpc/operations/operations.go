// Package operations provides the google.longrunning.Operations service. Most
// emulator operations complete synchronously (e.g. index creation returns a
// done=true operation directly), so by default an operation is reported as done
// with an empty response, letting SDK init paths that poll Operations terminate
// cleanly instead of erroring.
//
// Services that model genuinely asynchronous operations register a Resolver,
// which is consulted first: Dataproc cluster create/update/start/stop/delete are
// returned done=false and are completed lazily when the SDK polls
// Operations.GetOperation. A service that also persists operations implements
// the richer Registry (Cancel/Delete) and/or ListRegistry (List) surfaces, so
// the shared service can act on them.
//
// Unknown-name semantics are mode-dependent. The default (synchronous) contract
// is lenient — an operation name no resolver owns is reported terminal and a
// delete/cancel is a no-op success — because synchronous SDK init paths poll
// arbitrary names and must terminate. In the opt-in async mode (see
// internal/gcp/lro) SetStrict switches the service to real GCP semantics: a name
// no resolver or registry owns is NotFound.
//
// Endpoint scoping: real GCP serves google.longrunning.Operations on each
// service's own host, so ListOperations(parent=...) returns only that service's
// operations. The emulator serves one Operations on a single gRPC listener, so
// SetEndpointResolvers registers a service's resolvers under its endpoint host
// token (the request's HTTP/2 :authority first label) and a request addressed to
// that endpoint is served only by it — the gRPC analogue of the REST Host
// discriminator. Requests to an unregistered endpoint use the ordered fallback
// chain, and endpoint-registered resolvers are excluded from its List page so
// no service can answer another's list.
package operations

import (
	"context"
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Resolver resolves a fully-qualified operation name to its
// google.longrunning.Operation. handled reports whether the name belongs to the
// resolver's service; when false the caller falls through to the next resolver
// (or, for Get, the terminal stub). An error (e.g. NotFound) is returned to the
// client for names the resolver owns.
type Resolver interface {
	ResolveOperation(ctx context.Context, name string) (op *longrunningpb.Operation, handled bool, err error)
}

// Registry is an optional richer Resolver for services that persist operations
// and can serve Cancel/Delete. Each method returns handled=false for a name the
// service does not own, exactly like ResolveOperation, so services that share a
// namespace (Service Usage and Cloud Functions v1 both publish top-level
// operations/{id}) keep their ownership handshake.
//
// Listing is the separate ListRegistry surface: a service can persist
// operations and serve them over List without owning Cancel/Delete semantics
// (Cloud Functions v1 deliberately declines Cancel/Delete so its lenient
// resolver-owned-absent contract is not turned into a NotFound).
type Registry interface {
	Resolver
	// CancelOperation cancels (or validates) an operation. handled=false means
	// the name belongs to another service.
	CancelOperation(ctx context.Context, name string) (handled bool, err error)
	// DeleteOperation removes an operation. handled=false means the name
	// belongs to another service.
	DeleteOperation(ctx context.Context, name string) (handled bool, err error)
}

// ListRegistry is the optional List surface for services that persist
// operations. List's parent is the requested collection; an unknown or foreign
// parent is declined (handled=false) so another service's registry — or the
// empty page — answers. filter is the standard google.longrunning.Operations.List
// request parameter, forwarded verbatim: a registry that cannot evaluate a
// non-empty filter must fail loud rather than silently return unfiltered
// results. (returnPartialSuccess is handled once in Service.ListOperations, not
// per registry — see there.)
type ListRegistry interface {
	Resolver
	// ListOperations returns the operations under parent. handled=false means
	// the parent belongs to another service.
	ListOperations(ctx context.Context, parent string, pageSize int32, pageToken, filter string) (*longrunningpb.ListOperationsResponse, bool, error)
}

// Service implements longrunningpb.OperationsServer.
type Service struct {
	longrunningpb.UnimplementedOperationsServer
	// resolvers is the ordered fallback chain, consulted when the request's
	// endpoint does not name a service: name-based Get/Wait, the registry chain
	// for Cancel/Delete, and the ListRegistry chain for List.
	resolvers []Resolver
	// endpoints maps an endpoint host token (the first DNS label of the request's
	// HTTP/2 :authority) to the resolver(s) that own every
	// google.longrunning.Operations call addressed to that endpoint. Real GCP
	// serves Operations per service host, so a request addressed to
	// {service}.googleapis.com is served only by that service; the emulator's
	// single listener reproduces that with the authority token. A tokened request
	// never consults the fallback chain.
	endpoints map[string][]Resolver
	// endpointOnly marks the resolvers registered via SetEndpointResolvers. They
	// are excluded from the untokened List fallback so one service can never
	// answer another service's list page; they remain in the fallback chain for
	// name-based Get (operation names are unique).
	endpointOnly map[Resolver]bool
	// strict selects real GCP unknown-name semantics (NotFound) instead of the
	// lenient terminal stub / no-op. The opt-in async mode sets it; the default
	// synchronous contract leaves it false.
	strict bool
}

// New returns an Operations service. Resolvers are consulted in order when the
// request is not addressed to a registered endpoint; pass none for the terminal
// stub used by synchronous services.
func New(resolvers ...Resolver) *Service {
	return &Service{
		resolvers:    resolvers,
		endpoints:    map[string][]Resolver{},
		endpointOnly: map[Resolver]bool{},
	}
}

// SetEndpointResolvers registers rs as the owners of google.longrunning.Operations
// for the given endpoint host token — the first DNS label of the client's HTTP/2
// :authority (e.g. "managedkafka" for managedkafka.googleapis.com, or
// "managedkafka.localhost"). A request addressed to that endpoint is served
// only by rs, so its operations are isolated exactly as real GCP's
// per-endpoint Operations; a service can be registered under more than one token
// and a token can carry more than one resolver (Cloud Functions v1 and v2 share
// cloudfunctions.googleapis.com). The resolvers are excluded from the untokened
// List fallback but stay in the name-based Get chain.
func (s *Service) SetEndpointResolvers(token string, rs ...Resolver) {
	token = strings.ToLower(strings.TrimSpace(token))
	if token == "" {
		return
	}
	if s.endpoints == nil {
		s.endpoints = map[string][]Resolver{}
	}
	if s.endpointOnly == nil {
		s.endpointOnly = map[Resolver]bool{}
	}
	s.endpoints[token] = append(s.endpoints[token], rs...)
	for _, r := range rs {
		s.endpointOnly[r] = true
	}
}

// resolversFor returns the resolver chain for the request and whether it is
// endpoint-scoped. A request whose :authority names a registered service token
// is served by that endpoint's resolvers alone; every other request uses the
// ordered fallback chain.
func (s *Service) resolversFor(ctx context.Context) ([]Resolver, bool) {
	if tok := endpointToken(ctx); tok != "" {
		if rs, ok := s.endpoints[tok]; ok && len(rs) > 0 {
			return rs, true
		}
	}
	return s.resolvers, false
}

// endpointToken extracts the service host token from the request's HTTP/2
// :authority (Host) header: the first DNS label, lowercased, with the port and
// IPv6 brackets removed. It returns "" when there is no authority. grpc-go
// normalizes a plain Host header into :authority, and rejects a request with
// neither, so this covers both.
func endpointToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(":authority")
	if len(vals) == 0 {
		return ""
	}
	host := strings.TrimSpace(vals[0])
	if host == "" {
		return ""
	}
	if strings.HasPrefix(host, "[") {
		// IPv6 literal: [::1]:port — the label is the address, never a token.
		if end := strings.IndexByte(host, ']'); end >= 0 {
			host = host[:end+1]
		}
	} else if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if i := strings.IndexByte(host, '.'); i >= 0 {
		host = host[:i]
	}
	return strings.ToLower(host)
}

// SetStrict switches the unknown-name contract. false (the default) keeps the
// lenient terminal stub / no-op success the synchronous SDK init paths rely on;
// true (the opt-in async mode) reports a name no resolver or registry owns as
// NotFound, matching real google.longrunning.Operations.
func (s *Service) SetStrict(strict bool) { s.strict = strict }

// GetOperation first asks each registered resolver; if none owns the name it
// reports the operation as done with no result body, so SDK init paths that poll
// Operations for a terminal state terminate cleanly. In strict (async) mode an
// unowned name is NotFound instead.
func (s *Service) GetOperation(ctx context.Context, req *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	resolvers, _ := s.resolversFor(ctx)
	for _, r := range resolvers {
		op, handled, err := r.ResolveOperation(ctx, req.GetName())
		if !handled {
			continue
		}
		if err != nil {
			return nil, err
		}
		return op, nil
	}
	if s.strict {
		return nil, notFound(req.GetName())
	}
	return terminalOperation(req.GetName()), nil
}

// WaitOperation resolves the operation the same way GetOperation does, without
// blocking: the emulator's async transitions advance on read, so a single
// resolution already reflects the latest state. The request's timeout is
// ignored.
func (s *Service) WaitOperation(ctx context.Context, req *longrunningpb.WaitOperationRequest) (*longrunningpb.Operation, error) {
	return s.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: req.GetName()})
}

// terminalOperation is the shared shape the lenient Operations RPCs report: the
// requested name marked done, with no result or error body.
func terminalOperation(name string) *longrunningpb.Operation {
	return &longrunningpb.Operation{
		Name: name,
		Done: true,
	}
}

// notFound is the strict-mode error for a name no resolver or registry owns,
// shaped like real GCP's google.longrunning.Operations.
func notFound(name string) error {
	return status.Errorf(codes.NotFound, "Operation %s not found.", name)
}

// ListOperations asks each ListRegistry to list under the requested parent. The
// first registry that owns the parent wins; when none does, the list is empty
// (a well-formed parent with no operations). The lenient and strict modes agree
// here: an empty page, never an error. The standard `filter` parameter is
// forwarded to the owning registry, which either evaluates it or fails loud (a
// registry that cannot evaluate a filter must not silently return unfiltered
// results).
//
// A request addressed to a registered service endpoint (:authority) is
// endpoint-scoped: that service's page — empty included — is authoritative, so
// its operations are isolated exactly as real GCP's per-endpoint Operations.
// Endpoint-scoped resolvers are skipped in the untokened fallback chain so one
// service can never answer another service's list page.
//
// `returnPartialSuccess` is not supported: the canonical Operations.List proto
// and the per-service Discovery documents state the field "will result in an
// UNIMPLEMENTED error if set unless explicitly documented otherwise", and no
// emulator service documents support (it is only meaningful when reading across
// collections, e.g. a `locations/-` parent, which the emulator does not model).
// It is rejected here rather than per registry so every parent behaves the same.
func (s *Service) ListOperations(ctx context.Context, req *longrunningpb.ListOperationsRequest) (*longrunningpb.ListOperationsResponse, error) {
	if req.GetReturnPartialSuccess() {
		return nil, status.Error(codes.Unimplemented, "ListOperations returnPartialSuccess is not supported")
	}
	resolvers, endpointScoped := s.resolversFor(ctx)
	for _, r := range resolvers {
		if !endpointScoped && s.endpointOnly[r] {
			continue
		}
		reg, ok := r.(ListRegistry)
		if !ok {
			continue
		}
		resp, handled, err := reg.ListOperations(ctx, req.GetName(), req.GetPageSize(), req.GetPageToken(), req.GetFilter())
		// An endpoint-scoped request is owned by its service, so the service's
		// page (empty included) is authoritative and handled is ignored. The
		// untokened fallback keeps the handled handshake so a declining registry
		// yields to the next.
		if !endpointScoped && !handled {
			continue
		}
		if err != nil {
			return nil, err
		}
		if resp == nil {
			resp = &longrunningpb.ListOperationsResponse{}
		}
		return resp, nil
	}
	return &longrunningpb.ListOperationsResponse{}, nil
}

// DeleteOperation asks each Registry to delete the named operation. When no
// registry owns the name, strict (async) mode falls back to a Get-based
// existence check so an operation served by a Get-only resolver still deletes
// and an unknown name is NotFound. The lenient default stays an unconditional
// no-op success, exactly as before the registry seam existed.
func (s *Service) DeleteOperation(ctx context.Context, req *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	resolvers, _ := s.resolversFor(ctx)
	for _, r := range resolvers {
		reg, ok := r.(Registry)
		if !ok {
			continue
		}
		handled, err := reg.DeleteOperation(ctx, req.GetName())
		if !handled {
			continue
		}
		if err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}
	if s.strict {
		if _, err := s.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: req.GetName()}); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

// CancelOperation asks each Registry to cancel the named operation. Like
// DeleteOperation it falls back to a Get-based existence check only in strict
// (async) mode; the lenient default is an unconditional no-op success.
func (s *Service) CancelOperation(ctx context.Context, req *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	resolvers, _ := s.resolversFor(ctx)
	for _, r := range resolvers {
		reg, ok := r.(Registry)
		if !ok {
			continue
		}
		handled, err := reg.CancelOperation(ctx, req.GetName())
		if !handled {
			continue
		}
		if err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}
	if s.strict {
		if _, err := s.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: req.GetName()}); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}
