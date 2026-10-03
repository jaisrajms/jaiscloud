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
package operations

import (
	"context"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/codes"
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
	resolvers []Resolver
	// strict selects real GCP unknown-name semantics (NotFound) instead of the
	// lenient terminal stub / no-op. The opt-in async mode sets it; the default
	// synchronous contract leaves it false.
	strict bool
}

// New returns an Operations service. Resolvers are consulted in order; pass
// none for the terminal stub used by synchronous services.
func New(resolvers ...Resolver) *Service { return &Service{resolvers: resolvers} }

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
	for _, r := range s.resolvers {
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
	for _, r := range s.resolvers {
		reg, ok := r.(ListRegistry)
		if !ok {
			continue
		}
		resp, handled, err := reg.ListOperations(ctx, req.GetName(), req.GetPageSize(), req.GetPageToken(), req.GetFilter())
		if !handled {
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
	for _, r := range s.resolvers {
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
	for _, r := range s.resolvers {
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
