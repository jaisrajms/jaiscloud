package functions

import (
	"context"

	"jaiscloud/internal/gcp/paging"
	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// persistOperation records a mutation operation so operations.get / list and
// REST :wait can read it back after the mutation returns (and across a restart
// under --dsn). The version-independent Function response snapshot is stored;
// rendering happens on read. In the default synchronous mode the operation was
// built done with EndTime set; in async mode it is persisted done=false and
// settle derives completion on read.
func (s *Service) persistOperation(ctx context.Context, project string, op Operation) error {
	return s.functions.CreateOperation(ctx, project, op.Location, functionsstore.Operation{
		ID:         op.ID,
		Location:   op.Location,
		Done:       op.Done,
		Verb:       op.Verb,
		Target:     op.Target,
		Function:   op.Function,
		CreateTime: op.CreateTime,
		EndTime:    op.EndTime,
	})
}

// operationFromStore converts a persisted operation into the render-time
// Operation the shared OperationJSON expects.
func operationFromStore(stored functionsstore.Operation) Operation {
	return Operation{
		ID:         stored.ID,
		Location:   stored.Location,
		Verb:       stored.Verb,
		Target:     stored.Target,
		Function:   stored.Function,
		Done:       stored.Done,
		CreateTime: stored.CreateTime,
		EndTime:    stored.EndTime,
	}
}

// settle derives the rendered state of a persisted operation from its stored
// done flag and the configured timing mode. An in-flight operation (done=false)
// becomes done once the delay has elapsed, with a deterministic EndTime of
// createTime+delay (a zero delay yields createTime).
//
// The flip is derived on read rather than written back: the functions store has
// no UpdateOperation, and the persisted flag is an input while the settled state
// is a pure function of it and the clock. This mirrors the workflows pilot.
func (s *Service) settle(op Operation) Operation {
	if op.Done || s.lroMode.Pending(op.CreateTime) {
		return op
	}
	op.Done = true
	op.EndTime = op.CreateTime.Add(s.lroMode.Delay)
	return op
}

// GetOperationJSON renders the persisted operation for the requested name. An
// unknown operation is NotFound (real GCP never synthesizes a terminal op for an
// id it did not hand out).
func (s *Service) GetOperationJSON(ctx context.Context, project, name string, v Version) (map[string]any, error) {
	op, err := s.loadOperation(ctx, project, name)
	if err != nil {
		return nil, err
	}
	return OperationJSON(v, project, s.settle(op)), nil
}

// WaitOperation is the read side of the REST :wait custom method. It returns
// the same persisted operation GetOperation would: done inline in the default
// synchronous mode, or settled lazily once the async delay has elapsed.
func (s *Service) WaitOperation(ctx context.Context, project, name string, v Version) (map[string]any, error) {
	return s.GetOperationJSON(ctx, project, name, v)
}

// loadOperation parses an operation name and reads the persisted record. A
// top-level v1 name (operations/{id}) carries no location, so the location is
// recovered from the stored record.
func (s *Service) loadOperation(ctx context.Context, project, name string) (Operation, error) {
	location, id, err := ParseOperationName(name)
	if err != nil {
		return Operation{}, err
	}
	var stored functionsstore.Operation
	if location == "" {
		stored, err = s.functions.GetOperationByID(ctx, project, id)
	} else {
		stored, err = s.functions.GetOperation(ctx, project, location, id)
	}
	if err != nil {
		return Operation{}, mapErr(err)
	}
	return operationFromStore(stored), nil
}

// LoadOperation parses and loads a persisted operation for the gRPC
// google.longrunning.Operations resolver (which needs the typed core Operation
// to build the Any metadata/response).
func (s *Service) LoadOperation(ctx context.Context, project, name string) (Operation, error) {
	op, err := s.loadOperation(ctx, project, name)
	if err != nil {
		return Operation{}, err
	}
	return s.settle(op), nil
}

// ListOperations returns a cursor page of the persisted operations for a
// location, honoring an optional AIP-160 `filter` (the standard
// google.longrunning.Operations.List `filter`). v is the API version the caller
// renders operations as; it selects the resource-name form the `name` filter
// matches. The filter is evaluated against the settled operation so `done`
// reflects what a client would read, and it is applied before pagination so a
// page is always a page of matching results. An unparseable filter is
// InvalidArgument.
func (s *Service) ListOperations(ctx context.Context, project, location string, v Version, filter string, pageSize int, pageToken string) ([]Operation, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	ops, err := s.functions.ListOperations(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	pred, err := compileOperationFilter(filter, v, project)
	if err != nil {
		return nil, "", err
	}
	// Settle first, then filter: in async mode an operation's stored Done is
	// false until the delay elapses, but a caller filtering done=true must see
	// the operation it would actually read back.
	matched := make([]Operation, 0, len(ops))
	for _, stored := range ops {
		op := s.settle(operationFromStore(stored))
		if pred.match(op) {
			matched = append(matched, op)
		}
	}
	page, next := paging.Page(matched, func(op Operation) string { return op.ID }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// CancelOperation validates that the operation exists. The emulator does not
// model cancellation, so a known operation is a no-op success and an unknown one
// is NotFound.
func (s *Service) CancelOperation(ctx context.Context, project, name string) error {
	_, err := s.loadOperation(ctx, project, name)
	return err
}

// DeleteOperation removes a persisted operation. An unknown operation is
// NotFound (matching real google.longrunning.Operations.DeleteOperation).
func (s *Service) DeleteOperation(ctx context.Context, project, name string) error {
	location, id, err := ParseOperationName(name)
	if err != nil {
		return err
	}
	if location == "" {
		// v1 top-level name: the operation is not project/location scoped.
		return mapErr(s.functions.DeleteOperationByID(ctx, id))
	}
	if err := s.functions.DeleteOperation(ctx, project, location, id); err != nil {
		return mapErr(err)
	}
	return nil
}
