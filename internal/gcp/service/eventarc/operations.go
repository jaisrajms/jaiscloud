package eventarc

import (
	"context"
	"errors"

	"jaiscloud/internal/gcp/paging"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	"jaiscloud/internal/model"
)

// The shared google.longrunning.Operations surface over the persisted
// operations of Eventarc's control-plane mutations.

// GetOperation returns one persisted operation.
func (s *Service) GetOperation(ctx context.Context, project, location, id string) (Operation, error) {
	if location == "" || id == "" {
		return Operation{}, invalidArgument("missing location or operation id")
	}
	op, err := s.store.GetOperation(ctx, project, location, id)
	if err != nil {
		if errors.Is(err, eventarcstore.ErrNoSuchOperation) {
			return Operation{}, operationNotFound(id)
		}
		return Operation{}, err
	}
	return op, nil
}

// ListOperations returns a cursor page of a location's persisted operations.
func (s *Service) ListOperations(ctx context.Context, project, location string, pageSize int, pageToken string) ([]Operation, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	ops, err := s.store.ListOperations(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next, err := paging.Page(ops, func(op Operation) string { return op.ID }, pageParams(pageSize, pageToken))
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// CancelOperation returns a persisted operation. Eventarc mutations settle
// synchronously, so every stored operation is already done and cancel is a
// no-op that reports the current operation; an unknown name is NOT_FOUND, as
// real GCP answers.
func (s *Service) CancelOperation(ctx context.Context, project, location, id string) (Operation, error) {
	return s.GetOperation(ctx, project, location, id)
}

// DeleteOperation removes a persisted operation.
func (s *Service) DeleteOperation(ctx context.Context, project, location, id string) error {
	if location == "" || id == "" {
		return invalidArgument("missing location or operation id")
	}
	if err := s.store.DeleteOperation(ctx, project, location, id); err != nil {
		if errors.Is(err, eventarcstore.ErrNoSuchOperation) {
			return operationNotFound(id)
		}
		return err
	}
	return nil
}

// operationNotFound is the canonical NOT_FOUND both transports map onto their
// wire status.
func operationNotFound(id string) error {
	return model.NewProviderError("NotFound", "operation "+id+" not found", 404)
}
