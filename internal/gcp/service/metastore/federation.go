package metastore

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// --- Federations (google.cloud.metastore.v1.DataprocMetastoreFederation) ---

// federationNestedBlocks maps the proto snake_case nested config block roots to
// their camelCase JSON wire keys for a federation updateMask (backend_metastores
// is the only nested block; version/labels are top-level and pass through).
var federationNestedBlocks = map[string]string{
	"backend_metastores": "backendMetastores",
}

// CreateFederation creates a federation and returns it with the done create
// operation.
func (s *Service) CreateFederation(ctx context.Context, project, location, federationID string, cfg json.RawMessage) (metastorestore.Federation, metastorestore.Operation, error) {
	if location == "" || federationID == "" {
		return metastorestore.Federation{}, metastorestore.Operation{}, invalidArgument("missing location or federationId")
	}
	now := clock.Now().UTC()
	f := metastorestore.Federation{
		Location:   location,
		Name:       federationID,
		Labels:     labelsFromConfig(cfg),
		State:      "ACTIVE",
		CreateTime: now,
		UpdateTime: now,
	}
	if len(cfg) > 0 {
		f.Config = cfg
	}
	if err := s.store.CreateFederation(ctx, project, location, f); err != nil {
		return metastorestore.Federation{}, metastorestore.Operation{}, mapErr(err)
	}
	target := FederationName(project, location, federationID)
	op, err := s.storeOperation(ctx, project, location, "create", target, operationResponse(federationTypeURL, FederationJSON(f, project)))
	if err != nil {
		return metastorestore.Federation{}, metastorestore.Operation{}, err
	}
	return f, op, nil
}

// GetFederation returns one federation.
func (s *Service) GetFederation(ctx context.Context, project, location, federationID string) (metastorestore.Federation, error) {
	if location == "" || federationID == "" {
		return metastorestore.Federation{}, invalidArgument("missing location or federationId")
	}
	f, err := s.store.GetFederation(ctx, project, location, federationID)
	if err != nil {
		return metastorestore.Federation{}, mapErr(err)
	}
	return f, nil
}

// ListFederations returns a cursor page of the federations in a location.
func (s *Service) ListFederations(ctx context.Context, project, location string, pageSize int, pageToken string) ([]metastorestore.Federation, string, error) {
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	federations, err := s.store.ListFederations(ctx, project, location)
	if err != nil {
		return nil, "", err
	}
	page, next, err := paging.Page(federations, func(f metastorestore.Federation) string { return f.Name }, pageParams(pageSize, pageToken))
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// UpdateFederation merges the caller's fields into the stored federation and
// returns it with the done update operation. The merge honors updateMask
// (comma-separated; empty means apply every field in the body).
func (s *Service) UpdateFederation(ctx context.Context, project, location, federationID string, cfg json.RawMessage, mask string) (metastorestore.Federation, metastorestore.Operation, error) {
	if location == "" || federationID == "" {
		return metastorestore.Federation{}, metastorestore.Operation{}, invalidArgument("missing location or federationId")
	}
	updated, err := s.store.UpdateFederationAtomic(ctx, project, location, federationID, func(current metastorestore.Federation) (metastorestore.Federation, error) {
		apply := func(field string) bool {
			return mask == "" || containsMaskField(mask, field)
		}
		if apply("labels") {
			if labels := labelsFromConfig(cfg); labels != nil {
				current.Labels = labels
			}
		}
		// Echo the remaining body fields into the stored config verbatim
		// (top-level keys overwrite), honoring the updateMask's named paths.
		if len(cfg) > 0 {
			stored := map[string]any{}
			if len(current.Config) > 0 {
				_ = json.Unmarshal(current.Config, &stored)
			}
			incoming := map[string]any{}
			_ = json.Unmarshal(cfg, &incoming)
			if mask == "" {
				for k, v := range incoming {
					stored[k] = v
				}
			} else {
				for _, field := range splitMask(mask) {
					root, ok := maskJSONRootIn(field, federationNestedBlocks)
					if !ok {
						return metastorestore.Federation{}, invalidArgument("unsupported updateMask field " + field)
					}
					if v, present := incoming[root]; present {
						stored[root] = v
					}
				}
			}
			if data, err := json.Marshal(stored); err == nil {
				current.Config = data
			}
		}
		current.UpdateTime = clock.Now().UTC()
		return current, nil
	})
	if err != nil {
		return metastorestore.Federation{}, metastorestore.Operation{}, mapErr(err)
	}
	target := FederationName(project, location, federationID)
	op, err := s.storeOperation(ctx, project, location, "update", target, operationResponse(federationTypeURL, FederationJSON(updated, project)))
	if err != nil {
		return metastorestore.Federation{}, metastorestore.Operation{}, err
	}
	return updated, op, nil
}

// DeleteFederation deletes a federation and returns the done delete operation.
func (s *Service) DeleteFederation(ctx context.Context, project, location, federationID string) (metastorestore.Operation, error) {
	if location == "" || federationID == "" {
		return metastorestore.Operation{}, invalidArgument("missing location or federationId")
	}
	if err := s.store.DeleteFederation(ctx, project, location, federationID); err != nil {
		return metastorestore.Operation{}, mapErr(err)
	}
	target := FederationName(project, location, federationID)
	op, err := s.storeOperation(ctx, project, location, "delete", target, map[string]any{})
	if err != nil {
		return metastorestore.Operation{}, err
	}
	return op, nil
}
