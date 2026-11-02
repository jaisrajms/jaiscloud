package tasks

import (
	"context"
	"regexp"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

// kmsKeyPattern is the Cloud KMS key resource name Cloud Tasks accepts for a
// location's CMEK config:
// projects/{project}/locations/{location}/keyRings/{ring}/cryptoKeys/{key}.
var kmsKeyPattern = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)

// GetCmekConfig returns the location's CMEK config. An unconfigured location
// returns the documented default — a config with an empty kmsKey, not an error.
func (s *Service) GetCmekConfig(ctx context.Context, project, location string) (tasksstore.CmekConfig, error) {
	if project == "" || location == "" {
		return tasksstore.CmekConfig{}, invalidArgument("project and location are required")
	}
	cfg, err := s.store.GetCmekConfig(ctx, project, location)
	if err != nil {
		return tasksstore.CmekConfig{}, mapStoreErr(err)
	}
	cfg.ProjectID = project
	cfg.Location = location
	return cfg, nil
}

// UpdateCmekConfig creates or updates the location's CMEK config. An empty
// kmsKey clears it (the Discovery document: "Setting this as blank will turn
// off CMEK encryption"); a non-empty key must be a Cloud KMS key resource name.
//
// The config is metadata only: the emulator does not encrypt task payloads at
// rest, so the key is recorded and echoed but never used
// (docs/gcp/wire-contract.md §5).
func (s *Service) UpdateCmekConfig(ctx context.Context, project, location, kmsKey string) (tasksstore.CmekConfig, error) {
	if project == "" || location == "" {
		return tasksstore.CmekConfig{}, invalidArgument("project and location are required")
	}
	cfg := tasksstore.CmekConfig{ProjectID: project, Location: location, KmsKey: kmsKey}
	if kmsKey == "" {
		if err := s.store.DeleteCmekConfig(ctx, project, location); err != nil {
			return tasksstore.CmekConfig{}, mapStoreErr(err)
		}
		return cfg, nil
	}
	if !kmsKeyPattern.MatchString(kmsKey) {
		return tasksstore.CmekConfig{}, invalidArgument(
			"cmekConfig.kmsKey must be a Cloud KMS key resource name (projects/*/locations/*/keyRings/*/cryptoKeys/*)")
	}
	if err := s.store.SetCmekConfig(ctx, project, location, cfg); err != nil {
		return tasksstore.CmekConfig{}, mapStoreErr(err)
	}
	return cfg, nil
}
