package metastore

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/gcp/reference"
)

// networkFromConfig returns Service.network from a request body, or "" when the
// caller did not attach a VPC network.
func networkFromConfig(cfg json.RawMessage) string {
	if len(cfg) == 0 {
		return ""
	}
	var m map[string]any
	if json.Unmarshal(cfg, &m) != nil {
		return ""
	}
	network, _ := m["network"].(string)
	return network
}

// validateNetwork resolves Service.network against the compute network store
// (FC4-4). The documented form names the project by number
// ("projects/{project_number}/global/networks/{network}"); a malformed,
// cross-project or missing network is INVALID_ARGUMENT, the answer real Dataproc
// Metastore gives for an unusable network. The network is immutable after
// create, so an update only validates it when the mask applies it.
func (s *Service) validateNetwork(ctx context.Context, project string, cfg json.RawMessage) error {
	network := networkFromConfig(cfg)
	if network == "" {
		return nil
	}
	return reference.ValidateComputeNetwork(ctx, s.resources, project, network)
}
