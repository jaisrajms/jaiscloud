package handlers

import (
	"context"

	"jaiscloud/internal/model"
)

// Invoker calls a service through the provider registry ("Queue.CreateQueue").
// Handlers never hold a provider: whatever is registered under the key (the
// built-in or a plugin) answers, so a plugin that overrides a service also
// serves the resources CloudFormation creates in it.
type Invoker func(ctx context.Context, key string, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

// child builds a derived NormalizedRequest with a new params map, inheriting
// all other fields from the parent request.
func child(nr *model.NormalizedRequest, params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Region:     nr.Region,
		AccountID:  nr.AccountID,
		Port:       nr.Port,
		Cloud:      nr.Cloud,
		Clock:      nr.Clock,
		ResourceID: nr.ResourceID,
		Params:     params,
	}
}

// propStr returns the string value of props[key], or fallback if absent/empty.
func propStr(props map[string]any, key, fallback string) string {
	if v, ok := props[key].(string); ok && v != "" {
		return v
	}
	return fallback
}

// copyProps returns a shallow copy of props.
func copyProps(props map[string]any) map[string]any {
	out := make(map[string]any, len(props))
	for k, v := range props {
		out[k] = v
	}
	return out
}
