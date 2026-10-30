// Package dispatcher provides a ServiceDispatcher implementation backed by the
// provider.Registry, allowing the Step Functions execution engine to call
// other JaisCloud services (Lambda, DynamoDB, SQS, etc.) directly.
package dispatcher

import (
	"context"
	"fmt"

	"jaiscloud/internal/aws/arn"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// RegistryDispatcher implements provider.ServiceDispatcher using the JaisCloud
// provider registry. It bypasses HTTP and calls handlers directly.
type RegistryDispatcher struct {
	registry   *provider.Registry
	cloud      model.Cloud
	region     string
	accountID  string
	resourceID func(string, string) string
	clock      clock.Clock
	port       int
}

// New creates a RegistryDispatcher wired to the given registry and config.
func New(reg *provider.Registry, cfg *config.Config) *RegistryDispatcher {
	return &RegistryDispatcher{
		registry:   reg,
		cloud:      model.CloudAWS,
		region:     cfg.Region,
		accountID:  cfg.AccountID,
		resourceID: arn.ResourceID(cfg.Region, cfg.AccountID),
		clock:      cfg.Clock,
		port:       cfg.Port,
	}
}

// Dispatch invokes providerPrefix.action in the registry and returns its response.
func (d *RegistryDispatcher) Dispatch(ctx context.Context, providerPrefix, action string, params map[string]any) (map[string]any, error) {
	if params == nil {
		params = make(map[string]any)
	}
	nr := &model.NormalizedRequest{
		Cloud:      d.cloud,
		Region:     d.region,
		AccountID:  d.accountID,
		Service:    providerPrefix,
		Action:     action,
		Params:     params,
		Origin:     model.OriginService,
		Clock:      d.clock,
		Port:       d.port,
		ResourceID: d.resourceID,
	}
	key := fmt.Sprintf("%s.%s", providerPrefix, action)
	resp, err := d.registry.Dispatch(ctx, key, nr)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.Data, nil
}
