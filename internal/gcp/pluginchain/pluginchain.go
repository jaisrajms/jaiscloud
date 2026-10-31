// Package pluginchain runs a call through the plugin chain of a registry key for
// callers that do not reach the registry themselves: the gRPC transport (its
// adapters call the service cores directly) and the console services that work on
// typed store structs.
//
// The plugin links are installed on a shadow registry whose handlers do nothing
// but call the "built-in" the caller supplies, so the same plugin that overrides
// PubSub.TopicCreate or Scheduler.JobsGet for REST clients is in the path of
// these callers too. A key no plugin touches is not run at all (Covers), so the
// caller keeps its normal path and zero-plugin behaviour is unchanged.
package pluginchain

import (
	"context"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Host is the part of the plugin host the chain needs.
type Host interface {
	Covers(key string) bool
	CoveredKeys() []string
	Attach(reg *provider.Registry)
}

// Chain holds the shadow registry.
type Chain struct {
	host   Host
	shadow *provider.Registry
}

// New builds the chain for host. A nil host (no plugins) covers nothing.
func New(host Host) *Chain {
	c := &Chain{host: host, shadow: provider.NewRegistry()}
	if host == nil {
		return c
	}
	routes := map[string]provider.HandlerFunc{}
	for _, k := range host.CoveredKeys() {
		routes[k] = delegate
	}
	c.shadow.RegisterAll(routes)
	host.Attach(c.shadow)
	return c
}

// Covers reports whether any plugin replaces, or runs before/after, key.
func (c *Chain) Covers(key string) bool { return c != nil && c.host != nil && c.host.Covers(key) }

type nextKey struct{}

func delegate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return ctx.Value(nextKey{}).(provider.HandlerFunc)(ctx, nr)
}

// Run dispatches nr through the plugin chain of key with next as the innermost
// handler (what the call does when no plugin answers instead).
func (c *Chain) Run(ctx context.Context, key string, nr *model.NormalizedRequest, next provider.HandlerFunc) (*model.ProviderResponse, error) {
	return c.shadow.Dispatch(context.WithValue(ctx, nextKey{}, next), key, nr)
}
