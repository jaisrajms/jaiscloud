package provider

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"jaiscloud/internal/model"
)

// Registry dispatches NormalizedRequests to the registered HandlerFunc.
// Key format: "ProviderPrefix.ActionName" e.g. "Queue.SendMessage".
//
// Plugins register a wildcard handler via RegisterPlugin("EMR", fn).
// When Dispatch looks up "EMR.RunJobFlow" and finds no exact match,
// it falls back to the wildcard handler registered for prefix "EMR".
type Registry struct {
	handlers map[string]HandlerFunc
	plugins  map[string]HandlerFunc // prefix → wildcard handler (plugin fallback)
	switched map[string]bool        // keys whose innermost handler can be redirected (see WithCore)
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
		plugins:  make(map[string]HandlerFunc),
		switched: make(map[string]bool),
	}
}

// RegisterAll bulk-registers a route map. Returns the registry for chaining.
func (r *Registry) RegisterAll(routes map[string]HandlerFunc) *Registry {
	for k, v := range routes {
		r.handlers[k] = v
	}
	return r
}

// Register registers all routes returned by p.Routes(). Returns the registry
// for chaining so callers can write:
//
//	registry := provider.NewRegistry().
//	    Register(keyProv).
//	    Register(funcP).
//	    Register(queueP)
//
// Use RegisterAll directly only when registering a bare route map that is not
// the primary Routes() return of a provider (e.g., tableProvider.StreamRoutes()).
func (r *Registry) Register(p Provider) *Registry {
	return r.RegisterAll(p.Routes())
}

// RegisterPlugin registers a plugin as the wildcard handler for a provider prefix.
// When Dispatch finds no exact key match, it tries the plugin handler for the prefix.
// A plugin handler takes precedence over the built-in wildcard fallback.
func (r *Registry) RegisterPlugin(prefix string, h HandlerFunc) {
	// Override any built-in exact-match handlers whose prefix matches,
	// so the plugin receives all actions for this service.
	for key := range r.handlers {
		p := key
		for i, c := range key {
			if c == '.' {
				p = key[:i]
				break
			}
		}
		if p == prefix {
			r.handlers[key] = h
		}
	}
	r.plugins[prefix] = h
}

// Has reports whether an exact handler is registered for key.
func (r *Registry) Has(key string) bool {
	_, ok := r.handlers[key]
	return ok
}

// Keys returns the sorted exact-match keys whose provider prefix is prefix.
func (r *Registry) Keys(prefix string) []string {
	var out []string
	for key := range r.handlers {
		if keyPrefix(key) == prefix {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// Intercept wraps the handler registered for key. mw receives the current
// handler (the original built-in, or the result of an earlier Intercept) and
// returns its replacement, so the original stays reachable as `next`. When key
// has no handler, next answers UnknownAction as Dispatch does today.
//
// Like RegisterAll, it is for startup only: the maps are not locked.
func (r *Registry) Intercept(key string, mw func(next HandlerFunc) HandlerFunc) {
	next, ok := r.handlers[key]
	if !ok {
		next = unknownAction
	}
	if !r.switched[key] {
		// Innermost link: lets an internal caller substitute its own core (WithCore).
		next, r.switched[key] = coreSwitch(key, next), true
	}
	r.handlers[key] = mw(next)
}

type coreCtxKey struct{ key string }

// WithCore returns a context that makes the innermost handler of key (below
// every Intercept link) run core instead of the registered built-in. It lets a
// provider-internal delivery, such as SNS publishing into SQS, go through the
// same interceptor chain a client request takes while keeping its own
// built-in behaviour. Keys that were never intercepted are unaffected.
func WithCore(ctx context.Context, key string, core HandlerFunc) context.Context {
	return context.WithValue(ctx, coreCtxKey{key}, core)
}

func coreSwitch(key string, next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		if core, ok := ctx.Value(coreCtxKey{key}).(HandlerFunc); ok {
			return core(ctx, nr)
		}
		return next(ctx, nr)
	}
}

// InterceptFallback wraps the wildcard handler for prefix, which Dispatch
// consults when no exact key matches (actions the built-in does not have).
func (r *Registry) InterceptFallback(prefix string, mw func(next HandlerFunc) HandlerFunc) {
	next, ok := r.plugins[prefix]
	if !ok {
		next = unknownAction
	}
	r.plugins[prefix] = mw(next)
}

func unknownAction(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, model.NewProviderError("UnknownAction", fmt.Sprintf("no handler for %q", nr.Service+"."+nr.Action), 400)
}

func keyPrefix(key string) string {
	if i := strings.IndexByte(key, '.'); i >= 0 {
		return key[:i]
	}
	return key
}

// Dispatch routes a request to the matching handler.
// Lookup order:
//  1. Exact match in handlers (built-in providers).
//  2. Plugin wildcard for the provider prefix (full-mode plugins).
//  3. ProviderError(UnknownAction).
func (r *Registry) Dispatch(ctx context.Context, key string, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if h, ok := r.handlers[key]; ok {
		resp, err := h(ctx, nr)
		if err != nil {
			if _, isProvider := err.(*model.ProviderError); !isProvider {
				slog.Error("provider handler error",
					"key", key,
					"err", err,
				)
			}
		}
		return resp, err
	}

	// Extract prefix from "Prefix.Action"
	prefix := key
	for i, c := range key {
		if c == '.' {
			prefix = key[:i]
			break
		}
	}
	if h, ok := r.plugins[prefix]; ok {
		resp, err := h(ctx, nr)
		if err != nil {
			if _, isProvider := err.(*model.ProviderError); !isProvider {
				slog.Error("plugin handler error",
					"key", key,
					"prefix", prefix,
					"err", err,
				)
			}
		}
		return resp, err
	}

	slog.Error("no handler registered",
		"key", key,
		"service", nr.Service,
		"action", nr.Action,
	)
	return nil, model.NewProviderError("UnknownAction", fmt.Sprintf("no handler for %q", key), 400)
}
