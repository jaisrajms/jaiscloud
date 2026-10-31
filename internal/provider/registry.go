package provider

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"

	"jaiscloud/internal/model"
)

// Registry dispatches NormalizedRequests to the registered HandlerFunc.
// Key format: "ProviderPrefix.ActionName" e.g. "Queue.SendMessage".
//
// The registry is the ONLY way into a service: wire requests, calls from other
// services (SNS delivering to SQS, pollers, schedulers), and the console UI all
// call Dispatch with nr.Origin set. That is what lets a plugin replace a key
// and be used by every caller, as if the built-in did not exist.
//
// Plugins wrap handlers with Intercept (one key) and InterceptFallback (the
// actions a service does not have). When Dispatch finds no exact match for
// "EMR.RunJobFlow" it falls back to the handler for prefix "EMR", if any.
type Registry struct {
	// mu guards both maps: background workers dispatch while startup code (plugin
	// loading) is still wrapping handlers. Handlers run outside the lock.
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
	plugins  map[string]HandlerFunc // prefix → fallback handler (actions the built-in lacks)
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
		plugins:  make(map[string]HandlerFunc),
	}
}

// RegisterAll bulk-registers a route map. Returns the registry for chaining.
func (r *Registry) RegisterAll(routes map[string]HandlerFunc) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
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

// Fingerprint identifies the registry's contents: every key with the code
// address of its handler, plus the fallback handlers. Two registries built the
// same way have the same fingerprint; wrapping any handler changes it. Tests use
// it to prove that an operation (loading no plugins) left the registry as it was.
func (r *Registry) Fingerprint() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var lines []string
	for k, h := range r.handlers {
		lines = append(lines, fmt.Sprintf("%s=%x", k, reflect.ValueOf(h).Pointer()))
	}
	for k, h := range r.plugins {
		lines = append(lines, fmt.Sprintf("%s.*=%x", k, reflect.ValueOf(h).Pointer()))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// Has reports whether an exact handler is registered for key.
func (r *Registry) Has(key string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.handlers[key]
	return ok
}

// Keys returns the sorted exact-match keys whose provider prefix is prefix.
func (r *Registry) Keys(prefix string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
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
// It is for startup, but safe against concurrent Dispatch.
func (r *Registry) Intercept(key string, mw func(next HandlerFunc) HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next, ok := r.handlers[key]
	if !ok {
		next = unknownAction
	}
	r.handlers[key] = mw(next)
}

// InterceptFallback wraps the wildcard handler for prefix, which Dispatch
// consults when no exact key matches (actions the built-in does not have).
func (r *Registry) InterceptFallback(prefix string, mw func(next HandlerFunc) HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
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
//  2. The fallback handler for the provider prefix (plugin-added actions).
//  3. ProviderError(UnknownAction).
func (r *Registry) Dispatch(ctx context.Context, key string, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	prefix := keyPrefix(key)
	r.mu.RLock()
	exact, isExact := r.handlers[key]
	wild, isWild := r.plugins[prefix]
	r.mu.RUnlock()

	if isExact {
		resp, err := exact(ctx, nr)
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
	if isWild {
		resp, err := wild(ctx, nr)
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
