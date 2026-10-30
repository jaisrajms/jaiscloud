// Package pluginhost loads out-of-process plugins (see /sdk) and installs them
// into the existing provider.Registry as interceptor links.
//
// It is cloud-neutral shared infrastructure: it never imports internal/aws or
// internal/gcp. With no plugin configured, Load returns (nil, nil) and nothing
// else in this package runs.
package pluginhost

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// Deps are the already-built pieces plugins attach to. Load must run after the
// registry, adapter and stores exist and before the gateway starts serving.
type Deps struct {
	Cloud       model.Cloud
	Version     string // emulator version, passed to plugins at init
	Registry    *provider.Registry
	Adapter     adapter.CloudAdapter
	Resources   store.ResourceStore
	CallbackURL string        // base URL a plugin uses to reach this emulator
	Timeout     time.Duration // per HTTP call to a plugin
}

// Host holds the loaded plugins and serves their store callbacks.
type Host struct {
	plugins  []*plugin
	resource store.ResourceStore
	inflight inflight
}

type plugin struct {
	name, version, baseURL, token string
	routes                        []sdk.Route // only entries for this binary's cloud
	client                        *http.Client
	timeout                       time.Duration
	host                          *Host
}

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Load connects to every plugin spec, validates them, and installs their
// routes. Any failure aborts startup (the caller returns the error).
// Specs are "http://host:port" or "https://host:port"; precedence follows order.
func Load(ctx context.Context, specs []string, d Deps) (*Host, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	if d.Timeout <= 0 {
		d.Timeout = 10 * time.Second
	}
	h := &Host{resource: d.Resources, inflight: inflight{n: map[string]int{}}}
	seen := map[string]bool{}
	for _, spec := range specs {
		p, err := h.connect(ctx, spec, d)
		if err != nil {
			return nil, fmt.Errorf("plugin %q: %w", spec, err)
		}
		if seen[p.name] {
			return nil, fmt.Errorf("plugin %q: duplicate plugin name %q", spec, p.name)
		}
		seen[p.name] = true
		h.plugins = append(h.plugins, p)
	}
	if err := h.apply(d); err != nil {
		return nil, err
	}
	for _, p := range h.plugins {
		slog.Info("plugin loaded", "name", p.name, "version", p.version, "transport", "http",
			"url", p.baseURL, "abi", sdk.ABI, "routes", len(p.routes))
	}
	return h, nil
}

func (h *Host) connect(ctx context.Context, spec string, d Deps) (*plugin, error) {
	switch {
	case strings.HasPrefix(spec, "http://"), strings.HasPrefix(spec, "https://"):
	case strings.HasPrefix(spec, "so:"):
		return nil, fmt.Errorf("in-process plugins are not supported by this build")
	default:
		return nil, fmt.Errorf("unrecognised plugin spec (want http://… or https://…)")
	}
	p := &plugin{baseURL: strings.TrimRight(spec, "/"), client: &http.Client{}, timeout: d.Timeout, host: h}

	var m sdk.Manifest
	if err := p.do(ctx, http.MethodGet, "/manifest", nil, &m); err != nil {
		return nil, err
	}
	if m.ABI != sdk.ABI {
		return nil, fmt.Errorf("plugin ABI %d, emulator supports %d", m.ABI, sdk.ABI)
	}
	if !nameRe.MatchString(m.Name) {
		return nil, fmt.Errorf("invalid plugin name %q (want [a-z0-9-]+)", m.Name)
	}
	p.name, p.version = m.Name, m.Version
	for _, r := range m.Routes {
		if r.Cloud != string(d.Cloud) {
			continue // a cross-cloud plugin lists other clouds' entries; ignore them
		}
		if r.Mode != sdk.Replace && r.Mode != sdk.Before && r.Mode != sdk.After {
			return nil, fmt.Errorf("route %s/%s: unknown mode %q", r.Cloud, r.Service, r.Mode)
		}
		if len(r.Actions) == 0 {
			return nil, fmt.Errorf("route %s/%s: no actions", r.Cloud, r.Service)
		}
		prefix := d.Adapter.ServiceToProvider(r.Service)
		if len(d.Registry.Keys(prefix)) == 0 {
			return nil, fmt.Errorf("route %s/%s: no such service (adding new services is not supported yet)", r.Cloud, r.Service)
		}
		p.routes = append(p.routes, r)
	}
	if len(p.routes) == 0 {
		slog.Warn("plugin declares no routes for this cloud", "name", p.name, "cloud", d.Cloud)
	}

	tok := make([]byte, 16)
	if _, err := rand.Read(tok); err != nil {
		return nil, err
	}
	p.token = hex.EncodeToString(tok)
	init := sdk.InitRequest{ABI: sdk.ABI,
		Host:  sdk.InitHost{Cloud: string(d.Cloud), Version: d.Version},
		Store: sdk.StoreEndpoint{URL: strings.TrimRight(d.CallbackURL, "/") + storePath, Token: p.token}}
	if err := p.do(ctx, http.MethodPost, "/init", init, nil); err != nil {
		return nil, err
	}
	return p, nil
}

type modeSet struct{ replace, before, after bool }

func (m *modeSet) add(mode sdk.Mode) {
	switch mode {
	case sdk.Replace:
		m.replace = true
	case sdk.Before:
		m.before = true
	case sdk.After:
		m.after = true
	}
}

// claims is one plugin's expansion of its routes: modes per exact key, plus
// modes for actions the built-in does not have (the prefix fallback).
type claims struct {
	keys     map[string]*modeSet
	fallback map[string]*modeSet // provider prefix -> modes
}

func (h *Host) apply(d Deps) error {
	// Expand routes. Explicit replace claims first, so a wildcard replace can
	// skip actions another plugin explicitly owns.
	all := make([]*claims, len(h.plugins))
	replaceOwner := map[string]string{} // exact key -> plugin
	for i, p := range h.plugins {
		c := &claims{keys: map[string]*modeSet{}, fallback: map[string]*modeSet{}}
		all[i] = c
		for _, r := range p.routes {
			prefix := d.Adapter.ServiceToProvider(r.Service)
			for _, a := range r.Actions {
				if a == "*" {
					continue
				}
				key := prefix + "." + a
				ms := c.keys[key]
				if ms == nil {
					ms = &modeSet{}
					c.keys[key] = ms
				}
				ms.add(r.Mode)
				if r.Mode == sdk.Replace {
					if other, ok := replaceOwner[key]; ok && other != p.name {
						return fmt.Errorf("plugins %q and %q both replace %s", other, p.name, key)
					}
					replaceOwner[key] = p.name
				}
			}
		}
	}
	wildcardOwner := map[string]string{} // prefix -> plugin
	for i, p := range h.plugins {
		c := all[i]
		for _, r := range p.routes {
			prefix := d.Adapter.ServiceToProvider(r.Service)
			hasWild := false
			for _, a := range r.Actions {
				if a == "*" {
					hasWild = true
				}
			}
			if !hasWild {
				continue
			}
			if r.Mode == sdk.Replace {
				if other, ok := wildcardOwner[prefix]; ok && other != p.name {
					return fmt.Errorf("plugins %q and %q both replace every action of %s", other, p.name, r.Service)
				}
				wildcardOwner[prefix] = p.name
			}
			fb := c.fallback[prefix]
			if fb == nil {
				fb = &modeSet{}
				c.fallback[prefix] = fb
			}
			fb.add(r.Mode)
			for _, key := range d.Registry.Keys(prefix) {
				if r.Mode == sdk.Replace {
					if other, ok := replaceOwner[key]; ok && other != p.name {
						continue // specific replace by another plugin wins
					}
				}
				ms := c.keys[key]
				if ms == nil {
					ms = &modeSet{}
					c.keys[key] = ms
				}
				ms.add(r.Mode)
			}
		}
	}

	// Pass 1: replace links (innermost, directly around the built-in).
	for i, p := range h.plugins {
		for _, key := range sortedKeys(all[i].keys) {
			if all[i].keys[key].replace {
				d.Registry.Intercept(key, p.replaceLink())
			}
		}
		for _, prefix := range sortedKeys(all[i].fallback) {
			if all[i].fallback[prefix].replace {
				d.Registry.InterceptFallback(prefix, p.replaceLink())
			}
		}
	}
	// Pass 2: before/after links, plugin 1 outermost (so applied last).
	for i := len(h.plugins) - 1; i >= 0; i-- {
		p := h.plugins[i]
		for _, key := range sortedKeys(all[i].keys) {
			if ms := all[i].keys[key]; ms.before || ms.after {
				d.Registry.Intercept(key, p.aroundLink(ms.before, ms.after))
			}
		}
		for _, prefix := range sortedKeys(all[i].fallback) {
			if ms := all[i].fallback[prefix]; ms.before || ms.after {
				d.Registry.InterceptFallback(prefix, p.aroundLink(ms.before, ms.after))
			}
		}
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// do performs one JSON call to the plugin. Transport failures are mapped to
// ProviderErrors so they reach the client through the built-in codec; they
// never fall back to the built-in handler.
func (p *plugin) do(ctx context.Context, method, path string, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return model.NewProviderError("PluginUnsupportedPayload", "request is not JSON-serialisable: "+err.Error(), 500)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return model.NewProviderError("PluginTimeout", fmt.Sprintf("plugin %s timed out", p.nameOr()), 504)
		}
		return model.NewProviderError("PluginUnavailable", fmt.Sprintf("plugin %s unavailable: %v", p.nameOr(), err), 503)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(raw) > maxBody {
		return model.NewProviderError("PluginProtocolError", fmt.Sprintf("plugin %s: unreadable or oversize response", p.nameOr()), 502)
	}
	if resp.StatusCode/100 != 2 {
		return model.NewProviderError("PluginProtocolError",
			fmt.Sprintf("plugin %s: HTTP %d: %s", p.nameOr(), resp.StatusCode, truncate(raw)), 502)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return model.NewProviderError("PluginProtocolError", fmt.Sprintf("plugin %s: bad JSON: %v", p.nameOr(), err), 502)
		}
	}
	return nil
}

const maxBody = 32 << 20

func (p *plugin) nameOr() string {
	if p.name != "" {
		return p.name
	}
	return p.baseURL
}

func truncate(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

// inflight tracks which request ids currently have a plugin call open, so the
// store callbacks can reject writes that are not part of a live request.
type inflight struct {
	mu sync.Mutex
	n  map[string]int
}

func (f *inflight) enter(id string) { f.mu.Lock(); f.n[id]++; f.mu.Unlock() }
func (f *inflight) leave(id string) {
	f.mu.Lock()
	if f.n[id]--; f.n[id] <= 0 {
		delete(f.n, id)
	}
	f.mu.Unlock()
}
func (f *inflight) has(id string) bool { f.mu.Lock(); defer f.mu.Unlock(); return f.n[id] > 0 }
