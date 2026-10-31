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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
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
	Timeout     time.Duration // per HTTP attempt to a plugin (default 10s)
	StartWait   time.Duration // how long to keep retrying an unreachable plugin at startup (default 2m)
	CallRetry   time.Duration // how long to keep retrying an unreachable plugin during a request (default 30s)
	Secret      string        // optional shared secret sent to plugins as a bearer token
}

// Host holds the loaded plugins and serves their store callbacks.
type Host struct {
	cloud    string
	plugins  []*plugin
	resource store.ResourceStore
	registry *provider.Registry

	// The install plan, kept so the same plugins can be attached to another
	// registry (the gRPC transport's shadow registry, see Attach).
	keys     []map[string]*modeSet // per plugin: exact registry key -> modes
	fallback []map[string]*modeSet // per plugin: provider prefix -> modes for unknown actions
	covered  map[string]bool       // keys some plugin touches

	mu    sync.Mutex
	calls map[string]*activeCall // call id -> in-flight call
}

type plugin struct {
	name, version, baseURL, shownURL, token string
	abi                                     int
	secret                                  string
	routes                                  []sdk.Route // only entries for this binary's cloud
	client                                  *http.Client
	timeout, retry                          time.Duration
	initReq                                 sdk.InitRequest
	host                                    *Host
}

var nameRe = sync.OnceValue(func() *regexp.Regexp { return regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`) })

const (
	defaultTimeout   = 10 * time.Second
	defaultStartWait = 2 * time.Minute
	defaultRetry     = 30 * time.Second
	maxBody          = 32 << 20
)

// Codes the host raises itself.
const (
	codePluginPanic          = "PluginPanic"
	codePluginNotInitialized = "PluginNotInitialized"
)

// isNotInitialized reports whether err is the plugin saying it lost its /init
// (see sdk.ErrNotInitialized).
func isNotInitialized(err error) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.Code == codePluginNotInitialized
}

// Load connects to every plugin spec, validates them, and installs their
// routes. Any failure aborts startup (the caller returns the error).
// Specs are "http://host:port" or "https://host:port"; precedence follows order.
// An unreachable plugin is retried for d.StartWait before startup fails.
func Load(ctx context.Context, specs []string, d Deps) (*Host, error) {
	specs = cleanSpecs(specs)
	if len(specs) == 0 {
		return nil, nil
	}
	if d.Timeout <= 0 {
		d.Timeout = defaultTimeout
	}
	if d.StartWait <= 0 {
		d.StartWait = defaultStartWait
	}
	if d.CallRetry <= 0 {
		d.CallRetry = defaultRetry
	}
	h := &Host{cloud: string(d.Cloud), resource: d.Resources, registry: d.Registry,
		calls: map[string]*activeCall{}}
	seen := map[string]bool{}
	for _, spec := range specs {
		p, err := h.connect(ctx, spec, d)
		if err != nil {
			return nil, fmt.Errorf("plugin %q: %w", redact(spec), err)
		}
		if seen[p.name] {
			return nil, fmt.Errorf("plugin %q: duplicate plugin name %q", redact(spec), p.name)
		}
		seen[p.name] = true
		h.plugins = append(h.plugins, p)
	}
	if err := h.apply(d); err != nil {
		return nil, err
	}
	// Only now is the load certain to succeed, so a refused Load leaves no series behind.
	registerMetrics()
	for _, p := range h.plugins {
		slog.Info("plugin loaded", "name", p.name, "version", p.version, "transport", "http",
			"url", p.shownURL, "abi", p.abi, "routes", len(p.routes))
	}
	return h, nil
}

// cleanSpecs trims whitespace, splits comma-separated entries and drops empties,
// so both repeated flags and JAISCLOUD_PLUGINS="a, b" work.
func cleanSpecs(in []string) []string {
	var out []string
	for _, s := range in {
		for _, part := range strings.Split(s, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

// redact removes credentials and query strings from a plugin URL for logs/errors.
func redact(spec string) string {
	u, err := url.Parse(spec)
	if err != nil || u.Host == "" {
		return spec
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	return u.String()
}

func newHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 32
	return &http.Client{
		Transport: tr,
		// A plugin must answer directly; following a redirect would replay a POST elsewhere.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func (h *Host) connect(ctx context.Context, spec string, d Deps) (*plugin, error) {
	switch {
	case strings.HasPrefix(spec, "http://"), strings.HasPrefix(spec, "https://"):
	case strings.HasPrefix(spec, "so:"):
		return nil, fmt.Errorf("in-process plugins are not supported by this build")
	default:
		return nil, fmt.Errorf("unrecognised plugin spec (want http://… or https://…)")
	}
	p := &plugin{baseURL: strings.TrimRight(spec, "/"), shownURL: redact(strings.TrimRight(spec, "/")),
		client: newHTTPClient(), timeout: d.Timeout, retry: d.CallRetry, secret: d.Secret, host: h}

	var m sdk.Manifest
	if err := p.do(ctx, http.MethodGet, "/manifest", nil, &m, d.StartWait); err != nil {
		return nil, err
	}
	if m.ABI != sdk.ABI {
		return nil, fmt.Errorf("plugin ABI %d, emulator supports %d", m.ABI, sdk.ABI)
	}
	if !nameRe().MatchString(m.Name) {
		return nil, fmt.Errorf("invalid plugin name %q (want [a-z0-9-]+)", m.Name)
	}
	if len(m.Requires) > 0 {
		return nil, fmt.Errorf("plugin requires host capabilities %q, which this emulator does not provide (it provides: none)", m.Requires)
	}
	p.name, p.version, p.abi = m.Name, m.Version, m.ABI
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

	p.token = randomID(16)
	p.initReq = sdk.InitRequest{ABI: sdk.ABI,
		Host:  sdk.InitHost{Cloud: string(d.Cloud), Version: d.Version},
		Store: sdk.StoreEndpoint{URL: strings.TrimRight(d.CallbackURL, "/") + storePath, Token: p.token}}
	if err := p.do(ctx, http.MethodPost, "/init", p.initReq, nil, d.StartWait); err != nil {
		return nil, err
	}
	return p, nil
}

// reinit re-sends /init, used when a plugin restarted and lost its state.
func (p *plugin) reinit(ctx context.Context) error {
	return p.do(ctx, http.MethodPost, "/init", p.initReq, nil, p.retry)
}

func randomID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type modeSet struct {
	replace, before, after bool
	wire                   string // wire service name the plugin used in its route

	// allTransports is true when some route says nothing about transports (all of
	// them); otherwise transports is the union of what the routes list.
	allTransports bool
	transports    map[string]bool
}

// speaks reports whether the plugin understands the transport of nr.
func (m *modeSet) speaks(nr *model.NormalizedRequest) bool {
	if m.allTransports {
		return true
	}
	name := nr.Transport
	if name == "" {
		name = "rest" // the cloud's default wire shape
	}
	return m.transports[name]
}

func (m *modeSet) add(mode sdk.Mode, wire string, transports []string) {
	if len(transports) == 0 {
		m.allTransports = true
	}
	for _, t := range transports {
		if m.transports == nil {
			m.transports = map[string]bool{}
		}
		m.transports[t] = true
	}
	switch mode {
	case sdk.Replace:
		m.replace = true
	case sdk.Before:
		m.before = true
	case sdk.After:
		m.after = true
	}
	if m.wire == "" {
		m.wire = wire
	}
}

func get(m map[string]*modeSet, k string) *modeSet {
	ms := m[k]
	if ms == nil {
		ms = &modeSet{}
		m[k] = ms
	}
	return ms
}

func (h *Host) apply(d Deps) error {
	n := len(h.plugins)
	keys := make([]map[string]*modeSet, n)     // exact registry keys per plugin
	fallback := make([]map[string]*modeSet, n) // provider prefix -> modes for unknown actions
	replaceOwner := map[string]string{}        // exact key -> plugin
	newKeys := map[string]map[string]bool{}    // prefix -> keys named explicitly by any plugin

	// 1. Explicit action claims.
	for i, p := range h.plugins {
		keys[i], fallback[i] = map[string]*modeSet{}, map[string]*modeSet{}
		for _, r := range p.routes {
			prefix := d.Adapter.ServiceToProvider(r.Service)
			for _, a := range r.Actions {
				if a == "*" {
					continue
				}
				key := prefix + "." + a
				get(keys[i], key).add(r.Mode, r.Service, r.Transports)
				if newKeys[prefix] == nil {
					newKeys[prefix] = map[string]bool{}
				}
				newKeys[prefix][key] = true
				if r.Mode == sdk.Replace {
					if other, ok := replaceOwner[key]; ok && other != p.name {
						return fmt.Errorf("plugins %q and %q both replace %s", other, p.name, key)
					}
					replaceOwner[key] = p.name
				}
			}
		}
	}

	// 2. before/after only make sense on an action that exists or that some
	// plugin adds with replace; anything else is almost certainly a typo.
	for i, p := range h.plugins {
		for _, key := range sortedKeys(keys[i]) {
			ms := keys[i][key]
			if (ms.before || ms.after) && !d.Registry.Has(key) && replaceOwner[key] == "" {
				return fmt.Errorf("plugin %q: before/after on unknown action %s (typo?)", p.name, key)
			}
		}
	}

	// 3. Wildcards cover every existing action plus every action any plugin adds.
	wildcardOwner := map[string]string{} // prefix -> plugin owning replace "*"
	for i, p := range h.plugins {
		for _, r := range p.routes {
			if !contains(r.Actions, "*") {
				continue
			}
			prefix := d.Adapter.ServiceToProvider(r.Service)
			if r.Mode == sdk.Replace {
				if other, ok := wildcardOwner[prefix]; ok && other != p.name {
					return fmt.Errorf("plugins %q and %q both replace every action of %s", other, p.name, r.Service)
				}
				wildcardOwner[prefix] = p.name
			}
			get(fallback[i], prefix).add(r.Mode, r.Service, r.Transports)
			expanded := map[string]bool{}
			for _, k := range d.Registry.Keys(prefix) {
				expanded[k] = true
			}
			for k := range newKeys[prefix] {
				expanded[k] = true
			}
			for _, key := range sortedKeys(expanded) {
				if r.Mode == sdk.Replace {
					if other, ok := replaceOwner[key]; ok && other != p.name {
						continue // a specific replace by another plugin wins
					}
				}
				get(keys[i], key).add(r.Mode, r.Service, r.Transports)
			}
		}
	}

	h.keys, h.fallback, h.covered = keys, fallback, map[string]bool{}
	for _, m := range keys {
		for k := range m {
			h.covered[k] = true
		}
	}
	h.Attach(d.Registry)
	return nil
}

// Attach installs the plugins' links on reg, which must hold a handler for every
// key the plugins cover (normally a copy of the main registry's keys). Replace
// links go first (innermost, around the built-in), then before/after links with
// plugin 1 outermost (so it is applied last).
func (h *Host) Attach(reg *provider.Registry) {
	n := len(h.plugins)
	for i, p := range h.plugins {
		for _, key := range sortedKeys(h.keys[i]) {
			if ms := h.keys[i][key]; ms.replace {
				reg.Intercept(key, p.replaceLink(ms))
			}
		}
		for _, prefix := range sortedKeys(h.fallback[i]) {
			if ms := h.fallback[i][prefix]; ms.replace {
				reg.InterceptFallback(prefix, p.replaceLink(ms))
			}
		}
	}
	for i := n - 1; i >= 0; i-- {
		p := h.plugins[i]
		for _, key := range sortedKeys(h.keys[i]) {
			if ms := h.keys[i][key]; ms.before || ms.after {
				reg.Intercept(key, p.aroundLink(ms))
			}
		}
		for _, prefix := range sortedKeys(h.fallback[i]) {
			if ms := h.fallback[i][prefix]; ms.before || ms.after {
				reg.InterceptFallback(prefix, p.aroundLink(ms))
			}
		}
	}
}

// Covers reports whether any plugin replaces, or runs before/after, the exact
// registry key. A transport that bypasses the registry uses it to leave
// untouched calls on their normal path.
func (h *Host) Covers(key string) bool { return h != nil && h.covered[key] }

// CoveredKeys lists the exact registry keys some plugin touches, sorted.
func (h *Host) CoveredKeys() []string {
	if h == nil {
		return nil
	}
	return sortedKeys(h.covered)
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// connError marks a failure to even reach the plugin (nothing was delivered),
// which is the only kind of failure that is safe to retry for any request.
type connError struct{ err error }

func (e *connError) Error() string { return e.err.Error() }
func (e *connError) Unwrap() error { return e.err }

// do performs one JSON call to the plugin. If the plugin cannot be reached
// (connection refused / unresolved), it retries with backoff for up to `retry`
// before giving up. Failures are ProviderErrors so they reach the client
// through the built-in codec; they never fall back to the built-in handler.
func (p *plugin) do(ctx context.Context, method, path string, in, out any, retry time.Duration) error {
	var body []byte
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return model.NewProviderError("PluginUnsupportedPayload", "request is not JSON-serialisable: "+err.Error(), 500)
		}
		if len(b) > maxBody {
			return model.NewProviderError("PluginUnsupportedPayload", fmt.Sprintf(
				"plugin %s: the request is %d bytes once encoded, over the %d-byte limit for out-of-process plugins%s",
				p.nameOr(), len(b), maxBody, largestParam(in)), 501)
		}
		body = b
	}
	rctx := ctx
	if retry > 0 {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(ctx, retry)
		defer cancel()
	}
	backoff, logged := 200*time.Millisecond, false
	for {
		err := p.once(ctx, method, path, body, out)
		var ce *connError
		if !errors.As(err, &ce) {
			return toProviderError(err)
		}
		if retry <= 0 {
			return model.NewProviderError(model.CodePluginUnavailable, fmt.Sprintf("plugin %s unavailable: %v", p.nameOr(), ce.err), 503)
		}
		if !logged {
			slog.Warn("plugin unreachable, retrying", "plugin", p.nameOr(), "retry_for", retry.String(), "err", ce.err)
			logged = true
		}
		select {
		case <-rctx.Done():
			return model.NewProviderError(model.CodePluginUnavailable,
				fmt.Sprintf("plugin %s unavailable after retrying for %s: %v", p.nameOr(), retry, ce.err), 503)
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

func toProviderError(err error) error {
	if err == nil {
		return nil
	}
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		return pe
	}
	return model.NewProviderError(model.CodePluginUnavailable, err.Error(), 503)
}

func (p *plugin) once(ctx context.Context, method, path string, body []byte, out any) error {
	actx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(actx, method, p.baseURL+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	} else if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if p.secret != "" {
		req.Header.Set("Authorization", "Bearer "+p.secret)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		var oe *net.OpError
		switch {
		case errors.Is(actx.Err(), context.DeadlineExceeded):
			return model.NewProviderError(model.CodePluginTimeout, fmt.Sprintf("plugin %s timed out after %s", p.nameOr(), p.timeout), 504)
		case ctx.Err() != nil:
			return model.NewProviderError(model.CodePluginUnavailable, fmt.Sprintf("plugin %s call cancelled: %v", p.nameOr(), ctx.Err()), 503)
		case errors.As(err, &oe) && oe.Op == "dial":
			return &connError{err: err}
		}
		return model.NewProviderError(model.CodePluginUnavailable, fmt.Sprintf("plugin %s unavailable: %v", p.nameOr(), err), 503)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(raw) > maxBody {
		return model.NewProviderError("PluginProtocolError", fmt.Sprintf("plugin %s: unreadable or oversize response", p.nameOr()), 502)
	}
	if resp.StatusCode == http.StatusConflict {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &e) == nil && e.Error == sdk.ErrNotInitialized {
			return model.NewProviderError(codePluginNotInitialized, "plugin "+p.nameOr()+" has not been initialised", 503)
		}
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

// largestParam names the biggest request param, for the oversize error.
func largestParam(in any) string {
	hr, ok := in.(sdk.HandleRequest)
	if !ok || hr.Req == nil {
		return ""
	}
	best, size := "", 0
	for k, v := range hr.Req.Params {
		if b, err := json.Marshal(v); err == nil && len(b) > size {
			best, size = k, len(b)
		}
	}
	if best == "" {
		return ""
	}
	return fmt.Sprintf(" (largest param: %q, %d bytes)", best, size)
}

func (p *plugin) nameOr() string {
	if p.name != "" {
		return p.name
	}
	return p.shownURL
}

func truncate(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

// DoctorInfo describes the loaded plugins for GET /_jaiscloud/doctor.
func (h *Host) DoctorInfo() any {
	out := make([]map[string]any, 0, len(h.plugins))
	for _, p := range h.plugins {
		out = append(out, map[string]any{
			"name": p.name, "version": p.version, "transport": "http", "url": p.shownURL,
			"abi": p.abi, "routes": p.routes,
		})
	}
	return out
}

// Reset implements the admin Resetter: after the stores are wiped, tell
// plugins so they can drop caches.
func (h *Host) Reset(ctx context.Context) { h.notify(ctx, sdk.EventReset) }

// RestoreHook returns the admin PostRestoreHook for import and snapshot revert.
func (h *Host) RestoreHook() RestoreHook { return RestoreHook{h} }

// RestoreHook notifies plugins after state was restored from a snapshot.
type RestoreHook struct{ h *Host }

func (RestoreHook) Name() string { return "plugins" }
func (r RestoreHook) OnRestore(ctx context.Context) error {
	r.h.notify(ctx, sdk.EventRestore)
	return nil
}

func (h *Host) notify(ctx context.Context, event string) {
	for _, p := range h.plugins {
		p.notify(ctx, event)
	}
}

func (p *plugin) notify(ctx context.Context, event string) {
	if err := p.do(ctx, http.MethodPost, "/lifecycle", sdk.LifecycleRequest{Event: event}, nil, 5*time.Second); err != nil {
		slog.Warn("plugin lifecycle notification failed", "plugin", p.name, "event", event, "err", err)
	}
}

// Tick tells the plugins that have a route for the wire service that a test hook
// (eb-tick, cw-evaluate, ttl-sweep, firehose flush) was called, so they can run
// the pass the built-in would have run over state they own. See sdk.EventTickPrefix.
func (h *Host) Tick(ctx context.Context, service string) {
	for _, p := range h.plugins {
		for _, r := range p.routes {
			if r.Cloud == string(h.cloud) && r.Service == service {
				p.notify(ctx, sdk.EventTickPrefix+service)
				break
			}
		}
	}
}
