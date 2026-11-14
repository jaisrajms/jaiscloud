package sdk

import (
	"context"
	"sort"
)

// Func handles one phase of one action.
type Func func(ctx context.Context, c *Call) (Result, error)

// Mux is a Plugin built from per-action functions, so authors write one small
// function per behaviour instead of a switch.
//
//	m := sdk.NewMux("acme-audit", "0.1.0")
//	m.Before("aws", "sqs", "SendMessage", audit)
//	sdk.ListenAndServe(":9100", m)
type Mux struct {
	name, version string
	routes        map[muxKey]Func
	order         []muxKey
	lifecycle     func(ctx context.Context, event string)
	transports    []string
	requires      []string
	services      []Service
	serviceIndex  map[string]int
	endpoint      string
}

type muxKey struct {
	cloud, service, action string
	mode                   Mode
}

func NewMux(name, version string) *Mux {
	return &Mux{name: name, version: version, routes: map[muxKey]Func{}}
}

func (m *Mux) add(mode Mode, cloud, service, action string, f Func) {
	k := muxKey{cloud, service, action, mode}
	if _, dup := m.routes[k]; !dup {
		m.order = append(m.order, k)
	}
	m.routes[k] = f
}

// Replace answers instead of the built-in. action may be "*" for the whole service.
func (m *Mux) Replace(cloud, service, action string, f Func) {
	m.add(Replace, cloud, service, action, f)
}

// Before runs ahead of the core.
func (m *Mux) Before(cloud, service, action string, f Func) { m.add(Before, cloud, service, action, f) }

// After runs after the core.
func (m *Mux) After(cloud, service, action string, f Func) { m.add(After, cloud, service, action, f) }

// Service starts (or continues) the declaration of a wire service the plugin
// adds. Repeated calls for the same cloud and name return the same declaration,
// so paths accumulate into one Service. A service that has no paths yet is still
// declared: use Replace for a service that answers every action itself.
//
//	m.Service("gcp", "widgets").
//		Get("/v1/projects/{project}/widgets/{widget}", "WidgetsGet").
//		Replace(handle)
func (m *Mux) Service(cloud, name string) *ServiceBuilder {
	if m.serviceIndex == nil {
		m.serviceIndex = map[string]int{}
	}
	key := cloud + "\x00" + name
	i, ok := m.serviceIndex[key]
	if !ok {
		i = len(m.services)
		m.services = append(m.services, Service{Cloud: cloud, Name: name})
		m.serviceIndex[key] = i
	}
	return &ServiceBuilder{m: m, index: i}
}

// ServiceBuilder collects the paths of one declared service.
type ServiceBuilder struct {
	m     *Mux
	index int
}

// Get declares a GET path.
func (b *ServiceBuilder) Get(pattern, action string) *ServiceBuilder {
	return b.add("GET", pattern, action)
}

// Post declares a POST path.
func (b *ServiceBuilder) Post(pattern, action string) *ServiceBuilder {
	return b.add("POST", pattern, action)
}

// Put declares a PUT path.
func (b *ServiceBuilder) Put(pattern, action string) *ServiceBuilder {
	return b.add("PUT", pattern, action)
}

// Patch declares a PATCH path.
func (b *ServiceBuilder) Patch(pattern, action string) *ServiceBuilder {
	return b.add("PATCH", pattern, action)
}

// Delete declares a DELETE path.
func (b *ServiceBuilder) Delete(pattern, action string) *ServiceBuilder {
	return b.add("DELETE", pattern, action)
}

func (b *ServiceBuilder) add(method, pattern, action string) *ServiceBuilder {
	b.m.services[b.index].Paths = append(b.m.services[b.index].Paths, Path{Method: method, Pattern: pattern, Action: action})
	return b
}

// Replace answers every action of the service that is not registered for an
// exact action. It installs the service's wildcard replace route (calling it
// again replaces that function instead of adding a second route) and leaves the
// service registered even when it has no paths.
func (b *ServiceBuilder) Replace(f Func) *ServiceBuilder {
	s := b.m.services[b.index]
	b.m.add(Replace, s.Cloud, s.Name, "*", f)
	return b
}

// Transports restricts the plugin to the given wire transports ("rest", "grpc"); the
// default is all of them. Call it when the functions only understand one shape of
// Params, for example the REST body of a GCP request: a gRPC call to the same key then
// skips the plugin instead of handing it protojson.
func (m *Mux) Transports(transports ...string) *Mux {
	m.transports = transports
	return m
}

// Require lists host capabilities the plugin cannot work without (see CapInProcess).
// A plugin whose functions use Call.Next, or return bytes and streams in Data, should
// require sdk.CapInProcess so that loading it as an HTTP sidecar fails at startup.
func (m *Mux) Require(caps ...string) *Mux {
	m.requires = append(m.requires, caps...)
	return m
}

func (m *Mux) Manifest() Manifest {
	type g struct {
		cloud, service string
		mode           Mode
	}
	groups := map[g][]string{}
	var gorder []g
	for _, k := range m.order {
		key := g{k.cloud, k.service, k.mode}
		if _, ok := groups[key]; !ok {
			gorder = append(gorder, key)
		}
		groups[key] = append(groups[key], k.action)
	}
	mf := Manifest{ABI: ABI, Name: m.name, Version: m.version, Requires: m.requires}
	if len(m.services) > 0 {
		mf.Services = make([]Service, len(m.services))
		for i, s := range m.services {
			mf.Services[i] = Service{Cloud: s.Cloud, Name: s.Name, Paths: append([]Path(nil), s.Paths...)}
		}
		if !hasCap(mf.Requires, CapServices) {
			mf.Requires = append(append([]string(nil), m.requires...), CapServices)
		}
	}
	for _, key := range gorder {
		acts := groups[key]
		sort.Strings(acts)
		mf.Routes = append(mf.Routes, Route{Cloud: key.cloud, Service: key.service, Actions: acts, Mode: key.mode, Transports: m.transports})
	}
	return mf
}

func hasCap(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// HandleLifecycle registers a function called after the emulator resets or
// restores state (see LifecycleListener); use it to drop caches.
func (m *Mux) HandleLifecycle(f func(ctx context.Context, event string)) { m.lifecycle = f }

// OnLifecycle implements LifecycleListener.
func (m *Mux) OnLifecycle(ctx context.Context, event string) {
	if m.lifecycle != nil {
		m.lifecycle(ctx, event)
	}
}

// Init keeps the endpoint the host sends, so a handler can delegate to another
// service's REST API through Endpoint.
func (m *Mux) Init(_ context.Context, h Host) error {
	m.endpoint = h.Endpoint
	return nil
}

// Endpoint returns the emulator's base URL (http://127.0.0.1:<port>), for a
// handler that delegates to another service's REST API. It is empty when the
// host did not send one (an older emulator).
func (m *Mux) Endpoint() string { return m.endpoint }

// Handle dispatches to the exact action's function, else to the "*" function.
// An unregistered Replace is NotHandled; an unregistered Before/After is a no-op.
func (m *Mux) Handle(ctx context.Context, c *Call) (Result, error) {
	r := c.Req
	f, ok := m.routes[muxKey{r.Cloud, r.Service, r.Action, c.Phase}]
	if !ok {
		f, ok = m.routes[muxKey{r.Cloud, r.Service, "*", c.Phase}]
	}
	if !ok {
		return Result{NotHandled: c.Phase == Replace}, nil
	}
	return f(ctx, c)
}

func (m *Mux) Shutdown(context.Context) error { return nil }
