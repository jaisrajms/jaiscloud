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
	mf := Manifest{ABI: ABI, Name: m.name, Version: m.version}
	for _, key := range gorder {
		acts := groups[key]
		sort.Strings(acts)
		mf.Routes = append(mf.Routes, Route{Cloud: key.cloud, Service: key.service, Actions: acts, Mode: key.mode})
	}
	return mf
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

func (m *Mux) Init(context.Context, Host) error { return nil }

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
