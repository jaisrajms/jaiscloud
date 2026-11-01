package pluginhost

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"jaiscloud/internal/clock"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// Shutdown stops the in-process plugins, last loaded first. An out-of-process plugin
// belongs to whoever runs it, so nothing is sent to it. A failure is logged, never
// fatal, and each plugin has the time left on ctx.
func (h *Host) Shutdown(ctx context.Context) {
	if h == nil {
		return
	}
	for i := len(h.plugins) - 1; i >= 0; i-- {
		if p := h.plugins[i]; p.local != nil {
			p.shutdownLocal(ctx)
		}
	}
}

// ShutdownTimeout is how long in-process plugins get to stop.
const ShutdownTimeout = 10 * time.Second

// Snapshotters returns the state in-process plugins keep outside Store, by the name
// it is registered under with the admin handler, so reset, export/import and named
// snapshots include it.
func (h *Host) Snapshotters() map[string]sdk.Stateful {
	out := map[string]sdk.Stateful{}
	for _, p := range h.plugins {
		if p.stateful != nil {
			out["plugin:"+p.name] = p.stateful
		}
	}
	return out
}

// guardLocal runs fn, which is a call into in-process plugin code made outside a
// request (lifecycle, reset, shutdown): a panic there is logged against the plugin
// instead of taking the process down.
func (p *plugin) guardLocal(what string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("plugin panic", "plugin", p.name, "during", what, "panic", r, "stack", string(debug.Stack()))
		}
	}()
	fn()
}

func (p *plugin) notifyLocal(ctx context.Context, event string) {
	if l, ok := p.local.(sdk.LifecycleListener); ok {
		p.guardLocal("lifecycle "+event, func() { l.OnLifecycle(ctx, event) })
	}
}

func (p *plugin) resetLocal(ctx context.Context) {
	p.guardLocal("reset", func() { p.stateful.Reset(ctx) })
}

func (p *plugin) shutdownLocal(ctx context.Context) {
	p.guardLocal("shutdown", func() {
		start := clock.RealNow()
		if err := p.local.Shutdown(ctx); err != nil {
			slog.Warn("plugin shutdown failed", "plugin", p.name, "err", err, "after", clock.RealNow().Sub(start).String())
		}
	})
}
