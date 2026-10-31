package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gateway/middleware"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// replaceLink makes the plugin the core of a key. NotHandled falls through to
// the handler that was registered before (normally the built-in).
func (p *plugin) replaceLink(wire string) func(provider.HandlerFunc) provider.HandlerFunc {
	return func(next provider.HandlerFunc) provider.HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			res, err := p.call(ctx, wire, sdk.Replace, nr, nil, nil)
			if err != nil {
				return nil, err
			}
			if res.NotHandled {
				return next(ctx, nr)
			}
			if res.Resp == nil && res.Err == nil {
				return nil, model.NewProviderError("PluginProtocolError",
					"plugin "+p.name+": replace returned neither a response, an error, nor notHandled", 502)
			}
			return fromResult(res)
		}
	}
}

// aroundLink runs the plugin before and/or after the inner handler.
func (p *plugin) aroundLink(wire string, before, after bool) func(provider.HandlerFunc) provider.HandlerFunc {
	return func(next provider.HandlerFunc) provider.HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			if before {
				res, err := p.call(ctx, wire, sdk.Before, nr, nil, nil)
				if err != nil {
					return nil, err
				}
				if res.Params != nil {
					// A patch: keys the plugin omits are untouched, and codec
					// state (e.g. SQS's wire-protocol meta) lives on nr and
					// reaches the built-in codec untouched.
					nr.Params = mergeParams(nr.Params, res.Params)
				}
				if res.Resp != nil || res.Err != nil {
					return fromResult(res)
				}
			}
			resp, err := next(ctx, nr)
			if !after {
				return resp, err
			}
			var perr *model.ProviderError
			if err != nil {
				if !errors.As(err, &perr) {
					return resp, err // internal failures are not shown to plugins
				}
			}
			res, cerr := p.call(ctx, wire, sdk.After, nr, resp, perr)
			if cerr != nil {
				return nil, cerr
			}
			if res.Resp != nil || res.Err != nil {
				if res.Resp != nil && resp != nil && hasHidden(resp.Data) {
					return nil, model.NewProviderError("PluginUnsupportedPayload",
						fmt.Sprintf("plugin %s cannot replace a response that carries a stream; "+
							"an out-of-process plugin may observe it but not override it", p.name), 501)
				}
				return fromResult(res)
			}
			return resp, err
		}
	}
}

// call sends one phase to the plugin. The call gets its own unique id, which
// is the only credential its store callbacks accept, and which is revoked (and
// drained) when the call returns.
func (p *plugin) call(ctx context.Context, wire string, phase sdk.Mode, nr *model.NormalizedRequest, resp *model.ProviderResponse, perr *model.ProviderError) (res sdk.Result, err error) {
	// Nothing a plugin does may take the request, or the process, down with it:
	// a panic becomes a PluginPanic error that names the plugin. An out-of-process
	// plugin cannot panic into the host, but the same seam serves in-process ones.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("plugin panic", "plugin", p.name, "phase", phase, "service", nr.Service,
				"action", nr.Action, "panic", r, "stack", string(debug.Stack()))
			res, err = sdk.Result{}, model.NewProviderError(codePluginPanic,
				fmt.Sprintf("plugin %s panicked in %s of %s.%s", p.name, phase, nr.Service, nr.Action), 500)
		}
	}()
	callID := randomID(16)
	p.host.begin(callID, p, nr.AccountID, nr.Region)
	defer p.host.end(callID)

	in := sdk.HandleRequest{
		Phase: phase,
		Req: &sdk.Request{
			Cloud: p.host.cloud, Service: wire, Action: nr.Action, Params: project(nr.Params),
			AccountID: nr.AccountID, Region: nr.Region, AccessKey: nr.AccessKey,
			Origin: originOf(nr), SourceArn: nr.SourceArn, ServicePrincipal: nr.ServicePrincipal,
			RequestID: requestID(ctx, nr), CallID: callID,
			// Not nr.Clock: that is fixed at config load, while POST
			// /_jaiscloud/clock replaces the global clock.
			Now: clock.Now(),
		},
	}
	if resp != nil {
		in.Resp = &sdk.Response{Status: resp.HTTPStatus, Data: project(resp.Data)}
	}
	if perr != nil {
		in.Err = &sdk.Error{Code: perr.Code, Message: perr.Message, Status: perr.HTTPStatus, Data: perr.Data}
	}

	start := clock.RealNow()
	err = p.do(ctx, http.MethodPost, "/handle", in, &res, p.retry)
	if isNotInitialized(err) {
		// The plugin restarted and lost the store endpoint: hand it back and retry once.
		if ierr := p.reinit(ctx); ierr == nil {
			res, err = sdk.Result{}, p.do(ctx, http.MethodPost, "/handle", in, &res, p.retry)
		}
		if isNotInitialized(err) {
			err = model.NewProviderError(model.CodePluginUnavailable,
				"plugin "+p.name+" restarted and did not accept /init", 503)
		}
	}
	observe(p.name, string(phase), outcome(err, res), clock.RealNow().Sub(start))
	if err != nil {
		return sdk.Result{}, err
	}
	if verr := p.validate(res); verr != nil {
		return sdk.Result{}, verr
	}
	slog.Debug("plugin call", "plugin", p.name, "phase", phase, "service", nr.Service, "action", nr.Action)
	return res, nil
}

// requestID returns a stable id for the originating request: the HTTP request
// id when there is one, otherwise one generated once per nr (so before/after
// of an internal call correlate).
func requestID(ctx context.Context, nr *model.NormalizedRequest) string {
	if id := middleware.GetRequestID(ctx); id != "" {
		return id
	}
	if id := nr.GetMeta("plugin_rid"); id != "" {
		return id
	}
	id := "plg-" + randomID(8)
	nr.SetMeta("plugin_rid", id)
	return id
}

// validate rejects results that contradict themselves or the HTTP range, with
// a protocol error instead of letting them silently misbehave.
func (p *plugin) validate(res sdk.Result) error {
	bad := func(msg string) error {
		return model.NewProviderError("PluginProtocolError", "plugin "+p.name+": "+msg, 502)
	}
	if res.Resp != nil && res.Err != nil {
		return bad("result sets both resp and err")
	}
	if res.Resp != nil && res.Resp.Status != 0 && (res.Resp.Status < 100 || res.Resp.Status > 599) {
		return bad(fmt.Sprintf("resp.status %d is not a valid HTTP status", res.Resp.Status))
	}
	if res.Err != nil {
		if res.Err.Code == "" {
			return bad("err.code is required")
		}
		if res.Err.Status != 0 && (res.Err.Status < 100 || res.Err.Status > 599) {
			return bad(fmt.Sprintf("err.status %d is not a valid HTTP status", res.Err.Status))
		}
	}
	return nil
}

func fromResult(res sdk.Result) (*model.ProviderResponse, error) {
	if res.Err != nil {
		status := res.Err.Status
		if status == 0 {
			status = http.StatusInternalServerError
		}
		pe := model.NewProviderError(res.Err.Code, res.Err.Message, status)
		pe.Data = restoreMap(res.Err.Data)
		return nil, pe
	}
	status := res.Resp.Status
	if status == 0 {
		status = http.StatusOK
	}
	data := restoreMap(res.Resp.Data)
	if data == nil {
		data = map[string]any{}
	}
	return &model.ProviderResponse{HTTPStatus: status, Data: data}, nil
}

func originOf(nr *model.NormalizedRequest) string {
	if nr.Origin == model.OriginWire {
		return "wire"
	}
	return string(nr.Origin)
}
