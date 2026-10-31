package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
func (p *plugin) replaceLink(ms *modeSet) func(provider.HandlerFunc) provider.HandlerFunc {
	wire := ms.wire
	return func(next provider.HandlerFunc) provider.HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			if !ms.speaks(nr) {
				return next(ctx, nr) // the plugin does not handle this transport
			}
			res, err := p.call(ctx, wire, sdk.Replace, nr, nil, nil, next)
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
			return fromResult(res, p.local != nil)
		}
	}
}

// aroundLink runs the plugin before and/or after the inner handler.
func (p *plugin) aroundLink(ms *modeSet) func(provider.HandlerFunc) provider.HandlerFunc {
	wire, before, after := ms.wire, ms.before, ms.after
	return func(next provider.HandlerFunc) provider.HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			if !ms.speaks(nr) {
				return next(ctx, nr) // the plugin does not handle this transport
			}
			if before {
				res, err := p.call(ctx, wire, sdk.Before, nr, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				if res.Params != nil {
					// A patch: keys the plugin omits are untouched, and codec
					// state (e.g. SQS's wire-protocol meta) lives on nr and
					// reaches the built-in codec untouched.
					if p.local != nil {
						nr.Params = patchParams(nr.Params, res.Params)
					} else {
						nr.Params = mergeParams(nr.Params, res.Params)
					}
				}
				if res.Resp != nil || res.Err != nil {
					return fromResult(res, p.local != nil)
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
			res, cerr := p.call(ctx, wire, sdk.After, nr, resp, perr, nil)
			if cerr != nil {
				return nil, cerr
			}
			if res.Resp != nil || res.Err != nil {
				if p.local == nil && res.Resp != nil && resp != nil && hasHidden(resp.Data) {
					return nil, model.NewProviderError("PluginUnsupportedPayload",
						fmt.Sprintf("plugin %s cannot replace a response that carries a stream; "+
							"an out-of-process plugin may observe it but not override it", p.name), 501)
				}
				return fromResult(res, p.local != nil)
			}
			return resp, err
		}
	}
}

// call sends one phase to the plugin. The call gets its own unique id, which
// is the only credential its store callbacks accept, and which is revoked (and
// drained) when the call returns. next is the rest of the chain, given to an
// in-process replace as Call.Next.
func (p *plugin) call(ctx context.Context, wire string, phase sdk.Mode, nr *model.NormalizedRequest, resp *model.ProviderResponse, perr *model.ProviderError, next provider.HandlerFunc) (res sdk.Result, err error) {
	// Nothing a plugin does may take the request, or the process, down with it:
	// a panic becomes a PluginPanic error that names the plugin. An out-of-process
	// plugin cannot panic into the host; an in-process one runs on this goroutine,
	// so the recover also covers a panic in the built-in reached through Next.
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

	req := &sdk.Request{
		Cloud: p.host.cloud, Service: wire, Action: nr.Action,
		AccountID: nr.AccountID, Region: nr.Region, AccessKey: nr.AccessKey,
		Origin: originOf(nr), Transport: nr.Transport, SourceArn: nr.SourceArn, ServicePrincipal: nr.ServicePrincipal,
		RequestID: requestID(ctx, nr), CallID: callID,
		// Not nr.Clock: that is fixed at config load, while POST
		// /_jaiscloud/clock replaces the global clock.
		Now: clock.Now(),
	}

	start := clock.RealNow()
	if p.local != nil {
		res, err = p.callLocal(ctx, req, phase, nr, resp, perr, next)
	} else {
		res, err = p.callHTTP(ctx, req, phase, nr, resp, perr)
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

// callHTTP sends one phase to an out-of-process plugin as JSON.
func (p *plugin) callHTTP(ctx context.Context, req *sdk.Request, phase sdk.Mode, nr *model.NormalizedRequest, resp *model.ProviderResponse, perr *model.ProviderError) (res sdk.Result, err error) {
	req.Params = project(nr.Params)
	in := sdk.HandleRequest{Phase: phase, Req: req}
	if resp != nil {
		in.Resp = &sdk.Response{Status: resp.HTTPStatus, Data: project(resp.Data)}
	}
	if perr != nil {
		in.Err = &sdk.Error{Code: perr.Code, Message: perr.Message, Status: perr.HTTPStatus, Data: perr.Data}
	}
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
	return res, err
}

// callLocal calls an in-process plugin directly. It sees the emulator's own values:
// Params and Data are not projected to JSON, so bytes and streams reach it as they
// are, and what it returns is used as it is. The call runs on this goroutine, so
// there is no timeout to enforce and no connection to retry.
func (p *plugin) callLocal(ctx context.Context, req *sdk.Request, phase sdk.Mode, nr *model.NormalizedRequest, resp *model.ProviderResponse, perr *model.ProviderError, next provider.HandlerFunc) (sdk.Result, error) {
	req.Params = maps.Clone(nr.Params)
	c := &sdk.Call{Phase: phase, Req: req,
		Store: &localStore{h: p.host, p: p, callID: req.CallID, account: nr.AccountID, region: nr.Region}}
	if resp != nil {
		c.Resp = &sdk.Response{Status: resp.HTTPStatus, Data: maps.Clone(resp.Data)}
	}
	if perr != nil {
		c.Err = &sdk.Error{Code: perr.Code, Message: perr.Message, Status: perr.HTTPStatus, Data: perr.Data}
	}
	if phase == sdk.Replace && next != nil {
		c.ResourceID = nr.ResourceID
		c.Next = func(ctx context.Context, r *sdk.Request) (*sdk.Response, *sdk.Error) {
			if r != nil && r.Params != nil {
				nr.Params = r.Params
			}
			out, err := next(ctx, nr)
			if err != nil {
				var pe *model.ProviderError
				if errors.As(err, &pe) {
					return nil, &sdk.Error{Code: pe.Code, Message: pe.Message, Status: pe.HTTPStatus, Data: pe.Data}
				}
				return nil, &sdk.Error{Code: "InternalError", Message: err.Error(), Status: http.StatusInternalServerError}
			}
			if out == nil {
				return &sdk.Response{Status: http.StatusOK, Data: map[string]any{}}, nil
			}
			return &sdk.Response{Status: out.HTTPStatus, Data: out.Data}, nil
		}
	}
	res, err := p.local.Handle(ctx, c)
	if err != nil {
		return sdk.Result{}, model.NewProviderError("PluginError", "plugin "+p.name+": "+err.Error(), http.StatusInternalServerError)
	}
	return res, nil
}

// patchParams applies an in-process plugin's before result as a patch: a key it
// returns is set, a key it returns as null is removed, the rest is untouched. There
// is nothing hidden from an in-process plugin, so the values are used as they are.
func patchParams(orig, patch map[string]any) map[string]any {
	out := make(map[string]any, len(orig)+len(patch))
	maps.Copy(out, orig)
	for k, v := range patch {
		if v == nil {
			delete(out, k)
			continue
		}
		out[k] = v
	}
	return out
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

// fromResult turns a plugin's answer into the provider's. An out-of-process answer
// carries bytes as {"$base64": ...}, which is turned back into bytes; an in-process
// one already holds the values it means.
func fromResult(res sdk.Result, local bool) (*model.ProviderResponse, error) {
	restoreMap := restoreMap
	if local {
		restoreMap = func(m map[string]any) map[string]any { return m }
	}
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
