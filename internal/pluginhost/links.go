package pluginhost

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gateway/middleware"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// replaceLink makes the plugin the core of a key. NotHandled falls through to
// the handler that was registered before (normally the built-in).
func (p *plugin) replaceLink() func(provider.HandlerFunc) provider.HandlerFunc {
	return func(next provider.HandlerFunc) provider.HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			res, err := p.call(ctx, sdk.Replace, nr, nil, nil)
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
func (p *plugin) aroundLink(before, after bool) func(provider.HandlerFunc) provider.HandlerFunc {
	return func(next provider.HandlerFunc) provider.HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			if before {
				res, err := p.call(ctx, sdk.Before, nr, nil, nil)
				if err != nil {
					return nil, err
				}
				// Only Params is honoured: codec state (e.g. SQS's wire-protocol
				// meta) lives on nr and must reach the built-in codec untouched.
				if res.Req != nil && res.Req.Params != nil {
					nr.Params = res.Req.Params
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
				var ok bool
				if perr, ok = err.(*model.ProviderError); !ok {
					return resp, err // internal failures are not shown to plugins
				}
			}
			if hasInternalKeys(resp) {
				slog.Warn("plugin after-hook skipped: response carries non-JSON internal data",
					"plugin", p.name, "service", nr.Service, "action", nr.Action)
				return resp, err
			}
			res, cerr := p.call(ctx, sdk.After, nr, resp, perr)
			if cerr != nil {
				return nil, cerr
			}
			if res.Resp != nil || res.Err != nil {
				return fromResult(res)
			}
			return resp, err
		}
	}
}

// call sends one phase to the plugin and tracks the request as in flight.
func (p *plugin) call(ctx context.Context, phase sdk.Mode, nr *model.NormalizedRequest, resp *model.ProviderResponse, perr *model.ProviderError) (sdk.Result, error) {
	rid := middleware.GetRequestID(ctx)
	if rid == "" {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		rid = "plg-" + hex.EncodeToString(b)
	}
	p.host.inflight.enter(rid)
	defer p.host.inflight.leave(rid)

	in := sdk.HandleRequest{
		Phase: phase,
		Req: &sdk.Request{
			Cloud: string(nr.Cloud), Service: nr.Service, Action: nr.Action, Params: nr.Params,
			AccountID: nr.AccountID, Region: nr.Region, AccessKey: nr.AccessKey,
			RequestID: rid,
			// Not nr.Clock: that is fixed at config load, while POST
			// /_jaiscloud/clock replaces the global clock.
			Now: clock.Now(),
		},
	}
	if resp != nil {
		in.Resp = &sdk.Response{Status: resp.HTTPStatus, Data: resp.Data}
	}
	if perr != nil {
		in.Err = &sdk.Error{Code: perr.Code, Message: perr.Message, Status: perr.HTTPStatus, Data: perr.Data}
	}
	var res sdk.Result
	if err := p.do(ctx, http.MethodPost, "/handle", in, &res); err != nil {
		return sdk.Result{}, err
	}
	slog.Debug("plugin call", "plugin", p.name, "phase", phase, "service", nr.Service, "action", nr.Action)
	return res, nil
}

func fromResult(res sdk.Result) (*model.ProviderResponse, error) {
	if res.Err != nil {
		status := res.Err.Status
		if status == 0 {
			status = http.StatusInternalServerError
		}
		pe := model.NewProviderError(res.Err.Code, res.Err.Message, status)
		pe.Data = res.Err.Data
		return nil, pe
	}
	status := res.Resp.Status
	if status == 0 {
		status = http.StatusOK
	}
	data := res.Resp.Data
	if data == nil {
		data = map[string]any{}
	}
	return &model.ProviderResponse{HTTPStatus: status, Data: data}, nil
}

// hasInternalKeys reports whether Data carries "_"-prefixed internal values
// such as "_stream", which are not wire fields and cannot cross to a plugin.
func hasInternalKeys(resp *model.ProviderResponse) bool {
	if resp == nil {
		return false
	}
	for k := range resp.Data {
		if strings.HasPrefix(k, "_") {
			return true
		}
	}
	return false
}
