package functions

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	core "jaiscloud/internal/gcp/service/functions"
	"jaiscloud/internal/model"
)

// TriggerCodec decodes a request addressed to a deployed function's synthesized
// HTTPS-trigger URL and encodes the function's raw result back as the HTTP
// response.
//
// The trigger URL is host-scoped —
// "{location}-{project}.cloudfunctions.net/{functionId}" (see
// core.TriggerHostLabel / core.ParseTriggerHost) — so the GCP adapter selects
// this codec from the request Host rather than the service map
// (internal/gcp/adapter/gcp.go). The control plane (/v1/..., /v2/...) keeps the
// generic JSONCodec. It implements the shared adapter.Codec interface
// structurally, so it does not import the adapter package.
type TriggerCodec struct{}

// NewTriggerCodec returns the Cloud Functions HTTPS-trigger codec.
func NewTriggerCodec() *TriggerCodec { return &TriggerCodec{} }

// ServiceName implements adapter.Codec.
func (c *TriggerCodec) ServiceName() string { return "functions" }

// Decode maps a trigger request onto a Function.InvokeTrigger request: the host
// carries the trigger label ("{location}-{project}"), the first path segment is
// the function id, and the raw request body is the function payload (any HTTP
// method). The remainder of the path is the function's own internal route and is
// not modelled by the emulator.
//
// The host label is ambiguous on its own, so the location/project are left unset
// when they cannot be resolved against the advertised region catalog; the
// provider then resolves the label against the stored functions. A host or path
// that names no function at all is NotFound.
func (c *TriggerCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	label, ok := core.TriggerHostLabel(r.Host)
	if !ok {
		return nil, triggerNotFound()
	}
	id := triggerFunctionID(r.URL.Path)
	if id == "" {
		return nil, triggerNotFound()
	}
	params := map[string]any{
		"functionId":   id,
		"payload":      string(body),
		"triggerLabel": label,
	}
	if project, location, ok := core.ParseTriggerHost(r.Host); ok {
		params["project"] = project
		params["location"] = location
	}
	return &model.NormalizedRequest{
		Service: "functions",
		Action:  "InvokeTrigger",
		Raw:     r,
		Params:  params,
	}, nil
}

// Encode writes the function's result as the raw HTTP response body. The
// provider sets HTTPStatus (200 on success, 500 on an unhandled function error)
// and the body bytes; a trigger response is text/plain.
func (c *TriggerCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "text/plain; charset=utf-8")
	b, _ := resp.Data["body"].([]byte)
	return status, headers, b
}

// EncodeError serialises a ProviderError as the GCP JSON error envelope, so a
// trigger 404/500 is shaped like every other GCP surface.
func (c *TriggerCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	statusStr, _ := gcperr.Resolve(perr)
	out, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": perr.Message,
			"status":  statusStr,
		},
	})
	return status, headers, out
}

// triggerFunctionID returns the function id from a trigger path: the first path
// segment. Real Cloud Functions route the remainder of the path to the function
// itself (which the emulator does not model), so a sub-path still addresses the
// function. An empty path or a custom-method verb names no function.
func triggerFunctionID(path string) string {
	p := strings.Trim(path, "/")
	if p == "" {
		return ""
	}
	first, _, _ := strings.Cut(p, "/")
	if first == "" || strings.Contains(first, ":") {
		return ""
	}
	return first
}

// triggerNotFound is the canonical NotFound provider error for a trigger URL
// that serves no function.
func triggerNotFound() error {
	return model.NewProviderError("NotFound", "function not found", 404)
}
