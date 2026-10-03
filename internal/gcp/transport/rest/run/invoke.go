package run

import (
	"encoding/json"
	"net/http"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/model"
)

// InvocationCodec decodes a request addressed to a synthesized Cloud Run service
// host ("{id}-{token}.{location}.run.app", detected by Host) and encodes the
// revision runtime's raw HTTP response. The control plane keeps the generic JSON
// Codec. It implements the shared adapter.Codec interface structurally.
type InvocationCodec struct{}

// NewInvocationCodec returns the Cloud Run invocation codec.
func NewInvocationCodec() *InvocationCodec { return &InvocationCodec{} }

// ServiceName implements adapter.Codec.
func (c *InvocationCodec) ServiceName() string { return ServiceName }

// Decode maps a host-routed invocation onto a Run.Invoke request. The raw HTTP
// request (method/path/query/headers/body) is what the provider forwards.
func (c *InvocationCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	return &model.NormalizedRequest{
		Service: ServiceName,
		Action:  "Invoke",
		Raw:     r,
		Params:  map[string]any{"body": body},
	}, nil
}

// Encode writes the runtime's response through as the raw HTTP response.
func (c *InvocationCodec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	if hs, ok := resp.Data["headers"].(map[string]string); ok {
		for k, v := range hs {
			headers.Set(k, v)
		}
	}
	b, _ := resp.Data["body"].([]byte)
	return status, headers, b
}

// EncodeError serialises a ProviderError as the GCP JSON error envelope.
func (c *InvocationCodec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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
