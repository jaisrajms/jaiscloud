// Package cloudbilling is the REST transport for Cloud Billing v1
// (cloudbilling.googleapis.com/v1).
//
// Real GCP serves the API over REST only for the association surface modelled
// here; the emulator answers the one method that blocks Terraform's
// data "google_project" / resource "google_project" and the
// terraform-google-pubsub module:
//
//	GET /v1/projects/{project}/billingInfo   (cloudbilling.projects.getBillingInfo)
//
// The project singleton's write half (PUT …, updateBillingInfo) and the
// top-level /v1/billingAccounts collection are not served yet; every other
// method or path answers UnsupportedOperation 404. The Codec is a
// NormalizedRequest adapter (HTTP path/query ↔ the core's typed API); the
// Provider holds the routes. Neither owns business logic — both delegate to the
// core Service in internal/gcp/service/cloudbilling.
package cloudbilling

import (
	"encoding/json"
	"net/http"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name (the Discovery document's id).
const ServiceName = "cloudbilling"

// Codec decodes Cloud Billing v1 REST requests into a NormalizedRequest and
// encodes provider responses as the GCP JSON envelope. It satisfies
// adapter.Codec structurally (the adapter package imports this package, so this
// package must not import it).
type Codec struct{}

// NewCodec returns the Cloud Billing REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a v1 project billing path into a NormalizedRequest. Params carry
// project plus any query parameters (alt, prettyPrint, ...), which are ignored
// by the handler. Only the project billingInfo singleton with GET is supported;
// the segment match is case-sensitive ("billingInfo", capital I), exactly as the
// Discovery document declares it.
func (c *Codec) Decode(r *http.Request, _ []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	// projects/{project}/billingInfo: exactly one segment after the project, and
	// it is the singleton (no trailing resource id). The length check comes first
	// so a bare /v1/projects (or a project with no trailing segment) cannot index
	// past the slice.
	if pi < 0 || pi+2 != len(seg)-1 || seg[pi+1] == "" || seg[pi+2] != "billingInfo" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	if r.Method != http.MethodGet {
		// updateBillingInfo (PUT) is not served in this session.
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = seg[pi+1]
	queryToParams(r, nr.Params)
	nr.Action = "ProjectGetBillingInfo"
	return nr, nil
}

// Encode serialises a provider response as JSON.
func (c *Codec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	if raw, ok := resp.Data[wire.RawJSONKey].(json.RawMessage); ok {
		return status, headers, raw
	}
	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as a GCP error envelope.
func (c *Codec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	statusStr, _ := gcperr.Resolve(perr)
	env := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": perr.Message,
			"status":  statusStr,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}
