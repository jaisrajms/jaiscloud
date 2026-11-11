// Package cloudbilling is the REST transport for Cloud Billing v1
// (cloudbilling.googleapis.com/v1).
//
// Real GCP serves the API over REST only for the association surface modelled
// here; the emulator answers the methods that block Terraform's
// data "google_project" / resource "google_project" and the
// terraform-google-pubsub module:
//
//	GET  /v1/projects/{project}/billingInfo            (cloudbilling.projects.getBillingInfo)
//	PUT  /v1/projects/{project}/billingInfo            (cloudbilling.projects.updateBillingInfo)
//	GET  /v1/billingAccounts                           (cloudbilling.billingAccounts.list)
//	GET  /v1/billingAccounts/{account}                 (cloudbilling.billingAccounts.get)
//	POST /v1/billingAccounts/{account}:testIamPermissions
//	                                                   (cloudbilling.billingAccounts.testIamPermissions)
//
// The billing-account catalog itself (create/patch/move, subAccounts.*,
// organizations.billingAccounts.*, services.*, skus.*, the IAM policy methods)
// is deliberately not modelled (plan CB5). Those paths are still claimed — the
// codec decodes each to its own action so the Provider answers an explicit
// Unimplemented 501 naming the Discovery method (never a plausible empty body,
// and never the unknown-service 404 the path had before the document was
// vendored). The live document's unrelated tasks.* / message:send|stream /
// getCard cluster is not a Cloud Billing surface and stays unclaimed. Any other
// path answers UnsupportedOperation 404. The Codec is a NormalizedRequest
// adapter (HTTP path/query/body ↔ the core's typed API); the Provider holds the
// routes. Neither owns business logic — both delegate to the core Service in
// internal/gcp/service/cloudbilling.
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

// Decode parses a v1 Cloud Billing path into a NormalizedRequest. Params carry
// the path ids (project, billingAccount or organization) and the decoded JSON
// body (write methods), plus any query parameters (alt, prettyPrint, ...), which
// are ignored. Three path families are decoded:
//
//   - the top-level billing-account collection, /v1/billingAccounts..., which
//     has no project segment and is therefore claimed first;
//   - the organization billing-account collection,
//     /v1/organizations/{org}/billingAccounts..., claimed next;
//   - the top-level service/SKU catalog, /v1/services[/{service}/skus];
//   - the project billingInfo singleton, /v1/projects/{project}/billingInfo.
//     The segment match is case-sensitive ("billingInfo", capital I), exactly
//     as the Discovery document declares it.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	// billingAccounts is a top-level collection: /v1/billingAccounts[/{id}]
	// [:verb]. It has no project segment, so it is decoded before the projects
	// scan (which would otherwise treat the path as malformed).
	if len(seg) >= 2 && seg[0] == "v1" {
		switch seg[1] {
		case "billingAccounts":
			return c.decodeBillingAccounts(r, body, seg)
		case "organizations":
			return c.decodeOrganizationBillingAccounts(r, body, seg)
		case "services":
			return c.decodeServices(r, body, seg)
		}
	}
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

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = seg[pi+1]
	queryToParams(r, nr.Params)
	switch r.Method {
	case http.MethodGet:
		nr.Action = "ProjectGetBillingInfo"
	case http.MethodPut:
		// updateBillingInfo: the ProjectBillingInfo body carries
		// billingAccountName (billingEnabled is output-only and ignored).
		nr.Action = "ProjectUpdateBillingInfo"
		return c.withBody(nr, body)
	default:
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// decodeBillingAccounts decodes the top-level /v1/billingAccounts family. The
// catalog methods (create/patch/move, subAccounts.*, projects.list, the IAM
// policy verbs) are decoded to their own actions so the Provider answers an
// explicit Unimplemented 501 naming the method (plan CB5); list, get and the
// permissive testIamPermissions echo are served.
func (c *Codec) decodeBillingAccounts(r *http.Request, body []byte, seg []string) (*model.NormalizedRequest, error) {
	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)

	// /v1/billingAccounts — the collection: list (GET) / create (POST).
	if len(seg) == 2 {
		switch r.Method {
		case http.MethodGet:
			nr.Action = "BillingAccountsList"
			return nr, nil
		case http.MethodPost:
			nr.Action = "BillingAccountsCreate"
			return nr, nil
		}
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}

	account, custom := splitVerb(seg[2])
	if account != "" {
		nr.Params["billingAccount"] = account
	}
	if len(seg) == 3 && account != "" {
		switch {
		case custom == "" && r.Method == http.MethodGet:
			nr.Action = "BillingAccountsGet"
			return nr, nil
		case custom == "" && r.Method == http.MethodPatch:
			nr.Action = "BillingAccountsPatch"
			return c.withBody(nr, body)
		case custom == "move" && r.Method == http.MethodPost:
			nr.Action = "BillingAccountsMove"
			return c.withBody(nr, body)
		case custom == "testIamPermissions" && r.Method == http.MethodPost:
			// The default-permissive IAM echo: the handler returns exactly the
			// requested permission strings and enforces nothing.
			nr.Action = "BillingAccountTestIamPermissions"
			return c.withBody(nr, body)
		case custom == "getIamPolicy" && r.Method == http.MethodGet:
			nr.Action = "BillingAccountsGetIamPolicy"
			return c.withBody(nr, body)
		case custom == "setIamPolicy" && r.Method == http.MethodPost:
			nr.Action = "BillingAccountsSetIamPolicy"
			return c.withBody(nr, body)
		}
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}

	// /v1/billingAccounts/{account}/{sub-collection}: projects.list and the
	// sub-account collection. A single sub-account id is not declared by the
	// document, so it stays an unsupported operation.
	if len(seg) == 4 && account != "" {
		switch seg[3] {
		case "projects":
			if r.Method == http.MethodGet {
				nr.Action = "BillingAccountsProjectsList"
				return nr, nil
			}
		case "subAccounts":
			switch r.Method {
			case http.MethodGet:
				nr.Action = "BillingAccountsSubAccountsList"
				return nr, nil
			case http.MethodPost:
				nr.Action = "BillingAccountsSubAccountsCreate"
				return c.withBody(nr, body)
			}
		}
	}
	return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
}

// decodeOrganizationBillingAccounts decodes the organization-scoped catalog,
// /v1/organizations/{org}/billingAccounts[/{id}:move] (the live document
// declares organizations.billingAccounts create/list/move only). Every decoded
// action is an explicit Unimplemented 501 in the Provider; the project-level
// association is the only billing resource the emulator models.
func (c *Codec) decodeOrganizationBillingAccounts(r *http.Request, body []byte, seg []string) (*model.NormalizedRequest, error) {
	if len(seg) < 4 || seg[2] == "" || seg[3] != "billingAccounts" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)
	nr.Params["organization"] = seg[2]

	if len(seg) == 4 {
		switch r.Method {
		case http.MethodGet:
			nr.Action = "OrganizationsBillingAccountsList"
			return nr, nil
		case http.MethodPost:
			nr.Action = "OrganizationsBillingAccountsCreate"
			return c.withBody(nr, body)
		}
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	if len(seg) == 5 {
		// The document declares the move as GET (v1/{+destinationParent}/
		// {+name}:move); AIP-151-style clients send POST on a ':verb'. Both
		// decode to the same refusal, so neither spelling reaches the
		// unknown-service 404.
		id, custom := splitVerb(seg[4])
		if id != "" && custom == "move" && (r.Method == http.MethodGet || r.Method == http.MethodPost) {
			nr.Params["billingAccount"] = id
			nr.Action = "OrganizationsBillingAccountsMove"
			return c.withBody(nr, body)
		}
	}
	return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
}

// decodeServices decodes the pricing catalog: /v1/services (list) and
// /v1/services/{service}/skus (list). The emulator holds no SKU catalog, so both
// are explicit Unimplemented 501s in the Provider.
func (c *Codec) decodeServices(r *http.Request, body []byte, seg []string) (*model.NormalizedRequest, error) {
	if r.Method != http.MethodGet {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)
	if len(seg) == 2 {
		nr.Action = "ServicesList"
		return nr, nil
	}
	if len(seg) == 4 && seg[2] != "" && seg[3] == "skus" {
		nr.Params["service"] = seg[2]
		nr.Action = "ServicesSkusList"
		return nr, nil
	}
	return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
}

// withBody decodes the JSON request body into the request params (write and
// custom methods carry one; an empty body is not an error).
func (c *Codec) withBody(nr *model.NormalizedRequest, body []byte) (*model.NormalizedRequest, error) {
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}
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
