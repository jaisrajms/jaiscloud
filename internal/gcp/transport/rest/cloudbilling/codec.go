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
// organizations.*, services.*, skus.*, the IAM policy methods) is deliberately
// not modelled (plan CB5) and fails loud with Unimplemented 501 — never a
// plausible empty body. Any other path answers UnsupportedOperation 404. The
// Codec is a NormalizedRequest adapter (HTTP path/query/body ↔ the core's typed
// API); the Provider holds the routes. Neither owns business logic — both
// delegate to the core Service in internal/gcp/service/cloudbilling.
package cloudbilling

import (
	"encoding/json"
	"net/http"
	"strings"

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
// the path ids (project or billingAccount) and the decoded JSON body (write
// methods), plus any query parameters (alt, prettyPrint, ...), which are
// ignored. Two path families are decoded:
//
//   - the top-level billing-account collection, /v1/billingAccounts..., which
//     has no project segment and is therefore claimed first;
//   - the project billingInfo singleton, /v1/projects/{project}/billingInfo.
//     The segment match is case-sensitive ("billingInfo", capital I), exactly
//     as the Discovery document declares it.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	// billingAccounts is a top-level collection: /v1/billingAccounts[/{id}]
	// [:verb]. It has no project segment, so it is decoded before the projects
	// scan (which would otherwise treat the path as malformed).
	if len(seg) >= 2 && seg[0] == "v1" && seg[1] == "billingAccounts" {
		return c.decodeBillingAccounts(r, body, seg)
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
		m, err := parseJSON(body)
		if err != nil {
			return nil, model.NewProviderError("InvalidArgument", "malformed JSON body", 400)
		}
		if m != nil {
			nr.Params["body"] = m
		}
		nr.Action = "ProjectUpdateBillingInfo"
	default:
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// decodeBillingAccounts decodes the top-level /v1/billingAccounts family. Only
// list, get and the permissive testIamPermissions echo are modelled; the rest of
// the catalog and the IAM policy methods fail loud with Unimplemented 501 (plan
// CB5), naming the operation family so the refusal is self-describing.
func (c *Codec) decodeBillingAccounts(r *http.Request, body []byte, seg []string) (*model.NormalizedRequest, error) {
	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)

	// /v1/billingAccounts — the collection (list). A create (POST) is not
	// modelled; it falls through to the catalog refusal below.
	if len(seg) == 2 && r.Method == http.MethodGet {
		nr.Action = "BillingAccountsList"
		return nr, nil
	}

	account, custom := "", ""
	if len(seg) >= 3 {
		account, custom = splitVerb(seg[2])
	}
	if account != "" {
		nr.Params["billingAccount"] = account
	}
	if len(seg) == 3 && account != "" {
		switch {
		case custom == "" && r.Method == http.MethodGet:
			nr.Action = "BillingAccountsGet"
			return nr, nil
		case custom == "testIamPermissions" && r.Method == http.MethodPost:
			// The default-permissive IAM echo: the handler returns exactly the
			// requested permission strings and enforces nothing.
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidArgument", "malformed JSON body", 400)
			}
			if m != nil {
				nr.Params["body"] = m
			}
			nr.Action = "BillingAccountTestIamPermissions"
			return nr, nil
		case custom == "getIamPolicy" || custom == "setIamPolicy":
			return nil, model.NewProviderError("Unimplemented",
				"billingAccounts."+custom+": billing-account IAM policy is not modelled (plan CB5)", 501)
		}
	}
	family := billingAccountsFamily(seg, custom, r.Method)
	return nil, model.NewProviderError("Unimplemented",
		family+": the billing-account catalog is not modelled (plan CB5)", 501)
}

// billingAccountsFamily names the operation family an unmodelled
// /v1/billingAccounts path addresses ("billingAccounts.create",
// "billingAccounts.subAccounts.move", ...), for the 501 message. Account and
// sub-account ids are dropped: the family, not the instance, is what is refused.
func billingAccountsFamily(seg []string, custom, method string) string {
	parts := []string{"billingAccounts"}
	// seg[0] is "v1", seg[1] "billingAccounts", seg[2] the account (or the
	// collection's ':verb' pseudo-segment); a sub-collection follows it.
	rest := seg[2:]
	if len(rest) > 0 {
		rest = rest[1:]
	}
	for i, s := range rest {
		id, verb := splitVerb(s)
		if verb != "" && i == len(rest)-1 {
			custom = verb
			s = id
		}
		if i == 1 || s == "" {
			continue // the sub-account id
		}
		parts = append(parts, s)
	}
	if custom == "" {
		custom = methodFamily(method)
	}
	if custom != "" {
		parts = append(parts, custom)
	}
	return strings.Join(parts, ".")
}

// methodFamily names a collection-level operation from its HTTP method, for a
// path shape that carries no custom verb (billingAccounts.create, .../{id}.patch).
func methodFamily(method string) string {
	switch method {
	case http.MethodPost:
		return "create"
	case http.MethodPatch:
		return "patch"
	case http.MethodDelete:
		return "delete"
	case http.MethodPut:
		return "update"
	}
	return ""
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
