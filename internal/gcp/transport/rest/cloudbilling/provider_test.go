package cloudbilling

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	core "jaiscloud/internal/gcp/service/cloudbilling"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newTestProvider builds the codec + provider over an in-memory ResourceStore.
func newTestProvider() (*Codec, *Provider, store.ResourceStore) {
	resources := store.NewMemoryResourceStore()
	return NewCodec(), NewProvider(core.NewService(resources), "test"), resources
}

// call drives one body-less request through the codec and the provider's route
// table and encodes the response, so the assertions see the wire body a client
// would.
func call(t *testing.T, c *Codec, p *Provider, method, target string) (int, string) {
	t.Helper()
	return callBody(t, c, p, method, target, "")
}

// callBody is call with a JSON request body (the write surface).
func callBody(t *testing.T, c *Codec, p *Provider, method, target, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, target, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	nr, err := c.Decode(req, []byte(body))
	if err != nil {
		t.Fatalf("decode %s %s: %v", method, target, err)
	}
	h, ok := p.Routes()["CloudBilling."+nr.Action]
	if !ok {
		t.Fatalf("no route for action %q", nr.Action)
	}
	resp, err := h(context.Background(), nr)
	if err != nil {
		t.Fatalf("%s %s: %v", method, target, err)
	}
	status, _, out := c.Encode(nr, resp)
	return status, string(out)
}

// decodeErr runs the codec and requires it to fail with a ProviderError.
func decodeErr(t *testing.T, c *Codec, method, target, body string) *model.ProviderError {
	t.Helper()
	req, err := http.NewRequest(method, target, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	nr, err := c.Decode(req, []byte(body))
	if err == nil {
		t.Fatalf("%s %s decoded as %q, want an error", method, target, nr.Action)
	}
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("%s %s error %v is not a *model.ProviderError", method, target, err)
	}
	return perr
}

// handlerErr runs the decoded request's handler and requires it to fail,
// returning the status and encoded error envelope the adapter would send.
func handlerErr(t *testing.T, c *Codec, p *Provider, method, target, body string) (int, string, *model.ProviderError) {
	t.Helper()
	req, err := http.NewRequest(method, target, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	nr, err := c.Decode(req, []byte(body))
	if err != nil {
		t.Fatalf("decode %s %s: %v", method, target, err)
	}
	h, ok := p.Routes()["CloudBilling."+nr.Action]
	if !ok {
		t.Fatalf("no route for action %q", nr.Action)
	}
	_, err = h(context.Background(), nr)
	if err == nil {
		t.Fatalf("%s %s succeeded, want an error", method, target)
	}
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("%s %s error %v is not a *model.ProviderError", method, target, err)
	}
	status, _, out := c.EncodeError(nr, perr)
	return status, string(out), perr
}

// jsonMap decodes a wire body for field-level assertions.
func jsonMap(t *testing.T, body string) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode body %s: %v", body, err)
	}
	return got
}

// TestGetBillingInfoDefault locks the exact ProjectBillingInfo body served for a
// project with no billing association: the four Discovery fields, nothing else.
func TestGetBillingInfoDefault(t *testing.T) {
	c, p, _ := newTestProvider()

	status, body := call(t, c, p, http.MethodGet, "/v1/projects/p/billingInfo")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	want := map[string]any{
		"name":               "projects/p/billingInfo",
		"projectId":          "p",
		"billingAccountName": "",
		"billingEnabled":     false,
	}
	// DeepEqual also rejects an extra emulator-only field, so this pins the
	// whole schema (ProjectBillingInfo has exactly these four fields).
	if got := jsonMap(t, body); !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %v, want %v", got, want)
	}
}

// TestGetBillingInfoStored locks the read of a stored association: the account
// name is echoed verbatim and billingEnabled is derived from it (the field is
// output-only in the Discovery document).
func TestGetBillingInfoStored(t *testing.T) {
	c, p, resources := newTestProvider()

	data, err := json.Marshal(core.BillingInfo{ProjectID: "p", BillingAccountName: "billingAccounts/0X0X0X-0X0X0X-0X0X0X"})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	if err := resources.Upsert(context.Background(), "p", store.GlobalRegion, store.ResourceEntry{
		Type: "billingInfo", ID: "p", Data: data,
	}); err != nil {
		t.Fatalf("seed record: %v", err)
	}

	_, body := call(t, c, p, http.MethodGet, "/v1/projects/p/billingInfo")
	if got := jsonMap(t, body); got["billingAccountName"] != "billingAccounts/0X0X0X-0X0X0X-0X0X0X" || got["billingEnabled"] != true {
		t.Fatalf("body = %s, want the stored account with billingEnabled true", body)
	}
}

// TestGetBillingInfoIgnoresQueryParams locks that the standard apiary query
// parameters (alt/prettyPrint) do not change the decode or the body.
func TestGetBillingInfoIgnoresQueryParams(t *testing.T) {
	c, p, _ := newTestProvider()

	_, want := call(t, c, p, http.MethodGet, "/v1/projects/p/billingInfo")
	_, got := call(t, c, p, http.MethodGet, "/v1/projects/p/billingInfo?alt=json&prettyPrint=false")
	if got != want {
		t.Fatalf("body with query params = %s, want %s", got, want)
	}
}

// TestUpdateBillingInfoRoundTrip is the provider's write path: PUT stores the
// full account resource name and the following GET reads back exactly what the
// write stored, with billingEnabled derived (Terraform's apply-then-read-back).
func TestUpdateBillingInfoRoundTrip(t *testing.T) {
	c, p, _ := newTestProvider()
	const account = "billingAccounts/0X0X0X-0X0X0X-0X0X0X"
	want := map[string]any{
		"name":               "projects/p/billingInfo",
		"projectId":          "p",
		"billingAccountName": account,
		"billingEnabled":     true,
	}

	status, body := callBody(t, c, p, http.MethodPut, "/v1/projects/p/billingInfo",
		`{"billingAccountName":"`+account+`"}`)
	if status != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200 (body %s)", status, body)
	}
	if got := jsonMap(t, body); !reflect.DeepEqual(got, want) {
		t.Fatalf("PUT body = %v, want %v", got, want)
	}

	status, body = call(t, c, p, http.MethodGet, "/v1/projects/p/billingInfo")
	if status != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", status)
	}
	if got := jsonMap(t, body); !reflect.DeepEqual(got, want) {
		t.Fatalf("GET after PUT body = %v, want %v", got, want)
	}
}

// TestUpdateBillingInfoClears locks the disable path: PUT with an empty
// billingAccountName clears the association, and the read returns the four-field
// no-account default with billingEnabled false.
func TestUpdateBillingInfoClears(t *testing.T) {
	c, p, _ := newTestProvider()
	const account = "billingAccounts/0X0X0X-0X0X0X-0X0X0X"
	if status, body := callBody(t, c, p, http.MethodPut, "/v1/projects/p/billingInfo",
		`{"billingAccountName":"`+account+`"}`); status != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200 (body %s)", status, body)
	}

	status, body := callBody(t, c, p, http.MethodPut, "/v1/projects/p/billingInfo",
		`{"billingAccountName":""}`)
	if status != http.StatusOK {
		t.Fatalf("clearing PUT status = %d, want 200 (body %s)", status, body)
	}
	want := map[string]any{
		"name":               "projects/p/billingInfo",
		"projectId":          "p",
		"billingAccountName": "",
		"billingEnabled":     false,
	}
	if got := jsonMap(t, body); !reflect.DeepEqual(got, want) {
		t.Fatalf("clearing PUT body = %v, want %v", got, want)
	}
	if got := jsonMap(t, billingInfoBody(t, c, p)); !reflect.DeepEqual(got, want) {
		t.Fatalf("GET after clearing = %v, want %v", got, want)
	}
}

// billingInfoBody GETs the project singleton and returns its body.
func billingInfoBody(t *testing.T, c *Codec, p *Provider) string {
	t.Helper()
	status, body := call(t, c, p, http.MethodGet, "/v1/projects/p/billingInfo")
	if status != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", status)
	}
	return body
}

// TestUpdateBillingInfoRejectsNonResourceName locks the D4 form check: the body
// carries the full billingAccounts/{id} resource name, so anything else is
// InvalidArgument 400 rather than a stored account no client could resolve.
func TestUpdateBillingInfoRejectsNonResourceName(t *testing.T) {
	c, p, _ := newTestProvider()
	for _, account := range []string{
		"projects/x",              // foreign resource name
		"0X0X0X-0X0X0X-0X0X0X",    // the bare id (the provider prepends the prefix)
		"billingAccounts/",        // the prefix with no id
		"billingAccounts",         // no separator
		" BillingAccounts/0X0X0X", // leading whitespace
	} {
		status, body, perr := handlerErr(t, c, p, http.MethodPut, "/v1/projects/p/billingInfo",
			`{"billingAccountName":`+mustJSON(t, account)+`}`)
		if status != http.StatusBadRequest || perr.Code != "InvalidArgument" {
			t.Errorf("PUT account %q = %d/%q, want 400/InvalidArgument (body %s)",
				account, status, perr.Code, body)
		}
		if perr.Message == "" {
			t.Errorf("PUT account %q: empty error message", account)
		}
	}
	// A rejected write must not touch the stored association (none was stored).
	want := map[string]any{
		"name":               "projects/p/billingInfo",
		"projectId":          "p",
		"billingAccountName": "",
		"billingEnabled":     false,
	}
	if got := jsonMap(t, billingInfoBody(t, c, p)); !reflect.DeepEqual(got, want) {
		t.Fatalf("GET body after rejected writes = %v, want the no-account default", got)
	}
	// A malformed JSON body is InvalidArgument 400 at decode time.
	if perr := decodeErr(t, c, http.MethodPut, "/v1/projects/p/billingInfo", `{"billingAccountName":`); perr.HTTPStatus != http.StatusBadRequest || perr.Code != "InvalidArgument" {
		t.Fatalf("malformed body = %d/%q, want 400/InvalidArgument", perr.HTTPStatus, perr.Code)
	}
}

// mustJSON encodes a value as a JSON literal for embedding in a request body.
func mustJSON(t *testing.T, v any) string {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %v: %v", v, err)
	}
	return string(out)
}

// TestBillingAccountsList locks the catalog answer: a schema-valid empty list
// (no account lifecycle exists, plan CB5), not a fabricated account.
func TestBillingAccountsList(t *testing.T) {
	c, p, _ := newTestProvider()

	status, body := call(t, c, p, http.MethodGet, "/v1/billingAccounts")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if want := `{"billingAccounts":[]}`; body != want {
		t.Fatalf("body = %s, want %s", body, want)
	}
}

// TestBillingAccountsGetNotFound locks billingAccounts.get: no catalog means
// every id is NotFound 404 with a GCP error envelope.
func TestBillingAccountsGetNotFound(t *testing.T) {
	c, p, _ := newTestProvider()

	status, body, perr := handlerErr(t, c, p, http.MethodGet, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X", "")
	if status != http.StatusNotFound || perr.Code != "NotFound" {
		t.Fatalf("status/code = %d/%q, want 404/NotFound", status, perr.Code)
	}
	env := jsonMap(t, body)
	errObj, ok := env["error"].(map[string]any)
	if !ok {
		t.Fatalf("body %s has no error envelope", body)
	}
	if errObj["code"] != float64(http.StatusNotFound) || errObj["status"] != "NOT_FOUND" {
		t.Fatalf("error envelope = %v, want code 404 status NOT_FOUND", errObj)
	}
}

// TestBillingAccountTestIamPermissionsEcho locks the default-permissive IAM
// echo: exactly the requested permission strings come back, in order, and an
// absent or empty request echoes an empty list. It enforces nothing.
func TestBillingAccountTestIamPermissionsEcho(t *testing.T) {
	c, p, _ := newTestProvider()
	const base = "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X:testIamPermissions"
	want := `{"permissions":["billing.resourceAssociations.create","billing.resourceAssociations.delete"]}`

	status, body := callBody(t, c, p, http.MethodPost, base,
		`{"permissions":["billing.resourceAssociations.create","billing.resourceAssociations.delete"]}`)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", status, body)
	}
	if body != want {
		t.Fatalf("body = %s, want %s", body, want)
	}

	for _, req := range []string{"", "{}", `{"permissions":[]}`} {
		_, body := callBody(t, c, p, http.MethodPost, base, req)
		if want := `{"permissions":[]}`; body != want {
			t.Errorf("POST %s body = %s, want %s", req, body, want)
		}
	}
	// A malformed body is InvalidArgument 400, not a silent empty echo.
	if perr := decodeErr(t, c, http.MethodPost, base, `{"permissions":[`); perr.HTTPStatus != http.StatusBadRequest || perr.Code != "InvalidArgument" {
		t.Fatalf("malformed body = %d/%q, want 400/InvalidArgument", perr.HTTPStatus, perr.Code)
	}
}

// TestBillingAccountIamPolicyUnimplemented locks D6: only the permission echo is
// served. The IAM policy methods are registered routes that fail loud with
// Unimplemented 501 naming the method, never a fabricated policy.
func TestBillingAccountIamPolicyUnimplemented(t *testing.T) {
	c, p, _ := newTestProvider()
	for _, tc := range []struct{ method, verb string }{
		{http.MethodGet, "getIamPolicy"},
		{http.MethodPost, "setIamPolicy"},
	} {
		target := "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X:" + tc.verb
		status, _, perr := handlerErr(t, c, p, tc.method, target, `{}`)
		if status != http.StatusNotImplemented || perr.Code != "Unimplemented" {
			t.Errorf("%s = %d/%q, want 501/Unimplemented", target, status, perr.Code)
		}
		if !strings.Contains(perr.Message, tc.verb) || !strings.Contains(perr.Message, "plan CB5") {
			t.Errorf("%s message %q does not name the method and the plan", target, perr.Message)
		}
	}
}

// TestBillingAccountsCatalogUnimplemented locks the fail-loud rule for the rest
// of the billing-account surface (plan CB5): each catalog path decodes to its
// own action and the registered route refuses with Unimplemented 501 naming the
// Discovery method, not an empty success body and not an unknown-service 404.
func TestBillingAccountsCatalogUnimplemented(t *testing.T) {
	c, p, _ := newTestProvider()
	cases := []struct{ method, target, methodID string }{
		{http.MethodPost, "/v1/billingAccounts", "billingAccounts.create"},
		{http.MethodPatch, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X", "billingAccounts.patch"},
		{http.MethodPost, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X:move", "billingAccounts.move"},
		{http.MethodGet, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X/subAccounts", "billingAccounts.subAccounts.list"},
		{http.MethodPost, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X/subAccounts", "billingAccounts.subAccounts.create"},
		{http.MethodGet, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X/projects", "billingAccounts.projects.list"},
		{http.MethodGet, "/v1/organizations/123456789012/billingAccounts", "organizations.billingAccounts.list"},
		{http.MethodPost, "/v1/organizations/123456789012/billingAccounts", "organizations.billingAccounts.create"},
		{http.MethodGet, "/v1/organizations/123456789012/billingAccounts/0X0X0X-0X0X0X-0X0X0X:move", "organizations.billingAccounts.move"},
		{http.MethodGet, "/v1/services", "services.list"},
		{http.MethodGet, "/v1/services/6F81-5844-456A/skus", "services.skus.list"},
	}
	for _, tc := range cases {
		status, _, perr := handlerErr(t, c, p, tc.method, tc.target, `{}`)
		if status != http.StatusNotImplemented || perr.Code != "Unimplemented" {
			t.Errorf("%s %s = %d/%q, want 501/Unimplemented", tc.method, tc.target, status, perr.Code)
			continue
		}
		if !strings.Contains(perr.Message, tc.methodID) {
			t.Errorf("%s %s message %q does not name the method %q", tc.method, tc.target, perr.Message, tc.methodID)
		}
	}
}

// TestCatalogPathDecoding locks the (service, action) each catalog path decodes
// to, so the routing matrix and the fidelity cells key off the documented
// method rather than a guess.
func TestCatalogPathDecoding(t *testing.T) {
	c := NewCodec()
	cases := []struct{ method, target, action string }{
		{http.MethodGet, "/v1/billingAccounts", "BillingAccountsList"},
		{http.MethodPost, "/v1/billingAccounts", "BillingAccountsCreate"},
		{http.MethodGet, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X", "BillingAccountsGet"},
		{http.MethodPatch, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X", "BillingAccountsPatch"},
		{http.MethodGet, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X/subAccounts", "BillingAccountsSubAccountsList"},
		{http.MethodGet, "/v1/billingAccounts/0X0X0X-0X0X0X-0X0X0X/projects", "BillingAccountsProjectsList"},
		{http.MethodGet, "/v1/organizations/123456789012/billingAccounts", "OrganizationsBillingAccountsList"},
		{http.MethodPost, "/v1/organizations/123456789012/billingAccounts", "OrganizationsBillingAccountsCreate"},
		{http.MethodGet, "/v1/organizations/123456789012/billingAccounts/0X0X0X:move", "OrganizationsBillingAccountsMove"},
		// The live document declares the organization move as GET; an
		// AIP-151-style POST decodes to the same refusal.
		{http.MethodPost, "/v1/organizations/123456789012/billingAccounts/0X0X0X:move", "OrganizationsBillingAccountsMove"},
		{http.MethodGet, "/v1/services", "ServicesList"},
		{http.MethodGet, "/v1/services/6F81-5844-456A/skus", "ServicesSkusList"},
	}
	for _, tc := range cases {
		req, err := http.NewRequest(tc.method, tc.target, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		nr, err := c.Decode(req, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.target, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s action = %q, want %q", tc.method, tc.target, nr.Action, tc.action)
		}
		if nr.Service != ServiceName {
			t.Errorf("%s %s service = %q, want %q", tc.method, tc.target, nr.Service, ServiceName)
		}
	}

	// Shapes the document does not declare stay UnsupportedOperation 404 — the
	// codec must not widen a claim beyond the published surface.
	for _, tc := range []struct{ method, target string }{
		{http.MethodDelete, "/v1/billingAccounts"},
		{http.MethodGet, "/v1/billingAccounts/0X0X0X/subAccounts/sub1"},
		{http.MethodPut, "/v1/services/6F81-5844-456A/skus"},
		{http.MethodGet, "/v1/services/6F81-5844-456A"},
		{http.MethodGet, "/v1/organizations/123456789012/projects"},
	} {
		perr := decodeErr(t, c, tc.method, tc.target, "")
		if perr.HTTPStatus != http.StatusNotFound || perr.Code != "UnsupportedOperation" {
			t.Errorf("%s %s = %d/%q, want 404/UnsupportedOperation", tc.method, tc.target, perr.HTTPStatus, perr.Code)
		}
	}
}

// TestUnsupportedOperations locks the read/write scope of the singleton: every
// method other than GET and PUT on …/billingInfo, and every other path, is a 404
// UnsupportedOperation rather than a silent success.
func TestUnsupportedOperations(t *testing.T) {
	c, _, _ := newTestProvider()

	cases := []struct{ method, target string }{
		{http.MethodPost, "/v1/projects/p/billingInfo"},
		{http.MethodDelete, "/v1/projects/p/billingInfo"},
		{http.MethodPatch, "/v1/projects/p/billingInfo"},
		// The singleton has no trailing resource id.
		{http.MethodGet, "/v1/projects/p/billingInfo/extra"},
		// The segment match is case-sensitive.
		{http.MethodGet, "/v1/projects/p/billinginfo"},
		// Other paths are not part of the association surface.
		{http.MethodGet, "/v1/projects/p"},
		{http.MethodGet, "/v1/projects"},
		{http.MethodGet, "/v1/projects/p/services"},
	}
	for _, tc := range cases {
		perr := decodeErr(t, c, tc.method, tc.target, "")
		if perr.HTTPStatus != http.StatusNotFound || perr.Code != "UnsupportedOperation" {
			t.Errorf("%s %s = %d/%q, want 404/UnsupportedOperation", tc.method, tc.target, perr.HTTPStatus, perr.Code)
		}
	}
}
