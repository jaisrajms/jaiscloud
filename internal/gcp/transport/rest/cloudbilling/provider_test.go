package cloudbilling

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
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

// call drives one request through the codec and the provider's route table and
// encodes the response, so the assertions see the wire body a client would.
func call(t *testing.T, c *Codec, p *Provider, method, target string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, target, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	nr, err := c.Decode(req, nil)
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
	status, _, body := c.Encode(nr, resp)
	return status, string(body)
}

// TestGetBillingInfoDefault locks the exact ProjectBillingInfo body served for a
// project with no billing association: the four Discovery fields, nothing else.
func TestGetBillingInfoDefault(t *testing.T) {
	c, p, _ := newTestProvider()

	status, body := call(t, c, p, http.MethodGet, "/v1/projects/p/billingInfo")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode body %s: %v", body, err)
	}
	want := map[string]any{
		"name":               "projects/p/billingInfo",
		"projectId":          "p",
		"billingAccountName": "",
		"billingEnabled":     false,
	}
	// DeepEqual also rejects an extra emulator-only field, so this pins the
	// whole schema (ProjectBillingInfo has exactly these four fields).
	if !reflect.DeepEqual(got, want) {
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
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("decode body %s: %v", body, err)
	}
	if got["billingAccountName"] != "billingAccounts/0X0X0X-0X0X0X-0X0X0X" {
		t.Errorf("billingAccountName = %v", got["billingAccountName"])
	}
	if got["billingEnabled"] != true {
		t.Errorf("billingEnabled = %v, want true", got["billingEnabled"])
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

// TestUnsupportedOperations locks the read-only scope of this session: every
// method other than GET on the singleton, and every other path, is a 404
// UnsupportedOperation rather than a silent success.
func TestUnsupportedOperations(t *testing.T) {
	c, _, _ := newTestProvider()

	cases := []struct{ method, target string }{
		// The write half (updateBillingInfo) is not served yet.
		{http.MethodPut, "/v1/projects/p/billingInfo"},
		{http.MethodPost, "/v1/projects/p/billingInfo"},
		{http.MethodDelete, "/v1/projects/p/billingInfo"},
		// The singleton has no trailing resource id.
		{http.MethodGet, "/v1/projects/p/billingInfo/extra"},
		// The segment match is case-sensitive.
		{http.MethodGet, "/v1/projects/p/billinginfo"},
		// Other paths are not part of the association surface.
		{http.MethodGet, "/v1/projects/p"},
		{http.MethodGet, "/v1/projects"},
		{http.MethodGet, "/v1/billingAccounts"},
		{http.MethodGet, "/v1/projects/p/services"},
	}
	for _, tc := range cases {
		req, err := http.NewRequest(tc.method, tc.target, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		_, err = c.Decode(req, nil)
		if err == nil {
			t.Errorf("%s %s decoded without an error", tc.method, tc.target)
			continue
		}
		perr, ok := err.(*model.ProviderError)
		if !ok {
			t.Errorf("%s %s error %v is not a *model.ProviderError", tc.method, tc.target, err)
			continue
		}
		if perr.HTTPStatus != http.StatusNotFound || perr.Code != "UnsupportedOperation" {
			t.Errorf("%s %s = %d/%q, want 404/UnsupportedOperation", tc.method, tc.target, perr.HTTPStatus, perr.Code)
		}
	}
}
