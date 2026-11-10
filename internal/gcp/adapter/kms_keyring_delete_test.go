package gcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	restkms "jaiscloud/internal/gcp/transport/rest/kms"
	"jaiscloud/internal/store"
)

// TestKeyRingDeleteRoutedAndServed is the raw-REST pin for TF5-4: the Cloud KMS
// Discovery documents projects.locations.keyRings.delete (DELETE v1/{+name}, an
// LRO), so a client that sends it must reach a registered handler. The request
// goes through the same JSON codec the gateway uses and is dispatched through
// the provider's own route table — before the fix the codec derived no action
// and answered 501 UnsupportedOperation, and no route existed either.
func TestKeyRingDeleteRoutedAndServed(t *testing.T) {
	ctx := context.Background()
	p := restkms.New(kmscore.NewService(kmsstore.NewMemoryStore(), store.NewMemoryResourceStore(), "proj"))
	codec := &JSONCodec{Service: "kms"}

	// dispatch decodes one request the way the gateway does and runs the
	// provider's registered handler for the derived action.
	dispatch := func(method, path string, body []byte) (*http.Response, map[string]any) {
		t.Helper()
		r := httptest.NewRequest(method, path, nil)
		nr, err := codec.Decode(r, body)
		if err != nil {
			t.Fatalf("%s %s: decode: %v", method, path, err)
		}
		nr.AccountID = "proj"
		handler, ok := p.Routes()["KMS."+nr.Action]
		if !ok {
			t.Fatalf("%s %s: derived action %q is not registered in the provider's route table", method, path, nr.Action)
		}
		resp, err := handler(ctx, nr)
		if err != nil {
			t.Fatalf("%s %s: handler: %v", method, path, err)
		}
		return &http.Response{}, resp.Data
	}

	// Create the ring through the same surface, then delete it.
	_, _ = dispatch(http.MethodPost, "/v1/projects/proj/locations/global/keyRings?keyRingId=kr", []byte(`{}`))

	_, del := dispatch(http.MethodDelete, "/v1/projects/proj/locations/global/keyRings/kr", nil)
	if done, _ := del["done"].(bool); !done {
		t.Fatalf("keyRings.delete response = %v, want a done Operation", del)
	}
	if name, _ := del["name"].(string); name != "projects/proj/locations/global/keyRings/kr/operations/delete" {
		t.Fatalf("keyRings.delete operation name = %q", name)
	}

	// The ring is gone: a raw GET now answers 404.
	r := httptest.NewRequest(http.MethodGet, "/v1/projects/proj/locations/global/keyRings/kr", nil)
	nr, err := codec.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode get: %v", err)
	}
	nr.AccountID = "proj"
	handler := p.Routes()["KMS."+nr.Action]
	if handler == nil {
		t.Fatalf("KMS.%s is not registered", nr.Action)
	}
	if _, err := handler(ctx, nr); err == nil {
		t.Fatal("GET on the deleted key ring succeeded, want NOT_FOUND")
	}
}
