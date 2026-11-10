package serviceusage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"

	core "jaiscloud/internal/gcp/service/serviceusage"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// numberResolver maps a project number to a project ID, standing in for the
// Resource Manager registry.
type numberResolver map[string]string

func (r numberResolver) ProjectByNumber(_ context.Context, number string) (string, bool, error) {
	id, ok := r[number]
	return id, ok, nil
}

func newNumberProvider(r core.ProjectResolver) *Provider {
	opts := []core.Option{}
	if r != nil {
		opts = append(opts, core.WithProjectResolver(r))
	}
	return NewProvider(core.NewService(store.NewMemoryResourceStore(), opts...), "default-proj")
}

// serveBody drives the decoded request through the provider route and returns
// the encoded response/error exactly as the gateway would.
func serveBody(t *testing.T, p *Provider, method, path, body string) (int, []byte) {
	t.Helper()
	var raw []byte
	if body != "" {
		raw = []byte(body)
	}
	codec := NewCodec()
	nr, err := codec.Decode(httptest.NewRequest(method, path, strings.NewReader(body)), raw)
	if err != nil {
		t.Fatalf("decode %s %s: %v", method, path, err)
	}
	h, ok := p.Routes()["ServiceUsage."+nr.Action]
	if !ok {
		t.Fatalf("no handler for %s", nr.Action)
	}
	resp, err := h(context.Background(), nr)
	if err != nil {
		var pe *model.ProviderError
		if !errors.As(err, &pe) {
			t.Fatalf("non-provider error: %v", err)
		}
		status, _, out := codec.EncodeError(nr, pe)
		return status, out
	}
	status, _, out := codec.Encode(nr, resp)
	return status, out
}

func serve(t *testing.T, p *Provider, method, path string) (int, []byte) {
	t.Helper()
	return serveBody(t, p, method, path, "")
}

// TestRESTUnknownProjectNumber is the raw REST test for the provider's dummy
// project: the response is the documented 403 envelope, and the official
// apiary client turns it into the exact *googleapi.Error string the Terraform
// Google provider matches before it creates a project with
// auto_create_network = false (TF4-1-1).
func TestRESTUnknownProjectNumber(t *testing.T) {
	p := newNumberProvider(numberResolver{})
	status, body := serve(t, p, http.MethodGet,
		"/v1/projects/00000000000/services/serviceusage.googleapis.com")

	if status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body=%s)", status, body)
	}
	var env struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
			Errors  []struct {
				Domain  string `json:"domain"`
				Reason  string `json:"reason"`
				Message string `json:"message"`
			} `json:"errors"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	const message = "Project '00000000000' not found or permission denied."
	if env.Error.Code != 403 || env.Error.Status != "PERMISSION_DENIED" || env.Error.Message != message {
		t.Fatalf("envelope = %+v", env.Error)
	}
	if len(env.Error.Errors) != 1 ||
		env.Error.Errors[0].Domain != "global" ||
		env.Error.Errors[0].Reason != "forbidden" ||
		env.Error.Errors[0].Message != message {
		t.Fatalf("legacy errors[] = %+v", env.Error.Errors)
	}

	// The client-visible string: googleapi's Error() appends the single
	// errors[].reason when it equals the message, which is what the provider
	// compares against.
	apiErr := googleapi.CheckResponse(&http.Response{
		StatusCode: status,
		Header:     http.Header{},
		Body:       io.NopCloser(bytes.NewReader(body)),
	})
	if apiErr == nil {
		t.Fatal("apiary client accepted the unknown project")
	}
	const want = "googleapi: Error 403: Project '00000000000' not found or permission denied., forbidden"
	if apiErr.Error() != want {
		t.Fatalf("apiary error = %q, want %q", apiErr, want)
	}
}

// TestRESTUnknownProjectNumberVariants covers the other verbs and the
// batchGet names form on the wire.
func TestRESTUnknownProjectNumberVariants(t *testing.T) {
	p := newNumberProvider(numberResolver{})
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, "/v1/projects/00000000000/services", ""},
		{http.MethodPost, "/v1/projects/00000000000/services/run.googleapis.com:enable", ""},
		{http.MethodPost, "/v1/projects/00000000000/services/run.googleapis.com:disable", ""},
		{http.MethodPost, "/v1/projects/00000000000/services:batchEnable", `{"serviceIds":["run.googleapis.com"]}`},
		{http.MethodGet, "/v1/projects/00000000000/services:batchGet?names=projects%2F00000000000%2Fservices%2Frun.googleapis.com", ""},
	} {
		status, body := serveBody(t, p, tc.method, tc.path, tc.body)
		if status != http.StatusForbidden {
			t.Fatalf("%s %s: status = %d, want 403 (body=%s)", tc.method, tc.path, status, body)
		}
	}
}

// TestRESTProjectIDAndKnownNumberStillServed is the control: a project ID (the
// fixture/module form) and a known project's number both answer 200.
func TestRESTProjectIDAndKnownNumberStillServed(t *testing.T) {
	const number = "123456789012"
	p := newNumberProvider(numberResolver{number: "proj"})

	var svc struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}

	// A project id is synthesized on read.
	status, body := serve(t, p, http.MethodGet, "/v1/projects/proj/services/run.googleapis.com")
	if status != http.StatusOK {
		t.Fatalf("project id: status = %d, want 200 (body=%s)", status, body)
	}
	if err := json.Unmarshal(body, &svc); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if svc.Name != "projects/proj/services/run.googleapis.com" || svc.State != "DISABLED" {
		t.Fatalf("project id service = %+v", svc)
	}

	// A known project number resolves to the project ID.
	status, body = serve(t, p, http.MethodGet, "/v1/projects/"+number+"/services/run.googleapis.com")
	if status != http.StatusOK {
		t.Fatalf("project number: status = %d, want 200 (body=%s)", status, body)
	}
	if err := json.Unmarshal(body, &svc); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if svc.Name != "projects/proj/services/run.googleapis.com" || svc.State != "DISABLED" {
		t.Fatalf("project number service = %+v", svc)
	}
}
