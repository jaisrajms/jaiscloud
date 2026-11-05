package storage

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	gcpadapter "jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
)

// projectServiceAccountEmailRE pins the real GCS service-agent address shape:
// service-<12-digit project number>@gs-project-accounts.iam.gserviceaccount.com.
var projectServiceAccountEmailRE = regexp.MustCompile(
	`^service-\d{12}@gs-project-accounts\.iam\.gserviceaccount\.com$`)

// TestProjectsServiceAccountGet covers projects.serviceAccount.get, the call
// Terraform's google_storage_project_service_account data source makes at plan
// time: the handler answers 200 with the storage#serviceAccount body, the email
// is stable per project, differs across projects, and the GCS JSON codec
// serialises it as application/json.
func TestProjectsServiceAccountGet(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if p.Routes()["Storage.ProjectsServiceAccountGet"] == nil {
		t.Fatal("Storage.ProjectsServiceAccountGet is not registered in Routes()")
	}

	nr := &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{"project": "terra-proj"}}
	resp, err := p.ProjectsServiceAccountGet(ctx, nr)
	if err != nil {
		t.Fatalf("projects.serviceAccount.get: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Errorf("status = %d, want 200", resp.HTTPStatus)
	}
	if resp.Data["kind"] != "storage#serviceAccount" {
		t.Errorf("kind = %v, want storage#serviceAccount", resp.Data["kind"])
	}
	email, _ := resp.Data["email_address"].(string)
	if !projectServiceAccountEmailRE.MatchString(email) {
		t.Fatalf("email_address = %q, want service-<12 digits>@gs-project-accounts.iam.gserviceaccount.com", email)
	}
	// The number segment is the synthesized project number the emulator already
	// reports for storage#bucket.projectNumber.
	if want := "service-" + resource.ProjectNumber("terra-proj") + "@gs-project-accounts.iam.gserviceaccount.com"; email != want {
		t.Errorf("email_address = %q, want %q", email, want)
	}

	// Stable for the same project, different across projects.
	again, err := p.ProjectsServiceAccountGet(ctx, nr)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if again.Data["email_address"] != email {
		t.Errorf("email not stable: %v then %v", email, again.Data["email_address"])
	}
	other, err := p.ProjectsServiceAccountGet(ctx, &model.NormalizedRequest{
		AccountID: "proj", Params: map[string]any{"project": "other-proj"},
	})
	if err != nil {
		t.Fatalf("other project: %v", err)
	}
	if other.Data["email_address"] == email {
		t.Errorf("email identical across projects: %v", email)
	}

	// Without the path param (in-process dispatch) the identity-derived
	// AccountID is the fallback project.
	fallback, err := p.ProjectsServiceAccountGet(ctx, &model.NormalizedRequest{AccountID: "terra-proj", Params: map[string]any{}})
	if err != nil {
		t.Fatalf("account fallback: %v", err)
	}
	if fallback.Data["email_address"] != email {
		t.Errorf("fallback email = %v, want %v", fallback.Data["email_address"], email)
	}

	// The GCS JSON codec owns the wire content type.
	codec := &gcpadapter.GCSCodec{}
	status, headers, body := codec.Encode(nr, resp)
	if status != 200 {
		t.Errorf("encoded status = %d, want 200", status)
	}
	if ct := headers.Get("Content-Type"); ct != "application/json; charset=UTF-8" {
		t.Errorf("content-type = %q, want application/json; charset=UTF-8", ct)
	}
	var wire map[string]any
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatalf("decode encoded body: %v (body=%q)", err, body)
	}
	if wire["kind"] != "storage#serviceAccount" || wire["email_address"] != email {
		t.Errorf("encoded body = %v, want kind storage#serviceAccount and email %v", wire, email)
	}
}
