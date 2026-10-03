//go:build gcp_differential

package gcpdifferential

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

// request performs one authenticated/unauthenticated call as the target and
// returns the HTTP status and raw body. It is used by setup/cleanup helpers
// that live outside the captured scenario list.
func (t *Target) request(method, service, path, body, contentType string) (int, []byte, error) {
	var rd io.Reader
	if body != "" {
		rd = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, t.URLFor(service, path), rd)
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
	}
	if t.Token != "" {
		req.Header.Set("Authorization", "Bearer "+t.Token)
	}
	resp, err := t.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, b, nil
}

// EnsureKMS makes the fixed, reusable KMS keyring and crypto key exist. It is a
// precondition for the KMS scenarios in both record and replay mode. Existing
// resources (409 ALREADY_EXISTS) are fine.
//
// GCP cannot delete crypto keys (only schedule destruction, ~30 days) and a
// keyring cannot be deleted while it holds keys, so these two fixed resources
// are deliberately reused and never destroyed — see the package docs and the
// report's "out of scope" note.
func (t *Target) EnsureKMS() error {
	base := "/v1/projects/" + t.Project + "/locations/global/keyRings"
	ring := base + "/" + FixedKMSKeyRing

	steps := []struct {
		desc, method, path, body string
	}{
		{"keyring", http.MethodPost, base + "?keyRingId=" + FixedKMSKeyRing, "{}"},
		{"cryptokey", http.MethodPost, ring + "/cryptoKeys?cryptoKeyId=" + FixedKMSCryptoKey, `{"purpose":"ENCRYPT_DECRYPT"}`},
	}
	for _, s := range steps {
		status, body, err := t.request(s.method, "kms", s.path, s.body, "application/json")
		if err != nil {
			return fmt.Errorf("ensure kms %s: %w", s.desc, err)
		}
		switch status {
		case http.StatusOK, http.StatusConflict:
			// created or already present
		default:
			// Real GCP returns 400 with ALREADY_EXISTS in some paths; treat a
			// body that names ALREADY_EXISTS as success.
			if bytes.Contains(body, []byte("ALREADY_EXISTS")) || bytes.Contains(body, []byte("already exists")) {
				break
			}
			return fmt.Errorf("ensure kms %s: status %d: %s", s.desc, status, trimBody(body))
		}
	}
	return nil
}

// Cleanup deletes every resource the scenario set creates on the target so a
// capture leaves nothing behind. It is best-effort and idempotent: runs after
// both success and failure, and 404/missing resources are ignored. KMS is
// intentionally excluded (non-deletable; reused fixed names).
func (t *Target) Cleanup() []string {
	n := t.Names
	type del struct {
		service, desc, method, path, body string
	}
	ops := []del{
		{"bigquery", "dataset", http.MethodDelete, "/bigquery/v2/projects/" + t.Project + "/datasets/" + n.DS + "?deleteContents=true", ""},
		{"storage", "object", http.MethodDelete, "/storage/v1/b/" + n.Bucket + "/o/hello.txt", ""},
		{"storage", "bucket", http.MethodDelete, "/storage/v1/b/" + n.Bucket, ""},
		{"pubsub", "subscription", http.MethodDelete, "/v1/projects/" + t.Project + "/subscriptions/" + n.Sub, ""},
		{"pubsub", "topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.Topic, ""},
		{"secretmanager", "secret", http.MethodDelete, "/v1/projects/" + t.Project + "/secrets/" + n.Secret, ""},
		{"iam", "service account", http.MethodDelete, "/v1/projects/" + t.Project + "/serviceAccounts/" + n.ServiceAccount + "@" + t.Project + ".iam.gserviceaccount.com", ""},
		{"firestore", "document", http.MethodDelete, "/v1/projects/" + t.Project + "/databases/(default)/documents/" + n.FSCollection + "/" + n.FSDoc, ""},
		{"workflows", "workflow", http.MethodDelete, "/v1/projects/" + t.Project + "/locations/us-central1/workflows/" + n.Workflow, ""},
		// Cloud DNS mutations go through changes.create; deleting the record
		// set first leaves the zone empty, which real GCP requires before the
		// zone itself can be deleted.
		{"dns", "record set", http.MethodPost,
			"/dns/v1/projects/" + t.Project + "/managedZones/" + n.DNSZone + "/changes",
			fmt.Sprintf(`{"deletions":[{"name":%q,"type":"A","ttl":300,"rrdatas":["192.0.2.1"]}]}`, n.DNSRRSet)},
		{"dns", "managed zone", http.MethodDelete, "/dns/v1/projects/" + t.Project + "/managedZones/" + n.DNSZone, ""},
	}
	var log []string
	for _, op := range ops {
		status, _, err := t.request(op.method, op.service, op.path, op.body, "application/json")
		if err != nil {
			log = append(log, fmt.Sprintf("cleanup %s/%s: error: %v", op.service, op.desc, err))
			continue
		}
		log = append(log, fmt.Sprintf("cleanup %s/%s: HTTP %d", op.service, op.desc, status))
	}
	return log
}

// VerifyAbsent reads back every resource the capture creates and returns a
// human-readable line with the observed status. It is the "cleanup proof" used
// by the record workflow: a fully cleaned target reports 404 for all of them.
func (t *Target) VerifyAbsent() []string {
	n := t.Names
	checks := []struct {
		service, desc, path string
	}{
		{"storage", "bucket", "/storage/v1/b/" + n.Bucket},
		{"pubsub", "topic", "/v1/projects/" + t.Project + "/topics/" + n.Topic},
		{"pubsub", "subscription", "/v1/projects/" + t.Project + "/subscriptions/" + n.Sub},
		{"secretmanager", "secret", "/v1/projects/" + t.Project + "/secrets/" + n.Secret},
		{"bigquery", "dataset", "/bigquery/v2/projects/" + t.Project + "/datasets/" + n.DS},
		{"iam", "service account", "/v1/projects/" + t.Project + "/serviceAccounts/" + n.ServiceAccount + "@" + t.Project + ".iam.gserviceaccount.com"},
		{"firestore", "document", "/v1/projects/" + t.Project + "/databases/(default)/documents/" + n.FSCollection + "/" + n.FSDoc},
		{"dns", "managed zone", "/dns/v1/projects/" + t.Project + "/managedZones/" + n.DNSZone},
		{"workflows", "workflow", "/v1/projects/" + t.Project + "/locations/us-central1/workflows/" + n.Workflow},
	}
	var log []string
	for _, c := range checks {
		status, _, err := t.request(http.MethodGet, c.service, c.path, "", "")
		if err != nil {
			log = append(log, fmt.Sprintf("verify %s/%s: error: %v", c.service, c.desc, err))
			continue
		}
		log = append(log, fmt.Sprintf("verify %s/%s: HTTP %d", c.service, c.desc, status))
	}
	return log
}

func trimBody(b []byte) string {
	const max = 200
	s := string(bytes.TrimSpace(b))
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}
