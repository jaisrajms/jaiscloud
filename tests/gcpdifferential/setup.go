//go:build gcp_differential

package gcpdifferential

import (
	"bytes"
	"encoding/json"
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
	usBase := "/v1/projects/" + t.Project + "/locations/us/keyRings"
	usRing := usBase + "/" + FixedKMSKeyRingUS

	steps := []struct {
		desc, method, path, body string
	}{
		{"keyring", http.MethodPost, base + "?keyRingId=" + FixedKMSKeyRing, "{}"},
		{"cryptokey", http.MethodPost, ring + "/cryptoKeys?cryptoKeyId=" + FixedKMSCryptoKey, `{"purpose":"ENCRYPT_DECRYPT"}`},
		// The CMEK GCS bucket needs a US-multiregion key; a `global` keyring is
		// rejected for a US bucket.
		{"us keyring", http.MethodPost, usBase + "?keyRingId=" + FixedKMSKeyRingUS, "{}"},
		{"us cryptokey", http.MethodPost, usRing + "/cryptoKeys?cryptoKeyId=" + FixedKMSCryptoKey, `{"purpose":"ENCRYPT_DECRYPT"}`},
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

	// GCS reads/writes a CMEK object through the Cloud Storage service agent,
	// which must hold cloudkms.cryptoKeyEncrypterDecrypter on the key. Real GCP
	// does not auto-grant it for an API-created bucket, so do it here. The
	// emulator has no project number and enforces no authz, so this is a no-op
	// in replay.
	if t.ProjectNumber != "" {
		sa := "service-" + t.ProjectNumber + "@gs-project-accounts.iam.gserviceaccount.com"
		policy := fmt.Sprintf(`{"policy":{"bindings":[{"role":"roles/cloudkms.cryptoKeyEncrypterDecrypter","members":["serviceAccount:%s"]}]}}`, sa)
		status, body, err := t.request(http.MethodPost, "kms",
			usRing+"/cryptoKeys/"+FixedKMSCryptoKey+":setIamPolicy", policy, "application/json")
		if err != nil {
			return fmt.Errorf("ensure kms us key iam: %w", err)
		}
		if status != http.StatusOK {
			return fmt.Errorf("ensure kms us key iam: status %d: %s", status, trimBody(body))
		}
	}
	return nil
}

// EnsureDLQForwarding grants the Pub/Sub service agent the roles real GCP
// requires before it will forward a message to a dead-letter topic: publisher
// on the dead-letter topics and subscriber on the source subscriptions. Real
// GCP does not grant these for API-created resources, so without them the
// dead-letter golden captures an empty forward. The grant is applied at the
// project level (inherited by every topic/subscription the recorder creates)
// with a read-modify-write of the existing policy, so unrelated bindings and
// conditions survive. It is idempotent and a no-op in replay (the emulator
// enforces no authz and carries no project number).
func (t *Target) EnsureDLQForwarding() error {
	if t.ProjectNumber == "" {
		return nil
	}
	sa := "serviceAccount:service-" + t.ProjectNumber + "@gcp-sa-pubsub.iam.gserviceaccount.com"
	status, body, err := t.request(http.MethodPost, "resourcemanager",
		"/v1/projects/"+t.Project+":getIamPolicy", "{}", "application/json")
	if err != nil {
		return fmt.Errorf("dlq forwarding iam get: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("dlq forwarding iam get: status %d: %s", status, trimBody(body))
	}
	var pol map[string]any
	if err := json.Unmarshal(body, &pol); err != nil {
		return fmt.Errorf("dlq forwarding iam get: parse: %w", err)
	}
	bindings, _ := pol["bindings"].([]any)

	// Only add a role if the service agent is not already a member of it.
	want := map[string]bool{"roles/pubsub.publisher": true, "roles/pubsub.subscriber": true}
	for _, b := range bindings {
		bm, ok := b.(map[string]any)
		if !ok {
			continue
		}
		role, _ := bm["role"].(string)
		if !want[role] {
			continue
		}
		if members, ok := bm["members"].([]any); ok {
			for _, m := range members {
				if s, _ := m.(string); s == sa {
					delete(want, role)
					break
				}
			}
		}
	}
	for role := range want {
		bindings = append(bindings, map[string]any{"role": role, "members": []any{sa}})
	}
	pol["bindings"] = bindings

	out, err := json.Marshal(map[string]any{"policy": pol})
	if err != nil {
		return fmt.Errorf("dlq forwarding iam set: encode: %w", err)
	}
	status, body, err = t.request(http.MethodPost, "resourcemanager",
		"/v1/projects/"+t.Project+":setIamPolicy", string(out), "application/json")
	if err != nil {
		return fmt.Errorf("dlq forwarding iam set: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("dlq forwarding iam set: status %d: %s", status, trimBody(body))
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
		// PSM1 messaging-semantics resources (subscriptions before topics).
		{"pubsub", "msg subscription", http.MethodDelete, "/v1/projects/" + t.Project + "/subscriptions/" + n.MsgSub, ""},
		{"pubsub", "eod subscription", http.MethodDelete, "/v1/projects/" + t.Project + "/subscriptions/" + n.EODSub, ""},
		{"pubsub", "order subscription", http.MethodDelete, "/v1/projects/" + t.Project + "/subscriptions/" + n.OrderSub, ""},
		{"pubsub", "redel subscription", http.MethodDelete, "/v1/projects/" + t.Project + "/subscriptions/" + n.RedelSub, ""},
		{"pubsub", "redel2 subscription", http.MethodDelete, "/v1/projects/" + t.Project + "/subscriptions/" + n.Redel2Sub, ""},
		{"pubsub", "msg topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.MsgTopic, ""},
		{"pubsub", "eod topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.EODTopic, ""},
		{"pubsub", "order topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.OrderTopic, ""},
		{"pubsub", "redel topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.RedelTopic, ""},
		{"pubsub", "redel2 topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.Redel2Topic, ""},
		// PSM2 dead-letter resources (subscriptions before topics): the source
		// subscription and its dead-letter topic/subscription.
		{"pubsub", "dlq source subscription", http.MethodDelete, "/v1/projects/" + t.Project + "/subscriptions/" + n.DLQMainSub, ""},
		{"pubsub", "dlq subscription", http.MethodDelete, "/v1/projects/" + t.Project + "/subscriptions/" + n.DLQSub, ""},
		{"pubsub", "dlq source topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.DLQMainTopic, ""},
		{"pubsub", "dlq topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.DLQTopic, ""},
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
		// crypto-medallion demo parity (D1). Delete the demo mutations in
		// reverse dependency order. The Monitoring alert policy / notification
		// channel names are server-generated, so they come from the capture's
		// SavedVars (populated by Run).
		{"run", "service", http.MethodDelete, "/v2/projects/" + t.Project + "/locations/us-central1/services/" + n.RunService, ""},
		{"eventarc", "trigger", http.MethodDelete, "/v1/projects/" + t.Project + "/locations/us-central1/triggers/" + n.Trigger, ""},
		{"scheduler", "job", http.MethodDelete, "/v1/projects/" + t.Project + "/locations/us-central1/jobs/" + n.SchedJob, ""},
		{"monitoring", "time series", http.MethodDelete, "/v3/projects/" + t.Project + "/timeSeries", ""},
		{"storage", "bq load object", http.MethodDelete, "/storage/v1/b/" + n.LoadBucket + "/o/data.ndjson", ""},
		{"storage", "bq load bucket", http.MethodDelete, "/storage/v1/b/" + n.LoadBucket, ""},
		{"storage", "eventarc bucket", http.MethodDelete, "/storage/v1/b/" + n.EventBucket, ""},
		{"storage", "cmek bucket", http.MethodDelete, "/storage/v1/b/" + n.CMEKBucket, ""},
		{"pubsub", "notify topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.NotifyTopic, ""},
		// A crypto key cannot be deleted by GCP; destroy its primary version so
		// the billable active version does not persist (the key object itself is
		// the documented, unavoidable residue, like the fixed differential key).
		{"kms", "crypto key version", http.MethodPost,
			"/v1/projects/" + t.Project + "/locations/global/keyRings/" + FixedKMSKeyRing + "/cryptoKeys/" + n.CryptoKey + "/cryptoKeyVersions/1:destroy", ""},
	}
	if v := t.SavedVars["alertPolicy"]; v != "" {
		ops = append(ops, del{"monitoring", "alert policy", http.MethodDelete, "/v3/" + v, ""})
	}
	if v := t.SavedVars["notifyChannel"]; v != "" {
		ops = append(ops, del{"monitoring", "notification channel", http.MethodDelete, "/v3/" + v, ""})
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
		{"pubsub", "msg topic", "/v1/projects/" + t.Project + "/topics/" + n.MsgTopic},
		{"pubsub", "msg subscription", "/v1/projects/" + t.Project + "/subscriptions/" + n.MsgSub},
		{"pubsub", "order topic", "/v1/projects/" + t.Project + "/topics/" + n.OrderTopic},
		{"pubsub", "order subscription", "/v1/projects/" + t.Project + "/subscriptions/" + n.OrderSub},
		{"pubsub", "eod topic", "/v1/projects/" + t.Project + "/topics/" + n.EODTopic},
		{"pubsub", "eod subscription", "/v1/projects/" + t.Project + "/subscriptions/" + n.EODSub},
		{"pubsub", "redel topic", "/v1/projects/" + t.Project + "/topics/" + n.RedelTopic},
		{"pubsub", "redel subscription", "/v1/projects/" + t.Project + "/subscriptions/" + n.RedelSub},
		{"pubsub", "redel2 topic", "/v1/projects/" + t.Project + "/topics/" + n.Redel2Topic},
		{"pubsub", "redel2 subscription", "/v1/projects/" + t.Project + "/subscriptions/" + n.Redel2Sub},
		{"pubsub", "dlq source topic", "/v1/projects/" + t.Project + "/topics/" + n.DLQMainTopic},
		{"pubsub", "dlq source subscription", "/v1/projects/" + t.Project + "/subscriptions/" + n.DLQMainSub},
		{"pubsub", "dlq topic", "/v1/projects/" + t.Project + "/topics/" + n.DLQTopic},
		{"pubsub", "dlq subscription", "/v1/projects/" + t.Project + "/subscriptions/" + n.DLQSub},
		{"secretmanager", "secret", "/v1/projects/" + t.Project + "/secrets/" + n.Secret},
		{"bigquery", "dataset", "/bigquery/v2/projects/" + t.Project + "/datasets/" + n.DS},
		{"iam", "service account", "/v1/projects/" + t.Project + "/serviceAccounts/" + n.ServiceAccount + "@" + t.Project + ".iam.gserviceaccount.com"},
		{"firestore", "document", "/v1/projects/" + t.Project + "/databases/(default)/documents/" + n.FSCollection + "/" + n.FSDoc},
		{"dns", "managed zone", "/dns/v1/projects/" + t.Project + "/managedZones/" + n.DNSZone},
		{"workflows", "workflow", "/v1/projects/" + t.Project + "/locations/us-central1/workflows/" + n.Workflow},
		// crypto-medallion demo parity (D1).
		{"storage", "cmek bucket", "/storage/v1/b/" + n.CMEKBucket},
		{"storage", "bq load bucket", "/storage/v1/b/" + n.LoadBucket},
		{"storage", "eventarc bucket", "/storage/v1/b/" + n.EventBucket},
		{"run", "service", "/v2/projects/" + t.Project + "/locations/us-central1/services/" + n.RunService},
		{"eventarc", "trigger", "/v1/projects/" + t.Project + "/locations/us-central1/triggers/" + n.Trigger},
		{"scheduler", "job", "/v1/projects/" + t.Project + "/locations/us-central1/jobs/" + n.SchedJob},
		{"pubsub", "notify topic", "/v1/projects/" + t.Project + "/topics/" + n.NotifyTopic},
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
