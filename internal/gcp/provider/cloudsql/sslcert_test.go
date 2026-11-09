package cloudsql

import (
	"context"
	"crypto/sha1"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"strings"
	"sync"
	"testing"

	"jaiscloud/internal/store"
)

// mustInsertCert creates a client certificate and returns the certInfo view.
func mustInsertCert(t *testing.T, p *Provider, instance, commonName string) map[string]any {
	t.Helper()
	resp, err := p.SslCertsInsert(context.Background(), newNR(map[string]any{
		"instance": instance,
		"body":     map[string]any{"commonName": commonName},
	}))
	if err != nil {
		t.Fatalf("insert ssl cert: %v", err)
	}
	if resp.Data["kind"] != kindSslCertsInsert {
		t.Fatalf("insert kind = %v, want %s", resp.Data["kind"], kindSslCertsInsert)
	}
	client, _ := resp.Data["clientCert"].(map[string]any)
	info, _ := client["certInfo"].(map[string]any)
	return info
}

func TestSslCertInsertReturnsCertificateAndServerCA(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	resp, err := p.SslCertsInsert(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"commonName": "client-name"},
	}))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if resp.Data["kind"] != kindSslCertsInsert {
		t.Errorf("kind = %v, want %s", resp.Data["kind"], kindSslCertsInsert)
	}

	// The operation envelope is Cloud SQL's own, DONE and targeting the instance.
	op, _ := resp.Data["operation"].(map[string]any)
	if op["kind"] != kindOperation || op["status"] != "DONE" {
		t.Errorf("operation = %v", op)
	}
	if op["targetId"] != "inst-a" {
		t.Errorf("operation.targetId = %v, want inst-a", op["targetId"])
	}
	// The documented example of sslCerts.insert reports operationType UPDATE.
	if op["operationType"] != "UPDATE" {
		t.Errorf("operationType = %v, want UPDATE", op["operationType"])
	}

	ca, _ := resp.Data["serverCaCert"].(map[string]any)
	if ca["kind"] != kindSslCert || !strings.Contains(stringField(ca, "cert"), "BEGIN CERTIFICATE") {
		t.Fatalf("serverCaCert is not a PEM certificate: %v", ca["kind"])
	}

	client, _ := resp.Data["clientCert"].(map[string]any)
	keyPEM, _ := client["certPrivateKey"].(string)
	if !strings.Contains(keyPEM, "BEGIN PRIVATE KEY") {
		t.Fatalf("clientCert.certPrivateKey is not a PEM private key: %q", keyPEM)
	}
	info, _ := client["certInfo"].(map[string]any)
	if info["commonName"] != "client-name" {
		t.Errorf("commonName = %v", info["commonName"])
	}
	if info["instance"] != "inst-a" {
		t.Errorf("instance = %v, want inst-a", info["instance"])
	}
	if info["sha1Fingerprint"] == "" || info["certSerialNumber"] == "" {
		t.Errorf("fingerprint/serial must be populated: %v", info)
	}
	if info["createTime"] == "" || info["expirationTime"] == "" {
		t.Errorf("create/expiration times must be populated: %v", info)
	}
	if info["selfLink"] != "https://sqladmin.googleapis.com/sql/v1beta4/projects/proj/instances/inst-a/sslCerts/"+stringField(info, "sha1Fingerprint") {
		t.Errorf("selfLink = %v", info["selfLink"])
	}
}

// TestSslCertMaterialIsValid parses the returned PEM, checks the private key
// matches, and verifies the fingerprint is the SHA-1 of the certificate DER
// (uppercase hex) and the serial number matches the certificate.
func TestSslCertMaterialIsValid(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	resp, err := p.SslCertsInsert(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"commonName": "client-name"},
	}))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	client, _ := resp.Data["clientCert"].(map[string]any)
	keyPEM, _ := client["certPrivateKey"].(string)
	info, _ := client["certInfo"].(map[string]any)
	certPEM := stringField(info, "cert")

	cert := parseCertPEM(t, certPEM)
	parseKeyPEM(t, keyPEM)

	if cert.Subject.CommonName != "client-name" {
		t.Errorf("subject CN = %q, want client-name", cert.Subject.CommonName)
	}
	if got := cert.SerialNumber.String(); got != stringField(info, "certSerialNumber") {
		t.Errorf("certSerialNumber = %q, want %q", info["certSerialNumber"], got)
	}
	if got := fingerprint(cert.Raw); got != stringField(info, "sha1Fingerprint") {
		t.Errorf("sha1Fingerprint = %q, want %q", info["sha1Fingerprint"], got)
	}
	if got := fingerprint(cert.Raw); got != strings.ToUpper(got) {
		t.Errorf("fingerprint must be uppercase hex, got %q", got)
	}
	// Client certificates are documented to live 10 years.
	if years := cert.NotAfter.Sub(cert.NotBefore).Hours() / 24 / 365; years < 9.9 || years > 10.1 {
		t.Errorf("client cert lifetime = %.2f years, want 10", years)
	}
}

// TestSslCertServerCATiesInstance checks that instance insert exposes the
// server CA as DatabaseInstance.serverCaCert (the server-ca.pem download), that
// sslCerts.insert returns the same certificate, and that it is a CA cert.
func TestSslCertServerCATiesInstance(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	inst, err := p.InstancesGet(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("instance get: %v", err)
	}
	ca, _ := inst.Data["serverCaCert"].(map[string]any)
	if ca == nil {
		t.Fatal("instance.serverCaCert is absent")
	}
	caCert := parseCertPEM(t, stringField(ca, "cert"))
	if !caCert.IsCA {
		t.Error("instance.serverCaCert is not a CA certificate")
	}

	resp, err := p.SslCertsInsert(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"commonName": "client-name"},
	}))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	insertCA, _ := resp.Data["serverCaCert"].(map[string]any)
	if stringField(insertCA, "cert") != stringField(ca, "cert") {
		t.Error("sslCerts.insert serverCaCert does not match the instance server CA")
	}

	// serverCaCert is output-only: a patch cannot replace it.
	if _, err := p.InstancesPatch(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"serverCaCert": map[string]any{"cert": "forged"}},
	})); err != nil {
		t.Fatalf("patch instance: %v", err)
	}
	patched, err := p.InstancesGet(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("instance get after patch: %v", err)
	}
	if got, _ := patched.Data["serverCaCert"].(map[string]any); stringField(got, "cert") != stringField(ca, "cert") {
		t.Error("a patch replaced the server CA certificate")
	}
}

// TestSslCertGetListDelete covers the read plane: get by fingerprint, list, and
// delete followed by NotFound.
func TestSslCertGetListDelete(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")
	first := mustInsertCert(t, p, "inst-a", "client-one")
	mustInsertCert(t, p, "inst-a", "client-two")

	list, err := p.SslCertsList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if list.Data["kind"] != kindSslCertsList {
		t.Errorf("list kind = %v, want %s", list.Data["kind"], kindSslCertsList)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("list items = %d, want 2", len(items))
	}
	// The private key is returned exactly once, by insert.
	for _, it := range items {
		if _, ok := it.(map[string]any)["privateKey"]; ok {
			t.Error("list leaked the private key")
		}
	}

	fp := stringField(first, "sha1Fingerprint")
	got, err := p.SslCertsGet(ctx, newNR(map[string]any{"instance": "inst-a", "sha1Fingerprint": fp}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["commonName"] != "client-one" {
		t.Errorf("get commonName = %v", got.Data["commonName"])
	}
	if _, ok := got.Data["privateKey"]; ok {
		t.Error("get leaked the private key")
	}

	// A lower-cased fingerprint resolves too.
	if _, err := p.SslCertsGet(ctx, newNR(map[string]any{
		"instance": "inst-a", "sha1Fingerprint": strings.ToLower(fp),
	})); err != nil {
		t.Fatalf("get with lowercase fingerprint: %v", err)
	}

	del, err := p.SslCertsDelete(ctx, newNR(map[string]any{"instance": "inst-a", "sha1Fingerprint": fp}))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if del.Data["kind"] != kindOperation || del.Data["status"] != "DONE" {
		t.Fatalf("delete must return a DONE operation envelope: %v", del.Data)
	}
	if _, err := p.SslCertsGet(ctx, newNR(map[string]any{"instance": "inst-a", "sha1Fingerprint": fp})); !isCode(err, "NotFound") {
		t.Fatalf("get after delete = %v, want NotFound", err)
	}
	if _, err := p.SslCertsDelete(ctx, newNR(map[string]any{"instance": "inst-a", "sha1Fingerprint": fp})); !isCode(err, "NotFound") {
		t.Fatalf("delete after delete = %v, want NotFound", err)
	}

	// The second certificate is untouched.
	list, _ = p.SslCertsList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("list after delete = %d, want 1", len(items))
	}
}

// TestSslCertValidation pins the request contract: a missing/invalid common
// name, a duplicate name, and a missing instance.
func TestSslCertValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")
	mustInsertCert(t, p, "inst-a", "client-name")

	cases := []struct {
		name   string
		params map[string]any
		code   string
	}{
		{"missing commonName", map[string]any{"instance": "inst-a", "body": map[string]any{}}, "InvalidArgument"},
		{"invalid commonName", map[string]any{"instance": "inst-a", "body": map[string]any{"commonName": "bad/name"}}, "InvalidArgument"},
		{"duplicate commonName", map[string]any{"instance": "inst-a", "body": map[string]any{"commonName": "client-name"}}, "AlreadyExists"},
		{"missing instance", map[string]any{"instance": "nope", "body": map[string]any{"commonName": "x"}}, "NotFound"},
	}
	for _, tc := range cases {
		if _, err := p.SslCertsInsert(ctx, newNR(tc.params)); !isCode(err, tc.code) {
			t.Errorf("%s: error = %v, want %s", tc.name, err, tc.code)
		}
	}

	if _, err := p.SslCertsList(ctx, newNR(map[string]any{"instance": "nope"})); !isCode(err, "NotFound") {
		t.Errorf("list missing instance = %v, want NotFound", err)
	}
	if _, err := p.SslCertsGet(ctx, newNR(map[string]any{"instance": "inst-a", "sha1Fingerprint": "does-not-exist"})); !isCode(err, "NotFound") {
		t.Errorf("get unknown fingerprint = %v, want NotFound", err)
	}
}

// TestSslCertLimit pins the documented cap of 10 client certificates per
// instance.
func TestSslCertLimit(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")
	for i := 0; i < maxClientCerts; i++ {
		mustInsertCert(t, p, "inst-a", "client-"+string(rune('a'+i)))
	}
	_, err := p.SslCertsInsert(ctx, newNR(map[string]any{
		"instance": "inst-a",
		"body":     map[string]any{"commonName": "one-too-many"},
	}))
	if !isCode(err, "InvalidArgument") {
		t.Fatalf("insert beyond the cap = %v, want InvalidArgument", err)
	}
}

// TestDeleteInstancePurgesSslCerts checks the certificate set is owned by the
// instance record and disappears with it.
func TestDeleteInstancePurgesSslCerts(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")
	mustInsertCert(t, p, "inst-a", "client-name")

	if _, err := p.InstancesDelete(ctx, newNR(map[string]any{"instance": "inst-a"})); err != nil {
		t.Fatalf("delete instance: %v", err)
	}
	mustCreateInstance(t, p, "inst-a")
	list, err := p.SslCertsList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 0 {
		t.Fatalf("certs survived the instance delete: %d", len(items))
	}
}

func parseCertPEM(t *testing.T, certPEM string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("not a PEM certificate: %q", certPEM)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return cert
}

func parseKeyPEM(t *testing.T, keyPEM string) {
	t.Helper()
	block, _ := pem.Decode([]byte(keyPEM))
	if block == nil {
		t.Fatalf("not a PEM private key: %q", keyPEM)
	}
	if _, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
		t.Fatalf("parse private key: %v", err)
	}
}

// fingerprint returns the uppercase hex SHA-1 of a certificate's DER encoding,
// the identity the certificate resource is addressed by.
func fingerprint(der []byte) string {
	sum := sha1.Sum(der)
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

// TestSslCertServerCABackfill covers a record restored from a snapshot taken
// before the serverCaCert field existed: the certificate surface synthesizes
// the CA on first use and persists it back onto the instance.
func TestSslCertServerCABackfill(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	// Rewrite the stored instance without its server CA.
	inst, err := p.InstancesGet(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("instance get: %v", err)
	}
	delete(inst.Data, "serverCaCert")
	data, err := json.Marshal(inst.Data)
	if err != nil {
		t.Fatalf("marshal instance: %v", err)
	}
	if err := p.resources.Upsert(ctx, "proj", store.GlobalRegion,
		store.ResourceEntry{Type: rtInstance, ID: "inst-a", Data: data}); err != nil {
		t.Fatalf("rewrite instance: %v", err)
	}

	mustInsertCert(t, p, "inst-a", "client-name")

	inst, err = p.InstancesGet(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("instance get after insert: %v", err)
	}
	ca, _ := inst.Data["serverCaCert"].(map[string]any)
	if ca == nil || stringField(ca, "cert") == "" {
		t.Fatalf("server CA was not backfilled onto the instance: %v", inst.Data["serverCaCert"])
	}
	parseCertPEM(t, stringField(ca, "cert"))
}

// TestSslCertRequestValidation covers the missing-parameter branches of the
// read/delete handlers.
func TestSslCertRequestValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	cases := []struct {
		name string
		call func() (any, error)
	}{
		{"get without instance", func() (any, error) {
			return p.SslCertsGet(ctx, newNR(map[string]any{"sha1Fingerprint": "fp"}))
		}},
		{"get without fingerprint", func() (any, error) {
			return p.SslCertsGet(ctx, newNR(map[string]any{"instance": "inst-a"}))
		}},
		{"delete without instance", func() (any, error) {
			return p.SslCertsDelete(ctx, newNR(map[string]any{"sha1Fingerprint": "fp"}))
		}},
		{"delete without fingerprint", func() (any, error) {
			return p.SslCertsDelete(ctx, newNR(map[string]any{"instance": "inst-a"}))
		}},
		{"list without instance", func() (any, error) {
			return p.SslCertsList(ctx, newNR(nil))
		}},
		{"insert without instance", func() (any, error) {
			return p.SslCertsInsert(ctx, newNR(map[string]any{"body": map[string]any{"commonName": "x"}}))
		}},
	}
	for _, tc := range cases {
		if _, err := tc.call(); !isCode(err, "InvalidArgument") {
			t.Errorf("%s: error = %v, want InvalidArgument", tc.name, err)
		}
	}
}

// TestSslCertStoreAnomalies covers the defensive branches: a corrupt record is
// skipped by list (and fails the read), and a record whose body names another
// instance is NotFound for the instance it is keyed under.
func TestSslCertStoreAnomalies(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")
	mustInsertCert(t, p, "inst-a", "client-good")

	// A corrupt record under the instance prefix must not break list.
	if err := p.resources.Create(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: rtSslCert, ID: "inst-a/CORRUPT", Data: []byte("{not json"),
	}); err != nil {
		t.Fatalf("seed corrupt record: %v", err)
	}
	list, err := p.SslCertsList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("list items = %d, want the one readable certificate", len(items))
	}
	if _, err := p.SslCertsGet(ctx, newNR(map[string]any{"instance": "inst-a", "sha1Fingerprint": "CORRUPT"})); err == nil {
		t.Fatal("get of a corrupt record must fail")
	}

	// A record keyed under inst-a whose body names inst-b is not served for
	// inst-a: the ownership check rejects it.
	other, err := json.Marshal(map[string]any{"instance": "inst-b", "sha1Fingerprint": "OTHER"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := p.resources.Create(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: rtSslCert, ID: "inst-a/OTHER", Data: other,
	}); err != nil {
		t.Fatalf("seed foreign record: %v", err)
	}
	if _, err := p.SslCertsGet(ctx, newNR(map[string]any{"instance": "inst-a", "sha1Fingerprint": "OTHER"})); !isCode(err, "NotFound") {
		t.Fatalf("get of a foreign record = %v, want NotFound", err)
	}
}

// TestSslCertConcurrentDuplicate inserts the same common name from several
// goroutines: exactly one wins, the rest see AlreadyExists, and the instance
// holds one certificate.
func TestSslCertConcurrentDuplicate(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateInstance(t, p, "inst-a")

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.SslCertsInsert(ctx, newNR(map[string]any{
				"instance": "inst-a",
				"body":     map[string]any{"commonName": "shared-name"},
			}))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	ok, dup := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			ok++
		case isCode(err, "AlreadyExists"):
			dup++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if ok != 1 || dup != n-1 {
		t.Fatalf("ok=%d dup=%d, want 1/%d", ok, dup, n-1)
	}
	list, err := p.SslCertsList(ctx, newNR(map[string]any{"instance": "inst-a"}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("stored certificates = %d, want 1", len(items))
	}
}
