package cloudsql

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Cloud SQL client certificates: projects/{project}/instances/{instance}/sslCerts.
//
// The emulator serves the sslCerts CRUD plane (insert/get/list/delete) as
// metadata over the shared ResourceStore, like instances/databases/users, but
// unlike those it mints real certificate material: a self-signed X.509 client
// certificate (PEM) plus its private key is generated at insert, stored with the
// record, and the private key is returned exactly once — real Cloud SQL does
// the same (the key cannot be retrieved again). The certificate is signed by a
// per-instance self-signed server CA that is synthesized with the instance and
// exposed as DatabaseInstance.serverCaCert (the server-ca.pem a real client
// downloads), so a client that parses the PEM and verifies the fingerprint gets
// consistent material. Nothing listens on the synthesized instance, so the
// material has no data-plane effect.
//
// The certificate body is opaque to the wire contract, so the emulator signs the
// client cert with itself (there is no TLS handshake to complete). The SHA-1
// fingerprint is the SHA-1 of the certificate DER, uppercase hex, and is the
// resource id get/delete address.
//
// Deferred: sslCerts.createEphemeral stays an Unimplemented stub — it signs a
// caller-supplied public key with the instance CA for the Cloud SQL connectors,
// which only ever use the certificate for a TLS connection the emulator cannot
// terminate.

// maxClientCerts is the documented per-instance client-certificate limit
// ("You can create up to 10 client certificates for each instance", Cloud SQL
// docs, manage-ssl-instance). A create beyond it fails rather than growing the
// instance's cert set without bound. The 400 binding is doc-derived: the cap is
// documented, its error envelope is not captured.
const maxClientCerts = 10

// clientCertValidity is the documented default lifetime of a client certificate
// (10 years); server CA certificates get the same 10-year lifetime.
const clientCertValidity = 10 * 365 * 24 * time.Hour

// commonNameAlphabet is the documented certificate common-name constraint
// ("User supplied name. Constrained to [a-zA-Z.-_ ]+").
const commonNameAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-_ "

// ── Certificate operations ──────────────────────────────────────────────────

// SslCertsInsert creates a client certificate. The response is Cloud SQL's
// SslCertsInsertResponse envelope: the new certificate plus private key, the
// instance's server CA certificate, and the UPDATE operation envelope.
func (p *Provider) SslCertsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance := strParam(nr, "instance")
	if instance == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	inst, err := p.loadInstance(ctx, accountID(nr), instance)
	if err != nil {
		return nil, err
	}
	commonName := stringField(bodyMap(nr), "commonName")
	if commonName == "" {
		return nil, model.NewProviderError("InvalidArgument", "commonName is required", 400)
	}
	if !validCommonName(commonName) {
		return nil, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("Invalid common name %q: must match [a-zA-Z.-_ ]+", commonName), 400)
	}

	account := accountID(nr)
	p.sslMu.Lock()
	defer p.sslMu.Unlock()
	certs, err := p.listSslCerts(ctx, account, instance)
	if err != nil {
		return nil, err
	}
	if len(certs) >= maxClientCerts {
		return nil, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("instance %q already has the maximum number of client certificates (%d)", instance, maxClientCerts), 400)
	}
	for _, c := range certs {
		if stringField(c, "commonName") == commonName {
			return nil, model.NewProviderError("AlreadyExists",
				fmt.Sprintf("client certificate with common name %q already exists", commonName), 409)
		}
	}

	now := clock.Now()
	material, err := generateCertPEM(commonName, now, now.Add(clientCertValidity), false)
	if err != nil {
		return nil, err
	}
	rec := map[string]any{
		"kind":             kindSslCert,
		"instance":         instance,
		"commonName":       commonName,
		"cert":             material.certPEM,
		"certSerialNumber": material.serial,
		"sha1Fingerprint":  material.sha1,
		"createTime":       formatTimestamp(now),
		"expirationTime":   formatTimestamp(now.Add(clientCertValidity)),
		"selfLink":         sqlBasePath + resourcePath(nr, "cloudsql-sslcert", instance+"/"+material.sha1),
		// The private key is stored so the record survives a snapshot round trip;
		// it is never echoed by get/list (real Cloud SQL returns it only once).
		"privateKey": material.keyPEM,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	err = p.resources.Create(ctx, account, store.GlobalRegion,
		store.ResourceEntry{Type: rtSslCert, ID: sslCertKey(instance, material.sha1), Data: data})
	if err != nil {
		return nil, storeError(err, "ssl cert")
	}

	serverCA, err := p.serverCaCert(ctx, nr, account, instance, inst)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{
		"kind":         kindSslCertsInsert,
		"operation":    p.recordOperation(ctx, nr, "UPDATE", instance, instanceLink(nr, instance)),
		"serverCaCert": serverCA,
		"clientCert": map[string]any{
			"certInfo":       sslCertView(rec),
			"certPrivateKey": material.keyPEM,
		},
	}
	return provider.OK(resp), nil
}

// SslCertsGet returns the certificate addressed by its SHA-1 fingerprint.
func (p *Provider) SslCertsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance, fingerprint := strParam(nr, "instance"), strParam(nr, "sha1Fingerprint")
	if instance == "" || fingerprint == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance or sha1Fingerprint", 400)
	}
	rec, err := p.loadSslCert(ctx, accountID(nr), instance, fingerprint)
	if err != nil {
		return nil, err
	}
	return provider.OK(sslCertView(rec)), nil
}

// SslCertsList returns every client certificate of an instance. The Discovery
// document declares no pagination parameters for sslCerts.list, so the whole
// set is returned in creation order (a per-instance list is bounded by
// maxClientCerts).
func (p *Provider) SslCertsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance := strParam(nr, "instance")
	if instance == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance", 400)
	}
	if _, err := p.loadInstance(ctx, accountID(nr), instance); err != nil {
		return nil, err
	}
	certs, err := p.listSslCerts(ctx, accountID(nr), instance)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(certs))
	for _, c := range certs {
		items = append(items, sslCertView(c))
	}
	return provider.OK(map[string]any{"kind": kindSslCertsList, "items": items}), nil
}

// SslCertsDelete revokes a client certificate and returns Cloud SQL's operation
// envelope. Real Cloud SQL drops the certificate immediately; a subsequent read
// is NotFound.
func (p *Provider) SslCertsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	instance, fingerprint := strParam(nr, "instance"), strParam(nr, "sha1Fingerprint")
	if instance == "" || fingerprint == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing instance or sha1Fingerprint", 400)
	}
	rec, err := p.loadSslCert(ctx, accountID(nr), instance, fingerprint)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, accountID(nr), store.GlobalRegion, rtSslCert,
		sslCertKey(instance, stringField(rec, "sha1Fingerprint"))); err != nil {
		return nil, storeError(err, "ssl cert")
	}
	return provider.OK(p.recordOperation(ctx, nr, "UPDATE", instance, instanceLink(nr, instance))), nil
}

// ── Certificate storage ─────────────────────────────────────────────────────

func (p *Provider) listSslCerts(ctx context.Context, account, instance string) ([]map[string]any, error) {
	entries, err := p.resources.List(ctx, account, store.GlobalRegion, rtSslCert, instance)
	if err != nil {
		return nil, err
	}
	certs := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var m map[string]any
		if json.Unmarshal(e.Data, &m) == nil && stringField(m, "instance") == instance {
			certs = append(certs, m)
		}
	}
	sort.Slice(certs, func(i, j int) bool {
		ci, cj := stringField(certs[i], "createTime"), stringField(certs[j], "createTime")
		if ci != cj {
			return ci < cj
		}
		return stringField(certs[i], "sha1Fingerprint") < stringField(certs[j], "sha1Fingerprint")
	})
	return certs, nil
}

// loadSslCert resolves a certificate by fingerprint. The fingerprint is
// case-insensitive: the emulator mints uppercase hex, and a caller that
// lower-cases the id it read back must still resolve it.
func (p *Provider) loadSslCert(ctx context.Context, account, instance, fingerprint string) (map[string]any, error) {
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtSslCert, sslCertKey(instance, fingerprint))
	if err != nil {
		return nil, storeError(err, "ssl cert")
	}
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		return nil, err
	}
	if stringField(m, "instance") != instance {
		return nil, model.NewProviderError("NotFound", "ssl cert not found", 404)
	}
	return m, nil
}

// sslCertKey is the store key: the owning instance plus the normalized (upper
// case) fingerprint.
func sslCertKey(instance, fingerprint string) string {
	return instance + "/" + strings.ToUpper(fingerprint)
}

// sslCertView projects a stored record onto the wire SslCert shape. The private
// key is only ever returned by insert.
func sslCertView(rec map[string]any) map[string]any {
	view := map[string]any{
		"kind":             kindSslCert,
		"instance":         stringField(rec, "instance"),
		"commonName":       stringField(rec, "commonName"),
		"cert":             stringField(rec, "cert"),
		"certSerialNumber": stringField(rec, "certSerialNumber"),
		"sha1Fingerprint":  stringField(rec, "sha1Fingerprint"),
		"createTime":       stringField(rec, "createTime"),
		"expirationTime":   stringField(rec, "expirationTime"),
		"selfLink":         stringField(rec, "selfLink"),
	}
	return view
}

// ── Server CA certificate ───────────────────────────────────────────────────

// serverCaCert returns the instance's server CA certificate, synthesizing and
// persisting one when the stored instance predates the sslCerts surface (a
// record restored from a snapshot taken before the field existed). The
// certificate is the `serverCaCert` a real client downloads as server-ca.pem.
func (p *Provider) serverCaCert(ctx context.Context, nr *model.NormalizedRequest, account, instance string, inst map[string]any) (map[string]any, error) {
	if m, ok := inst["serverCaCert"].(map[string]any); ok && stringField(m, "cert") != "" {
		return m, nil
	}
	ca, err := newServerCaCert(nr, instance)
	if err != nil {
		return nil, err
	}
	inst["serverCaCert"] = ca
	data, err := json.Marshal(inst)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Upsert(ctx, account, store.GlobalRegion,
		store.ResourceEntry{Type: rtInstance, ID: instance, Data: data}); err != nil {
		return nil, err
	}
	return ca, nil
}

// newServerCaCert synthesizes the per-instance server CA certificate. A real
// Cloud SQL instance is born with one ("Cloud SQL creates a server certificate
// automatically when you create your instance"); per-instance CA certificates
// are valid for 10 years.
func newServerCaCert(nr *model.NormalizedRequest, instance string) (map[string]any, error) {
	now := clock.Now()
	commonName := "Google Cloud SQL Server CA"
	material, err := generateCertPEM(commonName, now, now.Add(clientCertValidity), true)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"kind":             kindSslCert,
		"instance":         instance,
		"commonName":       commonName,
		"cert":             material.certPEM,
		"certSerialNumber": material.serial,
		"sha1Fingerprint":  material.sha1,
		"createTime":       formatTimestamp(now),
		"expirationTime":   formatTimestamp(now.Add(clientCertValidity)),
		"selfLink":         sqlBasePath + resourcePath(nr, "cloudsql-sslcert", instance+"/"+material.sha1),
	}, nil
}

// ── Certificate material ────────────────────────────────────────────────────

// certMaterial is one minted certificate: the PEM certificate, its PEM private
// key, the decimal serial number, and the uppercase SHA-1 fingerprint.
type certMaterial struct {
	certPEM string
	keyPEM  string
	serial  string
	sha1    string
}

// generateCertPEM mints a self-signed X.509 certificate. The key is ECDSA
// P-256 (fast to generate and valid X.509; the wire only carries PEM text) in
// PKCS#8 form. isCA produces a CA certificate (the instance server CA) rather
// than a client-auth leaf.
func generateCertPEM(commonName string, notBefore, notAfter time.Time, isCA bool) (certMaterial, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return certMaterial{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 64))
	if err != nil {
		return certMaterial{}, err
	}
	serial.Add(serial, big.NewInt(1))

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             notBefore.Truncate(time.Second),
		NotAfter:              notAfter.Truncate(time.Second),
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	if isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	} else {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return certMaterial{}, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return certMaterial{}, err
	}
	sum := sha1.Sum(der)
	return certMaterial{
		certPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		keyPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})),
		serial:  serial.String(),
		sha1:    strings.ToUpper(hex.EncodeToString(sum[:])),
	}, nil
}

// validCommonName reports whether a common name matches the documented
// [a-zA-Z.-_ ]+ constraint.
func validCommonName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !strings.ContainsRune(commonNameAlphabet, r) {
			return false
		}
	}
	return true
}
