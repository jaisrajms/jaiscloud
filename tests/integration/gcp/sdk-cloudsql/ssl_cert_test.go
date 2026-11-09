// This file exercises the sslCerts control plane through the official Google
// REST apiary client against the emulator's Cloud SQL Admin v1beta4 surface:
// the insert envelope (certificate + one-time private key + server CA + DONE
// operation), list/get/delete round trips, and the certificate material itself.
package sdk_cloudsql_test

import (
	"crypto/sha1"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/sqladmin/v1beta4"
)

// TestSDKCloudSQLSslCerts covers google_sql_ssl_cert's consumer path:
// sslCerts.insert -> get -> list -> delete, plus the fingerprint addressing and
// the material's self-consistency.
func TestSDKCloudSQLSslCerts(t *testing.T) {
	resetState(t)
	svc := newService(t)
	project := projectID()

	instance := unique("inst")
	_, err := svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: instance, Region: "us-central1", Settings: &sqladmin.Settings{Tier: "db-f1-micro"},
	}).Do()
	require.NoError(t, err)

	// The common name is constrained to [a-zA-Z.-_ ]+ by the Discovery
	// description, so it carries no digits.
	commonName := "conf-client-cert"
	resp, err := svc.SslCerts.Insert(project, instance,
		&sqladmin.SslCertsInsertRequest{CommonName: commonName}).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#sslCertsInsert", resp.Kind)
	require.NotNil(t, resp.Operation)
	require.Equal(t, "DONE", resp.Operation.Status)
	require.Equal(t, instance, resp.Operation.TargetId)
	require.NotNil(t, resp.ServerCaCert)
	require.NotNil(t, resp.ClientCert)
	require.NotNil(t, resp.ClientCert.CertInfo)

	info := resp.ClientCert.CertInfo
	require.Equal(t, "sql#sslCert", info.Kind)
	require.Equal(t, instance, info.Instance)
	require.Equal(t, commonName, info.CommonName)
	require.NotEmpty(t, info.Sha1Fingerprint)
	require.NotEmpty(t, info.CertSerialNumber)
	require.NotEmpty(t, info.CreateTime)
	require.NotEmpty(t, info.ExpirationTime)
	require.True(t, strings.Contains(info.Cert, "BEGIN CERTIFICATE"))
	require.True(t, strings.Contains(resp.ServerCaCert.Cert, "BEGIN CERTIFICATE"))
	require.True(t, strings.Contains(resp.ClientCert.CertPrivateKey, "BEGIN PRIVATE KEY"))

	// The certificate is real X.509 material whose serial and SHA-1 fingerprint
	// match the metadata the API reports.
	block, _ := pem.Decode([]byte(info.Cert))
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	require.Equal(t, commonName, cert.Subject.CommonName)
	require.Equal(t, info.CertSerialNumber, cert.SerialNumber.String())
	sum := sha1.Sum(cert.Raw)
	require.Equal(t, strings.ToUpper(hex.EncodeToString(sum[:])), info.Sha1Fingerprint)

	// get addresses the certificate by its fingerprint; the emulator normalizes
	// the case so a client that lower-cases the id still resolves it.
	got, err := svc.SslCerts.Get(project, instance, info.Sha1Fingerprint).Do()
	require.NoError(t, err)
	require.Equal(t, info.Sha1Fingerprint, got.Sha1Fingerprint)
	require.Equal(t, commonName, got.CommonName)
	require.Equal(t, info.Cert, got.Cert)
	_, err = svc.SslCerts.Get(project, instance, strings.ToLower(info.Sha1Fingerprint)).Do()
	require.NoError(t, err)

	list, err := svc.SslCerts.List(project, instance).Do()
	require.NoError(t, err)
	require.Equal(t, "sql#sslCertsList", list.Kind)
	require.Len(t, list.Items, 1)
	require.Equal(t, info.Sha1Fingerprint, list.Items[0].Sha1Fingerprint)

	// Delete returns a DONE operation and drops the certificate.
	op, err := svc.SslCerts.Delete(project, instance, info.Sha1Fingerprint).Do()
	require.NoError(t, err)
	require.Equal(t, "DONE", op.Status)
	_, err = svc.SslCerts.Get(project, instance, info.Sha1Fingerprint).Do()
	requireAPIError(t, err, 404)
	list, err = svc.SslCerts.List(project, instance).Do()
	require.NoError(t, err)
	require.Empty(t, list.Items)

	// Nested under a missing instance is NotFound, and a second certificate with
	// the same common name is rejected (real Cloud SQL requires distinct names).
	_, err = svc.SslCerts.List(project, "inst-missing").Do()
	requireAPIError(t, err, 404)
	_, err = svc.SslCerts.Insert(project, instance,
		&sqladmin.SslCertsInsertRequest{CommonName: commonName}).Do()
	require.NoError(t, err)
	_, err = svc.SslCerts.Insert(project, instance,
		&sqladmin.SslCertsInsertRequest{CommonName: commonName}).Do()
	requireAPIError(t, err, 409)
}
