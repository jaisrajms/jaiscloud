package gateway

import (
	"context"
	"crypto/ecdsa"
	"crypto/x509"
	"testing"
	"time"

	"jaiscloud/internal/certstore"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"

	"github.com/stretchr/testify/require"
)

type fakeCertStore struct {
	cert *certstore.StoredCert
	err  error
}

func (f *fakeCertStore) Load(_ context.Context) (*certstore.StoredCert, error) {
	if f.cert == nil && f.err == nil {
		return nil, certstore.ErrNotFound
	}
	return f.cert, f.err
}

func (f *fakeCertStore) Save(_ context.Context, c *certstore.StoredCert) error {
	f.cert = c
	return nil
}

func newTestServer(cs certstore.CertStore) *Server {
	return &Server{
		cfg: &config.Config{
			Cloud:  "aws",
			Region: "us-east-1",
		},
		certs: cs,
	}
}

func TestLoadOrGenerateCert_GeneratesWhenMissing(t *testing.T) {
	store := &fakeCertStore{}
	s := newTestServer(store)

	cert, err := s.loadOrGenerateCert(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, cert.Certificate)

	require.NotNil(t, store.cert)
	require.NotEmpty(t, store.cert.CertPEM)
	require.True(t, store.cert.NotAfter.After(clock.RealNow()))
}

func TestLoadOrGenerateCert_ReusesValidStored(t *testing.T) {
	generated, err := generateSelfSignedCert("aws", "us-east-1")
	require.NoError(t, err)

	leaf, err := x509.ParseCertificate(generated.Certificate[0])
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(generated.PrivateKey.(*ecdsa.PrivateKey))
	require.NoError(t, err)

	store := &fakeCertStore{cert: &certstore.StoredCert{
		CertPEM:  pemEncode("CERTIFICATE", generated.Certificate[0]),
		KeyPEM:   pemEncode("EC PRIVATE KEY", keyDER),
		NotAfter: leaf.NotAfter,
	}}
	s := newTestServer(store)

	cert, err := s.loadOrGenerateCert(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, cert.Certificate)

	loaded, _ := x509.ParseCertificate(cert.Certificate[0])
	require.Equal(t, leaf.SerialNumber, loaded.SerialNumber)
}

func TestLoadOrGenerateCert_RegeneratesExpiring(t *testing.T) {
	generated, err := generateSelfSignedCert("aws", "us-east-1")
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(generated.PrivateKey.(*ecdsa.PrivateKey))
	require.NoError(t, err)

	store := &fakeCertStore{cert: &certstore.StoredCert{
		CertPEM:  pemEncode("CERTIFICATE", generated.Certificate[0]),
		KeyPEM:   pemEncode("EC PRIVATE KEY", keyDER),
		NotAfter: clock.RealNow().Add(10 * 24 * time.Hour),
	}}
	s := newTestServer(store)

	cert, err := s.loadOrGenerateCert(context.Background())
	require.NoError(t, err)

	loaded, _ := x509.ParseCertificate(cert.Certificate[0])
	require.True(t, loaded.NotAfter.After(clock.RealNow().Add(365*24*time.Hour)),
		"regenerated cert should have long validity")
}
