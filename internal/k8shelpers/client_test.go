package k8shelpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewClient_EnvFallback exercises the out-of-cluster path: with no
// in-cluster service host/port, NewClient must still build a client from the
// JAISCLOUD_K8S_* environment (a missing CA file selects the insecure dev
// fallback) rather than erroring.
func TestNewClient_EnvFallback(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("JAISCLOUD_K8S_APISERVER", "https://k8s.example.invalid:6443")
	t.Setenv("JAISCLOUD_K8S_TOKEN", "test-token")
	t.Setenv("JAISCLOUD_K8S_TOKEN_FILE", "")
	t.Setenv("JAISCLOUD_K8S_CA_FILE", "/nonexistent/ca.crt")
	t.Setenv("JAISCLOUD_K8S_CLIENT_CERT_FILE", "")

	client, err := NewClient()
	require.NoError(t, err)
	assert.NotNil(t, client)
}
