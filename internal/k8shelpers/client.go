package k8shelpers

import (
	"os"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// NewClient builds the shared Kubernetes client used by every executor. It is
// the single client bootstrap for the process: it prefers the in-cluster config
// (service-account token + CA) and otherwise falls back to the JAISCLOUD_K8S_*
// environment (APISERVER / TOKEN / TOKEN_FILE / CA_FILE / CLIENT_CERT_FILE), so
// out-of-cluster development and the deployment's explicit env both work.
//
// It deliberately sets no HTTP timeout: the same client backs long-lived
// watches (Spark/EMR ownership patchers, terminal waiters), and a client-wide
// timeout would sever them. Callers bound individual calls with contexts.
func NewClient() (kubernetes.Interface, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return kubernetes.NewForConfig(cfg)
	}

	token := strings.TrimSpace(os.Getenv("JAISCLOUD_K8S_TOKEN"))
	if token == "" {
		if f := os.Getenv("JAISCLOUD_K8S_TOKEN_FILE"); f != "" {
			if b, err := os.ReadFile(f); err == nil {
				token = strings.TrimSpace(string(b))
			}
		}
	}
	if token == "" {
		if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
			token = strings.TrimSpace(string(b))
		}
	}

	host := os.Getenv("JAISCLOUD_K8S_APISERVER")
	if host == "" {
		host = "https://kubernetes.default.svc"
	}

	tlsCfg := rest.TLSClientConfig{}
	caFile := os.Getenv("JAISCLOUD_K8S_CA_FILE")
	if caFile == "" {
		caFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	if _, err := os.Stat(caFile); err == nil {
		tlsCfg.CAFile = caFile
	} else {
		tlsCfg.Insecure = true //nolint:gosec // dev fallback when no CA
	}
	if certFile := os.Getenv("JAISCLOUD_K8S_CLIENT_CERT_FILE"); certFile != "" {
		tlsCfg.CertFile = certFile
		tlsCfg.KeyFile = os.Getenv("JAISCLOUD_K8S_CLIENT_KEY_FILE")
	}

	return kubernetes.NewForConfig(&rest.Config{
		Host:            host,
		BearerToken:     token,
		TLSClientConfig: tlsCfg,
	})
}
