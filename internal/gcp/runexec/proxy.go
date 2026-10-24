// This file holds the routing and reverse-proxy machinery shared by the k8s and
// docker Cloud Run runtime managers. Both launch a revision runtime and forward
// data-plane HTTP requests to it; keeping the host/service registry and the
// proxy status mapping in one place means the two orchestrators cannot drift
// (docker and k8s are interchangeable implementations of one RuntimeManager
// seam, not two behaviours).
package runexec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	runcore "jaiscloud/internal/gcp/service/run"
	"jaiscloud/internal/model"
)

// target is a running revision's registered invocation endpoint. backend is the
// URL the proxy dials: an in-cluster ClusterIP Service for k8s, or a published
// loopback host port for docker.
type target struct {
	serviceName string
	revision    string
	backend     string
}

// registry maps an invocation authority (Host) and a canonical service name to
// the latest ready revision's target. It is safe for concurrent use.
type registry struct {
	mu        sync.RWMutex
	byHost    map[string]*target
	byService map[string]*target
}

func newRegistry() *registry {
	return &registry{byHost: map[string]*target{}, byService: map[string]*target{}}
}

// put registers a revision target under its service name and invocation host.
func (r *registry) put(svcName, host string, tgt *target) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byHost[host] = tgt
	r.byService[svcName] = tgt
}

// dropRevision deregisters a revision's target, but only while it is still the
// registered latest for its service/host — a newer revision may have replaced
// it, in which case the newer target must survive.
func (r *registry) dropRevision(svcName, host, revID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t, ok := r.byHost[host]; ok && t.revision == revID {
		delete(r.byHost, host)
	}
	if t, ok := r.byService[svcName]; ok && t.revision == revID {
		delete(r.byService, svcName)
	}
}

// dropService deregisters a service's target unconditionally.
func (r *registry) dropService(svcName, host string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byService, svcName)
	delete(r.byHost, host)
}

// clear drops every registration.
func (r *registry) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byHost = map[string]*target{}
	r.byService = map[string]*target{}
}

// lookup reports whether a host and a service are registered (tests).
func (r *registry) lookup(svcName, host string) (hostOK, svcOK bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, hostOK = r.byHost[host]
	_, svcOK = r.byService[svcName]
	return hostOK, svcOK
}

// serviceTarget returns the registered target for a service, or nil (tests).
func (r *registry) serviceTarget(svcName string) *target {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byService[svcName]
}

// size returns the number of host and service registrations (tests).
func (r *registry) size() (hosts, services int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byHost), len(r.byService)
}

// resolve maps a request to the registered revision target: by Host first (the
// primary data-plane path), then by explicit service for the legacy path form.
func (r *registry) resolve(req runcore.InvocationRequest) (*target, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if req.Host != "" {
		if t, ok := r.byHost[normalizeHost(req.Host)]; ok {
			return t, nil
		}
	}
	if req.Service.ID != "" {
		svcName := runcore.ServiceName(req.Service.ProjectID, req.Service.Location, req.Service.ID)
		if t, ok := r.byService[svcName]; ok {
			return t, nil
		}
		return nil, model.NewProviderError("Unavailable", "Cloud Run service has no ready runtime: "+req.Service.ID, 503)
	}
	if req.Host != "" && runcore.IsInvocationHost(req.Host) {
		return nil, model.NewProviderError("NotFound", "Cloud Run service not found for host: "+req.Host, 404)
	}
	return nil, model.NewProviderError("NotFound", "Cloud Run service not found", 404)
}

// proxyInvoke forwards a data-plane request to a resolved target and returns its
// raw HTTP response. Missing service → 404, no ready runtime → 503, dial
// failure → 502, timeout → 504.
func proxyInvoke(ctx context.Context, client *http.Client, reg *registry, req runcore.InvocationRequest) (runcore.Invocation, error) {
	tgt, err := reg.resolve(req)
	if err != nil {
		return runcore.Invocation{}, err
	}
	upstream := tgt.backend + "/" + strings.TrimLeft(req.Path, "/")
	if req.Query != "" {
		upstream += "?" + req.Query
	}
	body := bytes.NewReader(req.Body)
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, upstream, body)
	if err != nil {
		return runcore.Invocation{}, model.NewProviderError("InvalidArgument", "invalid invocation request: "+err.Error(), 400)
	}
	for k, v := range req.Headers {
		if isHopByHop(k) {
			continue
		}
		httpReq.Header.Set(k, v)
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) {
			return runcore.Invocation{}, model.NewProviderError("DeadlineExceeded", "Cloud Run runtime request timed out", 504)
		}
		return runcore.Invocation{}, model.NewProviderError("BadGateway", "Cloud Run runtime connection failed: "+err.Error(), 502)
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return runcore.Invocation{}, model.NewProviderError("BadGateway", "Cloud Run runtime read failed: "+readErr.Error(), 502)
	}
	headers := make(map[string]string, len(resp.Header))
	for k, vs := range resp.Header {
		if len(vs) == 0 || isHopByHop(k) {
			continue
		}
		headers[k] = vs[0]
	}
	return runcore.Invocation{Status: resp.StatusCode, Headers: headers, Body: respBody}, nil
}

// normalizeHost lowercases a Host[:port] and drops the port so an authority
// registered with a port still matches a request that omits it (and vice versa).
func normalizeHost(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return ""
	}
	if hostOnly, _, err := net.SplitHostPort(h); err == nil {
		return hostOnly
	}
	return h
}

func isHopByHop(header string) bool {
	switch http.CanonicalHeaderKey(header) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
		"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length":
		return true
	}
	return false
}

// tcpProbe reports whether addr accepts a TCP connection.
func tcpProbe(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
