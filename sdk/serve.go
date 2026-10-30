package sdk

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Option configures Serve / ListenAndServe.
type Option func(*server)

// WithSecret requires "Authorization: Bearer <secret>" on every request from
// the emulator (pass the same value to jaiscloud with --plugin-secret).
// Use it whenever the plugin listens beyond loopback.
func WithSecret(secret string) Option { return func(s *server) { s.secret = secret } }

// Serve exposes a Plugin over the out-of-process HTTP/JSON transport.
//
// Requests carrying an Origin header are refused and POSTs must be
// application/json, so a web page in the user's browser cannot drive the
// plugin (no preflight is ever answered).
func Serve(p Plugin, opts ...Option) http.Handler {
	s := &server{p: p}
	for _, o := range opts {
		o(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /manifest", s.manifest)
	mux.HandleFunc("POST /init", s.init)
	mux.HandleFunc("POST /handle", s.handle)
	mux.HandleFunc("POST /lifecycle", s.lifecycle)
	return s.guard(mux)
}

// ListenAndServe serves p on addr. An address without a host (":9100") binds
// loopback only; to listen on every interface say so ("0.0.0.0:9100") and
// set WithSecret.
func ListenAndServe(addr string, p Plugin, opts ...Option) error {
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			slog.Warn("sdk: plugin listens beyond loopback; use sdk.WithSecret", "addr", addr)
		}
	}
	return http.ListenAndServe(addr, Serve(p, opts...))
}

type server struct {
	p      Plugin
	secret string
	mu     sync.RWMutex
	store  *StoreEndpoint
}

func (s *server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") != "" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "browser requests are not allowed"})
			return
		}
		if s.secret != "" {
			tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(tok), []byte(s.secret)) != 1 {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "bad or missing plugin secret"})
				return
			}
		}
		if r.Method == http.MethodPost && !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) manifest(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.p.Manifest())
}

func (s *server) init(w http.ResponseWriter, r *http.Request) {
	var in InitRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if in.ABI != ABI {
		writeJSON(w, http.StatusConflict, map[string]string{"error": fmt.Sprintf("host ABI %d, plugin ABI %d", in.ABI, ABI)})
		return
	}
	s.mu.Lock()
	st := in.Store
	s.store = &st
	s.mu.Unlock()
	if err := s.p.Init(r.Context(), Host{Cloud: in.Host.Cloud, Version: in.Host.Version, Log: slog.Default()}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) lifecycle(w http.ResponseWriter, r *http.Request) {
	var in LifecycleRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if l, ok := s.p.(LifecycleListener); ok {
		l.OnLifecycle(r.Context(), in.Event)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprint("panic: ", rec)})
		}
	}()
	var in HandleRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Req == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad handle request"})
		return
	}
	s.mu.RLock()
	st := s.store
	s.mu.RUnlock()
	if st == nil {
		// The emulator re-sends /init when it sees this.
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not initialised"})
		return
	}
	c := &Call{Phase: in.Phase, Req: in.Req, Resp: in.Resp, Err: in.Err,
		Store: &httpStore{url: st.URL, token: st.Token, callID: in.Req.CallID, account: in.Req.AccountID, region: in.Req.Region}}
	res, err := s.p.Handle(r.Context(), c)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// httpStore implements Store over the emulator's store callbacks.
type httpStore struct {
	url, token, callID, account, region string
}

var storeClient = &http.Client{Timeout: 10 * time.Second}

func (h *httpStore) call(ctx context.Context, op string, in StoreRequest) (StoreResponse, error) {
	in.CallID, in.AccountID, in.Region = h.callID, h.account, h.region
	body, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url+"/"+op, bytes.NewReader(body))
	if err != nil {
		return StoreResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := storeClient.Do(req)
	if err != nil {
		return StoreResponse{}, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusConflict {
		return StoreResponse{}, ErrConflict
	}
	if resp.StatusCode != http.StatusOK {
		return StoreResponse{}, fmt.Errorf("store %s: HTTP %d: %s", op, resp.StatusCode, bytes.TrimSpace(raw))
	}
	var out StoreResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return StoreResponse{}, fmt.Errorf("store %s: %w", op, err)
	}
	return out, nil
}

func (h *httpStore) Get(ctx context.Context, kind, id string) (Entry, bool, error) {
	out, err := h.call(ctx, "get", StoreRequest{Kind: kind, ID: id})
	if err != nil || !out.Found || out.Entry == nil {
		return Entry{}, false, err
	}
	return *out.Entry, true, nil
}

func (h *httpStore) Put(ctx context.Context, kind, id string, data json.RawMessage) error {
	_, err := h.call(ctx, "put", StoreRequest{Kind: kind, ID: id, Data: data})
	return err
}

func (h *httpStore) PutIf(ctx context.Context, kind, id string, data json.RawMessage, ifVersion int64) error {
	_, err := h.call(ctx, "put", StoreRequest{Kind: kind, ID: id, Data: data, IfVersion: &ifVersion})
	return err
}

func (h *httpStore) Delete(ctx context.Context, kind, id string) error {
	_, err := h.call(ctx, "delete", StoreRequest{Kind: kind, ID: id})
	return err
}

func (h *httpStore) List(ctx context.Context, kind, idPrefix string) ([]Entry, error) {
	out, err := h.call(ctx, "list", StoreRequest{Kind: kind, IDPrefix: idPrefix})
	return out.Entries, err
}
