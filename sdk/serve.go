package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Serve exposes a Plugin over the out-of-process HTTP/JSON transport.
func Serve(p Plugin) http.Handler {
	s := &server{p: p}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /manifest", s.manifest)
	mux.HandleFunc("POST /init", s.init)
	mux.HandleFunc("POST /handle", s.handle)
	return mux
}

// ListenAndServe serves p on addr until the process exits.
func ListenAndServe(addr string, p Plugin) error {
	return http.ListenAndServe(addr, Serve(p))
}

type server struct {
	p     Plugin
	mu    sync.RWMutex
	store *StoreEndpoint
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
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not initialised"})
		return
	}
	c := &Call{Phase: in.Phase, Req: in.Req, Resp: in.Resp, Err: in.Err,
		Store: &httpStore{url: st.URL, token: st.Token, rid: in.Req.RequestID, account: in.Req.AccountID, region: in.Req.Region}}
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
	url, token, rid, account, region string
}

var storeClient = &http.Client{Timeout: 10 * time.Second}

func (h *httpStore) call(ctx context.Context, op string, in StoreRequest) (StoreResponse, error) {
	in.RequestID, in.AccountID, in.Region = h.rid, h.account, h.region
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
	if resp.StatusCode != http.StatusOK {
		return StoreResponse{}, fmt.Errorf("store %s: HTTP %d: %s", op, resp.StatusCode, bytes.TrimSpace(raw))
	}
	var out StoreResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return StoreResponse{}, fmt.Errorf("store %s: %w", op, err)
	}
	return out, nil
}

func (h *httpStore) Get(ctx context.Context, kind, id string) (json.RawMessage, bool, error) {
	out, err := h.call(ctx, "get", StoreRequest{Kind: kind, ID: id})
	return out.Data, out.Found, err
}

func (h *httpStore) Put(ctx context.Context, kind, id string, data json.RawMessage) error {
	_, err := h.call(ctx, "put", StoreRequest{Kind: kind, ID: id, Data: data})
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
