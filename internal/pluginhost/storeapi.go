package pluginhost

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"jaiscloud/internal/store"

	"github.com/go-chi/chi/v5"
	"github.com/jaisrajms/jaiscloud/sdk"
)

const storePath = "/_jaiscloud/plugin-store"

var kindRe = regexp.MustCompile(`^[a-z0-9_-]+$`)

// Routes mounts the store callbacks. Pass it to gateway.WithExtraRoutes; extra
// routes sit outside the persistence barrier, which is intentional — a plugin
// call is already inside its own request's read lock.
func (h *Host) Routes(r chi.Router) {
	r.Post(storePath+"/{op}", h.storeHandler)
}

// resourceType maps a plugin-local kind to its ResourceStore type. Plugins can
// only ever address their own "plugin:<name>:*" types.
func resourceType(plugin, kind string) string { return "plugin:" + plugin + ":" + kind }

func (h *Host) authenticate(r *http.Request) *plugin {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	for _, p := range h.plugins {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(p.token)) == 1 {
			return p
		}
	}
	return nil
}

func (h *Host) storeHandler(w http.ResponseWriter, r *http.Request) {
	p := h.authenticate(r)
	if p == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var in sdk.StoreRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&in); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if !h.inflight.has(in.RequestID) {
		http.Error(w, "requestId is not an in-flight call", http.StatusForbidden)
		return
	}
	if in.AccountID == "" || in.Region == "" || !kindRe.MatchString(in.Kind) {
		http.Error(w, "accountId, region and a valid kind are required", http.StatusBadRequest)
		return
	}
	typ := resourceType(p.name, in.Kind)
	ctx := r.Context()
	var out sdk.StoreResponse
	var err error
	switch chi.URLParam(r, "op") {
	case "get":
		var e store.ResourceEntry
		if e, err = h.resource.Get(ctx, in.AccountID, in.Region, typ, in.ID); err == nil {
			out = sdk.StoreResponse{Found: true, Data: e.Data}
		} else if errors.Is(err, store.ErrNotFound) {
			out, err = sdk.StoreResponse{}, nil
		}
	case "put":
		if in.ID == "" || !json.Valid(in.Data) {
			http.Error(w, "put needs an id and valid JSON data", http.StatusBadRequest)
			return
		}
		err = h.resource.Upsert(ctx, in.AccountID, in.Region, store.ResourceEntry{Type: typ, ID: in.ID, Data: in.Data})
	case "delete":
		if err = h.resource.Delete(ctx, in.AccountID, in.Region, typ, in.ID); errors.Is(err, store.ErrNotFound) {
			err = nil
		}
	case "list":
		var es []store.ResourceEntry
		if es, err = h.resource.List(ctx, in.AccountID, in.Region, typ, in.IDPrefix); err == nil {
			out.Entries = make([]sdk.Entry, 0, len(es))
			for _, e := range es {
				out.Entries = append(out.Entries, sdk.Entry{ID: e.ID, Data: e.Data})
			}
		}
	default:
		http.Error(w, "unknown op", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
