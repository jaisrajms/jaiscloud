package pluginhost

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"jaiscloud/internal/store"

	"github.com/go-chi/chi/v5"
	"github.com/jaisrajms/jaiscloud/sdk"
)

const storePath = "/_jaiscloud/plugin-store"

// Bounds on what a plugin can ask of the store, so its own bug cannot become a
// host-side 500 or a memory spike.
const (
	maxIDLen       = 1024
	maxListRows    = 10000
	maxListPayload = maxBody // total bytes of one list answer
)

// validID rejects ids the backends cannot all store (NUL breaks Postgres text).
func validID(s string) bool { return len(s) <= maxIDLen && !strings.ContainsRune(s, 0) }

var kindRe = sync.OnceValue(func() *regexp.Regexp { return regexp.MustCompile(`^[a-z0-9_-]+$`) })

// Routes mounts the store callbacks. Pass it to gateway.WithExtraRoutes; extra
// routes sit outside the persistence barrier. That is safe because a callback
// is only accepted while the plugin call that authorises it is in flight, and
// that call is itself inside its request's barrier read lock: end() waits for
// callbacks to drain before the request can finish.
func (h *Host) Routes(r chi.Router) {
	r.Post(storePath+"/{op}", h.storeHandler)
}

// resourceType maps a plugin-local kind to its ResourceStore type. Plugins can
// only ever address their own "plugin:<name>:*" types.
func resourceType(plugin, kind string) string { return "plugin:" + plugin + ":" + kind }

// ---- in-flight calls ------------------------------------------------------

// activeCall is one open call to a plugin. It authorises store callbacks bound
// to that plugin, account and region, until the call ends.
type activeCall struct {
	plugin          *plugin
	account, region string
	wg              sync.WaitGroup // callbacks currently executing
}

func (h *Host) begin(id string, p *plugin, account, region string) {
	h.mu.Lock()
	h.calls[id] = &activeCall{plugin: p, account: account, region: region}
	h.mu.Unlock()
}

// end revokes the call and waits for callbacks that already passed the gate,
// so no write can land after the request that caused it has finished.
func (h *Host) end(id string) {
	h.mu.Lock()
	c := h.calls[id]
	delete(h.calls, id)
	h.mu.Unlock()
	if c != nil {
		c.wg.Wait()
	}
}

func (h *Host) acquire(id string, p *plugin, account, region string) (*activeCall, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.calls[id]
	if c == nil || c.plugin != p || c.account != account || c.region != region {
		return nil, false
	}
	c.wg.Add(1)
	return c, true
}

func (h *Host) authenticate(r *http.Request) *plugin {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	for _, p := range h.plugins {
		if p.local != nil {
			continue // an in-process plugin has its Store directly; it has no callback credential
		}
		if subtle.ConstantTimeCompare([]byte(tok), []byte(p.token)) == 1 {
			return p
		}
	}
	return nil
}

// ---- versioned rows -------------------------------------------------------

// Rows are stored as {"v": version, "d": data} so every write bumps a counter
// the host controls (timestamps are useless as etags under a frozen clock).
type envelope struct {
	V int64           `json:"v"`
	D json.RawMessage `json:"d"`
}

func decodeEntry(e store.ResourceEntry) (sdk.Entry, error) {
	var env envelope
	if err := json.Unmarshal(e.Data, &env); err != nil || env.V == 0 {
		return sdk.Entry{}, fmt.Errorf("plugin row %s/%s is not a versioned envelope", e.Type, e.ID)
	}
	return sdk.Entry{ID: e.ID, Data: env.D, Version: env.V}, nil
}

var errConflict = errors.New("version conflict")

// ---- operations -----------------------------------------------------------

// storeErr is a store failure with the HTTP status the callback route answers it with.
type storeErr struct {
	status int
	msg    string
}

func (e *storeErr) Error() string { return e.msg }

func badStore(msg string) *storeErr { return &storeErr{status: http.StatusBadRequest, msg: msg} }

// storeOp runs one store operation for plugin p. The caller has already proved that
// the call is in flight (acquire); the HTTP callback route and the in-process Store
// both come through here, so a plugin sees the same rules either way.
func (h *Host) storeOp(ctx context.Context, p *plugin, op string, in sdk.StoreRequest) (sdk.StoreResponse, *storeErr) {
	if in.AccountID == "" || in.Region == "" || !kindRe().MatchString(in.Kind) {
		return sdk.StoreResponse{}, badStore("accountId, region and a valid kind are required")
	}
	if !validID(in.ID) || !validID(in.IDPrefix) {
		return sdk.StoreResponse{}, badStore(fmt.Sprintf("id and idPrefix must be at most %d bytes and contain no NUL", maxIDLen))
	}
	typ := resourceType(p.name, in.Kind)
	var out sdk.StoreResponse
	var err error
	switch op {
	case "get":
		var e store.ResourceEntry
		if e, err = h.resource.Get(ctx, in.AccountID, in.Region, typ, in.ID); err == nil {
			var ent sdk.Entry
			if ent, err = decodeEntry(e); err == nil {
				out = sdk.StoreResponse{Found: true, Entry: &ent}
			}
		} else if errors.Is(err, store.ErrNotFound) {
			out, err = sdk.StoreResponse{}, nil
		}
	case "put":
		// JSONB rejects the NUL escape, and every backend must accept the same data.
		if in.ID == "" || !json.Valid(in.Data) || bytes.Contains(in.Data, []byte(`\u0000`)) {
			return sdk.StoreResponse{}, badStore("put needs an id and valid JSON data without \\u0000")
		}
		_, err = h.resource.UpsertAtomic(ctx, in.AccountID, in.Region, typ, in.ID,
			func(cur store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
				var curV int64
				if exists {
					ent, derr := decodeEntry(cur)
					if derr != nil {
						return store.ResourceEntry{}, derr
					}
					curV = ent.Version
				}
				if in.IfVersion != nil && *in.IfVersion != curV {
					return store.ResourceEntry{}, errConflict
				}
				b, _ := json.Marshal(envelope{V: curV + 1, D: in.Data})
				return store.ResourceEntry{Type: typ, ID: in.ID, Data: b}, nil
			})
		if errors.Is(err, errConflict) {
			return sdk.StoreResponse{}, &storeErr{status: http.StatusConflict, msg: "version conflict"}
		}
	case "delete":
		if err = h.resource.Delete(ctx, in.AccountID, in.Region, typ, in.ID); errors.Is(err, store.ErrNotFound) {
			err = nil
		}
	case "list":
		// The stores match ids by substring (and SQL wildcards in Postgres), so
		// they return a superset; enforce the documented prefix contract here.
		var es []store.ResourceEntry
		if es, err = h.resource.List(ctx, in.AccountID, in.Region, typ, in.IDPrefix); err == nil {
			out.Entries = make([]sdk.Entry, 0, len(es))
			payload := 0
			for _, e := range es {
				if !strings.HasPrefix(e.ID, in.IDPrefix) {
					continue
				}
				var ent sdk.Entry
				if ent, err = decodeEntry(e); err != nil {
					break
				}
				if payload += len(ent.Data); len(out.Entries) >= maxListRows || payload > maxListPayload {
					return sdk.StoreResponse{}, &storeErr{status: http.StatusRequestEntityTooLarge, msg: fmt.Sprintf(
						"list matches more than %d rows or %d bytes; narrow it with a longer idPrefix", maxListRows, maxListPayload)}
				}
				out.Entries = append(out.Entries, ent)
			}
		}
	default:
		return sdk.StoreResponse{}, &storeErr{status: http.StatusNotFound, msg: "unknown op"}
	}
	if err != nil {
		return sdk.StoreResponse{}, &storeErr{status: http.StatusInternalServerError, msg: err.Error()}
	}
	return out, nil
}

// ---- HTTP route -----------------------------------------------------------

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
	call, ok := h.acquire(in.CallID, p, in.AccountID, in.Region)
	if !ok {
		http.Error(w, "callId is not an in-flight call of this plugin, account and region", http.StatusForbidden)
		return
	}
	defer call.wg.Done()
	out, serr := h.storeOp(r.Context(), p, chi.URLParam(r, "op"), in)
	if serr != nil {
		http.Error(w, serr.msg, serr.status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// ---- in-process Store -----------------------------------------------------

// localStore is the Store of an in-process plugin: the same operations as the HTTP
// callbacks, without the round trip, and gated the same way. It works only while the
// call that carries its id is in flight, so a plugin goroutine that outlives the call
// cannot write after the request that caused it has finished.
type localStore struct {
	h               *Host
	p               *plugin
	callID          string
	account, region string
}

func (s *localStore) run(ctx context.Context, op string, in sdk.StoreRequest) (sdk.StoreResponse, error) {
	call, ok := s.h.acquire(s.callID, s.p, s.account, s.region)
	if !ok {
		return sdk.StoreResponse{}, errors.New("store: the call that owns this Store has finished")
	}
	defer call.wg.Done()
	in.AccountID, in.Region = s.account, s.region
	out, serr := s.h.storeOp(ctx, s.p, op, in)
	switch {
	case serr == nil:
		return out, nil
	case serr.status == http.StatusConflict:
		return sdk.StoreResponse{}, sdk.ErrConflict
	}
	return sdk.StoreResponse{}, fmt.Errorf("store %s: %s", op, serr.msg)
}

func (s *localStore) Get(ctx context.Context, kind, id string) (sdk.Entry, bool, error) {
	out, err := s.run(ctx, "get", sdk.StoreRequest{Kind: kind, ID: id})
	if err != nil || !out.Found || out.Entry == nil {
		return sdk.Entry{}, false, err
	}
	return *out.Entry, true, nil
}

func (s *localStore) Put(ctx context.Context, kind, id string, data json.RawMessage) error {
	_, err := s.run(ctx, "put", sdk.StoreRequest{Kind: kind, ID: id, Data: data})
	return err
}

func (s *localStore) PutIf(ctx context.Context, kind, id string, data json.RawMessage, ifVersion int64) error {
	_, err := s.run(ctx, "put", sdk.StoreRequest{Kind: kind, ID: id, Data: data, IfVersion: &ifVersion})
	return err
}

func (s *localStore) Delete(ctx context.Context, kind, id string) error {
	_, err := s.run(ctx, "delete", sdk.StoreRequest{Kind: kind, ID: id})
	return err
}

func (s *localStore) List(ctx context.Context, kind, idPrefix string) ([]sdk.Entry, error) {
	out, err := s.run(ctx, "list", sdk.StoreRequest{Kind: kind, IDPrefix: idPrefix})
	return out.Entries, err
}
