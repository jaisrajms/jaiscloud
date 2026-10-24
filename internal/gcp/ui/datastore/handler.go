package datastoreui

import (
	"encoding/json"
	"net/http"
	"strconv"

	"jaiscloud/internal/config"
	core "jaiscloud/internal/gcp/service/datastore"
	dsstore "jaiscloud/internal/gcp/store/datastore"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// defaultPageSize and maxPageSize bound the entities list page size.
const (
	defaultPageSize = 25
	maxPageSize     = 1000
)

// Handler serves Datastore UI API requests by calling the Datastore core.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// ─── Kinds / properties ───────────────────────────────────────────────────────

// GET /kinds?namespace=&database=
func (h *Handler) ListKinds(w http.ResponseWriter, r *http.Request) {
	q := &core.Query{
		Kind:      metadataKindKind,
		Namespace: r.URL.Query().Get("namespace"),
		Database:  r.URL.Query().Get("database"),
	}
	res, err := h.provider.RunQuery(r.Context(), h.account(r), q, nil)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	kinds := make([]Kind, 0, len(res.Entities))
	for _, er := range res.Entities {
		if k, ok := core.KeyFromCanonical(er.Entity.Key); ok {
			kinds = append(kinds, Kind{Name: k.Name})
		}
	}
	uihelper.WriteJSON(w, ListKindsResponse{Kinds: kinds, Total: len(kinds)})
}

// GET /kinds/{kind}/entities?namespace=&database=&pageSize=&pageToken=
func (h *Handler) ListEntities(w http.ResponseWriter, r *http.Request) {
	kind := uihelper.PathParam(r, "kind")
	if kind == "" {
		uihelper.UIError(w, "BadRequest", "kind is required", http.StatusBadRequest)
		return
	}
	offset, err := offsetFromToken(r.URL.Query().Get("pageToken"))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	limit := uihelper.PageSizeFrom(r, defaultPageSize, maxPageSize)
	q := &core.Query{
		Kind:      kind,
		Namespace: r.URL.Query().Get("namespace"),
		Database:  r.URL.Query().Get("database"),
		Offset:    offset,
		Limit:     &limit,
	}
	res, err := h.provider.RunQuery(r.Context(), h.account(r), q, nil)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	entities := coreEntitiesToUI(res.Entities)
	next := ""
	if res.MoreResults == core.MoreResultsAfterLimit {
		next = strconv.Itoa(offset + len(entities))
	}
	uihelper.WriteJSON(w, ListEntitiesResponse{Entities: entities, Total: len(entities), NextPageToken: next})
}

// GET /kinds/{kind}/properties?namespace=&database=
func (h *Handler) ListProperties(w http.ResponseWriter, r *http.Request) {
	kind := uihelper.PathParam(r, "kind")
	if kind == "" {
		uihelper.UIError(w, "BadRequest", "kind is required", http.StatusBadRequest)
		return
	}
	ancestor := core.CanonicalKey(core.Key{Kind: metadataKindKind, Name: kind, HasName: true})
	q := &core.Query{
		Kind:      metadataKindProperty,
		Namespace: r.URL.Query().Get("namespace"),
		Database:  r.URL.Query().Get("database"),
		Filter: &core.Filter{Property: &core.PropertyFilter{
			Property: keyProperty,
			Op:       core.PropertyHasAncestor,
			Value:    dsstore.Value{KeyValue: &ancestor},
		}},
	}
	res, err := h.provider.RunQuery(r.Context(), h.account(r), q, nil)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	props := make([]Property, 0, len(res.Entities))
	for _, er := range res.Entities {
		k, ok := core.KeyFromCanonical(er.Entity.Key)
		if !ok {
			continue
		}
		reps := []string{}
		if arr := er.Entity.Properties[propertyRepresentation].ArrayValue; arr != nil {
			for _, v := range arr.Values {
				if v.StringValue != nil {
					reps = append(reps, *v.StringValue)
				}
			}
		}
		props = append(props, Property{Name: k.Name, Representations: reps})
	}
	uihelper.WriteJSON(w, ListPropertiesResponse{Properties: props, Total: len(props)})
}

// ─── Entity CRUD ──────────────────────────────────────────────────────────────

// GET /entity?key=<KeyRef JSON>
func (h *Handler) GetEntity(w http.ResponseWriter, r *http.Request) {
	ref, err := keyRefFromQuery(r)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	k, err := keyRefToCore(ref)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	if !k.Complete() {
		uihelper.WriteError(w, invalidArgument("lookup key is incomplete"))
		return
	}
	res, err := h.provider.Lookup(r.Context(), h.account(r), []core.Key{k}, nil)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	if len(res.Found) == 0 {
		uihelper.UIError(w, "NotFound", "entity not found", http.StatusNotFound)
		return
	}
	uihelper.WriteJSON(w, entityFromCore(res.Found[0].Entity))
}

// PUT /entity  body: { key, properties }
func (h *Handler) UpsertEntity(w http.ResponseWriter, r *http.Request) {
	var req UpsertEntityRequest
	if err := decodeBody(r, &req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if len(req.Key.Path) == 0 {
		uihelper.WriteError(w, invalidArgument("key is required"))
		return
	}
	k, err := keyRefToCore(req.Key)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	e, err := entityToCore(Entity{Properties: req.Properties})
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	account := h.account(r)
	committed, err := h.provider.Commit(r.Context(), account, &core.CommitRequest{
		Mutations: []core.Mutation{{Op: core.MutationUpsert, Key: k, Entity: e}},
	})
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	resolved := k
	if len(committed.Results) > 0 && committed.Results[0].Key != nil {
		resolved = *committed.Results[0].Key
	}
	lookup, err := h.provider.Lookup(r.Context(), account, []core.Key{resolved}, nil)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	if len(lookup.Found) == 0 {
		uihelper.UIError(w, "InternalError", "entity was not persisted", http.StatusInternalServerError)
		return
	}
	uihelper.WriteJSON(w, entityFromCore(lookup.Found[0].Entity))
}

// DELETE /entity?key=<KeyRef JSON>
func (h *Handler) DeleteEntity(w http.ResponseWriter, r *http.Request) {
	ref, err := keyRefFromQuery(r)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	k, err := keyRefToCore(ref)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	if !k.Complete() {
		uihelper.WriteError(w, invalidArgument("delete key is incomplete"))
		return
	}
	if _, err := h.provider.Commit(r.Context(), h.account(r), &core.CommitRequest{
		Mutations: []core.Mutation{{Op: core.MutationDelete, DeleteKey: k}},
	}); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Query ────────────────────────────────────────────────────────────────────

// POST /query  body: { queryString, namespace?, database?, allowLiterals? }
func (h *Handler) RunQuery(w http.ResponseWriter, r *http.Request) {
	var req GQLQueryRequest
	if err := decodeBody(r, &req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueryString == "" {
		uihelper.WriteError(w, invalidArgument("queryString is required"))
		return
	}
	res, err := h.provider.RunQueryGQL(r.Context(), h.account(r),
		core.GQLQuery{QueryString: req.QueryString, AllowLiterals: req.AllowLiterals},
		nil, req.Namespace, req.Database)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	entities := coreEntitiesToUI(res.Entities)
	uihelper.WriteJSON(w, QueryResponse{
		Entities:       entities,
		Total:          len(entities),
		SkippedResults: res.Skipped,
		MoreResults:    res.MoreResults == core.MoreResultsAfterLimit,
	})
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func coreEntitiesToUI(ers []core.EntityResult) []Entity {
	out := make([]Entity, 0, len(ers))
	for _, er := range ers {
		out = append(out, entityFromCore(er.Entity))
	}
	return out
}

// offsetFromToken parses the entities list cursor, which is the decimal offset
// into the kind's (key-sorted) entity list. An empty token is offset 0.
func offsetFromToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(token)
	if err != nil || n < 0 {
		return 0, invalidArgument("invalid page token")
	}
	return n, nil
}

// keyRefFromQuery reads the ?key= JSON KeyRef parameter.
func keyRefFromQuery(r *http.Request) (KeyRef, error) {
	raw := r.URL.Query().Get("key")
	if raw == "" {
		return KeyRef{}, invalidArgument("key is required")
	}
	var ref KeyRef
	if err := json.Unmarshal([]byte(raw), &ref); err != nil {
		return KeyRef{}, invalidArgument("invalid key")
	}
	return ref, nil
}

// decodeBody decodes exactly one JSON object from the request body.
func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}
