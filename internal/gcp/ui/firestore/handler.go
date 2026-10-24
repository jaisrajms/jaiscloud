package firestoreui

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// defaultDatabase is the Firestore default database id.
const defaultDatabase = "(default)"

const documentsPrefix = "databases/" + defaultDatabase + "/documents"

// Handler serves Firestore UI API requests by calling the Firestore provider.
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

// ─── mapping helpers ─────────────────────────────────────────────────────────

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func lastSegment(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// segmentParam returns the decoded, single-segment value of a URL path
// parameter. A decoded '/' (from a %2F escape) is rejected: document IDs are
// single path segments, and Firestore forbids "." / ".." as an id.
func segmentParam(r *http.Request, key string) (string, bool) {
	v := uihelper.PathParam(r, key)
	if v == "" || strings.Contains(v, "/") || v == "." || v == ".." {
		return "", false
	}
	return v, true
}

// collectionParam returns the decoded collection path of a URL path parameter.
// A collection path is the alternating collection/document/collection sequence
// of a nested subcollection, e.g. "users/alice/orders"; a root collection is a
// single segment. The value is URL-encoded by the caller (%2F for '/'), so chi
// sees it as one path segment and uihelper.PathParam decodes it back.
//
// The path is validated: it must have an odd number of non-empty segments
// (ending on a collection id) and must not contain "." or "..".
func collectionParam(r *http.Request, key string) (string, bool) {
	value := uihelper.PathParam(r, key)
	if value == "" || strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") {
		return "", false
	}
	segments := strings.Split(value, "/")
	if len(segments)%2 == 0 {
		return "", false
	}
	for _, s := range segments {
		if s == "" || s == "." || s == ".." {
			return "", false
		}
	}
	return value, true
}

// documentFromMap converts a provider REST document map into the UI shape. The
// raw Firestore value encoding is passed through unchanged.
func documentFromMap(m map[string]any, collection string) Document {
	full := str(m, "name")
	fields, _ := m["fields"].(map[string]any)
	if fields == nil {
		fields = map[string]any{}
	}
	createTime := str(m, "createTime")
	updateTime := str(m, "updateTime")
	return Document{
		ID:         lastSegment(full),
		Name:       full,
		Collection: collection,
		Fields:     fields,
		CreateTime: createTime,
		UpdateTime: updateTime,
		// A returned document with neither timestamp has no underlying
		// document — it is a showMissing placeholder.
		Missing: createTime == "" && updateTime == "",
	}
}

// decodeBody decodes exactly one JSON object from the request body, rejecting
// trailing data. Numbers are left as json.Number for pass-through fidelity.
func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("unexpected trailing data in request body")
	}
	return nil
}

func (h *Handler) collectionPath(collection string) string {
	return documentsPrefix + "/" + collection
}

func (h *Handler) documentPath(collection, document string) string {
	return h.collectionPath(collection) + "/" + document
}

// ─── Collections ─────────────────────────────────────────────────────────────

// GET /collections
func (h *Handler) ListCollections(w http.ResponseWriter, r *http.Request) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.ListCollectionIds", "global", account)
	nr.Params["name"] = documentsPrefix
	pageParams(r, nr.Params)

	resp, err := h.provider.ListCollectionIds(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	uihelper.WriteJSON(w, collectionsFromResponse(resp))
}

// collectionsFromResponse maps a provider ListCollectionIds response into the UI
// shape. Shared by the root collections and subcollections listings.
func collectionsFromResponse(resp *model.ProviderResponse) ListCollectionsResponse {
	raw := uihelper.AsSlice(resp.Data["collectionIds"])
	collections := make([]Collection, 0, len(raw))
	for _, item := range raw {
		if id, ok := item.(string); ok {
			collections = append(collections, Collection{ID: id})
		}
	}
	next, _ := resp.Data["nextPageToken"].(string)
	return ListCollectionsResponse{Collections: collections, Total: len(collections), NextPageToken: next}
}

// ─── Documents ───────────────────────────────────────────────────────────────

// GET /collections/{collection}/documents
func (h *Handler) ListDocuments(w http.ResponseWriter, r *http.Request) {
	collection, ok := collectionParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection path", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.ListDocuments", "global", account)
	nr.Params["name"] = h.collectionPath(collection)
	nr.Params["showMissing"] = "true"
	pageParams(r, nr.Params)

	resp, err := h.provider.ListDocuments(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["documents"])
	docs := make([]Document, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			docs = append(docs, documentFromMap(m, collection))
		}
	}
	next, _ := resp.Data["nextPageToken"].(string)
	uihelper.WriteJSON(w, ListDocumentsResponse{Documents: docs, Total: len(docs), NextPageToken: next})
}

// ─── Subcollections ──────────────────────────────────────────────────────────

// GET /collections/{collection}/documents/{document}/collections
// Lists the subcollection IDs of a document. {collection} may itself be nested.
func (h *Handler) ListSubcollections(w http.ResponseWriter, r *http.Request) {
	collection, ok := collectionParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection path", http.StatusBadRequest)
		return
	}
	document, ok := segmentParam(r, "document")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid document id", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.ListCollectionIds", "global", account)
	nr.Params["name"] = h.documentPath(collection, document)
	pageParams(r, nr.Params)

	resp, err := h.provider.ListCollectionIds(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, collectionsFromResponse(resp))
}

// POST /collections/{collection}/documents  body: { documentId?, fields }
func (h *Handler) CreateDocument(w http.ResponseWriter, r *http.Request) {
	var req CreateDocumentRequest
	if err := decodeBody(r, &req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	collection, ok := collectionParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection path", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.CreateDocument", "global", account)
	nr.Params["name"] = h.collectionPath(collection)
	if req.DocumentID != "" {
		if strings.Contains(req.DocumentID, "/") {
			uihelper.UIError(w, "BadRequest", "document id must not contain '/'", http.StatusBadRequest)
			return
		}
		nr.Params["documentId"] = req.DocumentID
	}
	nr.Params["body"] = map[string]any{"fields": req.Fields}

	resp, err := h.provider.CreateDocument(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, documentFromMap(resp.Data, collection))
}

// GET /collections/{collection}/documents/{document}
func (h *Handler) GetDocument(w http.ResponseWriter, r *http.Request) {
	collection, ok := collectionParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection path", http.StatusBadRequest)
		return
	}
	document, ok := segmentParam(r, "document")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid document id", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.GetDocument", "global", account)
	nr.Params["name"] = h.documentPath(collection, document)

	resp, err := h.provider.DocumentsGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, documentFromMap(resp.Data, collection))
}

// PATCH /collections/{collection}/documents/{document}  body: { fields, updateTime? }
func (h *Handler) UpdateDocument(w http.ResponseWriter, r *http.Request) {
	var req UpdateDocumentRequest
	if err := decodeBody(r, &req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Fields == nil {
		uihelper.UIError(w, "BadRequest", "fields is required", http.StatusBadRequest)
		return
	}
	collection, ok := collectionParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection path", http.StatusBadRequest)
		return
	}
	document, ok := segmentParam(r, "document")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid document id", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.PatchDocument", "global", account)
	nr.Params["name"] = h.documentPath(collection, document)
	// No updateMask: the console editor holds the whole document, so the body
	// fields fully replace the stored fields (provider applyMask semantics).
	nr.Params["body"] = map[string]any{"fields": req.Fields}
	// Optimistic concurrency: fail rather than overwrite a concurrently changed
	// (or deleted) document when the caller passes the observed updateTime.
	if req.UpdateTime != "" {
		nr.Params["currentDocument.updateTime"] = req.UpdateTime
	}

	resp, err := h.provider.DocumentsPatch(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, documentFromMap(resp.Data, collection))
}

// DELETE /collections/{collection}/documents/{document}
func (h *Handler) DeleteDocument(w http.ResponseWriter, r *http.Request) {
	collection, ok := collectionParam(r, "collection")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection path", http.StatusBadRequest)
		return
	}
	document, ok := segmentParam(r, "document")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid document id", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.DeleteDocument", "global", account)
	nr.Params["name"] = h.documentPath(collection, document)

	if _, err := h.provider.DocumentsDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Query ───────────────────────────────────────────────────────────────────

// POST /query  body: { scope?, structuredQuery }
func (h *Handler) RunQuery(w http.ResponseWriter, r *http.Request) {
	var req RunQueryRequest
	if err := decodeBody(r, &req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.StructuredQuery == nil {
		uihelper.UIError(w, "BadRequest", "structuredQuery is required", http.StatusBadRequest)
		return
	}
	scope, ok := queryScope(req.Scope)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid scope path", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.RunQuery", "global", account)
	name := documentsPrefix
	if scope != "" {
		name += "/" + scope
	}
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"structuredQuery": req.StructuredQuery}

	resp, err := h.provider.RunQuery(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, queryResponseFromRaw(resp.Data))
}

// queryScope validates and normalises the RunQuery parent scope. A scope is a
// document path relative to the database documents root: empty (the whole
// database) or an even number of non-empty segments (collection/document/…/
// document). The leading/trailing '/' is tolerated.
func queryScope(raw string) (string, bool) {
	scope := strings.Trim(raw, "/")
	if scope == "" {
		return "", true
	}
	segments := strings.Split(scope, "/")
	for _, s := range segments {
		if s == "" || s == "." || s == ".." {
			return "", false
		}
	}
	if len(segments)%2 != 0 {
		return "", false
	}
	return scope, true
}

// queryResponseFromRaw maps the provider's newline-delimited runQuery response
// into the UI shape. The provider returns one `{document, readTime}` JSON line
// per result followed by a `{done:true, skippedResults?}` line. A malformed
// line is skipped: the provider is in-process and trusted, and a partial result
// set is more useful to the console than a hard failure.
func queryResponseFromRaw(data map[string]any) RunQueryResponse {
	out := RunQueryResponse{Documents: []Document{}}
	raw, _ := data[wire.RawJSONKey].(json.RawMessage)
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var item struct {
			Document       map[string]any `json:"document"`
			ReadTime       string         `json:"readTime"`
			Done           bool           `json:"done"`
			SkippedResults int            `json:"skippedResults"`
		}
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			continue
		}
		if item.Done {
			out.SkippedResults = item.SkippedResults
			continue
		}
		if item.Document == nil {
			continue
		}
		if item.ReadTime != "" {
			out.ReadTime = item.ReadTime
		}
		out.Documents = append(out.Documents,
			documentFromMap(item.Document, collectionOfDocumentName(str(item.Document, "name"))))
	}
	return out
}

// collectionOfDocumentName derives the collection path of a query result from
// its full document name. Unlike ListDocuments, a query (especially a
// collection-group query) can return documents from different collections, so
// the collection is read per document rather than passed in.
func collectionOfDocumentName(full string) string {
	const marker = "/documents/"
	i := strings.Index(full, marker)
	if i < 0 {
		return ""
	}
	rel := full[i+len(marker):]
	j := strings.LastIndexByte(rel, '/')
	if j < 0 {
		return ""
	}
	return rel[:j]
}

// ─── Indexes ─────────────────────────────────────────────────────────────────

// indexWildcard is the AIP-123 collection-group wildcard: list indexes across
// every collection group of the database.
const indexWildcard = "-"

// indexParentPath is the collection-group index collection name (relative to the
// project) that the provider expects. A concrete group scopes to that group's
// indexes; "-" lists the whole database.
func indexParentPath(collectionGroup string) string {
	return "databases/" + defaultDatabase + "/collectionGroups/" + collectionGroup + "/indexes"
}

func indexResourceName(collectionGroup, id string) string {
	return indexParentPath(collectionGroup) + "/" + id
}

// validIndexGroup reports whether collectionGroup is usable as a single path
// segment. The "-" wildcard is only valid for the list call.
func validIndexGroup(collectionGroup string, allowWildcard bool) bool {
	if collectionGroup == "" || strings.Contains(collectionGroup, "/") ||
		collectionGroup == "." || collectionGroup == ".." {
		return false
	}
	if collectionGroup == indexWildcard {
		return allowWildcard
	}
	return true
}

// collectionGroupOfIndexName extracts the collection group from a fully
// qualified index name
// (projects/{p}/databases/{db}/collectionGroups/{cg}/indexes/{id}).
func collectionGroupOfIndexName(full string) string {
	const marker = "/collectionGroups/"
	i := strings.Index(full, marker)
	if i < 0 {
		return ""
	}
	rest := full[i+len(marker):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		return rest[:j]
	}
	return rest
}

// indexFromMap converts a provider REST index map into the UI shape.
func indexFromMap(m map[string]any) Index {
	name := str(m, "name")
	rawFields := uihelper.AsSlice(m["fields"])
	fields := make([]IndexField, 0, len(rawFields))
	for _, item := range rawFields {
		f, ok := item.(map[string]any)
		if !ok {
			continue
		}
		fields = append(fields, IndexField{
			FieldPath:   str(f, "fieldPath"),
			Order:       str(f, "order"),
			ArrayConfig: str(f, "arrayConfig"),
		})
	}
	return Index{
		Name:            name,
		ID:              lastSegment(name),
		CollectionGroup: collectionGroupOfIndexName(name),
		QueryScope:      str(m, "queryScope"),
		State:           str(m, "state"),
		Fields:          fields,
	}
}

func indexesFromResponse(resp *model.ProviderResponse) ListIndexesResponse {
	raw := uihelper.AsSlice(resp.Data["indexes"])
	indexes := make([]Index, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			indexes = append(indexes, indexFromMap(m))
		}
	}
	next, _ := resp.Data["nextPageToken"].(string)
	return ListIndexesResponse{Indexes: indexes, Total: len(indexes), NextPageToken: next}
}

// GET /indexes?collectionGroup=&filter=&pageSize=&pageToken=
func (h *Handler) ListIndexes(w http.ResponseWriter, r *http.Request) {
	collectionGroup := r.URL.Query().Get("collectionGroup")
	if collectionGroup == "" {
		collectionGroup = indexWildcard
	}
	if !validIndexGroup(collectionGroup, true) {
		uihelper.UIError(w, "BadRequest", "invalid collection group", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.ListIndexes", "global", account)
	nr.Params["name"] = indexParentPath(collectionGroup)
	if filter := r.URL.Query().Get("filter"); filter != "" {
		nr.Params["filter"] = filter
	}
	pageParams(r, nr.Params)

	resp, err := h.provider.ListIndexes(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, indexesFromResponse(resp))
}

// POST /indexes  body: { collectionGroup, queryScope?, fields }
func (h *Handler) CreateIndex(w http.ResponseWriter, r *http.Request) {
	var req CreateIndexRequest
	if err := decodeBody(r, &req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if !validIndexGroup(req.CollectionGroup, false) {
		uihelper.UIError(w, "BadRequest", "a concrete collection group is required", http.StatusBadRequest)
		return
	}
	if len(req.Fields) < 2 {
		uihelper.UIError(w, "BadRequest", "a composite index requires at least 2 fields", http.StatusBadRequest)
		return
	}
	for _, f := range req.Fields {
		if f.FieldPath == "" {
			uihelper.UIError(w, "BadRequest", "each index field needs a field path", http.StatusBadRequest)
			return
		}
		if f.Order == "" && f.ArrayConfig == "" {
			uihelper.UIError(w, "BadRequest", "each index field needs an order or array config", http.StatusBadRequest)
			return
		}
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.CreateIndex", "global", account)
	nr.Params["name"] = indexParentPath(req.CollectionGroup)
	body := map[string]any{"fields": req.Fields}
	if req.QueryScope != "" {
		body["queryScope"] = req.QueryScope
	}
	nr.Params["body"] = body

	resp, err := h.provider.CreateIndex(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	// The provider wraps the created index in a done google.longrunning.Operation;
	// the console shows the settled index, so unwrap the response.
	created, ok := resp.Data["response"].(map[string]any)
	if !ok {
		uihelper.UIError(w, "InternalError", "index creation returned no index", http.StatusInternalServerError)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, indexFromMap(created))
}

// GET /indexes/{collectionGroup}/{indexId}
func (h *Handler) GetIndex(w http.ResponseWriter, r *http.Request) {
	collectionGroup, ok := indexedGroupParam(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection group", http.StatusBadRequest)
		return
	}
	id, ok := segmentParam(r, "indexId")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid index id", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.GetIndex", "global", account)
	nr.Params["name"] = indexResourceName(collectionGroup, id)

	resp, err := h.provider.GetIndex(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, indexFromMap(resp.Data))
}

// DELETE /indexes/{collectionGroup}/{indexId}
func (h *Handler) DeleteIndex(w http.ResponseWriter, r *http.Request) {
	collectionGroup, ok := indexedGroupParam(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid collection group", http.StatusBadRequest)
		return
	}
	id, ok := segmentParam(r, "indexId")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid index id", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firestore", "Firestore.DeleteIndex", "global", account)
	nr.Params["name"] = indexResourceName(collectionGroup, id)

	if _, err := h.provider.DeleteIndex(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// indexedGroupParam reads and validates a concrete (non-wildcard) collection
// group path parameter.
func indexedGroupParam(r *http.Request) (string, bool) {
	cg, ok := segmentParam(r, "collectionGroup")
	if !ok || !validIndexGroup(cg, false) {
		return "", false
	}
	return cg, true
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// pageParams copies pageSize/pageToken query parameters onto the request params.
func pageParams(r *http.Request, nr map[string]any) {
	if v := r.URL.Query().Get("pageSize"); v != "" {
		nr["pageSize"] = v
	}
	if v := r.URL.Query().Get("pageToken"); v != "" {
		nr["pageToken"] = v
	}
}
