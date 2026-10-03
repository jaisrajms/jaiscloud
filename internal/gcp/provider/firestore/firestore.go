// Package firestore implements the Cloud Firestore provider (document CRUD) on
// top of the dedicated FirestoreStore. This is the DynamoDB analogue: documents
// are schemaless, and collections are implicit path segments (no CreateTable/DDL).
//
// The domain logic lives in the transport-agnostic Service (service.go); this
// file holds the REST adapter (Provider) plus the shared helpers that the
// Service reuses. A future gRPC handler will call the same Service methods.
package firestore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	// maxDocumentSize is the Firestore per-document limit (1 MiB).
	maxDocumentSize = 1 << 20 // 1,048,576 bytes
	// maxFieldNameBytes is the per-field-name byte limit (document.proto).
	maxFieldNameBytes = 1500
	// maxStringBytes is the per-stringValue/bytesValue byte limit (1 MiB − 89).
	maxStringBytes = (1 << 20) - 89 // 1,048,487 bytes
	// autoIDLength is the length of a server-generated document ID.
	autoIDLength = 20
)

// autoIDAlphabet is the alphabet Firestore uses for auto-generated document IDs.
const autoIDAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// reservedNameRe matches reserved field names / document IDs (^__.*__$).
var reservedNameRe = regexp.MustCompile(`^__.*__$`)

// Provider is the Firestore documents REST adapter. It embeds the shared
// *Service so the REST surface and the future gRPC handler share one document
// store, resource store, and transaction read-set registry.
type Provider struct {
	*Service
}

// New returns a Firestore provider backed by the given document store and
// control-plane resource store (for composite indexes).
func New(s firestorestore.FirestoreStore, resources store.ResourceStore) *Provider {
	return &Provider{Service: newService(s, resources)}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Firestore.GetDocument":       p.DocumentsGet,
		"Firestore.CreateDocument":    p.CreateDocument,
		"Firestore.PatchDocument":     p.DocumentsPatch,
		"Firestore.DeleteDocument":    p.DocumentsDelete,
		"Firestore.ListDocuments":     p.ListDocuments,
		"Firestore.ListCollectionIds": p.ListCollectionIds,
		"Firestore.RunQuery":          p.RunQuery,
		"Firestore.Commit":            p.Commit,
		"Firestore.BatchWrite":        p.BatchWrite,
		"Firestore.BatchGet":          p.BatchGet,
		"Firestore.BeginTransaction":  p.BeginTransaction,
		"Firestore.Rollback":          p.Rollback,
		"Firestore.CreateIndex":       p.CreateIndex,
		"Firestore.ListIndexes":       p.ListIndexes,
		"Firestore.GetIndex":          p.GetIndex,
		"Firestore.DeleteIndex":       p.DeleteIndex,
	}
}

// ─── REST-boundary helpers ────────────────────────────────────────────────────

// relativeName returns the database id and relative document path encoded in
// nr.Params["name"] ("databases/{db}/documents/{path}").
func relativeName(nr *model.NormalizedRequest) (database, path string, err error) {
	name, _ := nr.Params["name"].(string)
	parts := strings.Split(name, "/")
	if len(parts) < 3 || parts[0] != "databases" || parts[2] != "documents" {
		return "", "", model.NewProviderError("InvalidArgument", "invalid document path", 400)
	}
	return parts[1], strings.Join(parts[3:], "/"), nil
}

// decodeBody round-trips nr.Params["body"] into a typed struct.
func decodeBody(nr *model.NormalizedRequest, v any) error {
	body, _ := nr.Params["body"]
	if body == nil {
		body = map[string]any{}
	}
	b, err := json.Marshal(body)
	if err != nil {
		return model.NewProviderError("InvalidArgument", "malformed request body", 400)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return model.NewProviderError("InvalidArgument", "malformed request body", 400)
	}
	return nil
}

// encodeTxn standard-base64-encodes transaction bytes for the wire.
func encodeTxn(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// decodeTxn standard-base64-decodes a wire transaction string to raw bytes.
// An empty string means "no transaction" and returns (nil, nil). Malformed
// base64 is rejected with a 400 INVALID_ARGUMENT error (matching real
// Firestore, which rejects an invalid `transaction` format:byte field).
func decodeTxn(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "invalid transaction token", 400)
	}
	return b, nil
}

// decodeTxnParam standard-base64-decodes the `transaction` query parameter.
func decodeTxnParam(nr *model.NormalizedRequest) ([]byte, error) {
	s, _ := nr.Params["transaction"].(string)
	return decodeTxn(s)
}

// pageFromNR extracts cursor-pagination inputs from the request query params.
func pageFromNR(nr *model.NormalizedRequest) pageParams {
	token, _ := nr.Params["pageToken"].(string)
	return pageParams{size: paging.PageSize(nr.Params), token: token}
}

// extractFields extracts the document fields from a decoded request body.
func extractFields(body map[string]any) (map[string]*firestorestore.Value, error) {
	if body == nil {
		return map[string]*firestorestore.Value{}, nil
	}
	raw, ok := body["fields"]
	if !ok || raw == nil {
		return map[string]*firestorestore.Value{}, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "malformed fields", 400)
	}
	var fields map[string]*firestorestore.Value
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, model.NewProviderError("InvalidArgument", "malformed field value", 400)
	}
	return fields, nil
}

// documentMap serialises a store Document to its REST wire form.
func documentMap(d firestorestore.Document) map[string]any {
	b, _ := json.Marshal(d)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// queryFieldPaths returns the field paths for a repeated query parameter (e.g.
// "mask.fieldPaths", "updateMask.fieldPaths"), honouring repeated and
// comma-separated values.
func queryFieldPaths(nr *model.NormalizedRequest, key string) []string {
	var paths []string
	add := func(v string) {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				paths = append(paths, part)
			}
		}
	}
	if nr.Raw != nil {
		for _, v := range nr.Raw.URL.Query()[key] {
			add(v)
		}
	}
	if len(paths) == 0 {
		if v, _ := nr.Params[key].(string); v != "" {
			add(v)
		}
	}
	return paths
}

// updateMaskPaths returns the updateMask field paths, honouring repeated and
// comma-separated updateMask.fieldPaths query parameters.
func updateMaskPaths(nr *model.NormalizedRequest) []string {
	return queryFieldPaths(nr, "updateMask.fieldPaths")
}

// maskFieldPaths returns the mask field paths (mask.fieldPaths query parameter).
func maskFieldPaths(nr *model.NormalizedRequest) []string {
	return queryFieldPaths(nr, "mask.fieldPaths")
}

// preconditionFromQuery parses the currentDocument precondition query parameters
// (currentDocument.exists / currentDocument.updateTime) used by patch/delete.
// It returns nil when no precondition is set.
func preconditionFromQuery(nr *model.NormalizedRequest) (*firestorestore.Precondition, error) {
	pre := &firestorestore.Precondition{}
	if s, _ := nr.Params["currentDocument.exists"].(string); s != "" {
		v, err := strconv.ParseBool(s)
		if err != nil {
			return nil, model.NewProviderError("InvalidArgument", "invalid currentDocument.exists precondition", 400)
		}
		pre.Exists = &v
	}
	if s, _ := nr.Params["currentDocument.updateTime"].(string); s != "" {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, model.NewProviderError("InvalidArgument", "invalid currentDocument.updateTime precondition", 400)
		}
		pre.UpdateTime = &t
	}
	if pre.Exists == nil && pre.UpdateTime == nil {
		return nil, nil
	}
	return pre, nil
}

// ─── shared domain helpers (reused by Service and the future gRPC handler) ───

// mapStoreError maps store sentinel errors to Firestore GCP error envelopes.
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, firestorestore.ErrDocumentNotFound):
		return model.NewProviderError("NotFound", "document not found", 404)
	case errors.Is(err, firestorestore.ErrDocumentExists):
		return model.NewProviderError("AlreadyExists", "document already exists", 409)
	case errors.Is(err, firestorestore.ErrInvalidArgument):
		return model.NewProviderError("InvalidArgument", "invalid document", 400)
	default:
		return err
	}
}

// validatePath rejects reserved collection/document IDs (^__.*__$) in a
// relative document path.
func validatePath(path string) error {
	if path == "" {
		return nil
	}
	for _, seg := range strings.Split(path, "/") {
		if reservedNameRe.MatchString(seg) {
			return model.NewProviderError("InvalidArgument", "collection or document id "+seg+" is reserved", 400)
		}
	}
	return nil
}

// validateDocumentID enforces Firestore document-id constraints: no '/' (the
// hierarchy separator) and no reserved ^__.*__$ prefix/suffix.
func validateDocumentID(id string) error {
	if strings.Contains(id, "/") {
		return model.NewProviderError("InvalidArgument", "document id must not contain '/'", 400)
	}
	if reservedNameRe.MatchString(id) {
		return model.NewProviderError("InvalidArgument", "document id "+id+" is reserved", 400)
	}
	return nil
}

// validateFields rejects reserved field names (^__.*__$) anywhere in the field
// tree.
func validateFields(fields map[string]*firestorestore.Value) error {
	for k, v := range fields {
		if reservedNameRe.MatchString(k) {
			return model.NewProviderError("InvalidArgument", "field name "+k+" is reserved", 400)
		}
		if err := validateValue(v); err != nil {
			return err
		}
	}
	return nil
}

func validateValue(v *firestorestore.Value) error {
	if v == nil {
		return nil
	}
	if v.MapValue != nil {
		for k, cv := range v.MapValue.Fields {
			if reservedNameRe.MatchString(k) {
				return model.NewProviderError("InvalidArgument", "field name "+k+" is reserved", 400)
			}
			if err := validateValue(cv); err != nil {
				return err
			}
		}
	}
	if v.ArrayValue != nil {
		for _, cv := range v.ArrayValue.Values {
			if err := validateValue(cv); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkSize enforces the 1 MiB per-document size limit by serializing the fields,
// and the per-field limits (field-name length, string/bytes value caps) via a
// recursive value-tree walk.
func checkSize(fields map[string]*firestorestore.Value) error {
	if err := checkFieldLimits(fields); err != nil {
		return err
	}
	b, err := json.Marshal(fields)
	if err != nil {
		return model.NewProviderError("InvalidArgument", "malformed document fields", 400)
	}
	if len(b) > maxDocumentSize {
		return model.NewProviderError("InvalidArgument", "document exceeds maximum size of 1 MiB", 400)
	}
	return nil
}

// checkFieldLimits walks the Value tree (top-level fields, nested mapValue.fields,
// arrayValue.values) enforcing the per-field limits: a field name must be
// non-empty and ≤ 1,500 UTF-8 bytes; a stringValue/bytesValue must be
// ≤ 1 MiB − 89 bytes.
func checkFieldLimits(fields map[string]*firestorestore.Value) error {
	for k, v := range fields {
		if err := checkFieldName(k); err != nil {
			return err
		}
		if err := checkValueLimits(v); err != nil {
			return err
		}
	}
	return nil
}

func checkFieldName(k string) error {
	if k == "" {
		return model.NewProviderError("InvalidArgument", "field name must not be empty", 400)
	}
	if len(k) > maxFieldNameBytes {
		return model.NewProviderError("InvalidArgument", "field name exceeds maximum size of 1500 bytes", 400)
	}
	return nil
}

func checkValueLimits(v *firestorestore.Value) error {
	if v == nil {
		return nil
	}
	if v.StringValue != nil && len(*v.StringValue) > maxStringBytes {
		return model.NewProviderError("InvalidArgument", "string value exceeds maximum size of 1048487 bytes", 400)
	}
	if v.BytesValue != nil && len(v.BytesValue) > maxStringBytes {
		return model.NewProviderError("InvalidArgument", "bytes value exceeds maximum size of 1048487 bytes", 400)
	}
	if v.MapValue != nil {
		for k, cv := range v.MapValue.Fields {
			if err := checkFieldName(k); err != nil {
				return err
			}
			if err := checkValueLimits(cv); err != nil {
				return err
			}
		}
	}
	if v.ArrayValue != nil {
		for _, cv := range v.ArrayValue.Values {
			if err := checkValueLimits(cv); err != nil {
				return err
			}
		}
	}
	return nil
}

// randomID generates a Firestore-style auto-ID of n characters.
func randomID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	for i := range b {
		b[i] = autoIDAlphabet[int(b[i])%len(autoIDAlphabet)]
	}
	return string(b)
}

// existencePreconditionErr returns the provider error for a failed
// currentDocument existence precondition, or nil when the precondition holds
// (or is unset). Real Firestore distinguishes the two directions:
//   - exists=true on a missing document  -> NOT_FOUND "No document to update: <name>"
//   - exists=false on a present document -> ALREADY_EXISTS "Document already exists: <name>"
//
// The unary REST/gRPC paths and the Commit/BatchWrite build paths all funnel
// through this helper, so the same contract holds for updates, transforms and
// deletes.
func existencePreconditionErr(name string, exists bool, pre *firestorestore.Precondition) error {
	if pre == nil || pre.Exists == nil {
		return nil
	}
	if *pre.Exists && !exists {
		return newUpdateMissingErr(name)
	}
	if !*pre.Exists && exists {
		return newAlreadyExistsErr(name)
	}
	return nil
}

// checkPrecondition validates a document precondition against the current state
// (exists and updateTime). Existence mismatches return the canonical NOT_FOUND /
// ALREADY_EXISTS errors; an updateTime mismatch (or an absent document under an
// updateTime precondition) returns FAILED_PRECONDITION.
func checkPrecondition(name string, exists bool, updateTime time.Time, pre *firestorestore.Precondition) error {
	if pre == nil {
		return nil
	}
	if pre.Exists != nil {
		return existencePreconditionErr(name, exists, pre)
	}
	if pre.UpdateTime != nil {
		if !exists || !updateTime.Equal(*pre.UpdateTime) {
			return newPreconditionErr("precondition failed: updateTime does not match")
		}
	}
	return nil
}

// maskFields projects a document's fields down to the given field paths. A path
// not present in the document is omitted; "__name__" is always implicit and
// skipped (the name is carried on the wire independently).
func maskFields(doc firestorestore.Document, paths []string) map[string]*firestorestore.Value {
	out := map[string]*firestorestore.Value{}
	for _, fp := range paths {
		if fp == "__name__" {
			continue
		}
		if v := fieldValue(&doc, fp); v != nil {
			setFieldPath(out, strings.Split(fp, "."), v)
		}
	}
	return out
}

// applyMask merges body fields into a document's fields per Firestore patch
// semantics:
//   - no mask: the body fields fully replace the document's fields.
//   - mask present: only the field paths in the mask are updated; a mask path
//     not present in the body is deleted; fields outside the mask are unchanged.
//
// base is the existing fields (nil when the document is being created).
func applyMask(fields map[string]*firestorestore.Value, mask []string, base map[string]*firestorestore.Value) map[string]*firestorestore.Value {
	if len(mask) == 0 {
		out := make(map[string]*firestorestore.Value, len(fields))
		for k, v := range fields {
			out[k] = v
		}
		return out
	}
	out := make(map[string]*firestorestore.Value, len(base)+len(mask))
	for k, v := range base {
		out[k] = v
	}
	for _, fp := range mask {
		parts := strings.Split(fp, ".")
		if len(parts) == 1 {
			if v, ok := fields[parts[0]]; ok {
				out[parts[0]] = v
			} else {
				delete(out, parts[0])
			}
			continue
		}
		// Nested path: extract the leaf value from the body subtree.
		leaf := valueAt(fields[parts[0]], parts[1:])
		setFieldPath(out, parts, leaf)
	}
	return out
}

// cloneFields returns a shallow copy of a fields map. Values are shared (they
// are never mutated in place — transform operations always build new Values).
func cloneFields(fields map[string]*firestorestore.Value) map[string]*firestorestore.Value {
	out := make(map[string]*firestorestore.Value, len(fields))
	for k, v := range fields {
		out[k] = v
	}
	return out
}

// valueAt returns the value at the dotted field path within a mapValue, or nil.
func valueAt(v *firestorestore.Value, path []string) *firestorestore.Value {
	cur := v
	for _, p := range path {
		if cur == nil || cur.MapValue == nil {
			return nil
		}
		cur = cur.MapValue.Fields[p]
	}
	return cur
}

// setFieldPath sets (or deletes, when val is nil) the field at the dotted path,
// creating intermediate mapValues as needed.
func setFieldPath(fields map[string]*firestorestore.Value, path []string, val *firestorestore.Value) {
	if len(path) == 1 {
		if val == nil {
			delete(fields, path[0])
		} else {
			fields[path[0]] = val
		}
		return
	}
	cur, ok := fields[path[0]]
	if !ok || cur == nil || cur.MapValue == nil {
		if val == nil {
			return // nothing to delete
		}
		cur = firestorestore.MapVal(map[string]*firestorestore.Value{})
		fields[path[0]] = cur
	}
	setFieldPath(cur.MapValue.Fields, path[1:], val)
}

// ─── REST handlers (thin adapters over the Service) ──────────────────────────

// DocumentsGet implements documents.get.
func (p *Provider) DocumentsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	name := docName(nr.AccountID, database, path)
	txn, err := decodeTxnParam(nr)
	if err != nil {
		return nil, err
	}
	doc, err := p.Service.GetDocument(ctx, name, txn, maskFieldPaths(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(documentMap(doc)), nil
}

// CreateDocument implements documents.createDocument. The document id comes
// from ?documentId=; when omitted the server generates a 20-character auto-ID
// and returns it in the response name.
func (p *Provider) CreateDocument(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	docID, _ := nr.Params["documentId"].(string)
	body, _ := nr.Params["body"].(map[string]any)
	fields, err := extractFields(body)
	if err != nil {
		return nil, err
	}
	doc, err := p.Service.CreateDocument(ctx, nr.AccountID, database, path, docID, fields)
	if err != nil {
		return nil, err
	}
	return provider.OK(documentMap(doc)), nil
}

// DocumentsPatch implements documents.patch. Patch upserts: a missing document
// is created. The updateMask (updateMask.fieldPaths) restricts which fields are
// updated; without it the body fields fully replace the document's fields.
func (p *Provider) DocumentsPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	fields, err := extractFields(body)
	if err != nil {
		return nil, err
	}
	mask := maskFieldPaths(nr)
	if len(mask) == 0 {
		mask = updateMaskPaths(nr)
	}
	pre, err := preconditionFromQuery(nr)
	if err != nil {
		return nil, err
	}
	doc, err := p.Service.PatchDocument(ctx, nr.AccountID, database, path, fields, mask, pre)
	if err != nil {
		return nil, err
	}
	return provider.OK(documentMap(doc)), nil
}

// DocumentsDelete implements documents.delete. Idempotent, unless a
// currentDocument precondition is set (then it is validated first).
func (p *Provider) DocumentsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	pre, err := preconditionFromQuery(nr)
	if err != nil {
		return nil, err
	}
	if err := p.Service.DeleteDocument(ctx, nr.AccountID, database, path, pre); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ListDocuments implements documents.list / documents.listDocuments (they share
// one wire path: GET on a collection).
func (p *Provider) ListDocuments(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	txn, err := decodeTxnParam(nr)
	if err != nil {
		return nil, err
	}
	docs, nextToken, err := p.Service.ListDocuments(ctx, nr.AccountID, database, path, txn, maskFieldPaths(nr), pageFromNR(nr))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(docs))
	for _, d := range docs {
		items = append(items, documentMap(d))
	}
	resp := map[string]any{"documents": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

// ListCollectionIds implements documents.listCollectionIds: returns the distinct
// subcollection IDs of a document.
func (p *Provider) ListCollectionIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	ids, nextToken, err := p.Service.ListCollectionIds(ctx, nr.AccountID, database, path, pageFromNR(nr))
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"collectionIds": ids}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

// RunQuery implements documents.runQuery over a StructuredQuery. The response
// is newline-delimited JSON (server-streaming).
func (p *Provider) RunQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req runQueryRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	if req.StructuredQuery == nil {
		return nil, model.NewProviderError("InvalidArgument", "runQuery requires a structuredQuery", 400)
	}

	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}

	txn, err := decodeTxn(req.Transaction)
	if err != nil {
		return nil, err
	}
	results, err := p.Service.RunQuery(ctx, nr.AccountID, database, path, req.StructuredQuery, txn)
	if err != nil {
		return nil, err
	}

	readTime := clock.Now()
	lines := make([]string, 0, len(results)+1)
	for _, d := range results {
		item := map[string]any{
			"document": documentMap(d),
			"readTime": readTime.Format(time.RFC3339Nano),
		}
		b, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(b))
	}
	final := map[string]any{"done": true}
	if req.StructuredQuery.Offset > 0 {
		final["skippedResults"] = req.StructuredQuery.Offset
	}
	b, err := json.Marshal(final)
	if err != nil {
		return nil, err
	}
	lines = append(lines, string(b))
	return provider.OK(map[string]any{wire.RawJSONKey: json.RawMessage(strings.Join(lines, "\n"))}), nil
}

// Commit implements documents.commit: an atomic batch of writes with optimistic
// preconditions and transaction read-set validation.
func (p *Provider) Commit(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req commitRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	txn, err := decodeTxn(req.Transaction)
	if err != nil {
		return nil, err
	}
	commitTime, results, err := p.Service.Commit(ctx, txn, req.Writes)
	if err != nil {
		return nil, err
	}
	wr := make([]any, 0, len(results))
	for _, r := range results {
		wr = append(wr, r)
	}
	return provider.OK(map[string]any{
		"commitTime":   commitTime.Format(time.RFC3339Nano),
		"writeResults": wr,
	}), nil
}

// BatchWrite implements documents.batchWrite: non-atomic, per-write results.
func (p *Provider) BatchWrite(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req batchWriteRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	statuses, writeResults, err := p.Service.BatchWrite(ctx, req.Writes)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"status":       statuses,
		"writeResults": writeResults,
	}), nil
}

// BatchGet implements documents.batchGet. The response body is newline-delimited
// JSON (server-streaming).
func (p *Provider) BatchGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req batchGetRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	txn, err := decodeTxn(req.Transaction)
	if err != nil {
		return nil, err
	}
	items, err := p.Service.BatchGet(ctx, req.Documents, txn)
	if err != nil {
		return nil, err
	}
	lines := make([]string, 0, len(items))
	for _, item := range items {
		b, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(b))
	}
	return provider.OK(map[string]any{wire.RawJSONKey: json.RawMessage(strings.Join(lines, "\n"))}), nil
}

// BeginTransaction implements documents.beginTransaction.
func (p *Provider) BeginTransaction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	txn, err := p.Service.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"transaction": encodeTxn(txn)}), nil
}

// Rollback implements documents.rollback.
func (p *Provider) Rollback(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req rollbackRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	txn, err := decodeTxn(req.Transaction)
	if err != nil {
		return nil, err
	}
	if err := p.Service.Rollback(ctx, txn); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// CreateIndex implements projects.databases.collectionGroups.indexes.create.
func (p *Provider) CreateIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	database, cg, _, ok := indexPath(name)
	if !ok {
		return nil, model.NewProviderError("InvalidArgument", "invalid index parent path", 400)
	}

	body, _ := nr.Params["body"].(map[string]any)
	b, _ := json.Marshal(body)
	var idx indexDef
	if err := json.Unmarshal(b, &idx); err != nil {
		return nil, model.NewProviderError("InvalidArgument", "malformed index definition", 400)
	}

	resp, err := p.Service.CreateIndex(ctx, nr.AccountID, database, cg, idx)
	if err != nil {
		return nil, err
	}
	return provider.OK(resp), nil
}

// ListIndexes implements projects.databases.collectionGroups.indexes.list.
func (p *Provider) ListIndexes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	database, cg, _, ok := indexPath(name)
	if !ok {
		return nil, model.NewProviderError("InvalidArgument", "invalid index parent path", 400)
	}
	filter, _ := nr.Params["filter"].(string)

	idxs, nextToken, err := p.Service.ListIndexes(ctx, nr.AccountID, database, cg, filter, pageFromNR(nr))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(idxs))
	for _, idx := range idxs {
		items = append(items, indexMap(idx))
	}
	resp := map[string]any{"indexes": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

// GetIndex implements projects.databases.collectionGroups.indexes.get.
func (p *Provider) GetIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	database, cg, id, ok := indexPath(name)
	if !ok || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "invalid index path", 400)
	}
	idx, err := p.Service.GetIndex(ctx, nr.AccountID, database, cg, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(indexMap(idx)), nil
}

// DeleteIndex implements projects.databases.collectionGroups.indexes.delete.
func (p *Provider) DeleteIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["name"].(string)
	database, cg, id, ok := indexPath(name)
	if !ok || id == "" {
		return nil, model.NewProviderError("InvalidArgument", "invalid index path", 400)
	}
	if err := p.Service.DeleteIndex(ctx, nr.AccountID, database, cg, id); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}
