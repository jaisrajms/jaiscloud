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

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	// maxDocumentSize is the Firestore per-document limit (1 MiB).
	maxDocumentSize = 1 << 20 // 1,048,576 bytes
	// maxFieldNameBytes is the per-field-name byte limit (document.proto).
	maxFieldNameBytes = 1500
	// maxPathElementBytes is the per-key-path-element byte limit: every
	// collection id (kind) and document id (name) must be no longer than
	// 1,500 bytes (Firestore quotas: "Constraints on collection IDs" /
	// "Constraints on document IDs"). The official emulator rejects an
	// over-long element with 400 INVALID_ARGUMENT. This is a distinct quota
	// from maxFieldNameBytes (a different constraint that shares the value).
	maxPathElementBytes = 1500
	// maxStringBytes is the per-stringValue/bytesValue byte limit (1 MiB − 89).
	maxStringBytes = (1 << 20) - 89 // 1,048,487 bytes
	// autoIDLength is the length of a server-generated document ID.
	autoIDLength = 20
	// maxDocumentNameBytes is the Firestore document-name byte limit (6 KiB).
	maxDocumentNameBytes = 6 << 10 // 6,144 bytes
	// maxSubcollectionDepth is the maximum number of collection levels a
	// document path may have (Firestore "Maximum depth of subcollections").
	// The official Firestore emulator rejects a 101-collection path with
	// 400 INVALID_ARGUMENT ("Key path is too long. Cannot exceed 100 elements.").
	maxSubcollectionDepth = 100
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
		"Firestore.PartitionQuery":    p.PartitionQuery,
		"Firestore.ExecutePipeline":   p.ExecutePipeline,
		"Firestore.Commit":            p.Commit,
		"Firestore.BatchWrite":        p.BatchWrite,
		"Firestore.BatchGet":          p.BatchGet,
		"Firestore.BeginTransaction":  p.BeginTransaction,
		"Firestore.Rollback":          p.Rollback,
		"Firestore.CreateIndex":       p.CreateIndex,
		"Firestore.ListIndexes":       p.ListIndexes,
		"Firestore.GetIndex":          p.GetIndex,
		"Firestore.DeleteIndex":       p.DeleteIndex,
		// Firestore Admin control plane (REST under the `firestore` service; the
		// gRPC twin is google.firestore.admin.v1.FirestoreAdmin).
		"Firestore.CreateDatabase":       p.CreateDatabase,
		"Firestore.ListDatabases":        p.ListDatabases,
		"Firestore.GetDatabase":          p.GetDatabase,
		"Firestore.UpdateDatabase":       p.UpdateDatabase,
		"Firestore.DeleteDatabase":       p.DeleteDatabase,
		"Firestore.CreateBackupSchedule": p.CreateBackupSchedule,
		"Firestore.ListBackupSchedules":  p.ListBackupSchedules,
		"Firestore.GetBackupSchedule":    p.GetBackupSchedule,
		"Firestore.UpdateBackupSchedule": p.UpdateBackupSchedule,
		"Firestore.DeleteBackupSchedule": p.DeleteBackupSchedule,
		"Firestore.CreateUserCreds":      p.CreateUserCreds,
		"Firestore.ListUserCreds":        p.ListUserCreds,
		"Firestore.GetUserCreds":         p.GetUserCreds,
		"Firestore.EnableUserCreds":      p.EnableUserCreds,
		"Firestore.DisableUserCreds":     p.DisableUserCreds,
		"Firestore.ResetUserPassword":    p.ResetUserPassword,
		"Firestore.DeleteUserCreds":      p.DeleteUserCreds,
		"Firestore.ListFields":           p.ListFields,
		"Firestore.GetField":             p.GetField,
		"Firestore.UpdateField":          p.UpdateField,
		"Firestore.ListBackups":          p.ListBackups,
		"Firestore.GetBackup":            p.GetBackup,
		"Firestore.DeleteBackup":         p.DeleteBackup,
		// Firestore Admin long-running operations (projects.databases.operations).
		"Firestore.GetOperation":    p.GetOperation,
		"Firestore.ListOperations":  p.ListOperations,
		"Firestore.DeleteOperation": p.DeleteOperation,
		"Firestore.CancelOperation": p.CancelOperation,
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

// decodeProtoBody decodes nr.Params["body"] into a protobuf message using
// protojson — the Firestore REST body encoding. It is used by request bodies
// whose shape is richer than the flat wire structs (the pipeline expression
// tree), so the REST handler can reuse the same proto types the gRPC transport
// decodes. An absent body leaves the message at its zero value.
func decodeProtoBody(nr *model.NormalizedRequest, m proto.Message) error {
	body, ok := nr.Params["body"]
	if !ok || body == nil {
		return nil
	}
	b, err := json.Marshal(body)
	if err != nil {
		return model.NewProviderError("InvalidArgument", "malformed request body", 400)
	}
	if err := protojson.Unmarshal(b, m); err != nil {
		return model.NewProviderError("InvalidArgument", "malformed request body: "+err.Error(), 400)
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
// A negative pageSize is InvalidArgument (AIP-158), so the error is returned
// rather than folded into the default.
func pageFromNR(nr *model.NormalizedRequest) (pageParams, error) {
	token, _ := nr.Params["pageToken"].(string)
	size, err := paging.PageSize(nr.Params)
	if err != nil {
		return pageParams{}, err
	}
	return pageParams{size: size, token: token}, nil
}

// boolParam reads a boolean request parameter, accepting the string forms the
// REST query decoder produces ("true"/"1") and a native bool.
func boolParam(nr *model.NormalizedRequest, key string) bool {
	switch v := nr.Params[key].(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1"
	}
	return false
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
// documentMap encodes a document for the wire. createTime/updateTime are omitted
// when unset so a showMissing placeholder (name only, no underlying document)
// matches real Firestore: it has a name but no fields or timestamps.
func documentMap(d firestorestore.Document) map[string]any {
	m := map[string]any{"name": d.Name}
	if len(d.Fields) > 0 {
		// Round-trip through JSON so fields are generic map[string]any (the wire
		// shape every caller expects), not the typed *Value form.
		b, _ := json.Marshal(d.Fields)
		var fields map[string]any
		json.Unmarshal(b, &fields)
		m["fields"] = fields
	}
	if !d.CreateTime.IsZero() {
		m["createTime"] = timestamp.Canonical(d.CreateTime)
	}
	if !d.UpdateTime.IsZero() {
		m["updateTime"] = timestamp.Canonical(d.UpdateTime)
	}
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

// responseMaskFromQuery returns the response field mask with its presence
// preserved: an absent mask is nil (return every field), while a supplied but
// empty `mask.fieldPaths=` is an explicit empty mask (return no fields).
func responseMaskFromQuery(nr *model.NormalizedRequest) *ResponseMask {
	if !maskParamPresent(nr, "mask.fieldPaths") {
		return nil
	}
	return &ResponseMask{Paths: maskFieldPaths(nr)}
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

// validateDocumentName enforces the Firestore limits that apply to every write
// target, for both the REST and gRPC transports (the shared Service calls it):
// the name must be a well-formed database document name, no longer than 6 KiB,
// with every collection/document path element no longer than 1,500 bytes, and
// no deeper than 100 collection levels. Real Firestore (and the official
// emulator, for the depth and element limits) rejects a violating name with 400
// INVALID_ARGUMENT. The whole-name 6 KiB pre-check intentionally runs first
// (before splitting), so a single element over 6 KiB reports the name-size
// error; every violation is still 400 INVALID_ARGUMENT.
func validateDocumentName(name string) error {
	// Reject an over-long name before splitting so a multi-megabyte request body
	// cannot force a large allocation first.
	if len(name) > maxDocumentNameBytes {
		return model.NewProviderError("InvalidArgument", "document name exceeds maximum size of 6 KiB", 400)
	}
	_, _, path, ok := firestorestore.ParseDocumentName(name)
	if !ok {
		return model.NewProviderError("InvalidArgument", "invalid document name", 400)
	}
	// A document path alternates collection/document segments, so the number of
	// collection levels is half the segment count; the root collection is the
	// first level. An empty path, an odd segment count, or an empty segment is
	// not a document.
	segs := splitRelativePath(path)
	if len(segs) == 0 || len(segs)%2 != 0 {
		return model.NewProviderError("InvalidArgument", "invalid document name", 400)
	}
	for i, seg := range segs {
		if seg == "" {
			return model.NewProviderError("InvalidArgument", "document name contains an empty path element", 400)
		}
		// Elements alternate collection id (kind) and document id (name),
		// starting with the root collection. Each is capped at 1,500 bytes;
		// the official emulator names the offending element type in the
		// message ("element name"/"element kind").
		if len(seg) > maxPathElementBytes {
			element := "kind"
			if i%2 == 1 {
				element = "name"
			}
			return model.NewProviderError("InvalidArgument",
				"The key path element "+element+" is longer than "+strconv.Itoa(maxPathElementBytes)+" bytes.", 400)
		}
	}
	if levels := len(segs) / 2; levels > maxSubcollectionDepth {
		return model.NewProviderError("InvalidArgument",
			"document path exceeds maximum depth of "+strconv.Itoa(maxSubcollectionDepth)+" collections", 400)
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
// semantics: only the field paths in the mask are updated; a mask path not
// present in the body is deleted; fields outside the mask are unchanged.
//
// base is the existing fields (nil when the document is being created).
//
// The mask must be populated: the omitted mask (replace every field) and the
// present-but-empty mask (AIP-134's no-op) are resolved by the caller, and an
// empty mask here leaves the base untouched rather than silently replacing it.
func applyMask(fields map[string]*firestorestore.Value, mask []string, base map[string]*firestorestore.Value) map[string]*firestorestore.Value {
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
	doc, err := p.Service.GetDocument(ctx, name, txn, responseMaskFromQuery(nr))
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
// is created. The write mask is `updateMask.fieldPaths`: a populated mask
// updates only the listed paths, an omitted one replaces every field, and a
// present-but-empty one is AIP-134's no-op. `mask.fieldPaths` is the unrelated
// response projection (fields to return), applied to the reply only.
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
	mask := updateMaskFromQuery(nr)
	pre, err := preconditionFromQuery(nr)
	if err != nil {
		return nil, err
	}
	doc, err := p.Service.PatchDocument(ctx, nr.AccountID, database, path, fields, mask, pre)
	if err != nil {
		return nil, err
	}
	return provider.OK(documentMap(ProjectDocument(doc, responseMaskFromQuery(nr)))), nil
}

// updateMaskFromQuery reads the documents.patch *write* mask from the
// `updateMask.fieldPaths` query parameter, preserving presence: a parameter
// supplied with no values (`?updateMask.fieldPaths=`) is a present-but-empty
// mask, which is AIP-134's no-op — not an omitted mask, which replaces every
// field. `mask.fieldPaths` is deliberately not consulted here; it selects the
// fields the response is projected to (see DocumentsPatch).
func updateMaskFromQuery(nr *model.NormalizedRequest) *UpdateMask {
	if !maskParamPresent(nr, "updateMask.fieldPaths") {
		return nil
	}
	return &UpdateMask{Paths: updateMaskPaths(nr)}
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
	page, err := pageFromNR(nr)
	if err != nil {
		return nil, err
	}
	docs, nextToken, err := p.Service.ListDocuments(ctx, nr.AccountID, database, path, txn, responseMaskFromQuery(nr), boolParam(nr, "showMissing"), page)
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
	page, err := pageFromNR(nr)
	if err != nil {
		return nil, err
	}
	ids, nextToken, err := p.Service.ListCollectionIds(ctx, nr.AccountID, database, path, page)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{"collectionIds": ids}
	if ids == nil {
		resp["collectionIds"] = []string{}
	}
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
			"readTime": timestamp.Canonical(readTime),
		}
		b, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(b))
	}
	final := map[string]any{"done": true, "readTime": timestamp.Canonical(readTime)}
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

// PartitionQuery implements documents.partitionQuery: cursor partition points
// over a structured query's ordered result set. As in the gRPC transport, an
// empty partition list is returned when the query yields too few documents to
// partition or a single partition is requested (the SDK treats that as one
// full-range partition).
func (p *Provider) PartitionQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req partitionQueryRequestWire
	if err := decodeBody(nr, &req); err != nil {
		return nil, err
	}
	database, path, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	if req.StructuredQuery == nil {
		return nil, model.NewProviderError("InvalidArgument", "partitionQuery requires a structuredQuery", 400)
	}
	count := int(req.PartitionCount)
	if count <= 1 {
		return provider.OK(map[string]any{"partitions": []any{}}), nil
	}
	docs, err := p.Service.RunQuery(ctx, nr.AccountID, database, path, req.StructuredQuery, nil)
	if err != nil {
		return nil, err
	}
	if len(docs) < count {
		return provider.OK(map[string]any{"partitions": []any{}}), nil
	}
	partitions := make([]any, 0, count-1)
	for i := 1; i < count; i++ {
		idx := len(docs) * i / count
		if idx >= len(docs) {
			break
		}
		partitions = append(partitions, map[string]any{
			"values": []any{map[string]any{"referenceValue": docs[idx].Name}},
		})
	}
	return provider.OK(map[string]any{"partitions": partitions}), nil
}

// ExecutePipeline implements documents.executePipeline. The request body is the
// protojson form of google.firestore.v1.ExecutePipelineRequest; the pipeline
// stages are evaluated by the shared engine (pipeline.go), the same one the gRPC
// ExecutePipeline RPC uses. The response carries executionTime, the result
// documents, and — when newTransaction was requested — the base64 transaction
// id. The whole stream is collapsed into one JSON response (a valid batching).
func (p *Provider) ExecutePipeline(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	var req firestorepb.ExecutePipelineRequest
	if err := decodeProtoBody(nr, &req); err != nil {
		return nil, err
	}
	database, _, err := relativeName(nr)
	if err != nil {
		return nil, err
	}
	// RunPipeline validates the stage list, so a missing/empty pipeline is the
	// shared 400 INVALID_ARGUMENT (the same error the gRPC adapter surfaces).
	res, err := p.Service.RunPipeline(ctx, nr.AccountID, database,
		req.GetStructuredPipeline().GetPipeline().GetStages(),
		req.GetTransaction(), req.GetNewTransaction() != nil)
	if err != nil {
		return nil, err
	}
	results := make([]any, 0, len(res.Documents))
	for _, d := range res.Documents {
		results = append(results, documentMap(d))
	}
	out := map[string]any{
		"executionTime": timestamp.Canonical(res.ExecutionTime),
		"results":       results,
	}
	if len(res.Transaction) > 0 {
		out["transaction"] = encodeTxn(res.Transaction)
	}
	return provider.OK(out), nil
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
		"commitTime":   timestamp.Canonical(commitTime),
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
	// Real Firestore starts a new transaction when newTransaction is set and
	// returns its id on the first streamed response. The Node SDK's
	// runTransaction begins transactions lazily through documents.batchGet.
	if len(req.NewTransaction) > 0 && string(req.NewTransaction) != "null" {
		txn, err = p.Service.BeginTransaction(ctx)
		if err != nil {
			return nil, err
		}
	}
	items, err := p.Service.BatchGet(ctx, req.Documents, txn)
	if err != nil {
		return nil, err
	}
	lines := make([]string, 0, len(items))
	for i, item := range items {
		if i == 0 && len(txn) > 0 {
			item["transaction"] = encodeTxn(txn)
		}
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

	page, err := pageFromNR(nr)
	if err != nil {
		return nil, err
	}
	idxs, nextToken, err := p.Service.ListIndexes(ctx, nr.AccountID, database, cg, filter, page)
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
