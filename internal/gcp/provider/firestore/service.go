package firestore

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Service owns the transport-agnostic Firestore domain logic. It holds the
// shared document/resource stores and the in-memory transaction read-set
// registry, so a REST adapter (Provider) and a future gRPC handler can share
// one instance. Its methods take typed inputs and return typed results; the
// wire encoding/decoding lives entirely at the transport boundary.
type Service struct {
	store     firestorestore.FirestoreStore
	resources store.ResourceStore

	// txnMu guards readSets: the in-memory transaction read-set registry
	// (transactions are ephemeral and never persisted).
	txnMu    sync.Mutex
	readSets map[string]*readSet

	// changeMu guards the change-feed: changeSeq (monotonic), changeLog (the
	// full ordered history used for resume-token replay), and changeSubs.
	changeMu   sync.Mutex
	changeSeq  uint64
	changeLog  []ChangeEvent
	changeSubs map[*changeSub]struct{}
}

// newService returns a Firestore service backed by the given document store and
// control-plane resource store (for composite indexes).
func newService(s firestorestore.FirestoreStore, resources store.ResourceStore) *Service {
	return &Service{
		store:     s,
		resources: resources,
		readSets:  make(map[string]*readSet),
	}
}

// Reset clears transaction read-sets (document state is reset via the store's
// own Resetter; index state via the resource store's).
func (s *Service) Reset(_ context.Context) {
	s.txnMu.Lock()
	s.readSets = make(map[string]*readSet)
	s.txnMu.Unlock()
}

// docName builds the full document resource name.
func docName(project, database, path string) string {
	return "projects/" + project + "/databases/" + database + "/documents/" + path
}

// parentDocOfCollection returns the document path that owns a collection path
// (strip the trailing collection-id segment).
func parentDocOfCollection(collPath string) string {
	i := strings.LastIndex(collPath, "/")
	if i < 0 {
		return ""
	}
	return collPath[:i]
}

// ─── transaction state ───────────────────────────────────────────────────────

// readSet records the documents read within a transaction for optimistic
// concurrency re-validation at commit.
type readSet struct {
	reads map[string]firestorestore.ReadRef
}

// newTxnID returns a 32-byte random transaction ID. The bytes are the internal
// identity; the REST boundary base64-encodes them for the wire.
func newTxnID() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return []byte(clock.Now().Format(time.RFC3339Nano))
	}
	return b
}

// recordRead registers a document read in a transaction's read-set. Missing
// documents are recorded with Exists=false so a concurrent create aborts.
func (s *Service) recordRead(txn []byte, name string, doc firestorestore.Document, exists bool) {
	if len(txn) == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	rs := s.readSets[string(txn)]
	if rs == nil {
		return
	}
	if rs.reads == nil {
		rs.reads = make(map[string]firestorestore.ReadRef)
	}
	rs.reads[name] = firestorestore.ReadRef{Name: name, Exists: exists, UpdateTime: doc.UpdateTime}
}

// readSetFor returns the read-set entries for a transaction, or nil.
func (s *Service) readSetFor(txn []byte) []firestorestore.ReadRef {
	if len(txn) == 0 {
		return nil
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	rs := s.readSets[string(txn)]
	if rs == nil || len(rs.reads) == 0 {
		return nil
	}
	out := make([]firestorestore.ReadRef, 0, len(rs.reads))
	for _, r := range rs.reads {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// clearReadSet removes a transaction's read-set (after commit/rollback).
func (s *Service) clearReadSet(txn []byte) {
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	delete(s.readSets, string(txn))
}

// requireActive returns an InvalidArgument error when transaction is non-empty
// but not currently active (rolled back, already committed, or never begun).
// An empty transaction denotes a non-transactional operation and always passes.
func (s *Service) requireActive(transaction []byte) error {
	if len(transaction) == 0 {
		return nil
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	if s.readSets[string(transaction)] == nil {
		return model.NewProviderError("InvalidArgument",
			"transaction is no longer active (rolled back, committed, or never begun)", 400)
	}
	return nil
}

// ─── pagination ──────────────────────────────────────────────────────────────

// pageParams carries cursor-pagination inputs (pageSize/pageToken) in a
// transport-agnostic form.
type pageParams struct {
	size  int
	token string
}

func (p pageParams) params() map[string]any {
	return map[string]any{"pageSize": p.size, "pageToken": p.token}
}

// ─── document methods ────────────────────────────────────────────────────────

// GetDocument returns a document by full name, recording the read in the
// transaction read-set and applying the field mask when present.
func (s *Service) GetDocument(ctx context.Context, name string, transaction []byte, mask []string) (firestorestore.Document, error) {
	if err := s.requireActive(transaction); err != nil {
		return firestorestore.Document{}, err
	}
	doc, err := s.store.GetDocument(ctx, name)
	if err != nil {
		if len(transaction) > 0 && errors.Is(err, firestorestore.ErrDocumentNotFound) {
			s.recordRead(transaction, name, firestorestore.Document{}, false)
		}
		return firestorestore.Document{}, mapStoreError(err)
	}
	if len(transaction) > 0 {
		s.recordRead(transaction, name, doc, true)
	}
	if len(mask) > 0 {
		doc.Fields = maskFields(doc, mask)
	}
	return doc, nil
}

// CreateDocument creates a document. The document id comes from docID; when
// empty the server generates a 20-character auto-ID (returned in the name).
func (s *Service) CreateDocument(ctx context.Context, project, database, path, docID string, fields map[string]*firestorestore.Value) (firestorestore.Document, error) {
	if err := validatePath(path); err != nil {
		return firestorestore.Document{}, err
	}
	if docID == "" {
		docID = randomID(autoIDLength)
	} else if err := validateDocumentID(docID); err != nil {
		return firestorestore.Document{}, err
	}
	name := docName(project, database, path+"/"+docID)
	if err := validateFields(fields); err != nil {
		return firestorestore.Document{}, err
	}
	if err := checkSize(fields); err != nil {
		return firestorestore.Document{}, err
	}
	now := clock.Now()
	doc := firestorestore.Document{
		Name:       name,
		Fields:     fields,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := s.store.CreateDocument(ctx, doc); err != nil {
		return firestorestore.Document{}, mapStoreError(err)
	}
	d := doc
	s.publishChange(ChangeEvent{Name: d.Name, Doc: &d})
	return doc, nil
}

// PatchDocument upserts a document: a missing document is created. The mask
// restricts which fields are updated; without it the fields fully replace the
// document's fields.
func (s *Service) PatchDocument(ctx context.Context, project, database, path string, fields map[string]*firestorestore.Value, mask []string, pre *firestorestore.Precondition) (firestorestore.Document, error) {
	if err := validatePath(path); err != nil {
		return firestorestore.Document{}, err
	}
	name := docName(project, database, path)
	if err := validateFields(fields); err != nil {
		return firestorestore.Document{}, err
	}

	now := clock.Now()

	existing, err := s.store.GetDocument(ctx, name)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return firestorestore.Document{}, err
	}

	// Optimistic precondition check (currentDocument.exists / updateTime).
	if exists {
		if err := checkPrecondition(true, existing.UpdateTime, pre); err != nil {
			return firestorestore.Document{}, err
		}
	} else if err := checkPrecondition(false, time.Time{}, pre); err != nil {
		return firestorestore.Document{}, err
	}

	var base map[string]*firestorestore.Value
	createTime := now
	if exists {
		base = existing.Fields
		createTime = existing.CreateTime
	}
	merged := applyMask(fields, mask, base)
	if err := checkSize(merged); err != nil {
		return firestorestore.Document{}, err
	}
	doc := firestorestore.Document{
		Name:       name,
		Fields:     merged,
		CreateTime: createTime,
		UpdateTime: now,
	}
	if exists {
		if err := s.store.UpdateDocument(ctx, doc); err != nil {
			return firestorestore.Document{}, mapStoreError(err)
		}
	} else {
		if err := s.store.CreateDocument(ctx, doc); err != nil {
			return firestorestore.Document{}, mapStoreError(err)
		}
	}
	d := doc
	s.publishChange(ChangeEvent{Name: d.Name, Doc: &d})
	return doc, nil
}

// DeleteDocument deletes a document. Idempotent, unless a currentDocument
// precondition is set (then it is validated first).
func (s *Service) DeleteDocument(ctx context.Context, project, database, path string, pre *firestorestore.Precondition) error {
	name := docName(project, database, path)
	if pre != nil {
		existing, err := s.store.GetDocument(ctx, name)
		exists := err == nil
		if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
			return err
		}
		var updateTime time.Time
		if exists {
			updateTime = existing.UpdateTime
		}
		if err := checkPrecondition(exists, updateTime, pre); err != nil {
			return err
		}
	}
	if err := s.store.DeleteDocument(ctx, name); err != nil {
		return mapStoreError(err)
	}
	s.publishChange(ChangeEvent{Name: name})
	return nil
}

// ListDocuments returns the direct children of a collection, paginated.
func (s *Service) ListDocuments(ctx context.Context, project, database, path string, transaction []byte, mask []string, page pageParams) ([]firestorestore.Document, string, error) {
	if err := s.requireActive(transaction); err != nil {
		return nil, "", err
	}
	docs, err := s.store.ListDocuments(ctx, project, database)
	if err != nil {
		return nil, "", err
	}
	coll := docName(project, database, path)
	children := make([]firestorestore.Document, 0)
	for _, d := range docs {
		if d.ParentPath == coll {
			children = append(children, d)
		}
	}
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })

	pageDocs, nextToken := paging.Page(children, func(d firestorestore.Document) string { return d.Name }, page.params())

	if len(transaction) > 0 {
		for _, d := range pageDocs {
			s.recordRead(transaction, d.Name, d, true)
		}
	}

	if len(mask) > 0 {
		for i := range pageDocs {
			pageDocs[i].Fields = maskFields(pageDocs[i], mask)
		}
	}
	return pageDocs, nextToken, nil
}

// ListCollectionIds returns the distinct subcollection IDs of a document,
// paginated.
func (s *Service) ListCollectionIds(ctx context.Context, project, database, path string, page pageParams) ([]string, string, error) {
	// parentDocOfCollection returns the parent without a trailing slash, so
	// trim the "/" that docName appends for an empty (top-level) path before
	// comparing.
	parent := strings.TrimSuffix(docName(project, database, path), "/")

	docs, err := s.store.ListDocuments(ctx, project, database)
	if err != nil {
		return nil, "", err
	}
	seen := map[string]bool{}
	var ids []string
	for _, d := range docs {
		if parentDocOfCollection(d.ParentPath) != parent {
			continue
		}
		if d.CollectionID == "" || seen[d.CollectionID] {
			continue
		}
		seen[d.CollectionID] = true
		ids = append(ids, d.CollectionID)
	}
	sort.Strings(ids)

	pageIDs, nextToken := paging.Page(ids, func(id string) string { return id }, page.params())
	return pageIDs, nextToken, nil
}

// RunQuery executes a StructuredQuery over the documents in a project+database,
// enforcing composite-index requirements and recording reads in the
// transaction read-set.
func (s *Service) RunQuery(ctx context.Context, project, database, path string, q *structuredQuery, transaction []byte) ([]firestorestore.Document, error) {
	if err := s.requireActive(transaction); err != nil {
		return nil, err
	}
	parent := "projects/" + project + "/databases/" + database + "/documents"
	if path != "" {
		parent += "/" + path
	}

	// Composite-index enforcement (strict): reject queries that require a
	// missing composite index before executing.
	required, err := analyzeQuery(q)
	if err != nil {
		return nil, err
	}
	if len(required) > 0 {
		ok, err := s.hasIndex(ctx, project, database, required)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, newPreconditionErr("the query requires a composite index on " +
				strings.Join(required, ", ") + " which is not defined")
		}
	}

	docs, err := s.store.ListDocuments(ctx, project, database)
	if err != nil {
		return nil, err
	}
	docPtrs := make([]*firestorestore.Document, len(docs))
	for i := range docs {
		docPtrs[i] = &docs[i]
	}
	results, err := executeQuery(docPtrs, q, parent)
	if err != nil {
		return nil, err
	}

	out := make([]firestorestore.Document, 0, len(results))
	for _, d := range results {
		if len(transaction) > 0 {
			s.recordRead(transaction, d.Name, *d, true)
		}
		out = append(out, *d)
	}
	return out, nil
}

// ─── transactions / commit ───────────────────────────────────────────────────

// BeginTransaction starts a new transaction and returns its 32-byte id.
func (s *Service) BeginTransaction(ctx context.Context) ([]byte, error) {
	txn := newTxnID()
	s.txnMu.Lock()
	if s.readSets == nil {
		s.readSets = make(map[string]*readSet)
	}
	s.readSets[string(txn)] = &readSet{reads: map[string]firestorestore.ReadRef{}}
	s.txnMu.Unlock()
	return txn, nil
}

// Rollback discards a transaction's read-set.
func (s *Service) Rollback(ctx context.Context, transaction []byte) error {
	s.clearReadSet(transaction)
	return nil
}

// Commit applies an atomic batch of writes with optimistic preconditions and
// transaction read-set validation.
func (s *Service) Commit(ctx context.Context, transaction []byte, writes []*writeWire) (time.Time, []map[string]any, error) {
	if err := s.requireActive(transaction); err != nil {
		return time.Time{}, nil, err
	}
	if len(writes) > maxWriteBatchSize {
		return time.Time{}, nil, model.NewProviderError("InvalidArgument",
			"a commit may contain at most "+strconv.Itoa(maxWriteBatchSize)+" writes", 400)
	}
	if size, err := writeBatchSize(writes); err != nil {
		return time.Time{}, nil, err
	} else if size > maxTxnBytes {
		return time.Time{}, nil, model.NewProviderError("InvalidArgument", "transaction exceeds the maximum size of 10 MiB", 400)
	}

	commitTime := clock.Now()
	ws, results, err := s.buildWrites(ctx, writes, commitTime)
	if err != nil {
		return time.Time{}, nil, err
	}
	reads := s.readSetFor(transaction)
	if err := s.store.Commit(ctx, reads, ws); err != nil {
		return time.Time{}, nil, mapCommitError(err)
	}
	s.clearReadSet(transaction)
	for _, w := range ws {
		if w.Document == nil {
			s.publishChange(ChangeEvent{Name: w.Name})
		} else {
			d := *w.Document
			s.publishChange(ChangeEvent{Name: w.Name, Doc: &d})
		}
	}
	return commitTime, results, nil
}

// BatchWrite applies writes non-atomically, returning per-write results.
func (s *Service) BatchWrite(ctx context.Context, writes []*writeWire) ([]any, []any, error) {
	if len(writes) > maxWriteBatchSize {
		return nil, nil, model.NewProviderError("InvalidArgument",
			"a batchWrite may contain at most "+strconv.Itoa(maxWriteBatchSize)+" writes", 400)
	}
	now := clock.Now()
	statuses := make([]any, 0, len(writes))
	writeResults := make([]any, 0, len(writes))
	for _, w := range writes {
		ws, results, err := s.buildWrites(ctx, []*writeWire{w}, now)
		if err != nil {
			statuses = append(statuses, statusWire(3, errMsg(err)))
			writeResults = append(writeResults, map[string]any{})
			continue
		}
		if err := s.store.Commit(ctx, nil, ws); err != nil {
			statuses = append(statuses, statusWireForError(err))
			writeResults = append(writeResults, map[string]any{})
			continue
		}
		statuses = append(statuses, statusWire(0, ""))
		writeResults = append(writeResults, results[0])
	}
	return statuses, writeResults, nil
}

// BatchGet returns the documents (found/missing wire items) for the given
// names, recording reads in the transaction read-set.
func (s *Service) BatchGet(ctx context.Context, documents []string, transaction []byte) ([]map[string]any, error) {
	if err := s.requireActive(transaction); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	readTime := clock.Now()
	items := make([]map[string]any, 0, len(documents))
	for _, name := range documents {
		if seen[name] {
			continue
		}
		seen[name] = true
		var item map[string]any
		doc, err := s.store.GetDocument(ctx, name)
		if err != nil {
			if errors.Is(err, firestorestore.ErrDocumentNotFound) {
				s.recordRead(transaction, name, firestorestore.Document{}, false)
				item = map[string]any{"missing": name, "readTime": readTime.Format(time.RFC3339Nano)}
			} else {
				return nil, err
			}
		} else {
			s.recordRead(transaction, name, doc, true)
			item = map[string]any{"found": documentMap(doc), "readTime": readTime.Format(time.RFC3339Nano)}
		}
		items = append(items, item)
	}
	return items, nil
}

// ─── write resolution ────────────────────────────────────────────────────────

// buildWrites translates wire writes into store writes + write-results,
// resolving masks and transforms against the current document state.
func (s *Service) buildWrites(ctx context.Context, wire []*writeWire, now time.Time) ([]firestorestore.Write, []map[string]any, error) {
	writes := make([]firestorestore.Write, 0, len(wire))
	results := make([]map[string]any, 0, len(wire))
	for _, w := range wire {
		pre, err := toPrecondition(w.CurrentDocument)
		if err != nil {
			return nil, nil, err
		}

		switch {
		case w.Delete != "":
			writes = append(writes, firestorestore.Write{Name: w.Delete, Precondition: pre})
			results = append(results, map[string]any{}) // no updateTime after delete

		case w.Transform != nil:
			doc, res, err := s.buildTransform(ctx, w.Transform, pre, now)
			if err != nil {
				return nil, nil, err
			}
			writes = append(writes, firestorestore.Write{Name: doc.Name, Document: &doc, Precondition: pre})
			results = append(results, res)

		case w.Update != nil:
			doc, res, err := s.buildUpdate(ctx, w.Update, w.UpdateMask, w.UpdateTransforms, pre, now)
			if err != nil {
				return nil, nil, err
			}
			writes = append(writes, firestorestore.Write{Name: doc.Name, Document: &doc, Precondition: pre})
			results = append(results, res)

		default:
			return nil, nil, model.NewProviderError("InvalidArgument", "write must specify update, delete, or transform", 400)
		}
	}
	return writes, results, nil
}

// buildUpdate resolves an update (with optional mask + transforms) against the
// current document.
func (s *Service) buildUpdate(ctx context.Context, dw *documentWire, mask *documentMaskWire, transforms []fieldTransformWire, pre *firestorestore.Precondition, now time.Time) (firestorestore.Document, map[string]any, error) {
	if dw.Name == "" {
		return firestorestore.Document{}, nil, model.NewProviderError("InvalidArgument", "update document name is required", 400)
	}
	if err := validateFields(dw.Fields); err != nil {
		return firestorestore.Document{}, nil, err
	}
	if err := checkSize(dw.Fields); err != nil {
		return firestorestore.Document{}, nil, err
	}

	existing, err := s.store.GetDocument(ctx, dw.Name)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return firestorestore.Document{}, nil, err
	}

	base := map[string]*firestorestore.Value{}
	if exists {
		base = existing.Fields
	}
	// Three mask states, matching real Firestore update semantics:
	//   - nil mask: replace-all (set) — document.fields fully replace the doc.
	//   - non-empty mask: patch — only the listed field paths are updated.
	//   - present-but-empty mask: transform-only update — document.fields is
	//     ignored entirely and the transforms apply on top of the existing doc.
	//
	// applyMask is NOT used for the empty-mask case because its len==0 → replace
	// contract is REST-patch-specific (it would wipe unrelated fields).
	var merged map[string]*firestorestore.Value
	switch {
	case mask == nil:
		merged = cloneFields(dw.Fields)
	case len(mask.FieldPaths) > 0:
		merged = applyMask(dw.Fields, mask.FieldPaths, base)
	default:
		merged = cloneFields(base)
	}

	// Apply post-update transforms to the merged fields.
	var transformResults []*firestorestore.Value
	if len(transforms) > 0 {
		var res []*firestorestore.Value
		merged, res, err = applyFieldTransforms(merged, transforms, now)
		if err != nil {
			return firestorestore.Document{}, nil, err
		}
		transformResults = res
	}
	if err := checkSize(merged); err != nil {
		return firestorestore.Document{}, nil, err
	}

	createTime := now
	if exists {
		createTime = existing.CreateTime
	}
	doc := firestorestore.Document{Name: dw.Name, Fields: merged, CreateTime: createTime, UpdateTime: now}
	res := map[string]any{"updateTime": now.Format(time.RFC3339Nano)}
	if len(transformResults) > 0 {
		tr := make([]any, 0, len(transformResults))
		for _, r := range transformResults {
			tr = append(tr, valueWire(r))
		}
		res["transformResults"] = tr
	}
	return doc, res, nil
}

// buildTransform resolves a document transform against the current document.
func (s *Service) buildTransform(ctx context.Context, tw *documentTransformWire, pre *firestorestore.Precondition, now time.Time) (firestorestore.Document, map[string]any, error) {
	if tw.Document == "" {
		return firestorestore.Document{}, nil, model.NewProviderError("InvalidArgument", "transform document name is required", 400)
	}
	existing, err := s.store.GetDocument(ctx, tw.Document)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return firestorestore.Document{}, nil, err
	}

	base := map[string]*firestorestore.Value{}
	createTime := now
	if exists {
		base = existing.Fields
		createTime = existing.CreateTime
	}
	fields, transformResults, err := applyFieldTransforms(base, tw.FieldTransforms, now)
	if err != nil {
		return firestorestore.Document{}, nil, err
	}
	if err := checkSize(fields); err != nil {
		return firestorestore.Document{}, nil, err
	}

	doc := firestorestore.Document{Name: tw.Document, Fields: fields, CreateTime: createTime, UpdateTime: now}
	tr := make([]any, 0, len(transformResults))
	for _, r := range transformResults {
		tr = append(tr, valueWire(r))
	}
	return doc, map[string]any{"updateTime": now.Format(time.RFC3339Nano), "transformResults": tr}, nil
}

// ─── index management ────────────────────────────────────────────────────────

// hasIndex reports whether a composite index covering the ordered required
// field paths exists for the database.
func (s *Service) hasIndex(ctx context.Context, project, database string, required []string) (bool, error) {
	if s.resources == nil {
		return false, nil
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtIndex, "databases/"+database+"/")
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		var idx indexDef
		if json.Unmarshal(e.Data, &idx) != nil {
			continue
		}
		var paths []string
		for _, f := range idx.Fields {
			if f.FieldPath != "__name__" {
				paths = append(paths, f.FieldPath)
			}
		}
		if len(paths) < len(required) {
			continue
		}
		match := true
		for i, r := range required {
			if paths[i] != r {
				match = false
				break
			}
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

// CreateIndex registers a composite index and returns its operation wrapper.
func (s *Service) CreateIndex(ctx context.Context, project, database, cg string, idx indexDef) (map[string]any, error) {
	if len(idx.Fields) < 2 {
		return nil, model.NewProviderError("InvalidArgument", "a composite index requires at least 2 fields", 400)
	}

	id := randomHex(12)
	idx.Name = fullIndexName(project, database, cg, id)
	if idx.QueryScope == "" {
		idx.QueryScope = "COLLECTION"
	}
	data, _ := json.Marshal(idx)

	if s.resources != nil {
		if err := s.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtIndex, ID: indexRelName(database, cg, id), Data: data}); err != nil {
			if errors.Is(err, store.ErrAlreadyExists) {
				return nil, model.NewProviderError("AlreadyExists", "index already exists", 409)
			}
			return nil, err
		}
	}
	// CreateIndex returns a google.longrunning.Operation wrapping the created
	// index (done=true), not the bare index body.
	opID := randomHex(12)
	opName := "projects/" + project + "/databases/" + database + "/operations/" + opID
	return map[string]any{
		"name":     opName,
		"done":     true,
		"response": indexMap(idx),
	}, nil
}

// ListIndexes returns the composite indexes for a collection group, paginated.
func (s *Service) ListIndexes(ctx context.Context, project, database, cg, filter string, page pageParams) ([]indexDef, string, error) {
	prefix := "databases/" + database + "/collectionGroups/" + cg + "/indexes/"
	var idxs []indexDef
	if s.resources != nil {
		entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtIndex, prefix)
		if err != nil {
			return nil, "", err
		}
		for _, e := range entries {
			var idx indexDef
			if json.Unmarshal(e.Data, &idx) == nil {
				idxs = append(idxs, idx)
			}
		}
	}
	// Optional `filter` query parameter: basic substring match on fieldPath.
	if filter != "" {
		kept := idxs[:0]
		for _, idx := range idxs {
			if indexMatchesFilter(idx, filter) {
				kept = append(kept, idx)
			}
		}
		idxs = kept
	}

	pageIdxs, nextToken := paging.Page(idxs, func(d indexDef) string { return d.Name }, page.params())
	return pageIdxs, nextToken, nil
}

// GetIndex returns a single composite index.
func (s *Service) GetIndex(ctx context.Context, project, database, cg, id string) (indexDef, error) {
	rel := indexRelName(database, cg, id)
	if s.resources == nil {
		return indexDef{}, model.NewProviderError("NotFound", "index not found", 404)
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtIndex, rel)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return indexDef{}, model.NewProviderError("NotFound", "index not found", 404)
		}
		return indexDef{}, err
	}
	var idx indexDef
	_ = json.Unmarshal(e.Data, &idx)
	return idx, nil
}

// DeleteIndex removes a composite index.
func (s *Service) DeleteIndex(ctx context.Context, project, database, cg, id string) error {
	if s.resources == nil {
		return nil
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtIndex, indexRelName(database, cg, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return model.NewProviderError("NotFound", "index not found", 404)
		}
		return err
	}
	return nil
}
