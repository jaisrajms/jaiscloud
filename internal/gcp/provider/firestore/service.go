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
	"jaiscloud/internal/gcp/resource"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/timestamp"
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
	// bounded ordered history used for resume-token replay), changeFloor (the
	// highest evicted sequence), and changeSubs.
	changeMu    sync.Mutex
	changeSeq   uint64
	changeLog   changeRing
	changeFloor uint64
	changeSubs  map[*changeSub]struct{}

	// changeRetentionOverride, when > 0, replaces changeLogRetention. Tests set
	// it to exercise eviction without writing a full retention window; the
	// zero value selects the package default.
	changeRetentionOverride int
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

// Reset clears transaction read-sets and the change-feed history/sequence
// (document state is reset via the store's own Resetter; index state via the
// resource store's). Live subscribers (changeSubs) are left attached: they
// only ever receive events published after Reset, from the fresh sequence, so
// leaving them subscribed is safe — clearing changeLog/changeSeq/changeFloor is
// what matters, since ChangesSince(seq) would otherwise keep replaying pre-reset
// history that references documents the reset store no longer has. A pre-reset
// resume token is rejected by ReplayableFrom (it is ahead of the fresh
// sequence), so it resnapshots rather than replaying stale deltas.
func (s *Service) Reset(_ context.Context) {
	s.txnMu.Lock()
	s.readSets = make(map[string]*readSet)
	s.txnMu.Unlock()

	s.changeMu.Lock()
	s.changeSeq = 0
	s.changeLog.reset()
	s.changeFloor = 0
	s.changeMu.Unlock()
}

// docName builds the full document resource name.
func docName(project, database, path string) string {
	return resource.ResourceID(project)("firestore-document", "databases/"+database+"/documents/"+path)
}

// subcollectionIDUnder reports whether a document at relative path relDocPath
// contributes a subcollection to the document parent (also a relative path;
// empty for the database's documents root). Firestore's listCollectionIds
// returns a collection id whenever parent/<id> is a prefix of a document path
// at any depth, so an intermediate document need not exist. Both paths are
// alternately collection/document segments and end on a document (or are
// empty), so the segment immediately after parent names a collection.
func subcollectionIDUnder(relDocPath, parent string) (string, bool) {
	segs := splitRelativePath(relDocPath)
	par := splitRelativePath(parent)
	if len(segs) <= len(par) {
		return "", false
	}
	for i := range par {
		if segs[i] != par[i] {
			return "", false
		}
	}
	return segs[len(par)], true
}

// splitRelativePath splits a relative Firestore path into segments; the empty
// path (the documents root) has no segments.
func splitRelativePath(path string) []string {
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

// validDocumentParent reports whether a relative path is a Firestore document
// path (an even number of alternating collection/document segments) or the
// documents root (empty). listCollectionIds requires a document parent; a
// collection path is rejected instead of being silently misread as one, which
// is what real Firestore does.
func validDocumentParent(path string) bool {
	return len(splitRelativePath(path))%2 == 0
}

// nextUpdateTime returns the update timestamp to stamp on a write, guaranteed
// to be strictly greater than the document's current update time.
//
// Firestore uses updateTime as its optimistic-concurrency token: a write only
// succeeds if the stored updateTime still equals the one the writer observed
// (a currentDocument.updateTime precondition, or a transaction read-set entry).
// The emulator derives updateTime from clock.Now(), which a frozen clock (time
// control, deterministic tests) can return unchanged for every write. Without a
// guard, two concurrent writers would both observe T, both compute T, and both
// pass the check — a silent lost update.
//
// Both stamps are truncated to microseconds (the granularity of the Postgres
// TIMESTAMPTZ column, so the token survives a round-trip through the persistent
// backend) and a microsecond is added whenever the clock does not advance. That
// keeps updateTime strictly monotonic per document, so the second writer's
// observed value no longer matches and its commit aborts instead of
// overwriting. Under a live clock now is almost always after prev, making the
// conditional advance a no-op.
func nextUpdateTime(prev, now time.Time) time.Time {
	prev = prev.Truncate(time.Microsecond)
	now = now.Truncate(time.Microsecond)
	if prev.IsZero() || now.After(prev) {
		return now
	}
	return prev.Add(time.Microsecond)
}

// documentTime returns the current business time truncated to microseconds, the
// precision both backends persist (the Postgres column is TIMESTAMPTZ). Stamping
// documents at that precision keeps a createTime/updateTime returned to a client
// equal to the value a subsequent read of the store yields, so it can be used as
// a currentDocument.updateTime precondition.
func documentTime() time.Time {
	return clock.Now().Truncate(time.Microsecond)
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
	if err := validateDocumentName(name); err != nil {
		return firestorestore.Document{}, err
	}
	if err := validateFields(fields); err != nil {
		return firestorestore.Document{}, err
	}
	if err := checkSize(fields); err != nil {
		return firestorestore.Document{}, err
	}
	now := documentTime()
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

// UpdateMask is a *write* mask (Firestore's `update_mask`, which fields to
// update) together with its presence. Presence is carried separately because
// the proto's message-presence is what tells an omitted mask from an empty one,
// and conflating the two silently rewrites data:
//   - nil: no mask was supplied — the body fields replace the document's fields.
//   - non-nil with paths: patch — only the listed field paths are written.
//   - non-nil without paths: AIP-134's no-op — the supplied fields are ignored
//     and the stored document is returned unmodified (on a missing document the
//     upsert still creates it, with no fields).
//
// This is not Firestore's response mask (`DocumentMask.mask`, "fields to
// return"), which projects the reply and never restricts the write.
type UpdateMask struct {
	Paths []string
}

// ProjectDocument applies a response field mask (`mask` / `mask.fieldPaths`):
// the returned document carries only the requested paths. A request without a
// mask, or with an empty one, returns every field, matching GetDocument.
func ProjectDocument(doc firestorestore.Document, paths []string) firestorestore.Document {
	if len(paths) == 0 {
		return doc
	}
	doc.Fields = maskFields(doc, paths)
	return doc
}

// PatchDocument upserts a document: a missing document is created. The mask
// restricts which fields are updated (see UpdateMask for the three states);
// without it the fields fully replace the document's fields.
func (s *Service) PatchDocument(ctx context.Context, project, database, path string, fields map[string]*firestorestore.Value, mask *UpdateMask, pre *firestorestore.Precondition) (firestorestore.Document, error) {
	if err := validatePath(path); err != nil {
		return firestorestore.Document{}, err
	}
	name := docName(project, database, path)
	if err := validateDocumentName(name); err != nil {
		return firestorestore.Document{}, err
	}
	if err := validateFields(fields); err != nil {
		return firestorestore.Document{}, err
	}

	now := documentTime()

	existing, err := s.store.GetDocument(ctx, name)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return firestorestore.Document{}, err
	}

	// Optimistic precondition check (currentDocument.exists / updateTime).
	// exists=true on a missing document -> NOT_FOUND; exists=false on a present
	// document -> ALREADY_EXISTS; updateTime mismatch -> FAILED_PRECONDITION.
	if err := checkPrecondition(name, exists, existing.UpdateTime, pre); err != nil {
		return firestorestore.Document{}, err
	}

	// Three mask states (UpdateMask): omitted replaces, a populated mask
	// patches, and a present-but-empty mask is AIP-134's no-op.
	noop := mask != nil && len(mask.Paths) == 0
	var doc firestorestore.Document
	switch {
	case noop && exists:
		// Return the stored document untouched: no field applied, no
		// update_time rotation, no change event. The write below still runs
		// through store.Commit so the precondition and the implicit read are
		// re-validated atomically, but it stores identical content — a
		// concurrent write landing in between therefore surfaces as a
		// retryable ABORTED rather than being silently ignored.
		doc = existing
	case noop:
		// No-op on a missing document. PATCH is an upsert, so the document is
		// created — but with no fields: the empty mask discards the body, the
		// same as the transform-only update path in buildUpdate.
		doc = firestorestore.Document{
			Name:       name,
			Fields:     map[string]*firestorestore.Value{},
			CreateTime: now,
			UpdateTime: now,
		}
	default:
		var base map[string]*firestorestore.Value
		createTime := now
		updateTime := now
		if exists {
			base = existing.Fields
			createTime = existing.CreateTime
			updateTime = nextUpdateTime(existing.UpdateTime, now)
		}
		var merged map[string]*firestorestore.Value
		if mask == nil {
			merged = cloneFields(fields)
		} else {
			merged = applyMask(fields, mask.Paths, base)
		}
		if err := checkSize(merged); err != nil {
			return firestorestore.Document{}, err
		}
		doc = firestorestore.Document{
			Name:       name,
			Fields:     merged,
			CreateTime: createTime,
			UpdateTime: updateTime,
		}
	}
	// Route through store.Commit (the same atomic check-and-apply primitive
	// transactions use) instead of a separate Get-then-Update/Create, so a
	// write that landed between our read above and this call is detected
	// instead of silently overwritten: the implicit ReadRef re-validates
	// (exists, updateTime) — Firestore's own optimistic-concurrency token,
	// the ETag/If-Match equivalent — against the live document under the
	// store's lock, atomically with applying the write. The lock is only
	// held for that fast in-memory check-and-apply, not across this
	// function's merge computation.
	implicitRead := firestorestore.ReadRef{Name: name, Exists: exists, UpdateTime: existing.UpdateTime}
	if err := s.store.Commit(ctx, []firestorestore.ReadRef{implicitRead}, []firestorestore.Write{
		{Name: name, Document: &doc, Precondition: pre},
	}); err != nil {
		return firestorestore.Document{}, mapCommitError(err)
	}
	if noop && exists {
		// Nothing changed, so no document change to fan out to listeners.
		return doc, nil
	}
	d := doc
	s.publishChange(ChangeEvent{Name: d.Name, Doc: &d})
	return doc, nil
}

// DeleteDocument deletes a document. Idempotent, unless a currentDocument
// precondition is set (then it is validated first).
func (s *Service) DeleteDocument(ctx context.Context, project, database, path string, pre *firestorestore.Precondition) error {
	name := docName(project, database, path)
	if err := validateDocumentName(name); err != nil {
		return err
	}
	if pre != nil {
		existing, err := s.store.GetDocument(ctx, name)
		exists := err == nil
		if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
			return err
		}
		if err := checkPrecondition(name, exists, existing.UpdateTime, pre); err != nil {
			return err
		}
	}
	if err := s.store.DeleteDocument(ctx, name); err != nil {
		return mapStoreError(err)
	}
	s.publishChange(ChangeEvent{Name: name})
	return nil
}

// ListDocuments returns the direct children of a collection, paginated. When
// showMissing is set, documents that do not exist but have sub-documents nested
// underneath them are also returned, keyed by name with no fields or timestamps
// (Firestore's documents.list showMissing semantics).
func (s *Service) ListDocuments(ctx context.Context, project, database, path string, transaction []byte, mask []string, showMissing bool, page pageParams) ([]firestorestore.Document, string, error) {
	if err := s.requireActive(transaction); err != nil {
		return nil, "", err
	}
	docs, err := s.store.ListDocuments(ctx, project, database)
	if err != nil {
		return nil, "", err
	}
	coll := docName(project, database, path)
	children := make([]firestorestore.Document, 0)
	existing := make(map[string]bool)
	for _, d := range docs {
		if d.ParentPath == coll {
			children = append(children, d)
			existing[docID(d.Name)] = true
		}
	}
	// A missing document is one that has no document but is an ancestor path of a
	// deeper document: it appears as the first sub-path segment under this
	// collection. Track it by full name so the read-set records Exists=false.
	missing := make(map[string]bool)
	if showMissing {
		prefix := coll + "/"
		for _, d := range docs {
			if !strings.HasPrefix(d.Name, prefix) {
				continue
			}
			id := d.Name[len(prefix):]
			if i := strings.IndexByte(id, '/'); i >= 0 {
				id = id[:i]
			}
			name := coll + "/" + id
			if id == "" || existing[id] || missing[name] {
				continue
			}
			missing[name] = true
			children = append(children, firestorestore.Document{Name: name})
		}
	}

	pageDocs, nextToken, err := paging.Page(children, func(d firestorestore.Document) string { return d.Name }, page.params())
	if err != nil {
		return nil, "", err
	}

	if len(transaction) > 0 {
		for _, d := range pageDocs {
			s.recordRead(transaction, d.Name, d, !missing[d.Name])
		}
	}

	if len(mask) > 0 {
		for i := range pageDocs {
			pageDocs[i].Fields = maskFields(pageDocs[i], mask)
		}
	}
	return pageDocs, nextToken, nil
}

// docID returns the final path segment of a document resource name.
func docID(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// ListCollectionIds returns the distinct subcollection IDs of a document,
// paginated. A collection is included whenever its path is a prefix of any
// document at any depth, even when the document that owns it does not itself
// exist as a stored document (a "missing" parent Firestore allows).
func (s *Service) ListCollectionIds(ctx context.Context, project, database, path string, page pageParams) ([]string, string, error) {
	if !validDocumentParent(path) {
		return nil, "", model.NewProviderError("InvalidArgument", "invalid parent document path", 400)
	}
	docs, err := s.store.ListDocuments(ctx, project, database)
	if err != nil {
		return nil, "", err
	}
	seen := map[string]bool{}
	var ids []string
	for _, d := range docs {
		_, _, rel, ok := firestorestore.ParseDocumentName(d.Name)
		if !ok {
			continue
		}
		id, ok := subcollectionIDUnder(rel, path)
		if !ok || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Strings(ids)

	pageIDs, nextToken, err := paging.Page(ids, func(id string) string { return id }, page.params())
	if err != nil {
		return nil, "", err
	}
	return pageIDs, nextToken, nil
}

// RunQuery executes a StructuredQuery over the documents in a project+database,
// enforcing composite-index requirements and recording reads in the
// transaction read-set.
func (s *Service) RunQuery(ctx context.Context, project, database, path string, q *structuredQuery, transaction []byte) ([]firestorestore.Document, error) {
	if err := s.requireActive(transaction); err != nil {
		return nil, err
	}
	parent := resource.ResourceID(project)("firestore-document", "databases/"+database+"/documents")
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
	ws, results, implicitReads, changed, err := s.buildWrites(ctx, writes, commitTime)
	if err != nil {
		return time.Time{}, nil, err
	}
	reads := append(s.readSetFor(transaction), implicitReads...)
	if err := s.store.Commit(ctx, reads, ws); err != nil {
		return time.Time{}, nil, mapCommitError(err)
	}
	s.clearReadSet(transaction)
	for i, w := range ws {
		if !changed[i] {
			continue
		}
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
	now := documentTime()
	statuses := make([]any, 0, len(writes))
	writeResults := make([]any, 0, len(writes))
	for _, w := range writes {
		ws, results, implicitReads, changed, err := s.buildWrites(ctx, []*writeWire{w}, now)
		if err != nil {
			statuses = append(statuses, statusWireForWriteError(err))
			writeResults = append(writeResults, map[string]any{})
			continue
		}
		if err := s.store.Commit(ctx, implicitReads, ws); err != nil {
			statuses = append(statuses, statusWireForWriteError(err))
			writeResults = append(writeResults, map[string]any{})
			continue
		}
		// A committed write fans out to listeners, exactly as in Commit: real
		// Firestore's batchWrite notifies Listen subscribers for every write it
		// applies (verified against real GCP) and stays silent for the
		// empty-mask no-op.
		if changed[0] {
			if w := ws[0]; w.Document == nil {
				s.publishChange(ChangeEvent{Name: w.Name})
			} else {
				d := *w.Document
				s.publishChange(ChangeEvent{Name: w.Name, Doc: &d})
			}
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
				item = map[string]any{"missing": name, "readTime": timestamp.Canonical(readTime)}
			} else {
				return nil, err
			}
		} else {
			s.recordRead(transaction, name, doc, true)
			item = map[string]any{"found": documentMap(doc), "readTime": timestamp.Canonical(readTime)}
		}
		items = append(items, item)
	}
	return items, nil
}

// ─── write resolution ────────────────────────────────────────────────────────

// buildWrites translates wire writes into store writes + write-results,
// resolving masks and transforms against the current document state. The
// returned reads are the implicit ReadRefs buildUpdate/buildTransform used as
// their merge base — the caller must fold these into the read-set passed to
// store.Commit (see buildUpdate's doc comment). Delete writes need no
// implicit ReadRef: an unconditional delete is idempotent by design (real
// Firestore's DeleteDocument doesn't error without an explicit precondition),
// so there's no "lost update" for a concurrent delete to cause. Existence
// preconditions are left to store.Commit, which validates the transaction
// read-set first and then the per-write precondition under its lock, returning
// the named DocumentMissingError / DocumentExistsError the caller maps to the
// canonical NOT_FOUND / ALREADY_EXISTS.
//
// changed reports, per write, whether the write actually altered the document.
// It is false only for AIP-134's present-but-empty update mask on an existing
// document with no field transforms — a no-op that real GCP answers with the
// stored update_time and does not report to listeners (verified against real
// Firestore: an empty-mask commit yields no DocumentChange on Listen, while the
// same commit with a transform or a real field write does). Both transports use
// it to skip publishChange.
func (s *Service) buildWrites(ctx context.Context, wire []*writeWire, now time.Time) ([]firestorestore.Write, []map[string]any, []firestorestore.ReadRef, []bool, error) {
	writes := make([]firestorestore.Write, 0, len(wire))
	results := make([]map[string]any, 0, len(wire))
	reads := make([]firestorestore.ReadRef, 0, len(wire))
	changed := make([]bool, 0, len(wire))
	for _, w := range wire {
		pre, err := toPrecondition(w.CurrentDocument)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		switch {
		case w.Delete != "":
			if err := validateDocumentName(w.Delete); err != nil {
				return nil, nil, nil, nil, err
			}
			writes = append(writes, firestorestore.Write{Name: w.Delete, Precondition: pre})
			results = append(results, map[string]any{}) // no updateTime after delete
			changed = append(changed, true)

		case w.Transform != nil:
			doc, res, readRef, err := s.buildTransform(ctx, w.Transform, pre, now)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			writes = append(writes, firestorestore.Write{Name: doc.Name, Document: &doc, Precondition: pre})
			results = append(results, res)
			reads = append(reads, readRef)
			changed = append(changed, true)

		case w.Update != nil:
			doc, res, readRef, writeChanged, err := s.buildUpdate(ctx, w.Update, w.UpdateMask, w.UpdateTransforms, pre, now)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			writes = append(writes, firestorestore.Write{Name: doc.Name, Document: &doc, Precondition: pre})
			results = append(results, res)
			reads = append(reads, readRef)
			changed = append(changed, writeChanged)

		default:
			return nil, nil, nil, nil, model.NewProviderError("InvalidArgument", "write must specify update, delete, or transform", 400)
		}
	}
	return writes, results, reads, changed, nil
}

// buildUpdate resolves an update (with optional mask + transforms) against the
// current document. The returned ReadRef is the document state this merge was
// computed from — the caller must fold it into the read-set passed to
// store.Commit so a write that lands between this read and the eventually
// computed write is detected (ErrAborted) instead of silently overwritten.
// changed is false for the empty-mask no-op on an existing document (see
// buildWrites).
func (s *Service) buildUpdate(ctx context.Context, dw *documentWire, mask *documentMaskWire, transforms []fieldTransformWire, pre *firestorestore.Precondition, now time.Time) (firestorestore.Document, map[string]any, firestorestore.ReadRef, bool, error) {
	now = now.Truncate(time.Microsecond)
	if dw.Name == "" {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, false, model.NewProviderError("InvalidArgument", "update document name is required", 400)
	}
	if err := validateDocumentName(dw.Name); err != nil {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, false, err
	}
	if err := validateFields(dw.Fields); err != nil {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, false, err
	}
	if err := checkSize(dw.Fields); err != nil {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, false, err
	}

	existing, err := s.store.GetDocument(ctx, dw.Name)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, false, err
	}
	readRef := firestorestore.ReadRef{Name: dw.Name, Exists: exists, UpdateTime: existing.UpdateTime}

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
	//
	// With no transforms too, the empty mask is AIP-134's no-op on an existing
	// document: the stored document comes back untouched, update_time does not
	// rotate, and listeners see no change (real Firestore: the write result
	// carries the stored update_time and Listen reports only NO_CHANGE). A
	// missing document is still created, fieldless, as the upsert real GCP does.
	// This mirrors PatchDocument's no-op branch (DC52).
	if mask != nil && len(mask.FieldPaths) == 0 && len(transforms) == 0 && exists {
		return existing, map[string]any{"updateTime": timestamp.Canonical(existing.UpdateTime)}, readRef, false, nil
	}
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
			return firestorestore.Document{}, nil, firestorestore.ReadRef{}, false, err
		}
		transformResults = res
	}
	if err := checkSize(merged); err != nil {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, false, err
	}

	createTime := now
	updateTime := now
	if exists {
		createTime = existing.CreateTime
		updateTime = nextUpdateTime(existing.UpdateTime, now)
	}
	doc := firestorestore.Document{Name: dw.Name, Fields: merged, CreateTime: createTime, UpdateTime: updateTime}
	res := map[string]any{"updateTime": timestamp.Canonical(updateTime)}
	if len(transformResults) > 0 {
		tr := make([]any, 0, len(transformResults))
		for _, r := range transformResults {
			tr = append(tr, valueWire(r))
		}
		res["transformResults"] = tr
	}
	return doc, res, readRef, true, nil
}

// buildTransform resolves a document transform against the current document.
// The returned ReadRef is the document state the transform was computed from
// — see buildUpdate's doc comment for why the caller must fold it into the
// read-set passed to store.Commit.
func (s *Service) buildTransform(ctx context.Context, tw *documentTransformWire, pre *firestorestore.Precondition, now time.Time) (firestorestore.Document, map[string]any, firestorestore.ReadRef, error) {
	now = now.Truncate(time.Microsecond)
	if tw.Document == "" {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, model.NewProviderError("InvalidArgument", "transform document name is required", 400)
	}
	if err := validateDocumentName(tw.Document); err != nil {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, err
	}
	existing, err := s.store.GetDocument(ctx, tw.Document)
	exists := err == nil
	if err != nil && !errors.Is(err, firestorestore.ErrDocumentNotFound) {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, err
	}
	readRef := firestorestore.ReadRef{Name: tw.Document, Exists: exists, UpdateTime: existing.UpdateTime}

	base := map[string]*firestorestore.Value{}
	createTime := now
	updateTime := now
	if exists {
		base = existing.Fields
		createTime = existing.CreateTime
		updateTime = nextUpdateTime(existing.UpdateTime, now)
	}
	fields, transformResults, err := applyFieldTransforms(base, tw.FieldTransforms, now)
	if err != nil {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, err
	}
	if err := checkSize(fields); err != nil {
		return firestorestore.Document{}, nil, firestorestore.ReadRef{}, err
	}

	doc := firestorestore.Document{Name: tw.Document, Fields: fields, CreateTime: createTime, UpdateTime: updateTime}
	tr := make([]any, 0, len(transformResults))
	for _, r := range transformResults {
		tr = append(tr, valueWire(r))
	}
	return doc, map[string]any{"updateTime": timestamp.Canonical(updateTime), "transformResults": tr}, readRef, nil
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

// CreateIndexDef registers a composite index and returns the stored definition
// together with its long-running operation name. It is the shared core behind
// both the REST CreateIndex wrapper and the FirestoreAdmin gRPC transport.
func (s *Service) CreateIndexDef(ctx context.Context, project, database, cg string, idx IndexDef) (IndexDef, string, error) {
	if len(idx.Fields) < 2 {
		return IndexDef{}, "", model.NewProviderError("InvalidArgument", "a composite index requires at least 2 fields", 400)
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
				return IndexDef{}, "", model.NewProviderError("AlreadyExists", "index already exists", 409)
			}
			return IndexDef{}, "", err
		}
	}
	// The operation name is the google.longrunning.Operation the index creation
	// is wrapped in (done=true); both transports complete it synchronously.
	opName := resource.ResourceID(project)("firestore-operation", database+"/"+randomHex(12))
	return idx, opName, nil
}

// CreateIndex registers a composite index and returns its operation wrapper.
func (s *Service) CreateIndex(ctx context.Context, project, database, cg string, idx IndexDef) (map[string]any, error) {
	created, opName, err := s.CreateIndexDef(ctx, project, database, cg, idx)
	if err != nil {
		return nil, err
	}
	// CreateIndex returns a google.longrunning.Operation wrapping the created
	// index (done=true), not the bare index body.
	return map[string]any{
		"name":     opName,
		"done":     true,
		"response": indexMap(created),
	}, nil
}

// ListIndexes returns composite indexes, paginated. A specific collection group
// (cg) lists only that group's indexes; cg == "-" is the AIP-123 wildcard that
// lists every collection group's indexes, matching the real API behavior used by
// `gcloud firestore indexes composite list` and the Firestore console.
func (s *Service) ListIndexes(ctx context.Context, project, database, cg, filter string, page pageParams) ([]indexDef, string, error) {
	// The trailing slash is significant: for a concrete group the prefix is
	// scoped to that group's /indexes/ collection; for the wildcard it spans
	// every collectionGroups/*/indexes entry under the database.
	prefix := "databases/" + database + "/collectionGroups/" + cg + "/indexes/"
	if cg == "-" {
		prefix = "databases/" + database + "/collectionGroups/"
	}
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

	pageIdxs, nextToken, err := paging.Page(idxs, func(d indexDef) string { return d.Name }, page.params())
	if err != nil {
		return nil, "", err
	}
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
