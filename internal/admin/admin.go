package admin

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/persistence/snapshot"
	"jaiscloud/internal/persistence/version"
	"jaiscloud/internal/snapshottypes"
)

// Resetter is an alias for the canonical snapshottypes.Resetter.
type Resetter = snapshottypes.Resetter

// Snapshotter is an alias for the canonical snapshottypes.Snapshotter.
type Snapshotter = snapshottypes.Snapshotter

// ScopedResetter is implemented by stores that support per-account/region wipes.
type ScopedResetter interface {
	Resetter
	ResetScope(account, region string)
	ResetAccount(account string)
}

// CloudMismatchError is returned when the snapshot cloud does not match the running binary cloud.
// Field is named Code (not Error) to avoid shadowing the error interface method.
type CloudMismatchError struct {
	Code          string `json:"error"`
	Message       string `json:"message"`
	EnvelopeCloud string `json:"envelope_cloud"`
	InstanceCloud string `json:"instance_cloud"`
}

func (e *CloudMismatchError) Error() string { return e.Message }

// NonEmptyStateError is returned when import finds existing data.
type NonEmptyStateError struct {
	Code           string   `json:"error"`
	Message        string   `json:"message"`
	NonEmptyStores []string `json:"non_empty_stores"`
}

func (e *NonEmptyStateError) Error() string { return e.Message }

// SnapshotEnvelope is the top-level container for exported state (schema v3).
// DefaultRegion is informational — stores are account+region-keyed internally.
type SnapshotEnvelope struct {
	SchemaVersion int                        `json:"schema_version"`
	InstanceID    string                     `json:"instance_id,omitempty"`
	Cloud         string                     `json:"cloud,omitempty"`
	DefaultRegion string                     `json:"default_region,omitempty"`
	AccountID     string                     `json:"account_id,omitempty"`
	Stores        map[string]json.RawMessage `json:"stores"`
}

// HandlerMeta carries identity fields injected into every export envelope.
type HandlerMeta struct {
	InstanceID string
	Cloud      string
	Region     string
	AccountID  string
	StateDir   string // used for --new-instance instance-id persistence
}

// exportSoftLimitDefault is the default soft-warn threshold for export size (2 GiB).
const exportSoftLimitDefault = 2 * 1024 * 1024 * 1024

// SnapshotBlobStore is implemented by any blob store that supports export/import via tar archives.
type SnapshotBlobStore interface {
	CreateSnapshot(ctx context.Context, tw *tar.Writer) error
	RestoreSnapshot(ctx context.Context, tr *tar.Reader) error
}

// PostRestoreHook is called after all stores are successfully restored from a snapshot.
type PostRestoreHook interface {
	Name() string
	OnRestore(ctx context.Context) error
}

// Handler serves the /_jaiscloud/* admin endpoints.
type Handler struct {
	mu               sync.Mutex
	meta             HandlerMeta
	resetters        []Resetter
	snapshotters     map[string]Snapshotter
	postRestoreHooks []PostRestoreHook
	lambdaCode       LambdaCodeFetcher
	firehoseFlusher  FirehoseFlusher
	cwAlarmEvaluator CWAlarmEvaluator
	blobStore        SnapshotBlobStore
	kekFingerprint   string
	dataDir          string
	exportSoftLimit  int64
	barrier          *snapshot.Barrier
	clockMode        string // "real" | "fixed" | "offset"; "" means real
	ttlSweeper       TTLSweeper
	ebScheduler      EBSchedulerTicker
}

func NewHandler() *Handler {
	return &Handler{
		snapshotters:    make(map[string]Snapshotter),
		exportSoftLimit: exportSoftLimitDefault,
	}
}

// RegisterBlobStore wires a blob store into the handler so that
// Export includes blob data and Import restores it.
func (h *Handler) RegisterBlobStore(b SnapshotBlobStore) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.blobStore = b
}

// SetKEKFingerprint stores the running instance's KEK fingerprint for
// inclusion in export envelopes and validation during import.
func (h *Handler) SetKEKFingerprint(fp string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.kekFingerprint = fp
}

// SetDataDir sets the data directory used for blob staging during import.
func (h *Handler) SetDataDir(d string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dataDir = d
}

// RegisterPostRestoreHook adds a hook that is called after all stores are
// successfully restored during an import.
func (h *Handler) RegisterPostRestoreHook(hook PostRestoreHook) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.postRestoreHooks = append(h.postRestoreHooks, hook)
}

// SetBarrier wires the persistence barrier so that Import and Reset acquire
// the write lock before mutating state. If nil, barrier is bypassed (tests).
func (h *Handler) SetBarrier(b *snapshot.Barrier) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.barrier = b
}

// resetNoBarrier resets all stores without acquiring the barrier.
// Must be called while the write lock is already held (inside WriteBegin).
func (h *Handler) resetNoBarrier(ctx context.Context) {
	for _, rs := range h.resetters {
		rs.Reset(ctx)
	}
}

// SetMeta stores identity metadata used when building export envelopes.
func (h *Handler) SetMeta(m HandlerMeta) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.meta = m
}

// Meta returns the handler's identity metadata (safe for concurrent use).
func (h *Handler) Meta() HandlerMeta {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.meta
}

// RegisterResetter adds a store that will be reset on POST /_jaiscloud/reset.
func (h *Handler) RegisterResetter(r Resetter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.resetters = append(h.resetters, r)
}

// RegisterSnapshotter adds a named store for export/import.
func (h *Handler) RegisterSnapshotter(name string, s Snapshotter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapshotters[name] = s
}

// Snapshotters returns a copy of the registered snapshotters map.
// Keys are store names; values satisfy admin.Snapshotter.
func (h *Handler) Snapshotters() map[string]Snapshotter {
	h.mu.Lock()
	defer h.mu.Unlock()
	cp := make(map[string]Snapshotter, len(h.snapshotters))
	for k, v := range h.snapshotters {
		cp[k] = v
	}
	return cp
}

// Health handles GET /_jaiscloud/health.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// SetClock handles POST /_jaiscloud/clock — replaces the global clock.
func (h *Handler) SetClock(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode string    `json:"mode"`
		Time time.Time `json:"time"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	switch req.Mode {
	case "fixed":
		if req.Time.IsZero() {
			http.Error(w, "time must not be zero for fixed mode", http.StatusBadRequest)
			return
		}
		clock.SetGlobalClock(clock.FixedClock{T: req.Time})
	case "offset":
		if req.Time.IsZero() {
			http.Error(w, "time must not be zero for offset mode", http.StatusBadRequest)
			return
		}
		clock.SetGlobalClock(clock.NewOffsetClock(req.Time))
	case "real":
		clock.SetGlobalClock(clock.RealClock{})
	default:
		http.Error(w, "unknown mode: must be fixed, offset, or real", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	h.clockMode = req.Mode
	h.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// GetClock handles GET /_jaiscloud/clock — returns current clock state.
func (h *Handler) GetClock(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	mode := h.clockMode
	h.mu.Unlock()
	if mode == "" {
		mode = "real"
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"mode": mode,
		"time": clock.Now().Format(time.RFC3339Nano),
	})
}

// Doctor handles GET /_jaiscloud/doctor — extended health check with verbose info.
// Query param ?verbose=true returns additional fields.
func (h *Handler) Doctor(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	meta := h.meta
	kekFP := h.kekFingerprint
	dataDir := h.dataDir
	hasBarrier := h.barrier != nil
	h.mu.Unlock()

	if kekFP == "" {
		kekFP = "none"
	}

	resp := map[string]any{
		"status": "ok",
		"cloud":  meta.Cloud,
	}

	if r.URL.Query().Get("verbose") == "true" {
		backend := "memory"
		if dataDir != "" {
			backend = "file"
		}
		resp["import_in_progress"] = hasBarrier && false // barrier held = true; not easily detectable here
		resp["backend"] = backend
		resp["data_dir"] = dataDir
		resp["kek_fingerprint"] = kekFP
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Reset handles POST /_jaiscloud/reset — wipes state.
// Optional query params narrow the scope:
//
//	?account=111111111111              — wipes one account across all regions
//	?account=111111111111&region=us-east-1 — wipes one (account, region)
//
// Without params, all state is wiped (original behaviour).
func (h *Handler) Reset(w http.ResponseWriter, r *http.Request) {
	account := r.URL.Query().Get("account")
	region := r.URL.Query().Get("region")

	h.mu.Lock()
	resetters := make([]Resetter, len(h.resetters))
	copy(resetters, h.resetters)
	barrier := h.barrier
	h.mu.Unlock()

	// Acquire write lock so all in-flight cloud requests complete before reset.
	if barrier != nil {
		release, err := barrier.WriteBegin(r.Context())
		if err != nil {
			http.Error(w, "reset: acquire barrier: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer release()
	}

	ctx := r.Context()
	for _, rs := range resetters {
		if account == "" {
			rs.Reset(ctx)
			continue
		}
		if sr, ok := rs.(ScopedResetter); ok {
			if region != "" {
				sr.ResetScope(account, region)
			} else {
				sr.ResetAccount(account)
			}
		} else {
			// Non-scoped resetter: over-wipe (acceptable — no per-account state).
			rs.Reset(ctx)
		}
	}
	clock.SetGlobalClock(clock.RealClock{})
	h.mu.Lock()
	h.clockMode = "real"
	h.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "reset"}); err != nil {
		slog.Warn("admin: reset encode failed", "err", err)
	}
}

// countingWriter wraps an http.ResponseWriter and counts bytes written.
type countingWriter struct {
	http.ResponseWriter
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.ResponseWriter.Write(p)
	cw.n += int64(n)
	return n, err
}

// Export handles GET /_jaiscloud/export — returns a gzip-compressed tar archive.
// The archive contains:
//   - envelope.json  — schema-v3 envelope with all store snapshots
//   - blobs/<path>   — blob data from the registered blobStore (if any)
func (h *Handler) Export(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	snapshots := make(map[string]Snapshotter, len(h.snapshotters))
	for name, s := range h.snapshotters {
		snapshots[name] = s
	}
	meta := h.meta
	blobStore := h.blobStore
	kekFP := h.kekFingerprint
	softLimit := h.exportSoftLimit
	barrier := h.barrier
	h.mu.Unlock()

	// Acquire shared read lock so a concurrent reset/import cannot modify stores
	// while we are reading them. Multiple concurrent exports coexist fine.
	if barrier != nil {
		release, err := barrier.ReadBegin(r.Context())
		if err != nil {
			http.Error(w, "export: acquire barrier: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer release()
	}

	stores := make(map[string]json.RawMessage, len(snapshots))
	for name, s := range snapshots {
		var buf bytes.Buffer
		if err := s.Snapshot(r.Context(), &buf); err != nil {
			http.Error(w, "snapshot error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		stores[name] = json.RawMessage(buf.Bytes())
	}

	env := version.Envelope{
		SchemaVersion:  version.CodeSnapshotVersion,
		InstanceID:     meta.InstanceID,
		Cloud:          meta.Cloud,
		Region:         meta.Region,
		AccountID:      meta.AccountID,
		// not the emulated service time.
		CreatedAt:      clock.RealNow(),
		KEKFingerprint: kekFP,
		Stores:         stores,
	}
	envJSON, err := json.Marshal(env)
	if err != nil {
		http.Error(w, "marshal envelope: "+err.Error(), http.StatusInternalServerError)
		return
	}

	cw := &countingWriter{ResponseWriter: w}
	w.Header().Set("Content-Type", "application/gzip")

	gz := gzip.NewWriter(cw)
	tw := tar.NewWriter(gz)

	// Write envelope.json as the first entry.
	hdr := &tar.Header{
		Name:     "envelope.json",
		Typeflag: tar.TypeReg,
		Size:     int64(len(envJSON)),
		Mode:     0600,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		slog.Warn("admin: export write envelope header", "err", err)
		return
	}
	if _, err := tw.Write(envJSON); err != nil {
		slog.Warn("admin: export write envelope body", "err", err)
		return
	}

	// Write blob data if a blobStore is registered.
	if blobStore != nil {
		if err := blobStore.CreateSnapshot(r.Context(), tw); err != nil {
			slog.Warn("admin: export write blobs", "err", err)
			return
		}
	}

	if err := tw.Close(); err != nil {
		slog.Warn("admin: export tar close", "err", err)
		return
	}
	if err := gz.Close(); err != nil {
		slog.Warn("admin: export gzip close", "err", err)
		return
	}

	if softLimit > 0 && cw.n > softLimit {
		slog.Warn("admin: export size exceeds soft limit",
			"bytes", cw.n, "limit", softLimit)
	}
}

// Import handles POST /_jaiscloud/import — restores state from a snapshot.
//
// Accepts two formats:
//   - gzip-compressed tar archive (magic bytes \x1f\x8b): new format with
//     envelope.json + optional blobs/ entries.
//   - Plain JSON: legacy schema-v3 SnapshotEnvelope (backward compat for tests).
//
// Returns 409 if the envelope's cloud field does not match the running cloud.
// Returns 409 if the KEK fingerprint does not match.
// Returns 409 if any registered store already has state (non-empty guard).
// Query param ?new_instance=true generates a fresh instance ID instead of
// preserving the snapshot's instance ID; it also blocks imports of snapshots
// containing KMS key material (which would be undecryptable under a new ID).
func (h *Handler) Import(w http.ResponseWriter, r *http.Request) {
	dryRun := r.URL.Query().Get("dry_run") == "true"

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Detect format by gzip magic bytes.
	isTarball := len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b

	var stores map[string]json.RawMessage
	var envCloud, envInstanceID, envKEKFP string

	var blobData []byte
	var envSchemaVersion int
	if isTarball {
		var parsed version.Envelope
		parsed, stores, blobData, err = h.parseTarball(body)
		if err != nil {
			http.Error(w, "parse tarball: "+err.Error(), http.StatusBadRequest)
			return
		}
		envCloud = parsed.Cloud
		envInstanceID = parsed.InstanceID
		envKEKFP = parsed.KEKFingerprint
		envSchemaVersion = parsed.SchemaVersion
	} else {
		// Legacy plain JSON path.
		var env SnapshotEnvelope
		if err := json.Unmarshal(body, &env); err != nil || env.Stores == nil {
			http.Error(w, "invalid snapshot body: must be a schema-v3 SnapshotEnvelope with non-nil stores", http.StatusBadRequest)
			return
		}
		stores = env.Stores
		envCloud = env.Cloud
		envInstanceID = env.InstanceID
		envSchemaVersion = env.SchemaVersion
	}

	// Step 1: Schema version check — reject snapshots this binary cannot handle.
	if err := version.CheckSnapshotVersion(envSchemaVersion); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "schema_version_mismatch",
			"message": err.Error(),
		})
		return
	}

	// Step 2: Cloud identity check — must match running binary.
	h.mu.Lock()
	cloud := h.meta.Cloud
	kekFP := h.kekFingerprint
	h.mu.Unlock()

	if cloud != "" && envCloud != "" && envCloud != cloud {
		resp := &CloudMismatchError{
			Code:          "cloud_mismatch",
			Message:       fmt.Sprintf("snapshot cloud %q does not match this binary (%s). Use jaiscloud-%s to import this snapshot.", envCloud, cloud, envCloud),
			EnvelopeCloud: envCloud,
			InstanceCloud: cloud,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.Warn("admin: import cloud-mismatch encode failed", "err", err)
		}
		return
	}

	// Step 3: KEK fingerprint check (tarball path only).
	if isTarball && envKEKFP != "" {
		if err := version.CheckKEKFingerprint(envKEKFP, kekFP); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{
				"error":   "kek_mismatch",
				"message": err.Error(),
			})
			return
		}
	}

	newInstance := r.URL.Query().Get("new_instance") == "true"

	// Dry-run: validate without mutating state.
	if dryRun {
		h.mu.Lock()
		snapshotters := h.snapshotters
		h.mu.Unlock()

		instanceEmpty := true
		for _, s := range snapshotters {
			empty, _ := s.IsEmpty(r.Context())
			if !empty {
				instanceEmpty = false
				break
			}
		}

		kekMatch := true
		if isTarball && envKEKFP != "" {
			kekMatch = version.CheckKEKFingerprint(envKEKFP, kekFP) == nil
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"dry_run":          true,
			"valid":            true,
			"schema_version":   version.CodeSnapshotVersion,
			"cloud":            envCloud,
			"kek_match":        kekMatch,
			"stores_parseable": len(stores),
			"instance_empty":   instanceEmpty,
		})
		return
	}

	// KMS safety gate: if --new-instance, refuse if the snapshot contains KMS key material.
	if newInstance && snapshotHasKMSMaterial(stores) {
		http.Error(w,
			"snapshot contains KMS key material that cannot be decrypted under a new instance identity. "+
				"Import without --new-instance (rollback workflow) or strip kms_key rows from the snapshot first.",
			http.StatusConflict)
		return
	}

	// Instance ID handling.
	h.mu.Lock()
	stateDir := h.meta.StateDir
	h.mu.Unlock()

	if envInstanceID != "" && stateDir != "" {
		if newInstance {
			newID, err := config.GenerateNewInstanceID(stateDir)
			if err != nil {
				http.Error(w, "generate new instance id: "+err.Error(), http.StatusInternalServerError)
				return
			}
			slog.Info("admin: import --new-instance; assigned fresh instance id",
				"old", envInstanceID, "new", newID)
		} else {
			if err := config.WriteInstanceID(stateDir, envInstanceID); err != nil {
				http.Error(w, "write instance id: "+err.Error(), http.StatusInternalServerError)
				return
			}
			slog.Info("admin: import preserved snapshot instance id", "id", envInstanceID)
		}
	}

	h.mu.Lock()
	snapshotters := make(map[string]Snapshotter, len(h.snapshotters))
	for name, s := range h.snapshotters {
		snapshotters[name] = s
	}
	barrier := h.barrier
	h.mu.Unlock()

	// Acquire write lock — blocks all cloud requests while we mutate state.
	if barrier != nil {
		release, err := barrier.WriteBegin(r.Context())
		if err != nil {
			http.Error(w, "import: acquire barrier: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer release()
	}

	// ?reset_first=true clears all state before improting (analogous to snapshot revert).
	resetFirst := r.URL.Query().Get("reset_first") == "true"
	if resetFirst {
		h.mu.Lock()
		resetters := make([]Resetter, len(h.resetters))
		copy(resetters, h.resetters)
		h.mu.Unlock()
		for _, rs := range resetters {
			rs.Reset(r.Context())
		}
	}

	// Restore blobs after any reset so reset_first doesn't deelte just restored blob data.
	if err := h.restoreBlobs(r.Context(), blobData); err != nil {
		http.Error(w, "restore blobs: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Non-empty guard: refuse import if any store already has state.
	// Skipped when reset_first=true since the instance is already being wiped.
	if !resetFirst {
		var nonEmpty []string
		for name, s := range snapshotters {
			empty, err := s.IsEmpty(r.Context())
			if err != nil {
				http.Error(w, "isempty "+name+": "+err.Error(), http.StatusInternalServerError)
				return
			}
			if !empty {
				nonEmpty = append(nonEmpty, name)
			}
		}
		if len(nonEmpty) > 0 {
			sort.Strings(nonEmpty)
			resp := &NonEmptyStateError{
				Code:           "non_empty_state",
				Message:        "existing state found in stores: " + fmt.Sprintf("%v", nonEmpty) + ". Clear state first using one of:\n  1. POST /_jaiscloud/import?reset_first=true\n  2. POST /_jaiscloud/reset (then retry import)\n  3. Restart the server with --fresh-start",
				NonEmptyStores: nonEmpty,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				slog.Warn("admin: import non-empty encode failed", "err", err)
			}
			return
		}
	}

	// Restore all stores; on any failure, rollback by resetting all stores.
	rollbackCtx := r.Context()
	var restoreErr error
	defer func() {
		if restoreErr != nil {
			h.mu.Lock()
			resetters := make([]Resetter, len(h.resetters))
			copy(resetters, h.resetters)
			h.mu.Unlock()
			for _, rs := range resetters {
				rs.Reset(rollbackCtx)
			}
		}
	}()

	storesRestored := 0
	for name, s := range snapshotters {
		data, ok := stores[name]
		if !ok {
			continue
		}
		if err := s.Restore(r.Context(), bytes.NewReader(data)); err != nil {
			restoreErr = err
			http.Error(w, "restore "+name+": "+err.Error(), http.StatusInternalServerError)
			return
		}
		storesRestored++
	}

	// Call post-restore hooks. Failures are logged but do not fail the import.
	h.mu.Lock()
	hooks := make([]PostRestoreHook, len(h.postRestoreHooks))
	copy(hooks, h.postRestoreHooks)
	h.mu.Unlock()
	for _, hook := range hooks {
		if err := hook.OnRestore(r.Context()); err != nil {
			slog.Warn("admin: post-restore hook failed", "hook", hook.Name(), "err", err)
		}
	}

	clock.SetGlobalClock(clock.RealClock{})
	h.mu.Lock()
	h.clockMode = "real"
	h.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"status":          "imported",
		"stores_restored": storesRestored,
	}); err != nil {
		slog.Warn("admin: import encode failed", "err", err)
	}
}

// parseTarball decompresses a gzip tarball and returns the parsed envelope,
// its stores map, and a prebuilt tar archieve containing only blob entries.
// Blob restoreation is deferred so callers can reset state first before calling
// restoreBlobs - this prevents reset_first from deleting just-restored blobs.
func (h *Handler) parseTarball(body []byte) (version.Envelope, map[string]json.RawMessage, []byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return version.Envelope{}, nil, nil, fmt.Errorf("gzip open: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)

	var env version.Envelope
	gotEnvelope := false

	// Buffer blob enteries for deferred restoration.
	var blobBuf bytes.Buffer
	blobTW := tar.NewWriter(&blobBuf)

	h.mu.Lock()
	hasBlobStore := h.blobStore != nil
	h.mu.Unlock()

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return version.Envelope{}, nil, nil, fmt.Errorf("tar next: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		switch {
		case hdr.Name == "envelope.json":
			data, err := io.ReadAll(tr)
			if err != nil {
				return version.Envelope{}, nil, nil, fmt.Errorf("read envelope.json: %w", err)
			}
			if err := json.Unmarshal(data, &env); err != nil {
				return version.Envelope{}, nil, nil, fmt.Errorf("parse envelope.json: %w", err)
			}
			gotEnvelope = true

		case strings.HasPrefix(hdr.Name, "blobs/") && len(hdr.Name) > len("blobs/"):
			rel := hdr.Name[len("blobs/"):]
			clean := filepath.Clean(rel)
			if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") {
				return version.Envelope{}, nil, nil, fmt.Errorf("tarball: unsafe blob path %q", hdr.Name)
			}
			if !hasBlobStore {
				continue
			}
			data, err := io.ReadAll(tr)
			if err != nil {
				return version.Envelope{}, nil, nil, fmt.Errorf("read blob %s: %w", hdr.Name, err)
			}
			entryHdr := *hdr
			entryHdr.Size = int64(len(data))
			if err := blobTW.WriteHeader(&entryHdr); err != nil {
				return version.Envelope{}, nil, nil, fmt.Errorf("buffer blob header %s: %w", hdr.Name, err)
			}
			if _, err := blobTW.Write(data); err != nil {
				return version.Envelope{}, nil, nil, fmt.Errorf("buffer blob data %s: %w", hdr.Name, err)
			}

		}
	}
	blobTW.Close()

	if !gotEnvelope {
		return version.Envelope{}, nil, nil, fmt.Errorf("tarball missing envelope.json")
	}
	if env.Stores == nil {
		env.Stores = make(map[string]json.RawMessage)
	}
	return env, env.Stores, blobBuf.Bytes(), nil
}

// restoreBlobs restores blob data from the given tar archive bytes.
func (h *Handler) restoreBlobs(ctx context.Context, blobData []byte) error {
	if len(blobData) == 0 {
		return nil
	}

	h.mu.Lock()
	blobStore := h.blobStore
	h.mu.Unlock()

	if blobStore == nil {
		return nil // no blob store registered; skip restoration
	}
	tr := tar.NewReader(bytes.NewReader(blobData))
	return blobStore.RestoreSnapshot(ctx, tr)
}

// snapshotHasKMSMaterial returns true if the stores map contains a non-empty
// KMS key store snapshot. The KMS store is registered under "keys".
func snapshotHasKMSMaterial(stores map[string]json.RawMessage) bool {
	data, ok := stores["keys"]
	if !ok {
		return false
	}
	// An empty JSON object "{}" or null/empty means no keys.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return true // fail-safe: unparseable → treat as having material
	}
	return len(raw) > 0
}
