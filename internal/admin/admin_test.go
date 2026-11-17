package admin

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"jaiscloud/internal/persistence/version"
)

// mockSnapshotter is a test double implementing admin.Snapshotter.
type mockSnapshotter struct {
	data    map[string]json.RawMessage
	isEmpty bool
	snapErr error
	restErr error
}

func (m *mockSnapshotter) Snapshot(_ context.Context, w io.Writer) error {
	if m.snapErr != nil {
		return m.snapErr
	}
	return json.NewEncoder(w).Encode(m.data)
}

func (m *mockSnapshotter) Restore(_ context.Context, r io.Reader) error {
	if m.restErr != nil {
		return m.restErr
	}
	return json.NewDecoder(r).Decode(&m.data)
}

func (m *mockSnapshotter) IsEmpty(_ context.Context) (bool, error) {
	return m.isEmpty, nil
}

func (m *mockSnapshotter) Reset(ctx context.Context) { m.data = nil; m.isEmpty = true }

// mockScopedResetter tracks which Reset variant was called.
type mockScopedResetter struct {
	resetCalled       bool
	resetAccountCalls []string
	resetScopeCalls   [][2]string
}

func (m *mockScopedResetter) Reset(ctx context.Context) { m.resetCalled = true }
func (m *mockScopedResetter) ResetAccount(account string) {
	m.resetAccountCalls = append(m.resetAccountCalls, account)
}
func (m *mockScopedResetter) ResetScope(account, region string) {
	m.resetScopeCalls = append(m.resetScopeCalls, [2]string{account, region})
}

// --- snapshotHasKMSMaterial ---

func TestSnapshotHasKMSMaterial_Null(t *testing.T) {
	stores := map[string]json.RawMessage{
		"keys": json.RawMessage("null"),
	}
	if snapshotHasKMSMaterial(stores) {
		t.Fatal("expected false for null JSON value")
	}
}

func TestSnapshotHasKMSMaterial_Empty(t *testing.T) {
	stores := map[string]json.RawMessage{
		"keys": json.RawMessage("{}"),
	}
	if snapshotHasKMSMaterial(stores) {
		t.Fatal("expected false for empty JSON object")
	}
}

func TestSnapshotHasKMSMaterial_Unparseable(t *testing.T) {
	stores := map[string]json.RawMessage{
		"keys": json.RawMessage("not-json"),
	}
	if !snapshotHasKMSMaterial(stores) {
		t.Fatal("expected true (fail-safe) for unparseable bytes")
	}
}

func TestSnapshotHasKMSMaterial_HasKeys(t *testing.T) {
	stores := map[string]json.RawMessage{
		"keys": json.RawMessage(`{"key-id-1":{}}`),
	}
	if !snapshotHasKMSMaterial(stores) {
		t.Fatal("expected true when keys map is non-empty")
	}
}

func TestSnapshotHasKMSMaterial_WrongKey(t *testing.T) {
	// Old "kms-keys" key must NOT trigger the gate — only "keys" should.
	stores := map[string]json.RawMessage{
		"kms-keys": json.RawMessage(`{"key-id-1":{}}`),
	}
	if snapshotHasKMSMaterial(stores) {
		t.Fatal("expected false: gate must use key 'keys', not 'kms-keys'")
	}
}

// --- Export helpers ---

// decodeExportEnvelope extracts the envelope.json entry from a gzip tarball
// written by Handler.Export and decodes it into a version.Envelope.
func decodeExportEnvelope(t *testing.T, body []byte) version.Envelope {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip open: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next: %v", err)
		}
		if hdr.Name == "envelope.json" {
			data, err := io.ReadAll(tr)
			if err != nil {
				t.Fatalf("read envelope.json: %v", err)
			}
			var env version.Envelope
			if err := json.Unmarshal(data, &env); err != nil {
				t.Fatalf("parse envelope.json: %v", err)
			}
			return env
		}
	}
	t.Fatal("tarball missing envelope.json")
	return version.Envelope{}
}

// --- Handler.Export ---

// TestDoctorReportsAgentGuide pins the discovery field: an agent that can only
// reach the HTTP API learns how to get the integration guide from doctor.
func TestDoctorReportsAgentGuide(t *testing.T) {
	for _, cloud := range []string{"aws", "gcp"} {
		h := NewHandler()
		h.SetMeta(HandlerMeta{Cloud: cloud})
		rec := httptest.NewRecorder()
		h.Doctor(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/doctor", nil))

		var got map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("%s: doctor is not JSON: %v", cloud, err)
		}
		want := "jaiscloud-" + cloud + " agent-guide"
		if got["agent_guide"] != want {
			t.Fatalf("%s: agent_guide = %v, want %q", cloud, got["agent_guide"], want)
		}
		if got["cloud"] != cloud {
			t.Fatalf("%s: cloud = %v", cloud, got["cloud"])
		}
	}
}

func TestHandler_Export_IncludesAllRegisteredStores(t *testing.T) {
	h := NewHandler()
	for _, name := range []string{"resources", "keys", "secrets", "parameters"} {
		h.RegisterSnapshotter(name, &mockSnapshotter{isEmpty: true})
	}
	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))

	env := decodeExportEnvelope(t, rec.Body.Bytes())
	for _, name := range []string{"resources", "keys", "secrets", "parameters"} {
		if _, ok := env.Stores[name]; !ok {
			t.Errorf("expected store key %q in export", name)
		}
	}
}

func TestHandler_Export_SchemaVersion3(t *testing.T) {
	h := NewHandler()
	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))

	env := decodeExportEnvelope(t, rec.Body.Bytes())
	if env.SchemaVersion != 3 {
		t.Errorf("expected schema_version 3, got %d", env.SchemaVersion)
	}
}

func TestHandler_Export_CloudField(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})
	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))

	env := decodeExportEnvelope(t, rec.Body.Bytes())
	if env.Cloud != "aws" {
		t.Errorf("expected cloud 'aws', got %q", env.Cloud)
	}
}

func TestHandler_Export_SnapshotterError(t *testing.T) {
	h := NewHandler()
	snap := &mockSnapshotter{}
	snap.snapErr = errTest("boom")
	h.RegisterSnapshotter("resources", snap)

	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestHandler_Export_KEKFingerprint(t *testing.T) {
	h := NewHandler()
	h.SetKEKFingerprint("abc1234567890def")

	rec := httptest.NewRecorder()
	h.Export(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/export", nil))

	env := decodeExportEnvelope(t, rec.Body.Bytes())
	if env.KEKFingerprint != "abc1234567890def" {
		t.Errorf("expected kek_fingerprint 'abc1234567890def', got %q", env.KEKFingerprint)
	}
}

// --- Handler.Import ---

func TestHandler_Import_CloudMismatch(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "gcp", Stores: map[string]json.RawMessage{}}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
	var resp CloudMismatchError
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp.Code != "cloud_mismatch" {
		t.Errorf("expected error 'cloud_mismatch', got %q", resp.Code)
	}
	if resp.EnvelopeCloud != "gcp" {
		t.Errorf("expected envelope_cloud 'gcp', got %q", resp.EnvelopeCloud)
	}
	if resp.InstanceCloud != "aws" {
		t.Errorf("expected instance_cloud 'aws', got %q", resp.InstanceCloud)
	}
	if resp.Message == "" {
		t.Error("expected non-empty message")
	}
}

func TestHandler_Import_NonEmptyState_409Body(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	// store-a is non-empty, store-b is empty
	h.RegisterSnapshotter("store-a", &mockSnapshotter{isEmpty: false})
	h.RegisterSnapshotter("store-b", &mockSnapshotter{isEmpty: true})

	stores := map[string]json.RawMessage{"store-a": json.RawMessage("{}"), "store-b": json.RawMessage("{}")}
	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "aws", Stores: stores}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
	var resp NonEmptyStateError
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp.Code != "non_empty_state" {
		t.Errorf("expected error 'non_empty_state', got %q", resp.Code)
	}
	if len(resp.NonEmptyStores) == 0 {
		t.Error("expected non_empty_stores to be non-empty")
	}
	found := false
	for _, s := range resp.NonEmptyStores {
		if s == "store-a" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'store-a' in non_empty_stores, got %v", resp.NonEmptyStores)
	}
	// message must contain at least one escape path
	for _, hint := range []string{"POST /_jaiscloud/reset", "--fresh-start", "--data-dir"} {
		if bytes.Contains([]byte(resp.Message), []byte(hint)) {
			goto foundHint
		}
	}
	t.Error("expected message to contain at least one escape path (reset / --fresh-start / --data-dir)")
foundHint:
}

func TestHandler_Import_V1BareMapRejected(t *testing.T) {
	h := NewHandler()
	// bare map (v1) must be rejected
	body := []byte(`{"resources":"{}"}`)
	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for bare-map body, got %d", rec.Code)
	}
}

func TestHandler_Import_UnknownStoreKey(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})
	h.RegisterSnapshotter("resources", &mockSnapshotter{isEmpty: true})

	stores := map[string]json.RawMessage{
		"unknown-store": json.RawMessage("{}"),
	}
	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "aws", Stores: stores}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 (unknown key silently skipped), got %d", rec.Code)
	}
}

func TestHandler_Import_RestoreError(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})
	snap := &mockSnapshotter{isEmpty: true}
	snap.restErr = errTest("restore failed")
	h.RegisterSnapshotter("resources", snap)

	stores := map[string]json.RawMessage{"resources": json.RawMessage("{}")}
	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "aws", Stores: stores}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

// --- Handler.Reset ---

func TestHandler_Reset_NoParams(t *testing.T) {
	h := NewHandler()
	r := &mockScopedResetter{}
	h.RegisterResetter(r)

	rec := httptest.NewRecorder()
	h.Reset(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/reset", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if !r.resetCalled {
		t.Error("expected Reset() to be called")
	}
}

func TestHandler_Reset_AccountScope(t *testing.T) {
	h := NewHandler()
	r := &mockScopedResetter{}
	h.RegisterResetter(r)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/reset?account=111111111111", nil)
	h.Reset(rec, req)

	if len(r.resetAccountCalls) != 1 || r.resetAccountCalls[0] != "111111111111" {
		t.Errorf("expected ResetAccount('111111111111'), got %v", r.resetAccountCalls)
	}
	if r.resetCalled {
		t.Error("expected Reset() NOT to be called when account param is set")
	}
}

func TestHandler_Reset_AccountRegionScope(t *testing.T) {
	h := NewHandler()
	r := &mockScopedResetter{}
	h.RegisterResetter(r)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/reset?account=111111111111&region=us-east-1", nil)
	h.Reset(rec, req)

	if len(r.resetScopeCalls) != 1 {
		t.Fatalf("expected one ResetScope call, got %d", len(r.resetScopeCalls))
	}
	got := r.resetScopeCalls[0]
	if got[0] != "111111111111" || got[1] != "us-east-1" {
		t.Errorf("expected ResetScope('111111111111','us-east-1'), got %v", got)
	}
	if r.resetCalled {
		t.Error("expected Reset() NOT to be called when account+region params are set")
	}
}

func TestImport_RestoreFailure_RollsBackToEmpty(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	// snap1 restores successfully, snap2 fails — both should be rolled back.
	snap1 := &mockSnapshotter{isEmpty: true}
	snap2 := &mockSnapshotter{isEmpty: true, restErr: errTest("restore failed")}
	h.RegisterSnapshotter("store1", snap1)
	h.RegisterSnapshotter("store2", snap2)
	// Register them as resetters too so rollback can clear them.
	h.RegisterResetter(snap1)
	h.RegisterResetter(snap2)

	stores := map[string]json.RawMessage{
		"store1": json.RawMessage(`{"k":"v"}`),
		"store2": json.RawMessage(`{"k":"v"}`),
	}
	env := SnapshotEnvelope{SchemaVersion: 3, Cloud: "aws", Stores: stores}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(body)))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on restore error, got %d", rec.Code)
	}
	// After rollback, snap1 must have been reset (data cleared).
	if snap1.data != nil {
		t.Error("expected snap1 to be rolled back (data nil) after restore failure")
	}
}

func TestImport_KEKMismatch_Rejected(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})
	h.SetKEKFingerprint("aaaa000011112222") // instance fingerprint

	// Build a tarball whose envelope has a different KEK fingerprint.
	env := version.Envelope{
		SchemaVersion:  version.CodeSnapshotVersion,
		Cloud:          "aws",
		KEKFingerprint: "bbbb999988887777", // different
		Stores:         map[string]json.RawMessage{},
	}
	tarball := buildTestTarball(t, env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(tarball)))

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 on KEK mismatch, got %d: %s", rec.Code, rec.Body.String())
	}
}

// buildTestTarball creates a minimal gzip tarball with envelope.json for testing.
func buildTestTarball(t *testing.T, env version.Envelope) []byte {
	t.Helper()
	envJSON, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	hdr := &tar.Header{
		Name:     "envelope.json",
		Typeflag: tar.TypeReg,
		Size:     int64(len(envJSON)),
		Mode:     0600,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if _, err := tw.Write(envJSON); err != nil {
		t.Fatalf("write body: %v", err)
	}
	tw.Close()
	gz.Close()
	return buf.Bytes()
}

// errTest is a simple error value for tests.
type errTest string

func (e errTest) Error() string { return string(e) }

// --- Additional Handler.Import tests ---

// TestHandler_Import_MissingStoreKey registers a snapshotter for "resources" but the
// snapshot's stores map only contains "other-store". Import should succeed (200)
// and the "resources" snapshotter is left untouched (Restore never called).
func TestHandler_Import_MissingStoreKey(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	snap := &mockSnapshotter{isEmpty: true}
	h.RegisterSnapshotter("resources", snap)

	env := version.Envelope{
		SchemaVersion: version.CodeSnapshotVersion,
		Cloud:         "aws",
		Stores:        map[string]json.RawMessage{"other-store": json.RawMessage("{}")},
	}
	tarball := buildTestTarball(t, env)

	rec := httptest.NewRecorder()
	h.Import(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(tarball)))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	// Restore was never called — data must still be nil.
	if snap.data != nil {
		t.Error("expected snapshotter data to remain nil (Restore should not have been called)")
	}
}

// TestHandler_Import_KMSMaterial_NewInstance checks that ?new_instance=true blocks
// import when the snapshot contains non-empty KMS key material.
func TestHandler_Import_KMSMaterial_NewInstance(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	// Plain JSON path (no gzip magic bytes) — KMS gate runs before barrier acquisition.
	env := SnapshotEnvelope{
		SchemaVersion: 3,
		Cloud:         "aws",
		Stores:        map[string]json.RawMessage{"keys": json.RawMessage(`{"key-id-1":{}}`)},
	}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/import?new_instance=true", bytes.NewReader(body))
	h.Import(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 when importing KMS material with new_instance=true, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestHandler_Import_KMSMaterial_NewInstance_Empty checks that ?new_instance=true
// does NOT block import when the "keys" store is an empty object.
func TestHandler_Import_KMSMaterial_NewInstance_Empty(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	snap := &mockSnapshotter{isEmpty: true}
	h.RegisterSnapshotter("keys", snap)

	// Plain JSON envelope with "keys": {} — empty, so KMS gate must not fire.
	env := SnapshotEnvelope{
		SchemaVersion: 3,
		Cloud:         "aws",
		Stores:        map[string]json.RawMessage{"keys": json.RawMessage(`{}`)},
	}
	body, _ := json.Marshal(env)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/import?new_instance=true", bytes.NewReader(body))
	h.Import(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 when keys store is empty with new_instance=true, got %d: %s", rec.Code, rec.Body.String())
	}
}

// blockingSnapshotter is a Snapshotter whose Restore blocks until ready is closed
// or ctx is cancelled. Used to test context cancellation mid-restore.
type blockingSnapshotter struct {
	ready    chan struct{}
	failErr  error
	isEmpty  bool
	restored atomic.Bool
}

func (b *blockingSnapshotter) Snapshot(_ context.Context, w io.Writer) error { return nil }

func (b *blockingSnapshotter) Restore(ctx context.Context, _ io.Reader) error {
	select {
	case <-b.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	b.restored.Store(true)
	return b.failErr
}

func (b *blockingSnapshotter) IsEmpty(_ context.Context) (bool, error) { return b.isEmpty, nil }

func (b *blockingSnapshotter) Reset(ctx context.Context) { b.isEmpty = true }

// TestImport_ContextCancel_MidRestore verifies that when the request context is
// already cancelled before Import runs, the handler does not return 200 and
// rollback (Reset) is invoked on the blocking snapshotter.
func TestImport_ContextCancel_MidRestore(t *testing.T) {
	h := NewHandler()
	h.SetMeta(HandlerMeta{Cloud: "aws"})

	blocker := &blockingSnapshotter{
		ready:   make(chan struct{}), // never closed — Restore will block until ctx cancel
		isEmpty: true,
	}
	h.RegisterSnapshotter("resources", blocker)
	h.RegisterResetter(blocker)

	env := version.Envelope{
		SchemaVersion: version.CodeSnapshotVersion,
		Cloud:         "aws",
		Stores:        map[string]json.RawMessage{"resources": json.RawMessage("{}")},
	}
	tarball := buildTestTarball(t, env)

	// Use an already-cancelled context so any blocking select immediately returns.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/import", bytes.NewReader(tarball)).WithContext(ctx)
	h.Import(rec, req)

	// The handler must NOT succeed (200 would mean we restored with a cancelled ctx).
	if rec.Code == http.StatusOK {
		t.Fatalf("expected non-200 response when context is cancelled, got 200")
	}

	// Rollback must have run — blocker.isEmpty is set to true by Reset().
	if !blocker.isEmpty {
		t.Error("expected blocker.isEmpty == true after rollback (Reset was called)")
	}
}
