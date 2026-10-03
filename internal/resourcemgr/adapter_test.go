package resourcemgr_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"jaiscloud/internal/resourcemgr"
	"jaiscloud/internal/store"
)

// ─── minimal in-memory store.ResourceStore for testing ───────────────────────

type memStore struct {
	entries map[string]store.ResourceEntry
}

func newMemStore() *memStore {
	return &memStore{entries: make(map[string]store.ResourceEntry)}
}

func (m *memStore) key(t, id string) string { return t + "\x00" + id }

func (m *memStore) Create(_ context.Context, _, _ string, e store.ResourceEntry) error {
	k := m.key(e.Type, e.ID)
	if _, ok := m.entries[k]; ok {
		return store.ErrAlreadyExists
	}
	m.entries[k] = e
	return nil
}

func (m *memStore) Get(_ context.Context, _, _, t, id string) (store.ResourceEntry, error) {
	e, ok := m.entries[m.key(t, id)]
	if !ok {
		return store.ResourceEntry{}, store.ErrNotFound
	}
	return e, nil
}

func (m *memStore) Update(_ context.Context, _, _ string, e store.ResourceEntry) error {
	m.entries[m.key(e.Type, e.ID)] = e
	return nil
}

func (m *memStore) Upsert(_ context.Context, _, _ string, e store.ResourceEntry) error {
	m.entries[m.key(e.Type, e.ID)] = e
	return nil
}

func (m *memStore) UpsertAtomic(_ context.Context, _, _, t, id string, mutate func(store.ResourceEntry, bool) (store.ResourceEntry, error)) (store.ResourceEntry, error) {
	current, exists := m.entries[m.key(t, id)]
	next, err := mutate(current, exists)
	if err != nil {
		return store.ResourceEntry{}, err
	}
	m.entries[m.key(next.Type, next.ID)] = next
	return next, nil
}

func (m *memStore) Delete(_ context.Context, _, _, t, id string) error {
	delete(m.entries, m.key(t, id))
	return nil
}

func (m *memStore) List(_ context.Context, _, _, t, _ string) ([]store.ResourceEntry, error) {
	var out []store.ResourceEntry
	for _, e := range m.entries {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *memStore) Purge(_ context.Context, _, _, t string) error {
	for k, e := range m.entries {
		if e.Type == t {
			delete(m.entries, k)
		}
	}
	return nil
}

func (m *memStore) Reset(ctx context.Context) {
	m.entries = make(map[string]store.ResourceEntry)
}

// ─── Exists ───────────────────────────────────────────────────────────────────

func TestStoreAdapter_Exists_True(t *testing.T) {
	ms := newMemStore()
	ms.Create(context.Background(), "000000000000", "", store.ResourceEntry{Type: "cluster", ID: "c1", Data: json.RawMessage(`{}`)})
	a := resourcemgr.NewStoreAdapter(ms)

	ok, err := a.Exists(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil || !ok {
		t.Errorf("expected Exists=true, err=nil, got ok=%v err=%v", ok, err)
	}
}

func TestStoreAdapter_Exists_False(t *testing.T) {
	a := resourcemgr.NewStoreAdapter(newMemStore())
	ok, err := a.Exists(context.Background(), "000000000000", "", "cluster", "missing")
	if err != nil || ok {
		t.Errorf("expected Exists=false, err=nil, got ok=%v err=%v", ok, err)
	}
}

// ─── Get ──────────────────────────────────────────────────────────────────────

func TestStoreAdapter_Get_Found(t *testing.T) {
	ms := newMemStore()
	payload := json.RawMessage(`{"name":"test"}`)
	ms.Create(context.Background(), "000000000000", "", store.ResourceEntry{Type: "cluster", ID: "c1", Data: payload})
	a := resourcemgr.NewStoreAdapter(ms)

	e, err := a.Get(context.Background(), "000000000000", "", "cluster", "c1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if e.ID != "c1" || string(e.Data) != string(payload) {
		t.Errorf("unexpected entry: %+v", e)
	}
}

func TestStoreAdapter_Get_NotFound(t *testing.T) {
	a := resourcemgr.NewStoreAdapter(newMemStore())
	_, err := a.Get(context.Background(), "000000000000", "", "cluster", "missing")
	if err == nil {
		t.Fatal("expected error for missing resource")
	}
}

// ─── Create / Update / Delete ─────────────────────────────────────────────────

func TestStoreAdapter_Create(t *testing.T) {
	a := resourcemgr.NewStoreAdapter(newMemStore())
	err := a.Create(context.Background(), "000000000000", "", resourcemgr.ResourceEntry{
		Type: "cluster", ID: "c1", Data: []byte(`{"x":1}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	ok, _ := a.Exists(context.Background(), "000000000000", "", "cluster", "c1")
	if !ok {
		t.Error("entry should exist after Create")
	}
}

func TestStoreAdapter_Update(t *testing.T) {
	ms := newMemStore()
	ms.Create(context.Background(), "000000000000", "", store.ResourceEntry{Type: "cluster", ID: "c1", Data: json.RawMessage(`{"v":1}`)})
	a := resourcemgr.NewStoreAdapter(ms)

	err := a.Update(context.Background(), "000000000000", "", resourcemgr.ResourceEntry{
		Type: "cluster", ID: "c1", Data: []byte(`{"v":2}`),
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	e, _ := a.Get(context.Background(), "000000000000", "", "cluster", "c1")
	if string(e.Data) != `{"v":2}` {
		t.Errorf("data not updated: %s", e.Data)
	}
}

func TestStoreAdapter_Delete(t *testing.T) {
	ms := newMemStore()
	ms.Create(context.Background(), "000000000000", "", store.ResourceEntry{Type: "cluster", ID: "c1", Data: json.RawMessage(`{}`)})
	a := resourcemgr.NewStoreAdapter(ms)

	if err := a.Delete(context.Background(), "000000000000", "", "cluster", "c1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ok, _ := a.Exists(context.Background(), "000000000000", "", "cluster", "c1")
	if ok {
		t.Error("entry should not exist after Delete")
	}
}

// ─── List ─────────────────────────────────────────────────────────────────────

func TestStoreAdapter_List(t *testing.T) {
	ms := newMemStore()
	for _, id := range []string{"c1", "c2", "c3"} {
		ms.Create(context.Background(), "000000000000", "", store.ResourceEntry{Type: "cluster", ID: id, Data: json.RawMessage(`{}`)})
	}
	ms.Create(context.Background(), "000000000000", "", store.ResourceEntry{Type: "step", ID: "s1", Data: json.RawMessage(`{}`)})
	a := resourcemgr.NewStoreAdapter(ms)

	entries, err := a.List(context.Background(), "000000000000", "", "cluster", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("expected 3 clusters, got %d", len(entries))
	}
	for _, e := range entries {
		if e.Type != "cluster" {
			t.Errorf("unexpected type %s in cluster list", e.Type)
		}
	}
}

// ─── ErrNotFound propagation ──────────────────────────────────────────────────

func TestStoreAdapter_Exists_PropagatesNonNotFoundError(t *testing.T) {
	// A store that always returns a non-ErrNotFound error from Get
	a := resourcemgr.NewStoreAdapter(&errStore{err: errors.New("storage unavailable")})
	_, err := a.Exists(context.Background(), "000000000000", "", "cluster", "c1")
	if err == nil {
		t.Fatal("expected error propagation from store")
	}
}

// errStore always returns the given error from Get.
type errStore struct{ err error }

func (s *errStore) Create(_ context.Context, _, _ string, _ store.ResourceEntry) error {
	return s.err
}
func (s *errStore) Get(_ context.Context, _, _, _, _ string) (store.ResourceEntry, error) {
	return store.ResourceEntry{}, s.err
}
func (s *errStore) Update(_ context.Context, _, _ string, _ store.ResourceEntry) error {
	return s.err
}
func (s *errStore) Upsert(_ context.Context, _, _ string, _ store.ResourceEntry) error {
	return s.err
}
func (s *errStore) UpsertAtomic(_ context.Context, _, _, _, _ string, _ func(store.ResourceEntry, bool) (store.ResourceEntry, error)) (store.ResourceEntry, error) {
	return store.ResourceEntry{}, s.err
}
func (s *errStore) Delete(_ context.Context, _, _, _, _ string) error { return s.err }
func (s *errStore) List(_ context.Context, _, _, _, _ string) ([]store.ResourceEntry, error) {
	return nil, s.err
}
func (s *errStore) Purge(_ context.Context, _, _, _ string) error { return s.err }
func (s *errStore) Reset(ctx context.Context)                     {}
