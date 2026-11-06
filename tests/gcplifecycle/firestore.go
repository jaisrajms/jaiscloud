package gcplifecycle

import (
	"context"
	"fmt"
	"testing"
	"time"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"jaiscloud/internal/gcp/grpc/firestoreadmin"
	fsprovider "jaiscloud/internal/gcp/provider/firestore"
	"jaiscloud/internal/gcp/resource"
	fsstore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
)

// Firestore Admin control-plane resource types (the child collections a
// database owns). Backups are location-scoped, which is why they are retained.
const (
	fsTypeField    = "firestore_field"
	fsTypeIndex    = "firestore_index"
	fsTypeUserCred = "firestore_user_creds"
	fsTypeSchedule = "firestore_backup_schedule"
	fsTypeBackup   = "firestore_backup"
	fsTypeStream   = "firestore_change_stream"
)

// firestoreManifest is the FC5 ownership contract for a Firestore database:
// the data plane and every database-scoped admin child cascade; backups are
// deliberately retained ("Deleting the source database does not automatically
// delete related backups").
var firestoreManifest = manifest{
	parent: "firestore database",
	policy: Cascade,
	children: []childSpec{
		{"documents", Cascade},
		{"indexes", Cascade},
		{"fields", Cascade},
		{"userCreds", Cascade},
		{"backupSchedules", Cascade},
		{"changeStreams", Cascade},
		{"backups", Retain},
	},
	note: "delete tombstones the database first, then purges the children; a create that replaces a tombstone re-runs the purge (TGM16)",
}

type firestoreAdapter struct {
	seq       int
	prov      *fsprovider.Provider
	docs      fsstore.FirestoreStore
	resources store.ResourceStore
	client    adminpb.FirestoreAdminClient
	project   string
}

func newFirestoreAdapter(t *testing.T) adapter {
	t.Helper()
	resources := store.NewMemoryResourceStore()
	docs := fsstore.NewMemoryStore()
	prov := fsprovider.New(docs, resources)
	conn := serveGRPC(t, func(srv *grpc.Server) {
		adminpb.RegisterFirestoreAdminServer(srv, firestoreadmin.NewService(prov.Service, "proj"))
	})
	return &firestoreAdapter{
		prov:      prov,
		docs:      docs,
		resources: resources,
		client:    adminpb.NewFirestoreAdminClient(conn),
		project:   "proj",
	}
}

func (a *firestoreAdapter) name() string { return "firestore database" }

func (a *firestoreAdapter) create(ctx context.Context, tr transport, id string) (string, error) {
	if tr == grpcT {
		op, err := a.client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
			Parent:     "projects/" + a.project,
			DatabaseId: id,
			Database:   &adminpb.Database{LocationId: "nam5"},
		})
		if err != nil {
			return "", err
		}
		db := &adminpb.Database{}
		if err := op.GetResponse().UnmarshalTo(db); err != nil {
			return "", err
		}
		return db.GetUid(), nil
	}
	resp, err := a.prov.CreateDatabase(ctx, a.nr("databases", map[string]any{
		"databaseId": id,
		"body":       map[string]any{"locationId": "nam5"},
	}))
	if err != nil {
		return "", err
	}
	inner, _ := resp.Data["response"].(map[string]any)
	uid, _ := inner["uid"].(string)
	return uid, nil
}

func (a *firestoreAdapter) get(ctx context.Context, tr transport, id string) (bool, error) {
	name := "projects/" + a.project + "/databases/" + id
	if tr == grpcT {
		_, err := a.client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
		if err != nil {
			if isNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	_, err := a.prov.GetDatabase(ctx, a.nr("databases/"+id, nil))
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (a *firestoreAdapter) list(ctx context.Context, tr transport) ([]string, error) {
	if tr == grpcT {
		resp, err := a.client.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: "projects/" + a.project})
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(resp.GetDatabases()))
		for _, db := range resp.GetDatabases() {
			out = append(out, shortName(db.GetName()))
		}
		return out, nil
	}
	resp, err := a.prov.ListDatabases(ctx, a.nr("databases", nil))
	if err != nil {
		return nil, err
	}
	items, _ := resp.Data["databases"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		m, _ := it.(map[string]any)
		name, _ := m["name"].(string)
		out = append(out, shortName(name))
	}
	return out, nil
}

func (a *firestoreAdapter) update(ctx context.Context, tr transport, id string) error {
	name := "projects/" + a.project + "/databases/" + id
	if tr == grpcT {
		if _, err := a.client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database:   &adminpb.Database{Name: name, ConcurrencyMode: adminpb.Database_PESSIMISTIC},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
		}); err != nil {
			return err
		}
		got, err := a.client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
		if err != nil {
			return err
		}
		if got.GetConcurrencyMode() != adminpb.Database_PESSIMISTIC {
			return fmt.Errorf("concurrencyMode did not round-trip over gRPC: %v", got.GetConcurrencyMode())
		}
		return nil
	}
	_, err := a.prov.UpdateDatabase(ctx, a.nr("databases/"+id, map[string]any{
		"updateMask": "concurrencyMode",
		"body":       map[string]any{"concurrencyMode": "PESSIMISTIC"},
	}))
	if err != nil {
		return err
	}
	got, err := a.prov.GetDatabase(ctx, a.nr("databases/"+id, nil))
	if err != nil {
		return err
	}
	if got.Data["concurrencyMode"] != "PESSIMISTIC" {
		return fmt.Errorf("concurrencyMode did not round-trip over REST: %v", got.Data["concurrencyMode"])
	}
	return nil
}

func (a *firestoreAdapter) remove(ctx context.Context, tr transport, id string) error {
	etag, err := a.currentEtag(ctx, tr, id)
	if err != nil {
		return err
	}
	return a.deleteWithEtag(ctx, tr, id, etag)
}

func (a *firestoreAdapter) deleteWithEtag(ctx context.Context, tr transport, id, etag string) error {
	name := "projects/" + a.project + "/databases/" + id
	if tr == grpcT {
		_, err := a.client.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: name, Etag: etag})
		return err
	}
	_, err := a.prov.DeleteDatabase(ctx, a.nr("databases/"+id, map[string]any{"etag": etag}))
	return err
}

func (a *firestoreAdapter) currentEtag(ctx context.Context, tr transport, id string) (string, error) {
	name := "projects/" + a.project + "/databases/" + id
	if tr == grpcT {
		db, err := a.client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
		if err != nil {
			return "", err
		}
		return db.GetEtag(), nil
	}
	resp, err := a.prov.GetDatabase(ctx, a.nr("databases/"+id, nil))
	if err != nil {
		return "", err
	}
	etag, _ := resp.Data["etag"].(string)
	return etag, nil
}

func (a *firestoreAdapter) setProtection(ctx context.Context, tr transport, id, state string) error {
	name := "projects/" + a.project + "/databases/" + id
	if tr == grpcT {
		enum := adminpb.Database_DELETE_PROTECTION_DISABLED
		if state == "DELETE_PROTECTION_ENABLED" {
			enum = adminpb.Database_DELETE_PROTECTION_ENABLED
		}
		_, err := a.client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database:   &adminpb.Database{Name: name, DeleteProtectionState: enum},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_protection_state"}},
		})
		return err
	}
	_, err := a.prov.UpdateDatabase(ctx, a.nr("databases/"+id, map[string]any{
		"updateMask": "deleteProtectionState",
		"body":       map[string]any{"deleteProtectionState": state},
	}))
	return err
}

func (a *firestoreAdapter) blockedDelete(ctx context.Context, tr transport, id string) []blockedResult {
	var out []blockedResult
	etag, err := a.currentEtag(ctx, tr, id)
	if err != nil {
		return []blockedResult{{name: "read-etag (staging must succeed)", err: err}}
	}
	out = append(out, blockedResult{
		name:     "stale-etag",
		err:      a.deleteWithEtag(ctx, tr, id, etag+"-stale"),
		wantCode: "FailedPrecondition",
	})

	if err := a.setProtection(ctx, tr, id, "DELETE_PROTECTION_ENABLED"); err != nil {
		out = append(out, blockedResult{name: "enable-delete-protection (staging must succeed)", err: err})
		return out
	}
	etag, err = a.currentEtag(ctx, tr, id)
	if err != nil {
		out = append(out, blockedResult{name: "read-etag-protected (staging must succeed)", err: err})
		return out
	}
	out = append(out, blockedResult{
		name:     "delete-protection",
		err:      a.deleteWithEtag(ctx, tr, id, etag),
		wantCode: "FailedPrecondition",
	})
	// Leave the database deletable for the rest of the sequence.
	if err := a.setProtection(ctx, tr, id, "DELETE_PROTECTION_DISABLED"); err != nil {
		out = append(out, blockedResult{name: "disable-delete-protection (staging must succeed)", err: err})
	}
	return out
}

func (a *firestoreAdapter) populate(ctx context.Context, id string) error {
	a.seq++
	n := fmt.Sprintf("c%d", a.seq)
	doc := fsstore.Document{
		Name:   fmt.Sprintf("projects/%s/databases/%s/documents/c/%s", a.project, id, n),
		Fields: map[string]*fsstore.Value{},
	}
	if err := a.docs.CreateDocument(ctx, doc); err != nil {
		return err
	}
	base := "databases/" + id
	for _, entry := range []store.ResourceEntry{
		{Type: fsTypeIndex, ID: base + "/collectionGroups/c/indexes/i" + n, Data: []byte(`{"name":"i"}`)},
		{Type: fsTypeField, ID: base + "/collectionGroups/c/fields/f" + n, Data: []byte(`{"name":"f"}`)},
		{Type: fsTypeUserCred, ID: base + "/userCreds/cred" + n, Data: []byte(`{"name":"cred"}`)},
		{Type: fsTypeSchedule, ID: base + "/backupSchedules/" + n, Data: []byte(`{"name":"sched"}`)},
		{Type: fsTypeStream, ID: base + "/changeStreams/cs" + n, Data: []byte(`{"name":"cs"}`)},
		{Type: fsTypeBackup, ID: "locations/nam5/backups/b1", Data: []byte(`{"name":"b1"}`)},
	} {
		if err := a.resources.Upsert(ctx, a.project, store.GlobalRegion, entry); err != nil {
			return err
		}
	}
	return nil
}

func (a *firestoreAdapter) counts(ctx context.Context, id string) (map[string]childCount, error) {
	out := map[string]childCount{}
	docs, err := a.docs.ListDocuments(ctx, a.project, id)
	if err != nil {
		return nil, err
	}
	out["documents"] = childCount{total: len(docs), attached: len(docs)}
	prefix := "databases/" + id + "/"
	for name, typ := range map[string]string{
		"indexes":         fsTypeIndex,
		"fields":          fsTypeField,
		"userCreds":       fsTypeUserCred,
		"backupSchedules": fsTypeSchedule,
		"changeStreams":   fsTypeStream,
	} {
		rows, err := a.resources.List(ctx, a.project, store.GlobalRegion, typ, prefix)
		if err != nil {
			return nil, err
		}
		out[name] = childCount{total: len(rows), attached: len(rows)}
	}
	backups, err := a.resources.List(ctx, a.project, store.GlobalRegion, fsTypeBackup, "locations/")
	if err != nil {
		return nil, err
	}
	out["backups"] = childCount{total: len(backups), attached: len(backups)}
	return out, nil
}

// clearRejecting is a no-op: a Firestore database never refuses a delete because
// of its children (the data plane cascades).
func (a *firestoreAdapter) clearRejecting(context.Context, string) error { return nil }

// extra pins the TGM16 recreate edge at the harness level: after deleting a
// database whose children were staged, the first child create on the re-created
// database must succeed (the pre-fix code answered 409 "backup_schedule
// already exists" because both transports assigned schedule id "1").
func (a *firestoreAdapter) extra(t *testing.T, ctx context.Context, tr transport, id string) {
	t.Helper()
	if _, err := a.create(ctx, tr, id); err != nil {
		t.Fatalf("create before schedule recreate: %v", err)
	}
	if err := a.populate(ctx, id); err != nil {
		t.Fatalf("populate before schedule recreate: %v", err)
	}
	if err := a.remove(ctx, tr, id); err != nil {
		t.Fatalf("delete before schedule recreate: %v", err)
	}
	if _, err := a.create(ctx, tr, id); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	name := "projects/" + a.project + "/databases/" + id
	if tr == grpcT {
		_, err := a.client.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
			Parent: name,
			BackupSchedule: &adminpb.BackupSchedule{
				Retention:  durationpb.New(24 * time.Hour),
				Recurrence: &adminpb.BackupSchedule_DailyRecurrence{DailyRecurrence: &adminpb.DailyRecurrence{}},
			},
		})
		if err != nil {
			t.Fatalf("first schedule create on a re-created database = %v, want success (the TGM16 stale-schedule 409)", err)
		}
		return
	}
	_, err := a.prov.CreateBackupSchedule(ctx, &model.NormalizedRequest{
		AccountID: a.project,
		Params: map[string]any{
			"name": "databases/" + id + "/backupSchedules",
			"body": map[string]any{"retention": "86400s", "dailyRecurrence": map[string]any{}},
		},
	})
	if err != nil {
		t.Fatalf("first schedule create on a re-created database = %v, want success (the TGM16 stale-schedule 409)", err)
	}
}

// nr builds the REST adapter's NormalizedRequest for a database-scoped path.
func (a *firestoreAdapter) nr(name string, extra map[string]any) *model.NormalizedRequest {
	params := map[string]any{"name": name}
	for k, v := range extra {
		params[k] = v
	}
	return &model.NormalizedRequest{AccountID: a.project, Params: params, ResourceID: resource.ResourceID(a.project)}
}

// shortName trims a full resource name to its last path segment.
func shortName(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			return name[i+1:]
		}
	}
	return name
}
