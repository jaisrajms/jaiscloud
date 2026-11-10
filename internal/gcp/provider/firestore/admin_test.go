package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/gcperr"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newAdminService() *Service { return newService(nil, store.NewMemoryResourceStore()) }

// assertDeletePrecondition asserts the failure projects.databases.delete
// answers for a blocked delete (FAILED_PRECONDITION on HTTP 400) — whether the
// block is a stale etag or enabled delete protection. It is deliberately not
// the AIP-154 ABORTED an update answers.
func assertDeletePrecondition(t *testing.T, err error, what string) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.Code != "FailedPrecondition" || pe.HTTPStatus != 400 {
		t.Fatalf("%s delete = %s/%d (%v), want FailedPrecondition/400", what, pe.Code, pe.HTTPStatus, err)
	}
	// The wire status both transports resolve (REST envelope, gRPC code).
	if name, httpStatus := gcperr.Resolve(pe); name != gcperr.FailedPrecondition || httpStatus != 400 {
		t.Fatalf("%s delete resolves to %s/%d, want FAILED_PRECONDITION/400", what, name, httpStatus)
	}
}

// TestDatabaseRecreateAfterDelete pins the emulator's immediate id reuse: a
// deleted database leaves its id available at once, so a Terraform
// destroy -> apply (or the delete+create half of a ForceNew replacement)
// converges without a cooldown, while the tombstone stays observable through
// showDeleted until the create replaces it. Real GCP refuses to reuse the id
// for about five minutes ("You cannot reuse a database ID until 5 minutes
// after the delete happens"); the emulator deliberately does not model that
// server-side window (TGM13).
func TestDatabaseRecreateAfterDelete(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, _, err := p.CreateDatabaseDef(ctx, "proj", "reuse-db", databaseDef{LocationID: "us-central1"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "reuse-db", databaseDef{LocationID: "us-central1"}); err == nil {
		t.Error("duplicate CreateDatabaseDef succeeded, want AlreadyExists")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}

	deleted, _, err := p.DeleteDatabaseDef(ctx, "proj", "reuse-db", created.Etag)
	if err != nil {
		t.Fatalf("DeleteDatabaseDef: %v", err)
	}
	if !deleted.Deleted || deleted.DeleteTime.IsZero() {
		t.Errorf("deleted record = %+v, want Deleted + DeleteTime", deleted)
	}
	if _, err := p.GetDatabase(ctx, "proj", "reuse-db"); !isNotFound(err) {
		t.Fatalf("GetDatabase(deleted) = %v, want NotFound", err)
	}
	// The tombstone stays visible through showDeleted (real GCP keeps deleted
	// databases readable that way; only the id-reuse cooldown is not modelled).
	tombstoned := false
	if listed, err := p.ListDatabases(ctx, "proj", true); err != nil {
		t.Fatalf("ListDatabases(showDeleted): %v", err)
	} else {
		for _, d := range listed {
			if d.Name == databaseName("proj", "reuse-db") {
				tombstoned = d.Deleted && !d.DeleteTime.IsZero()
			}
		}
	}
	if !tombstoned {
		t.Error("ListDatabases(showDeleted) omitted the tombstone")
	}

	// The create succeeds immediately, replacing the tombstone with fresh fields.
	recreated, _, err := p.CreateDatabaseDef(ctx, "proj", "reuse-db", databaseDef{
		LocationID:            "nam5",
		DeleteProtectionState: deleteProtectionEnabled,
	})
	if err != nil {
		t.Fatalf("CreateDatabaseDef(after delete): %v", err)
	}
	if recreated.LocationID != "nam5" || recreated.Uid == created.Uid || recreated.Etag == created.Etag ||
		recreated.Deleted || !recreated.DeleteTime.IsZero() ||
		recreated.DeleteProtectionState != deleteProtectionEnabled {
		t.Errorf("recreated = %+v, want the new fields and a fresh uid/etag", recreated)
	}
	if got, err := p.GetDatabase(ctx, "proj", "reuse-db"); err != nil || got.LocationID != "nam5" || got.Deleted {
		t.Errorf("GetDatabase(recreated) = %+v err=%v, want the live record", got, err)
	}
	// One record, not two: the create replaced the tombstone in place.
	records := 0
	if listed, err := p.ListDatabases(ctx, "proj", true); err != nil {
		t.Fatalf("ListDatabases(showDeleted) after recreate: %v", err)
	} else {
		for _, d := range listed {
			if d.Name == databaseName("proj", "reuse-db") {
				records++
				if d.Deleted {
					t.Error("recreated database still reports Deleted")
				}
			}
		}
	}
	if records != 1 {
		t.Errorf("ListDatabases(showDeleted) has %d reuse-db records, want 1", records)
	}
}

// TestDeleteDatabaseCascadesOwnedResources pins projects.databases.delete's
// ownership contract: a database's documents and every database-scoped admin
// resource are removed with it, so a re-created database starts clean. Backups
// are the documented exception and are covered separately.
func TestDeleteDatabaseCascadesOwnedResources(t *testing.T) {
	ctx := context.Background()
	p := newService(firestorestore.NewMemoryStore(), store.NewMemoryResourceStore())

	db, _, err := p.CreateDatabaseDef(ctx, "proj", "cascade-db", databaseDef{LocationID: "us-central1"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "cascade-db", "",
		backupScheduleDef{Retention: 7 * 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	if _, err := p.CreateUserCredsDef(ctx, "proj", "cascade-db", "cred1", userCredsDef{}); err != nil {
		t.Fatalf("CreateUserCredsDef: %v", err)
	}
	if _, err := p.CreateChangeStreamDef(ctx, "proj", "cascade-db", "stream-1",
		changeStreamDef{RetentionPeriod: 24 * time.Hour, DatabaseScope: true}); err != nil {
		t.Fatalf("CreateChangeStreamDef: %v", err)
	}
	if _, _, err := p.UpdateFieldDef(ctx, "proj", "cascade-db", "cities", "population",
		fieldDef{TtlConfig: &fieldTtlConfig{ExpirationOffset: time.Hour}}, []string{"ttl_config"}, true); err != nil {
		t.Fatalf("UpdateFieldDef: %v", err)
	}
	if _, _, err := p.CreateIndexDef(ctx, "proj", "cascade-db", "cities", indexDef{
		QueryScope: "COLLECTION",
		Fields:     []indexField{{FieldPath: "a", Order: "ASCENDING"}, {FieldPath: "b", Order: "ASCENDING"}},
	}); err != nil {
		t.Fatalf("CreateIndexDef: %v", err)
	}
	for _, path := range []string{"cities/one", "cities/two"} {
		if err := p.store.CreateDocument(ctx, firestorestore.Document{
			Name: "projects/proj/databases/cascade-db/documents/" + path,
		}); err != nil {
			t.Fatalf("CreateDocument(%s): %v", path, err)
		}
	}

	// A sibling database's children must survive: the purge is scoped to the
	// deleted database, not the project, and "cascade-db2" is a string
	// superstring of "cascade-db" (the store's List is a contains match).
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "cascade-db2", databaseDef{LocationID: "us-central1"}); err != nil {
		t.Fatalf("CreateDatabaseDef(cascade-db2): %v", err)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "cascade-db2", "",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef(cascade-db2): %v", err)
	}

	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "cascade-db", db.Etag); err != nil {
		t.Fatalf("DeleteDatabaseDef: %v", err)
	}

	// Every owned record is gone.
	if _, err := p.GetBackupSchedule(ctx, "proj", "cascade-db", "1"); !isNotFound(err) {
		t.Errorf("GetBackupSchedule after database delete = %v, want NotFound", err)
	}
	if _, err := p.GetUserCreds(ctx, "proj", "cascade-db", "cred1"); !isNotFound(err) {
		t.Errorf("GetUserCreds after database delete = %v, want NotFound", err)
	}
	if _, err := p.GetChangeStream(ctx, "proj", "cascade-db", "stream-1"); !isNotFound(err) {
		t.Errorf("GetChangeStream after database delete = %v, want NotFound", err)
	}
	if _, err := p.GetField(ctx, "proj", "cascade-db", "cities", "population"); !isNotFound(err) {
		t.Errorf("GetField after database delete = %v, want NotFound", err)
	}
	if docs, err := p.store.ListDocuments(ctx, "proj", "cascade-db"); err != nil || len(docs) != 0 {
		t.Errorf("documents after database delete len=%d err=%v, want none", len(docs), err)
	}
	for _, rt := range []string{rtIndex, rtField} {
		entries, err := p.resources.List(ctx, "proj", store.GlobalRegion, rt, "databases/cascade-db/")
		if err != nil || len(entries) != 0 {
			t.Errorf("%s records after database delete len=%d err=%v, want none", rt, len(entries), err)
		}
	}
	// The parent is gone, so its child collections answer NOT_FOUND rather than
	// serving an empty list.
	if _, err := p.ListBackupSchedules(ctx, "proj", "cascade-db"); !isNotFound(err) {
		t.Errorf("ListBackupSchedules(deleted db) = %v, want NotFound", err)
	}
	if _, err := p.ListUserCreds(ctx, "proj", "cascade-db"); !isNotFound(err) {
		t.Errorf("ListUserCreds(deleted db) = %v, want NotFound", err)
	}
	if _, _, err := p.ListFields(ctx, "proj", "cascade-db", "-", "", pageParams{}); !isNotFound(err) {
		t.Errorf("ListFields(deleted db) = %v, want NotFound", err)
	}
	if _, _, err := p.ListIndexes(ctx, "proj", "cascade-db", "-", "", pageParams{}); !isNotFound(err) {
		t.Errorf("ListIndexes(deleted db) = %v, want NotFound", err)
	}
	if _, err := p.ListChangeStreams(ctx, "proj", "cascade-db"); !isNotFound(err) {
		t.Errorf("ListChangeStreams(deleted db) = %v, want NotFound", err)
	}

	// The sibling is untouched.
	if _, err := p.GetBackupSchedule(ctx, "proj", "cascade-db2", "1"); err != nil {
		t.Errorf("sibling schedule was removed: %v", err)
	}
	// The tombstone is still observable.
	tombstoned := false
	if listed, err := p.ListDatabases(ctx, "proj", true); err != nil {
		t.Fatalf("ListDatabases(showDeleted): %v", err)
	} else {
		for _, d := range listed {
			if d.Name == databaseName("proj", "cascade-db") {
				tombstoned = d.Deleted
			}
		}
	}
	if !tombstoned {
		t.Error("ListDatabases(showDeleted) omitted the deleted database")
	}
}

// TestDeleteDatabaseRetainsBackups pins the documented exception: "Deleting the
// source database does not automatically delete related backups". The schedule
// goes away (so no new backups are produced), the existing ones stay readable
// until their retention window closes, and a re-created database still sees them.
func TestDeleteDatabaseRetainsBackups(t *testing.T) {
	ctx := context.Background()
	p := newService(firestorestore.NewMemoryStore(), store.NewMemoryResourceStore())
	base := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(clock.RealClock{})

	db, _, err := p.CreateDatabaseDef(ctx, "proj", "retain-db", databaseDef{LocationID: "us-central1"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "retain-db", "",
		backupScheduleDef{Retention: 7 * 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(24*time.Hour + time.Minute)})
	before, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil || len(before) != 1 {
		t.Fatalf("ListBackups before delete len=%d err=%v, want 1", len(before), err)
	}

	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "retain-db", db.Etag); err != nil {
		t.Fatalf("DeleteDatabaseDef: %v", err)
	}
	if _, err := p.GetBackupSchedule(ctx, "proj", "retain-db", "1"); !isNotFound(err) {
		t.Errorf("schedule survived the database delete: %v", err)
	}
	kept, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil || len(kept) != 1 || kept[0].Name != before[0].Name {
		t.Fatalf("backups after database delete = %+v err=%v, want the retained backup", kept, err)
	}
	id := before[0].Name[strings.LastIndex(before[0].Name, "/")+1:]
	if _, err := p.GetBackup(ctx, "proj", "us-central1", id); err != nil {
		t.Errorf("GetBackup(retained) = %v, want the backup", err)
	}

	// Re-creating the id does not resurrect the schedule nor hide the backup.
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "retain-db", databaseDef{LocationID: "nam5"}); err != nil {
		t.Fatalf("CreateDatabaseDef(after delete): %v", err)
	}
	if list, err := p.ListBackupSchedules(ctx, "proj", "retain-db"); err != nil || len(list) != 0 {
		t.Errorf("ListBackupSchedules(recreated) len=%d err=%v, want none", len(list), err)
	}
	if list, err := p.ListBackups(ctx, "proj", "us-central1", ""); err != nil || len(list) != 1 {
		t.Errorf("ListBackups(recreated) len=%d err=%v, want the retained backup", len(list), err)
	}

	// The retained backup expires on its original window, with no schedule to
	// renew it.
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(8*24*time.Hour + 2*time.Minute)})
	if list, err := p.ListBackups(ctx, "proj", "us-central1", ""); err != nil || len(list) != 0 {
		t.Errorf("ListBackups after retention len=%d err=%v, want the expired backup purged", len(list), err)
	}
}

// TestDeleteDatabaseFailureKeepsChildren asserts a blocked delete (stale etag or
// delete protection) leaves every child record in place: the etag/protection
// precondition runs before the cascade.
func TestDeleteDatabaseFailureKeepsChildren(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, _, err := p.CreateDatabaseDef(ctx, "proj", "keep-db", databaseDef{LocationID: "us-central1"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "keep-db", "",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}

	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "keep-db", "stale-etag"); err == nil {
		t.Fatal("DeleteDatabaseDef(stale etag) succeeded")
	} else {
		assertDeletePrecondition(t, err, "stale etag")
	}
	if _, err := p.GetBackupSchedule(ctx, "proj", "keep-db", "1"); err != nil {
		t.Fatalf("stale-etag delete removed the schedule: %v", err)
	}

	protected, _, err := p.UpdateDatabaseDef(ctx, "proj", "keep-db",
		databaseDef{DeleteProtectionState: deleteProtectionEnabled}, []string{"delete_protection_state"}, true, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(enable protection): %v", err)
	}
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "keep-db", protected.Etag); err == nil {
		t.Fatal("DeleteDatabaseDef(delete protection) succeeded")
	} else {
		assertDeletePrecondition(t, err, "delete protection")
	}
	if _, err := p.GetBackupSchedule(ctx, "proj", "keep-db", "1"); err != nil {
		t.Fatalf("protected delete removed the schedule: %v", err)
	}
}

// TestCreateDatabaseOverTombstonePurgesLeftovers covers a database tombstoned by
// a build that did not cascade (or a cascade interrupted by a store error): the
// create that replaces the tombstone re-runs the purge, so the new incarnation
// inherits neither the old schedule nor the old documents.
func TestCreateDatabaseOverTombstonePurgesLeftovers(t *testing.T) {
	ctx := context.Background()
	p := newService(firestorestore.NewMemoryStore(), store.NewMemoryResourceStore())

	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "legacy-db", databaseDef{LocationID: "us-central1"}); err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "legacy-db", "",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: "projects/proj/databases/legacy-db/documents/cities/one",
	}); err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}

	// Write the tombstone by hand, leaving the children behind.
	entry, err := p.resources.Get(ctx, "proj", store.GlobalRegion, rtDatabase, databaseRel("legacy-db"))
	if err != nil {
		t.Fatalf("Get database record: %v", err)
	}
	var rec databaseDef
	if err := json.Unmarshal(entry.Data, &rec); err != nil {
		t.Fatalf("unmarshal database record: %v", err)
	}
	rec.Deleted = true
	rec.DeleteTime = clock.Now()
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal database record: %v", err)
	}
	if err := p.resources.Upsert(ctx, "proj", store.GlobalRegion,
		store.ResourceEntry{Type: rtDatabase, ID: databaseRel("legacy-db"), Data: data}); err != nil {
		t.Fatalf("write tombstone: %v", err)
	}
	if _, err := p.GetBackupSchedule(ctx, "proj", "legacy-db", "1"); err != nil {
		t.Fatalf("leftover schedule missing before recreate: %v", err)
	}

	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "legacy-db", databaseDef{LocationID: "nam5"}); err != nil {
		t.Fatalf("CreateDatabaseDef(over tombstone): %v", err)
	}
	if _, err := p.GetBackupSchedule(ctx, "proj", "legacy-db", "1"); !isNotFound(err) {
		t.Errorf("recreate kept the leftover schedule: %v", err)
	}
	if docs, err := p.store.ListDocuments(ctx, "proj", "legacy-db"); err != nil || len(docs) != 0 {
		t.Errorf("recreate kept %d leftover documents (err=%v)", len(docs), err)
	}
}

// TestDatabaseChildSurfacesRequireParent pins the parent-existence rule for the
// database-scoped admin surfaces: a named database that was never created (or
// one that was deleted) answers NOT_FOUND instead of an empty list or an
// accepted create.
func TestDatabaseChildSurfacesRequireParent(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	assertMissing := func(what string, err error) {
		t.Helper()
		if !isNotFound(err) {
			t.Errorf("%s on a missing database = %v, want NotFound", what, err)
		}
	}
	daily := backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}
	_, err := p.ListBackupSchedules(ctx, "proj", "missing-db")
	assertMissing("ListBackupSchedules", err)
	_, err = p.CreateBackupScheduleDef(ctx, "proj", "missing-db", "", daily)
	assertMissing("CreateBackupScheduleDef", err)
	_, err = p.ListUserCreds(ctx, "proj", "missing-db")
	assertMissing("ListUserCreds", err)
	_, err = p.CreateUserCredsDef(ctx, "proj", "missing-db", "cred1", userCredsDef{})
	assertMissing("CreateUserCredsDef", err)
	_, _, err = p.ListFields(ctx, "proj", "missing-db", "-", "", pageParams{})
	assertMissing("ListFields", err)
	_, _, err = p.UpdateFieldDef(ctx, "proj", "missing-db", "cities", "population",
		fieldDef{TtlConfig: &fieldTtlConfig{ExpirationOffset: time.Hour}}, []string{"ttl_config"}, true)
	assertMissing("UpdateFieldDef", err)
	_, _, err = p.ListIndexes(ctx, "proj", "missing-db", "-", "", pageParams{})
	assertMissing("ListIndexes", err)
	_, _, err = p.CreateIndexDef(ctx, "proj", "missing-db", "cities", indexDef{
		QueryScope: "COLLECTION",
		Fields:     []indexField{{FieldPath: "a"}, {FieldPath: "b"}},
	})
	assertMissing("CreateIndexDef", err)
	_, err = p.ListChangeStreams(ctx, "proj", "missing-db")
	assertMissing("ListChangeStreams", err)
	_, err = p.CreateChangeStreamDef(ctx, "proj", "missing-db", "stream-1",
		changeStreamDef{RetentionPeriod: 24 * time.Hour, DatabaseScope: true})
	assertMissing("CreateChangeStreamDef", err)

	// A deleted parent behaves the same way.
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "gone-db", databaseDef{LocationID: "us-central1"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "gone-db", created.Etag); err != nil {
		t.Fatalf("DeleteDatabaseDef: %v", err)
	}
	_, err = p.ListBackupSchedules(ctx, "proj", "gone-db")
	assertMissing("ListBackupSchedules", err)
	_, err = p.CreateBackupScheduleDef(ctx, "proj", "gone-db", "", daily)
	assertMissing("CreateBackupScheduleDef", err)
}

// TestBackupScheduleOnePerRecurrence pins the documented limit — one DAILY and
// one WEEKLY schedule per database — and the system-assigned id: the adapter no
// longer hardcodes "1", so a second recurrence is accepted instead of 409ing.
func TestBackupScheduleOnePerRecurrence(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	daily, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: 7 * 24 * time.Hour, Recurrence: "DAILY"})
	if err != nil {
		t.Fatalf("CreateBackupScheduleDef(daily): %v", err)
	}
	if want := "projects/proj/databases/(default)/backupSchedules/1"; daily.Name != want {
		t.Errorf("daily name = %q, want %q", daily.Name, want)
	}
	weekly, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: 7 * 24 * time.Hour, Recurrence: "WEEKLY", DayOfWeek: 1})
	if err != nil {
		t.Fatalf("CreateBackupScheduleDef(weekly): %v", err)
	}
	if want := "projects/proj/databases/(default)/backupSchedules/2"; weekly.Name != want {
		t.Errorf("weekly name = %q, want %q", weekly.Name, want)
	}

	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}); err == nil {
		t.Error("second daily schedule accepted")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "WEEKLY", DayOfWeek: 2}); err == nil {
		t.Error("second weekly schedule accepted")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}
	if list, err := p.ListBackupSchedules(ctx, "proj", "(default)"); err != nil || len(list) != 2 {
		t.Fatalf("ListBackupSchedules len=%d err=%v, want 2", len(list), err)
	}
	// The weekly day is part of the immutable recurrence.
	if _, err := p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "2",
		backupScheduleDef{Recurrence: "WEEKLY", DayOfWeek: 3}, []string{"weekly_recurrence"}, true); err == nil {
		t.Error("weekly day change succeeded, want InvalidArgument")
	} else {
		assertProviderCode(t, err, "InvalidArgument", 400)
	}

	// A deleted schedule frees its recurrence and its id.
	if err := p.DeleteBackupSchedule(ctx, "proj", "(default)", "1"); err != nil {
		t.Fatalf("DeleteBackupSchedule: %v", err)
	}
	again, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"})
	if err != nil {
		t.Fatalf("CreateBackupScheduleDef(daily, after delete): %v", err)
	}
	if again.Name != daily.Name {
		t.Errorf("re-created daily name = %q, want the freed %q", again.Name, daily.Name)
	}

	// A recurrence-less create is classified as the daily default (the same
	// classification the materializer uses), so while a daily exists it is
	// ALREADY_EXISTS. A weekly without a real day, an unknown recurrence, and a
	// retention beyond the documented 14 weeks are INVALID_ARGUMENT.
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: 24 * time.Hour}); err == nil {
		t.Error("second (implicit daily) schedule accepted")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "WEEKLY"}); err == nil {
		t.Error("weekly create without a day accepted")
	} else {
		assertProviderCode(t, err, "InvalidArgument", 400)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "HOURLY"}); err == nil {
		t.Error("unknown recurrence accepted")
	} else {
		assertProviderCode(t, err, "InvalidArgument", 400)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Retention: maxBackupRetention + time.Hour, Recurrence: "DAILY"}); err == nil {
		t.Error("retention beyond 14 weeks accepted")
	} else {
		assertProviderCode(t, err, "InvalidArgument", 400)
	}
	// An omitted retention keeps the documented 7-day fallback.
	if err := p.DeleteBackupSchedule(ctx, "proj", "(default)", "1"); err != nil {
		t.Fatalf("DeleteBackupSchedule: %v", err)
	}
	fallback, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "",
		backupScheduleDef{Recurrence: "DAILY"})
	if err != nil || fallback.Retention != 0 {
		t.Errorf("create without retention = %+v err=%v, want the stored zero and the materialization fallback", fallback, err)
	}
}

func TestDatabaseCRUD(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, op, err := p.CreateDatabaseDef(ctx, "proj", "test-db", databaseDef{LocationID: "nam5"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if want := "projects/proj/databases/test-db"; created.Name != want {
		t.Errorf("name = %q, want %q", created.Name, want)
	}
	if created.Type != "FIRESTORE_NATIVE" || created.ConcurrencyMode != concurrencyPessimistic {
		t.Errorf("defaults = type %q concurrency %q", created.Type, created.ConcurrencyMode)
	}
	// Delete protection is documented as disabled by default.
	if created.DeleteProtectionState != deleteProtectionDisabled {
		t.Errorf("default delete protection = %q, want %q", created.DeleteProtectionState, deleteProtectionDisabled)
	}
	if !strings.Contains(op, "/operations/") {
		t.Errorf("operation name = %q, want /operations/", op)
	}
	if created.Etag == "" {
		t.Error("etag empty")
	}

	got, err := p.GetDatabase(ctx, "proj", "test-db")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.Uid != created.Uid || got.LocationID != "nam5" {
		t.Errorf("GetDatabase round-trip = %+v", got)
	}

	// The (default) database is synthesized when no record exists.
	def, err := p.GetDatabase(ctx, "proj", "(default)")
	if err != nil {
		t.Fatalf("GetDatabase(default): %v", err)
	}
	if def.Name != "projects/proj/databases/(default)" {
		t.Errorf("default name = %q", def.Name)
	}

	list, err := p.ListDatabases(ctx, "proj", false)
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("ListDatabases len = %d, want 2", len(list))
	}

	// Unknown database is NotFound.
	if _, err := p.GetDatabase(ctx, "proj", "nope"); !isNotFound(err) {
		t.Errorf("GetDatabase(missing) err = %v, want NotFound", err)
	}

	// The (default) read above materialised it, so a create is AlreadyExists
	// (create-if-absent for a fresh project is TestDefaultDatabaseCreate).
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "(default)", databaseDef{}); err == nil {
		t.Error("CreateDatabase((default)) succeeded, want AlreadyExists")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}
}

func TestDefaultDatabaseOutputFields(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	// The implicit (default) database is a real resource on real GCP: it must
	// carry a uid, create/update timestamps, an etag and the documented
	// concurrency default for STANDARD (DC46/DC53), not just a name.
	def, err := p.GetDatabase(ctx, "proj", "(default)")
	if err != nil {
		t.Fatalf("GetDatabase(default): %v", err)
	}
	if def.Uid == "" || def.Uid == "default" {
		t.Errorf("default uid = %q, want a generated value", def.Uid)
	}
	if def.CreateTime.IsZero() || def.UpdateTime.IsZero() {
		t.Errorf("default timestamps = create %v update %v, want both set", def.CreateTime, def.UpdateTime)
	}
	if def.Etag == "" {
		t.Error("default etag empty: the delete precondition is inoperative")
	}
	if def.ConcurrencyMode != concurrencyPessimistic {
		t.Errorf("default concurrency = %q, want PESSIMISTIC (STANDARD edition)", def.ConcurrencyMode)
	}

	// The record is stable across reads — a per-call synthesis would rotate the
	// output fields, and the etag could never satisfy a delete precondition.
	again, err := p.GetDatabase(ctx, "proj", "(default)")
	if err != nil {
		t.Fatalf("GetDatabase(default) again: %v", err)
	}
	if again.Etag != def.Etag || again.Uid != def.Uid || !again.CreateTime.Equal(def.CreateTime) {
		t.Errorf("default record is not stable: %+v vs %+v", again, def)
	}

	// The etag read above is the documented precondition: a stale one blocks the
	// delete, the current one proceeds, and the tombstoned default stays gone.
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "(default)", "stale-etag"); err == nil {
		t.Fatal("stale-etag delete of (default) succeeded")
	} else {
		assertDeletePrecondition(t, err, "default")
	}
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "(default)", def.Etag); err != nil {
		t.Fatalf("DeleteDatabaseDef(default): %v", err)
	}
	if _, err := p.GetDatabase(ctx, "proj", "(default)"); !isNotFound(err) {
		t.Errorf("deleted (default) read = %v, want NotFound (no resurrection)", err)
	}
	// The tombstone neither reappears in a list nor fails one.
	if listed, err := p.ListDatabases(ctx, "proj", false); err != nil || len(listed) != 0 {
		t.Errorf("ListDatabases after deleting (default) = %+v err=%v, want none", listed, err)
	}
	if listed, err := p.ListDatabases(ctx, "proj", true); err != nil || len(listed) != 1 || !listed[0].Deleted {
		t.Errorf("ListDatabases(showDeleted) = %+v err=%v, want the tombstoned default", listed, err)
	}

	// ListDatabases materializes the default on a project that never touched it.
	listed, err := p.ListDatabases(ctx, "other", false)
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(listed) != 1 || listed[0].Etag == "" || listed[0].Uid == "" {
		t.Errorf("ListDatabases(other) = %+v, want the materialized default with output fields", listed)
	}

	// A record written by a pre-DC46 build (persisted by an update on the
	// synthesized default) keeps the placeholder uid and no timestamps/etag;
	// reading it repairs those fields once, so the output is stable and usable.
	legacy := databaseDef{
		Name:                          databaseName("legacy", defaultDatabaseID),
		Uid:                           "default",
		Type:                          "FIRESTORE_NATIVE",
		ConcurrencyMode:               "OPTIMISTIC",
		PointInTimeRecoveryEnablement: pitrDisabled,
	}
	if err := p.putResource(ctx, "legacy", rtDatabase, databaseRel(defaultDatabaseID), legacy, false); err != nil {
		t.Fatalf("seed legacy default: %v", err)
	}
	repaired, err := p.GetDatabase(ctx, "legacy", "(default)")
	if err != nil {
		t.Fatalf("GetDatabase(legacy default): %v", err)
	}
	if repaired.Uid == "default" || repaired.Uid == "" || repaired.Etag == "" ||
		repaired.CreateTime.IsZero() || repaired.UpdateTime.IsZero() {
		t.Errorf("legacy default was not repaired: %+v", repaired)
	}
	if repaired.VersionRetentionPeriod != time.Hour {
		t.Errorf("legacy default retention = %v, want the derived 1h", repaired.VersionRetentionPeriod)
	}
	againLegacy, err := p.GetDatabase(ctx, "legacy", "(default)")
	if err != nil {
		t.Fatalf("GetDatabase(legacy default) again: %v", err)
	}
	if againLegacy.Uid != repaired.Uid || againLegacy.Etag != repaired.Etag ||
		!againLegacy.CreateTime.Equal(repaired.CreateTime) {
		t.Errorf("legacy default repair is not stable: %+v -> %+v", repaired, againLegacy)
	}
	if _, _, err := p.DeleteDatabaseDef(ctx, "legacy", "(default)", repaired.Etag); err != nil {
		t.Errorf("DeleteDatabaseDef(legacy default) with the repaired etag: %v", err)
	}
}

// TestDefaultDatabaseCreate pins create-if-absent for the implicit (default)
// database: real GCP creates it when the project has none (the Firebase
// Terraform shape, google_firestore_database { name = "(default)" }), records
// the requested location, answers ALREADY_EXISTS for a live record, and accepts
// a create after the default was deleted.
func TestDefaultDatabaseCreate(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, op, err := p.CreateDatabaseDef(ctx, "proj", defaultDatabaseID, databaseDef{LocationID: "us-central1"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef(default): %v", err)
	}
	if !strings.Contains(op, "/operations/") {
		t.Errorf("operation name = %q, want /operations/", op)
	}
	if want := "projects/proj/databases/(default)"; created.Name != want {
		t.Errorf("created name = %q, want %q", created.Name, want)
	}
	// The location is echoed verbatim: the Firestore provider has no case
	// normalisation, so a lowercase region must not be upper-cased.
	if created.LocationID != "us-central1" {
		t.Errorf("created location = %q, want us-central1", created.LocationID)
	}
	if created.Uid == "" || created.Etag == "" || created.CreateTime.IsZero() || created.UpdateTime.IsZero() {
		t.Errorf("created default lacks server fields: %+v", created)
	}
	if created.Type != "FIRESTORE_NATIVE" || created.ConcurrencyMode != concurrencyPessimistic ||
		created.DeleteProtectionState != deleteProtectionDisabled || created.Deleted {
		t.Errorf("created default defaults = %+v", created)
	}

	got, err := p.GetDatabase(ctx, "proj", defaultDatabaseID)
	if err != nil {
		t.Fatalf("GetDatabase(default): %v", err)
	}
	if got.LocationID != "us-central1" || got.Uid != created.Uid || got.Etag != created.Etag {
		t.Errorf("GetDatabase(default) = location %q uid %q, want the created record", got.LocationID, got.Uid)
	}

	// A live record is ALREADY_EXISTS, whatever location the request carries.
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", defaultDatabaseID, databaseDef{LocationID: "nam5"}); err == nil {
		t.Error("second CreateDatabaseDef(default) succeeded, want AlreadyExists")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}

	// A first read materialises the default too, with no location — there is no
	// project region to source one from — and that project then has a live
	// default, so its create is ALREADY_EXISTS.
	materialised, err := p.GetDatabase(ctx, "lazy", defaultDatabaseID)
	if err != nil {
		t.Fatalf("GetDatabase(lazy default): %v", err)
	}
	if materialised.LocationID != "" {
		t.Errorf("materialised default location = %q, want none (no project region)", materialised.LocationID)
	}
	if _, _, err := p.CreateDatabaseDef(ctx, "lazy", defaultDatabaseID, databaseDef{LocationID: "us-central1"}); err == nil {
		t.Error("CreateDatabaseDef(default after GET) succeeded, want AlreadyExists")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}

	// A tombstone counts as absent: delete (protection is disabled by default)
	// then create converges, with the new location and a fresh uid.
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", defaultDatabaseID, got.Etag); err != nil {
		t.Fatalf("DeleteDatabaseDef(default): %v", err)
	}
	if _, err := p.GetDatabase(ctx, "proj", defaultDatabaseID); !isNotFound(err) {
		t.Fatalf("GetDatabase(deleted default) = %v, want NotFound", err)
	}
	recreated, _, err := p.CreateDatabaseDef(ctx, "proj", defaultDatabaseID, databaseDef{LocationID: "nam5"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef(default after delete): %v", err)
	}
	if recreated.LocationID != "nam5" || recreated.Uid == created.Uid || recreated.Deleted {
		t.Errorf("recreated default = location %q uid %q, want the new location and a fresh uid", recreated.LocationID, recreated.Uid)
	}
	if reread, err := p.GetDatabase(ctx, "proj", defaultDatabaseID); err != nil || reread.LocationID != "nam5" {
		t.Errorf("GetDatabase(recreated default) = %+v err=%v, want location nam5", reread, err)
	}
	// One database, not two: the create replaces the tombstone.
	if listed, err := p.ListDatabases(ctx, "proj", true); err != nil || len(listed) != 1 || listed[0].LocationID != "nam5" {
		t.Errorf("ListDatabases(showDeleted) = %+v err=%v, want the single recreated default", listed, err)
	}
}

func TestDatabaseUpdateEtagAndDelete(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "db01", databaseDef{})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}

	updated, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{ConcurrencyMode: "PESSIMISTIC", DeleteProtectionState: deleteProtectionEnabled},
		[]string{"concurrency_mode", "delete_protection_state"}, true, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef: %v", err)
	}
	if updated.ConcurrencyMode != "PESSIMISTIC" || updated.DeleteProtectionState != deleteProtectionEnabled {
		t.Errorf("update = %+v", updated)
	}
	if updated.Etag == created.Etag {
		t.Error("etag not rotated on update")
	}

	// created.Etag was superseded by the update above, so it is genuinely
	// stale (not just an arbitrary non-matching token).
	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{ConcurrencyMode: "OPTIMISTIC"}, []string{"concurrency_mode"}, true, created.Etag); err == nil {
		t.Error("stale etag accepted, want Aborted")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "Aborted" || pe.HTTPStatus != 409 {
			t.Errorf("stale etag on update = %v, want Aborted/409 (AIP-154)", err)
		}
	}

	// A stale etag blocks the delete with the proto's documented
	// FAILED_PRECONDITION, and the database is left intact.
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "db01", created.Etag); err == nil {
		t.Error("stale etag accepted by DeleteDatabaseDef, want FailedPrecondition")
	} else {
		assertDeletePrecondition(t, err, "stale-etag")
	}
	if got, err := p.GetDatabase(ctx, "proj", "db01"); err != nil || got.Etag != updated.Etag {
		t.Errorf("stale-etag delete changed the database: etag %q (err %v), want %q", got.Etag, err, updated.Etag)
	}

	// An omitted update_mask is an implied mask of the populated fields
	// (AIP-134): the patch applies and fields absent from it are untouched.
	implied, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{ConcurrencyMode: "OPTIMISTIC"}, nil, false, updated.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(omitted mask): %v", err)
	}
	if implied.ConcurrencyMode != "OPTIMISTIC" {
		t.Errorf("omitted mask did not apply concurrency_mode: %+v", implied)
	}
	if implied.DeleteProtectionState != deleteProtectionEnabled {
		t.Errorf("omitted mask cleared an unpopulated field: %+v", implied)
	}
	if implied.Etag == updated.Etag {
		t.Error("etag not rotated on maskless update")
	}

	// Delete protection survived the maskless patch and now blocks the delete
	// (Firestore: "You cannot delete a database with delete protection enabled
	// until you disable delete protection"), leaving the record intact.
	if _, _, err := p.DeleteDatabaseDef(ctx, "proj", "db01", implied.Etag); err == nil {
		t.Error("delete succeeded with delete protection enabled, want FailedPrecondition")
	} else {
		assertDeletePrecondition(t, err, "delete-protected")
	}
	kept, err := p.GetDatabase(ctx, "proj", "db01")
	if err != nil || kept.Deleted {
		t.Errorf("blocked delete changed the database: %+v (err %v)", kept, err)
	} else if kept.Etag != implied.Etag {
		t.Errorf("blocked delete rotated the etag %q -> %q", implied.Etag, kept.Etag)
	}

	// Disabling delete protection then admits the delete. An explicit
	// DELETE_PROTECTION_STATE_UNSPECIFIED is documented as equivalent to
	// disabled, so it must normalize rather than persist verbatim.
	unprotected, _, err := p.UpdateDatabaseDef(ctx, "proj", "db01",
		databaseDef{DeleteProtectionState: deleteProtectionUnspecified}, []string{"delete_protection_state"}, true, implied.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(disable delete protection): %v", err)
	}
	if unprotected.DeleteProtectionState != deleteProtectionDisabled {
		t.Fatalf("delete protection = %q, want %q", unprotected.DeleteProtectionState, deleteProtectionDisabled)
	}

	deleted, _, err := p.DeleteDatabaseDef(ctx, "proj", "db01", unprotected.Etag)
	if err != nil {
		t.Fatalf("DeleteDatabaseDef: %v", err)
	}
	// The operation's documented response is the soft-deleted Database.
	if !deleted.Deleted || deleted.DeleteTime.IsZero() {
		t.Errorf("DeleteDatabaseDef returned %+v, want a soft-deleted record with deleteTime", deleted)
	}
	if !strings.HasSuffix(deleted.Name, "/databases/db01") {
		t.Errorf("deleted database name = %q", deleted.Name)
	}
	if _, err := p.GetDatabase(ctx, "proj", "db01"); !isNotFound(err) {
		t.Errorf("GetDatabase after delete err = %v, want NotFound", err)
	}
	// A delete tombstone is only shown when requested.
	list, _ := p.ListDatabases(ctx, "proj", false)
	for _, d := range list {
		if strings.HasSuffix(d.Name, "/db01") {
			t.Error("deleted database returned without showDeleted")
		}
	}
	all, _ := p.ListDatabases(ctx, "proj", true)
	found := false
	for _, d := range all {
		if strings.HasSuffix(d.Name, "/db01") {
			found = true
		}
	}
	if !found {
		t.Error("deleted database missing with showDeleted")
	}
}

// TestDatabaseServerOwnedFields pins database.proto's field ownership on both
// write paths: key_prefix (OUTPUT_ONLY) and version_retention_period
// (OUTPUT_ONLY) are never written by a patch, whatever the mask spells, and the
// retention window is derived from the PITR toggle — 7 days enabled, 1 hour
// otherwise — rather than from the request. database_edition is IMMUTABLE: a
// masked write resending the current value is a no-op, while a change is
// INVALID_ARGUMENT (AIP-203). Real GCP never accepts the retention window as
// input.
func TestDatabaseServerOwnedFields(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	// Creation: database_edition is IMMUTABLE and therefore valid on create,
	// while the OUTPUT_ONLY fields in the body are ignored.
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "server-owned", databaseDef{
		LocationID:                    "nam5",
		DatabaseEdition:               "ENTERPRISE",
		KeyPrefix:                     "client-supplied",
		VersionRetentionPeriod:        2 * time.Hour,
		PointInTimeRecoveryEnablement: pitrEnabled,
	})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if created.DatabaseEdition != "ENTERPRISE" {
		t.Errorf("create dropped the immutable database_edition: %q", created.DatabaseEdition)
	}
	if created.KeyPrefix != "" {
		t.Errorf("create stored the OUTPUT_ONLY key_prefix: %q", created.KeyPrefix)
	}
	if created.VersionRetentionPeriod != 7*24*time.Hour {
		t.Errorf("create retention = %v, want 168h (PITR enabled)", created.VersionRetentionPeriod)
	}

	// An explicit `*` patch that carries the unchanged immutable edition plus
	// changed OUTPUT_ONLY fields applies the mutable ones; because `*` is a full
	// replacement the patch must restate the mutable fields it wants to keep, so
	// PITR is carried explicitly and the OUTPUT_ONLY retention in the body is
	// still ignored in favour of the derived window.
	starred, _, err := p.UpdateDatabaseDef(ctx, "proj", "server-owned", databaseDef{
		ConcurrencyMode:               "PESSIMISTIC",
		PointInTimeRecoveryEnablement: pitrEnabled,
		DatabaseEdition:               "ENTERPRISE",
		KeyPrefix:                     "client-supplied",
		VersionRetentionPeriod:        30 * time.Minute,
	}, []string{"*"}, true, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(*): %v", err)
	}
	if starred.ConcurrencyMode != "PESSIMISTIC" {
		t.Errorf("`*` patch dropped the mutable concurrency_mode: %q", starred.ConcurrencyMode)
	}
	if starred.DatabaseEdition != "ENTERPRISE" {
		t.Errorf("`*` patch changed the immutable database_edition: %q", starred.DatabaseEdition)
	}
	if starred.KeyPrefix != "" {
		t.Errorf("`*` patch wrote the OUTPUT_ONLY key_prefix: %q", starred.KeyPrefix)
	}
	if starred.VersionRetentionPeriod != 7*24*time.Hour {
		t.Errorf("`*` patch retention = %v, want 168h (PITR still enabled)", starred.VersionRetentionPeriod)
	}

	// A masked write that would change the IMMUTABLE edition is INVALID_ARGUMENT
	// (AIP-203), on the explicit path and on the implied `*` mask alike, and
	// leaves the record untouched.
	for _, masked := range [][]string{{"database_edition"}, {"*"}} {
		_, _, err := p.UpdateDatabaseDef(ctx, "proj", "server-owned",
			databaseDef{DatabaseEdition: "STANDARD"}, masked, true, starred.Etag)
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("UpdateDatabaseDef(%v) edition change = %v, want InvalidArgument/400", masked, err)
		}
	}
	// A mask that does not name database_edition leaves the body value alone
	// (AIP-134: only masked fields are applied).
	unmasked, _, err := p.UpdateDatabaseDef(ctx, "proj", "server-owned",
		databaseDef{DatabaseEdition: "STANDARD", ConcurrencyMode: "OPTIMISTIC"}, []string{"concurrency_mode"}, true, starred.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(concurrency_mode) with an unmasked edition: %v", err)
	}
	if unmasked.DatabaseEdition != "ENTERPRISE" || unmasked.ConcurrencyMode != "OPTIMISTIC" {
		t.Errorf("unmasked edition patch = edition %q concurrency %q, want ENTERPRISE/OPTIMISTIC", unmasked.DatabaseEdition, unmasked.ConcurrencyMode)
	}

	// A maskless patch (the common form) re-derives the window from the PITR
	// toggle it sets: disabling PITR drops it back to 1 hour even though the body
	// carries a longer window. The edition is resent unchanged, as a client that
	// read the record would do.
	maskless, _, err := p.UpdateDatabaseDef(ctx, "proj", "server-owned", databaseDef{
		DatabaseEdition:               "ENTERPRISE",
		PointInTimeRecoveryEnablement: pitrDisabled,
		VersionRetentionPeriod:        12 * time.Hour,
	}, nil, false, unmasked.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(omitted mask): %v", err)
	}
	if maskless.VersionRetentionPeriod != time.Hour {
		t.Errorf("maskless patch retention = %v, want 1h (PITR disabled)", maskless.VersionRetentionPeriod)
	}

	// A mask that names the OUTPUT_ONLY field writes nothing — including the
	// previously unhandled `0s` value that the `> 0` guard silently dropped.
	etag := maskless.Etag
	for _, retained := range []time.Duration{0, 7 * 24 * time.Hour} {
		got, _, err := p.UpdateDatabaseDef(ctx, "proj", "server-owned",
			databaseDef{VersionRetentionPeriod: retained}, []string{"version_retention_period"}, true, etag)
		if err != nil {
			t.Fatalf("UpdateDatabaseDef(version_retention_period=%v): %v", retained, err)
		}
		if got.VersionRetentionPeriod != time.Hour {
			t.Errorf("explicit retention patch (%v) stored %v, want the derived 1h", retained, got.VersionRetentionPeriod)
		}
		etag = got.Etag
	}

	// Read paths report the derived value as well.
	read, err := p.GetDatabase(ctx, "proj", "server-owned")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if read.VersionRetentionPeriod != time.Hour {
		t.Errorf("GetDatabase retention = %v, want 1h", read.VersionRetentionPeriod)
	}

	// A record persisted by an older build may carry a client-supplied window;
	// the read repairs it to the derived one.
	if err := p.putResource(ctx, "proj", rtDatabase, databaseRel("legacy-db"), databaseDef{
		Name:                          databaseName("proj", "legacy-db"),
		Type:                          "FIRESTORE_NATIVE",
		PointInTimeRecoveryEnablement: pitrEnabled,
		VersionRetentionPeriod:        90 * time.Minute,
	}, false); err != nil {
		t.Fatalf("seed legacy database: %v", err)
	}
	legacy, err := p.GetDatabase(ctx, "proj", "legacy-db")
	if err != nil {
		t.Fatalf("GetDatabase(legacy): %v", err)
	}
	if legacy.VersionRetentionPeriod != 7*24*time.Hour {
		t.Errorf("legacy record retention = %v, want the derived 168h", legacy.VersionRetentionPeriod)
	}
}

// TestDatabaseUpdateRejectsLocationChange pins FC2-7: a database's location is
// fixed at creation (Firestore documents that "once you provision a database
// instance, you cannot change its location setting"). A masked write that would
// change location_id is INVALID_ARGUMENT/400 and leaves the record untouched;
// resending the stored value is a no-op on the explicit path, on an explicit `*`
// replacement and on an implied `*` (a maskless patch) alike.
func TestDatabaseUpdateRejectsLocationChange(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "pinned", databaseDef{LocationID: "nam5"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}

	// A different location is refused on each mask shape.
	for _, mask := range [][]string{{"location_id"}, {"*"}} {
		_, _, err := p.UpdateDatabaseDef(ctx, "proj", "pinned",
			databaseDef{LocationID: "us-central1"}, mask, true, created.Etag)
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("location change (mask %v) = %v, want InvalidArgument/400", mask, err)
		}
	}
	// The implied mask (`*` from an omitted update_mask) is guarded too.
	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "pinned",
		databaseDef{LocationID: "us-central1"}, nil, false, created.Etag); err == nil {
		t.Error("implied-mask location change succeeded, want InvalidArgument")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("implied-mask location change = %v, want InvalidArgument/400", err)
		}
	}

	// A rejected patch left the record untouched (same values, same etag).
	after, err := p.GetDatabase(ctx, "proj", "pinned")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if after.LocationID != "nam5" || after.Etag != created.Etag {
		t.Fatalf("rejected patches changed the record: %+v", after)
	}

	// Resending the stored location is a no-op, not a rejection, on the explicit
	// path and on an explicit `*` replacement (which must restate the mutable
	// fields it keeps).
	noop, _, err := p.UpdateDatabaseDef(ctx, "proj", "pinned",
		databaseDef{LocationID: "nam5"}, []string{"location_id"}, true, created.Etag)
	if err != nil {
		t.Fatalf("resend of the immutable location: %v", err)
	}
	starred, _, err := p.UpdateDatabaseDef(ctx, "proj", "pinned", databaseDef{
		LocationID:                    "nam5",
		ConcurrencyMode:               "PESSIMISTIC",
		PointInTimeRecoveryEnablement: pitrDisabled,
	}, []string{"*"}, true, noop.Etag)
	if err != nil {
		t.Fatalf("`*` patch that resends the immutable location: %v", err)
	}
	if starred.LocationID != "nam5" {
		t.Errorf("`*` patch changed the immutable location: %q", starred.LocationID)
	}
	if starred.ConcurrencyMode != "PESSIMISTIC" {
		t.Errorf("`*` patch dropped the mutable concurrency_mode: %q", starred.ConcurrencyMode)
	}
	// A mask that does not name it ignores a differing body value (AIP-134).
	unmasked, _, err := p.UpdateDatabaseDef(ctx, "proj", "pinned",
		databaseDef{LocationID: "us-central1", ConcurrencyMode: "OPTIMISTIC"},
		[]string{"concurrency_mode"}, true, starred.Etag)
	if err != nil {
		t.Fatalf("unmasked immutable value: %v", err)
	}
	if unmasked.LocationID != "nam5" {
		t.Errorf("unmasked patch changed the immutable location: %q", unmasked.LocationID)
	}
}

// TestDatabaseTypeSwitchRequiresEmptyDatabase pins FC2-8: database.proto
// documents on DatabaseType that "mode changes are only allowed if the database
// is empty". A Native/Datastore switch therefore applies while the database
// holds no documents, resending the stored mode is always a no-op, and a change
// with documents present is INVALID_ARGUMENT/400 and leaves the record alone.
func TestDatabaseTypeSwitchRequiresEmptyDatabase(t *testing.T) {
	ctx := context.Background()
	p := newService(firestorestore.NewMemoryStore(), store.NewMemoryResourceStore())
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "mode-db", databaseDef{LocationID: "nam5"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}

	// Empty database: the documented mode change is applied.
	switched, _, err := p.UpdateDatabaseDef(ctx, "proj", "mode-db",
		databaseDef{Type: "DATASTORE_MODE"}, []string{"type"}, true, created.Etag)
	if err != nil {
		t.Fatalf("mode change on an empty database: %v", err)
	}
	if switched.Type != "DATASTORE_MODE" {
		t.Fatalf("type = %q after an empty-database switch, want DATASTORE_MODE", switched.Type)
	}

	// With data in the database, the mode is pinned; a resend still no-ops.
	if err := p.store.CreateDocument(ctx, firestorestore.Document{
		Name: "projects/proj/databases/mode-db/documents/cities/SF",
	}); err != nil {
		t.Fatalf("CreateDocument: %v", err)
	}
	noop, _, err := p.UpdateDatabaseDef(ctx, "proj", "mode-db",
		databaseDef{Type: "DATASTORE_MODE"}, []string{"type"}, true, switched.Etag)
	if err != nil {
		t.Fatalf("resend of the stored type: %v", err)
	}
	for _, mask := range [][]string{{"type"}, {"*"}} {
		_, _, err := p.UpdateDatabaseDef(ctx, "proj", "mode-db",
			databaseDef{Type: "FIRESTORE_NATIVE"}, mask, true, noop.Etag)
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("mode change with data (mask %v) = %v, want InvalidArgument/400", mask, err)
		}
	}
	// The implied mask is guarded too.
	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "mode-db",
		databaseDef{Type: "FIRESTORE_NATIVE"}, nil, false, noop.Etag); err == nil {
		t.Error("implied-mask mode change with data succeeded, want InvalidArgument")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("implied-mask mode change = %v, want InvalidArgument/400", err)
		}
	}
	got, err := p.GetDatabase(ctx, "proj", "mode-db")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.Type != "DATASTORE_MODE" || got.Etag != noop.Etag {
		t.Fatalf("rejected mode changes altered the record: type %q etag %q", got.Type, got.Etag)
	}
	// A mask that does not name type ignores a differing body value (AIP-134).
	unmasked, _, err := p.UpdateDatabaseDef(ctx, "proj", "mode-db",
		databaseDef{Type: "FIRESTORE_NATIVE", ConcurrencyMode: "PESSIMISTIC"},
		[]string{"concurrency_mode"}, true, got.Etag)
	if err != nil {
		t.Fatalf("unmasked type value: %v", err)
	}
	if unmasked.Type != "DATASTORE_MODE" {
		t.Errorf("unmasked patch changed the type to %q", unmasked.Type)
	}
}

// TestDatabaseCmekConfig pins Database.cmek_config (TGM11). A create that
// carries the block stores and echoes the KMS key on get/list; a database
// created without one never grows a block (the field's presence is the CMEK
// enablement signal, so synthesizing it would claim encryption the database
// does not have). The block is create-only in practice: a masked resend of the
// stored key is a no-op, while enabling CMEK on an existing database or
// switching keys is INVALID_ARGUMENT; neither `*` form clears it, and
// cmek_config.active_key_version is OUTPUT_ONLY, so a mask naming it writes
// nothing (AIP-161).
func TestDatabaseCmekConfig(t *testing.T) {
	// nam5's KMS counterpart is the "us" multi-region (database.proto), so a key
	// in us is the documented match for a nam5 database.
	const key = "projects/proj/locations/us/keyRings/jc-ring/cryptoKeys/jc-key"
	const otherKey = "projects/proj/locations/us/keyRings/jc-ring/cryptoKeys/jc-other"
	ctx := context.Background()
	p := newAdminService()

	assertInvalid := func(label string, err error) {
		t.Helper()
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("%s = %v, want InvalidArgument/400", label, err)
		}
	}

	// A present-but-empty block is how the gcloud CLI serializes an unset
	// CmekConfig (`"cmekConfig": {}` on every `firestore databases create`).
	// Real GCP treats it as "no CMEK", so the create succeeds and stores no block.
	empty, _, err := p.CreateDatabaseDef(ctx, "proj", "empty-cmek", databaseDef{
		CmekConfig: &cmekConfigDef{},
	})
	if err != nil {
		t.Fatalf("create with an empty cmek block: %v", err)
	}
	if empty.CmekConfig != nil {
		t.Errorf("empty cmek block stored as %+v, want none", empty.CmekConfig)
	}
	if got, err := p.GetDatabase(ctx, "proj", "empty-cmek"); err != nil {
		t.Fatalf("GetDatabase(empty-cmek): %v", err)
	} else if got.CmekConfig != nil {
		t.Errorf("GetDatabase(empty-cmek) cmek_config = %+v, want none", got.CmekConfig)
	}

	// A database created without cmek_config never grows one: an absent block is
	// the documented signal that CMEK is not enabled (database.proto: "Presence
	// indicates CMEK is enabled for this database").
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "plain-db", databaseDef{LocationID: "nam5"}); err != nil {
		t.Fatalf("CreateDatabaseDef(plain): %v", err)
	}

	// A client-supplied active_key_version is OUTPUT_ONLY and not stored.
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "cmek-db", databaseDef{
		LocationID: "nam5",
		CmekConfig: &cmekConfigDef{KmsKeyName: key, ActiveKeyVersion: []string{"client-supplied"}},
	})
	if err != nil {
		t.Fatalf("CreateDatabaseDef(cmek): %v", err)
	}
	if created.CmekConfig == nil || created.CmekConfig.KmsKeyName != key {
		t.Fatalf("created cmek_config = %+v, want kms_key_name %q", created.CmekConfig, key)
	}
	if len(created.CmekConfig.ActiveKeyVersion) != 0 {
		t.Errorf("create stored the OUTPUT_ONLY active_key_version: %v", created.CmekConfig.ActiveKeyVersion)
	}

	got, err := p.GetDatabase(ctx, "proj", "cmek-db")
	if err != nil {
		t.Fatalf("GetDatabase(cmek): %v", err)
	}
	if got.CmekConfig == nil || got.CmekConfig.KmsKeyName != key {
		t.Errorf("GetDatabase cmek_config = %+v, want the stored key", got.CmekConfig)
	}

	list, err := p.ListDatabases(ctx, "proj", false)
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	var listed bool
	for _, d := range list {
		switch {
		case d.Name == databaseName("proj", "cmek-db"):
			listed = true
			if d.CmekConfig == nil || d.CmekConfig.KmsKeyName != key {
				t.Errorf("ListDatabases cmek_config = %+v, want the stored key", d.CmekConfig)
			}
		case d.CmekConfig != nil:
			t.Errorf("a Google-managed database reports a fabricated cmek_config: %s %+v", d.Name, d.CmekConfig)
		}
	}
	if !listed {
		t.Fatal("ListDatabases omitted cmek-db")
	}

	// Resending the stored key under cmek_config is a no-op: no error and the
	// key survives (the etag may rotate, as for any accepted update).
	same, _, err := p.UpdateDatabaseDef(ctx, "proj", "cmek-db",
		databaseDef{CmekConfig: &cmekConfigDef{KmsKeyName: key}}, []string{"cmek_config"}, true, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(cmek_config, same key): %v", err)
	}
	if same.CmekConfig == nil || same.CmekConfig.KmsKeyName != key {
		t.Errorf("resend changed cmek_config: %+v", same.CmekConfig)
	}
	// The nested path is the same field.
	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "cmek-db",
		databaseDef{CmekConfig: &cmekConfigDef{KmsKeyName: key}}, []string{"cmek_config.kms_key_name"}, true, same.Etag); err != nil {
		t.Fatalf("UpdateDatabaseDef(cmek_config.kms_key_name, same key): %v", err)
	}

	// Changing the key is INVALID_ARGUMENT: Firestore documents that "you can't
	// change a key for a CMEK-protected database".
	current, err := p.GetDatabase(ctx, "proj", "cmek-db")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "cmek-db",
		databaseDef{CmekConfig: &cmekConfigDef{KmsKeyName: otherKey}}, []string{"cmek_config"}, true, current.Etag); err != nil {
		assertInvalid("cmek key change", err)
	} else {
		t.Error("cmek key change succeeded, want InvalidArgument")
	}
	// Enabling CMEK on a database that has none is INVALID_ARGUMENT too: it can
	// only be set at creation.
	plain, err := p.GetDatabase(ctx, "proj", "plain-db")
	if err != nil {
		t.Fatalf("GetDatabase(plain-db): %v", err)
	}
	if plain.CmekConfig != nil {
		t.Errorf("a Google-managed database grew a cmek_config: %+v", plain.CmekConfig)
	}
	if _, _, err := p.UpdateDatabaseDef(ctx, "proj", "plain-db",
		databaseDef{CmekConfig: &cmekConfigDef{KmsKeyName: key}}, []string{"cmek_config"}, true, plain.Etag); err != nil {
		assertInvalid("enabling cmek on an existing database", err)
	} else {
		t.Error("enabling cmek on an existing database succeeded, want InvalidArgument")
	}
	if after, err := p.GetDatabase(ctx, "proj", "plain-db"); err != nil {
		t.Errorf("GetDatabase(plain-db): %v", err)
	} else if after.CmekConfig != nil {
		t.Errorf("the rejected enable wrote a cmek_config: %+v", after.CmekConfig)
	}

	// An explicit `*` replace whose body omits cmek_config must preserve the
	// stored block: there is no API that disables CMEK.
	current, err = p.GetDatabase(ctx, "proj", "cmek-db")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	starred, _, err := p.UpdateDatabaseDef(ctx, "proj", "cmek-db", databaseDef{
		ConcurrencyMode:               concurrencyOptimistic,
		PointInTimeRecoveryEnablement: pitrEnabled,
	}, []string{"*"}, true, current.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(*) without cmek_config: %v", err)
	}
	if starred.ConcurrencyMode != concurrencyOptimistic || starred.PointInTimeRecoveryEnablement != pitrEnabled {
		t.Errorf("`*` patch did not apply the mutable fields: %+v", starred)
	}
	if starred.CmekConfig == nil || starred.CmekConfig.KmsKeyName != key {
		t.Errorf("`*` patch cleared cmek_config: %+v", starred.CmekConfig)
	}

	// The same holds for an implied `*` (an omitted mask) — the common form.
	implied, _, err := p.UpdateDatabaseDef(ctx, "proj", "cmek-db",
		databaseDef{ConcurrencyMode: concurrencyPessimistic}, nil, false, starred.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(implied mask): %v", err)
	}
	if implied.CmekConfig == nil || implied.CmekConfig.KmsKeyName != key {
		t.Errorf("implied `*` patch cleared cmek_config: %+v", implied.CmekConfig)
	}

	// A mask naming only the OUTPUT_ONLY active_key_version writes nothing: the
	// request's (empty) list is ignored and the key stays.
	afterVersion, _, err := p.UpdateDatabaseDef(ctx, "proj", "cmek-db",
		databaseDef{CmekConfig: &cmekConfigDef{KmsKeyName: otherKey}}, []string{"cmek_config.active_key_version"}, true, implied.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(active_key_version): %v", err)
	}
	if afterVersion.CmekConfig == nil || afterVersion.CmekConfig.KmsKeyName != key {
		t.Errorf("active_key_version mask touched the key: %+v", afterVersion.CmekConfig)
	}
	if len(afterVersion.CmekConfig.ActiveKeyVersion) != 0 {
		t.Errorf("active_key_version mask stored a fabricated version: %v", afterVersion.CmekConfig.ActiveKeyVersion)
	}
}

// TestCMEKActiveKeyVersionsFromKMS pins the OUTPUT_ONLY
// cmek_config.active_key_version derivation: with a KMS store attached, a read
// lists the key's ENABLED versions (the "currently in-use" set), a rotation is
// visible on the next read, and a disabled version drops out. The derivation is
// per response, not persisted, so it also covers ListDatabases.
func TestCMEKActiveKeyVersionsFromKMS(t *testing.T) {
	ctx := context.Background()
	keys := kmsstore.NewMemoryStore()
	seedKMSKey(t, keys, "proj", "us", "ring", "key", "ENCRYPT_DECRYPT", "GOOGLE_SYMMETRIC_ENCRYPTION")
	key := "projects/proj/locations/us/keyRings/ring/cryptoKeys/key"

	p := newService(nil, store.NewMemoryResourceStore())
	p.SetKMSStore(keys)

	created, _, err := p.CreateDatabaseDef(ctx, "proj", "kms-cmek", databaseDef{
		LocationID: "nam5",
		CmekConfig: &cmekConfigDef{KmsKeyName: key, ActiveKeyVersion: []string{"client-supplied"}},
	})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	want := []string{key + "/cryptoKeyVersions/1"}
	if !reflect.DeepEqual(created.CmekConfig.ActiveKeyVersion, want) {
		t.Errorf("create active_key_version = %v, want %v", created.CmekConfig.ActiveKeyVersion, want)
	}

	// A rotation (version 2) keeps version 1 ENABLED, so both are in use.
	if _, err := keys.CreateVersion(ctx, "proj", "us", "ring", "key", kmsstore.Version{State: "ENABLED"}); err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	want = []string{key + "/cryptoKeyVersions/1", key + "/cryptoKeyVersions/2"}
	got, err := p.GetDatabase(ctx, "proj", "kms-cmek")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if !reflect.DeepEqual(got.CmekConfig.ActiveKeyVersion, want) {
		t.Errorf("GetDatabase active_key_version = %v, want %v", got.CmekConfig.ActiveKeyVersion, want)
	}
	list, err := p.ListDatabases(ctx, "proj", false)
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	found := false
	for _, d := range list {
		if d.Name == databaseName("proj", "kms-cmek") {
			found = true
			if !reflect.DeepEqual(d.CmekConfig.ActiveKeyVersion, want) {
				t.Errorf("ListDatabases active_key_version = %v, want %v", d.CmekConfig.ActiveKeyVersion, want)
			}
		}
	}
	if !found {
		t.Fatal("ListDatabases omitted kms-cmek")
	}

	// A disabled version is no longer in use.
	if err := keys.UpdateVersionState(ctx, "proj", "us", "ring", "key", "2", "DISABLED"); err != nil {
		t.Fatalf("UpdateVersionState: %v", err)
	}
	got, err = p.GetDatabase(ctx, "proj", "kms-cmek")
	if err != nil {
		t.Fatalf("GetDatabase after disable: %v", err)
	}
	want = []string{key + "/cryptoKeyVersions/1"}
	if !reflect.DeepEqual(got.CmekConfig.ActiveKeyVersion, want) {
		t.Errorf("active_key_version after disabling v2 = %v, want %v", got.CmekConfig.ActiveKeyVersion, want)
	}

	// eur3's documented KMS counterpart is the "europe" multi-region.
	seedKMSKey(t, keys, "proj", "europe", "ring3", "key3", "ENCRYPT_DECRYPT", "GOOGLE_SYMMETRIC_ENCRYPTION")
	eurKey := "projects/proj/locations/europe/keyRings/ring3/cryptoKeys/key3"
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "eur-cmek", databaseDef{
		LocationID: "eur3",
		CmekConfig: &cmekConfigDef{KmsKeyName: eurKey},
	}); err != nil {
		t.Fatalf("CreateDatabaseDef(eur3/europe): %v", err)
	}
}

// TestCMEKValidation pins the create-time CMEK constraints from database.proto:
// kms_key_name is REQUIRED inside the block and must have the documented shape,
// the key must be in the database's location (with the nam5→us / eur3→europe
// multi-region mapping), and it must exist and be an ENCRYPT_DECRYPT key.
func TestCMEKValidation(t *testing.T) {
	ctx := context.Background()
	keys := kmsstore.NewMemoryStore()
	seedKMSKey(t, keys, "proj", "us", "ring", "sym", "ENCRYPT_DECRYPT", "GOOGLE_SYMMETRIC_ENCRYPTION")
	seedKMSKey(t, keys, "proj", "us-central1", "ring2", "sign", "ASYMMETRIC_SIGN", "RSA_SIGN_PKCS1_2048_SHA256")

	p := newService(nil, store.NewMemoryResourceStore())
	p.SetKMSStore(keys)

	cases := []struct{ name, location, key string }{
		{"malformed name", "nam5", "not-a-key"},
		{"missing key", "nam5", "projects/proj/locations/us/keyRings/ring/cryptoKeys/nope"},
		{"missing key ring", "nam5", "projects/proj/locations/us/keyRings/nope/cryptoKeys/key"},
		{"location mismatch", "nam5", "projects/proj/locations/us-central1/keyRings/ring2/cryptoKeys/sign"},
		{"non-encryption purpose", "us-central1", "projects/proj/locations/us-central1/keyRings/ring2/cryptoKeys/sign"},
	}
	for _, tc := range cases {
		if _, _, err := p.CreateDatabaseDef(ctx, "proj", "bad-cmek", databaseDef{
			LocationID: tc.location,
			CmekConfig: &cmekConfigDef{KmsKeyName: tc.key},
		}); err == nil {
			t.Errorf("%s: create succeeded, want InvalidArgument", tc.name)
		} else {
			assertProviderCode(t, err, "InvalidArgument", 400)
		}
	}

	// Without a KMS store the wire-shaped constraints still apply (unit tests).
	bare := newService(nil, store.NewMemoryResourceStore())
	if _, _, err := bare.CreateDatabaseDef(ctx, "proj", "no-store", databaseDef{
		LocationID: "nam5",
		CmekConfig: &cmekConfigDef{KmsKeyName: "not-a-key"},
	}); err == nil {
		t.Error("malformed key accepted without a KMS store")
	} else {
		assertProviderCode(t, err, "InvalidArgument", 400)
	}
	if _, _, err := bare.CreateDatabaseDef(ctx, "proj", "no-store", databaseDef{
		LocationID: "nam5",
		CmekConfig: &cmekConfigDef{KmsKeyName: "projects/proj/locations/us/keyRings/ring/cryptoKeys/sym"},
	}); err != nil {
		t.Errorf("shape-valid key rejected without a KMS store: %v", err)
	}
}

func seedKMSKey(t *testing.T, keys kmsstore.Store, project, location, ring, key, purpose, algorithm string) {
	t.Helper()
	ctx := context.Background()
	if err := keys.CreateKeyRing(ctx, project, location, ring, kmsstore.KeyRing{Location: location, ID: ring}); err != nil {
		t.Fatalf("CreateKeyRing(%s/%s): %v", location, ring, err)
	}
	if err := keys.CreateCryptoKeyWithVersion(ctx, project, location, ring, key, kmsstore.CryptoKey{
		Location: location, KeyRingID: ring, ID: key, Purpose: purpose, Algorithm: algorithm,
	}, kmsstore.Version{}, true); err != nil {
		t.Fatalf("CreateCryptoKeyWithVersion(%s): %v", key, err)
	}
}

func TestEmptyUpdateMaskIsNoop(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	// Database: a present-but-empty mask is AIP-134's no-op — the record comes
	// back unchanged and neither etag nor update_time rotates, even though the
	// body carries a value. (An omitted mask still applies; see
	// TestDatabaseUpdateEtagAndDelete.)
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "noopdb", databaseDef{})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	noop, _, err := p.UpdateDatabaseDef(ctx, "proj", "noopdb",
		databaseDef{ConcurrencyMode: "PESSIMISTIC"}, nil, true, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(empty mask): %v", err)
	}
	if noop.ConcurrencyMode != created.ConcurrencyMode {
		t.Errorf("empty mask applied concurrency_mode = %q, want %q", noop.ConcurrencyMode, created.ConcurrencyMode)
	}
	if noop.Etag != created.Etag {
		t.Errorf("empty mask rotated the etag %q -> %q", created.Etag, noop.Etag)
	}
	if !noop.UpdateTime.Equal(created.UpdateTime) {
		t.Errorf("empty mask rotated update_time %v -> %v", created.UpdateTime, noop.UpdateTime)
	}

	// Field: the stored config is returned untouched and a no-op on a missing
	// field creates nothing.
	if _, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "population",
		fieldDef{TtlConfig: &fieldTtlConfig{State: "ACTIVE", ExpirationOffset: 2 * time.Hour}},
		[]string{"ttl_config"}, true); err != nil {
		t.Fatalf("UpdateFieldDef: %v", err)
	}
	fieldNoop, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "population",
		fieldDef{TtlConfig: &fieldTtlConfig{State: "ACTIVE", ExpirationOffset: 9 * time.Hour}}, nil, true)
	if err != nil {
		t.Fatalf("UpdateFieldDef(empty mask): %v", err)
	}
	if fieldNoop.TtlConfig == nil || fieldNoop.TtlConfig.ExpirationOffset != 2*time.Hour {
		t.Errorf("empty mask applied ttl_config = %+v, want the stored 2h", fieldNoop.TtlConfig)
	}
	// A no-op on a missing field writes nothing and reports the miss rather
	// than fabricating a field.
	if _, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "ghost",
		fieldDef{TtlConfig: &fieldTtlConfig{State: "ACTIVE"}}, nil, true); !isNotFound(err) {
		t.Errorf("UpdateFieldDef(empty mask, missing field) err = %v, want NotFound", err)
	}
	if _, err := p.GetField(ctx, "proj", "(default)", "cities", "ghost"); !isNotFound(err) {
		t.Errorf("empty mask created a field: GetField err = %v, want NotFound", err)
	}

	// The no-op leaves the *stored* record untouched too, not only the response.
	reloaded, err := p.GetDatabase(ctx, "proj", "noopdb")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if reloaded.ConcurrencyMode != created.ConcurrencyMode || reloaded.Etag != created.Etag {
		t.Errorf("empty mask changed the stored database = %+v", reloaded)
	}
	storedField, err := p.GetField(ctx, "proj", "(default)", "cities", "population")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if storedField.TtlConfig == nil || storedField.TtlConfig.ExpirationOffset != 2*time.Hour {
		t.Errorf("empty mask changed the stored field = %+v", storedField)
	}

	// Backup schedule: retention and the recurrence oneof stay as stored.
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 7 * 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	bsNoop, err := p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 14 * 24 * time.Hour, Recurrence: "WEEKLY", DayOfWeek: 1}, nil, true)
	if err != nil {
		t.Fatalf("UpdateBackupScheduleDef(empty mask): %v", err)
	}
	if bsNoop.Retention != 7*24*time.Hour || bsNoop.Recurrence != "DAILY" {
		t.Errorf("empty mask applied the schedule = %+v, want the stored 7d daily", bsNoop)
	}
}

func TestFieldUpdateGetList(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	_, op, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "population",
		fieldDef{IndexConfig: &fieldIndexConfig{UsesAncestorConfig: false, AncestorField: "cities", Reverting: true}},
		[]string{"index_config"}, true)
	if err != nil {
		t.Fatalf("UpdateFieldDef: %v", err)
	}
	if !strings.Contains(op, "/operations/") {
		t.Errorf("operation name = %q", op)
	}

	// The three index-config members are OUTPUT_ONLY: the patch's values are
	// ignored and the derived inheriting form is reported (DC49).
	wantAncestor := "projects/proj/databases/(default)/collectionGroups/cities/fields/__name__"
	got, err := p.GetField(ctx, "proj", "(default)", "cities", "population")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if got.IndexConfig == nil || !got.IndexConfig.UsesAncestorConfig ||
		got.IndexConfig.AncestorField != wantAncestor || got.IndexConfig.Reverting {
		t.Errorf("field index config = %+v, want the derived inherited form", got.IndexConfig)
	}

	// A second field so pagination has something to page over.
	if _, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "region",
		fieldDef{TtlConfig: &fieldTtlConfig{State: "ACTIVE", ExpirationOffset: time.Hour}}, []string{"ttl_config"}, true); err != nil {
		t.Fatalf("UpdateFieldDef(region): %v", err)
	}

	page1, next, err := p.ListFields(ctx, "proj", "(default)", "-", "", NewPageParams(1, ""))
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	if len(page1) != 1 || next == "" {
		t.Fatalf("ListFields page1 len=%d next=%q", len(page1), next)
	}
	page2, next2, err := p.ListFields(ctx, "proj", "(default)", "-", "", NewPageParams(1, next))
	if err != nil {
		t.Fatalf("ListFields page2: %v", err)
	}
	if len(page2) != 1 || next2 != "" {
		t.Errorf("ListFields page2 len=%d next=%q", len(page2), next2)
	}

	// Documented filter expressions select by config, not by name substring.
	byAncestor, _, err := p.ListFields(ctx, "proj", "(default)", "-", "indexConfig.usesAncestorConfig:true", NewPageParams(0, ""))
	if err != nil || len(byAncestor) != 1 || !strings.HasSuffix(byAncestor[0].Name, "/population") {
		t.Errorf("ListFields indexConfig.usesAncestorConfig:true = %+v err=%v", byAncestor, err)
	}
	byTTL, _, err := p.ListFields(ctx, "proj", "(default)", "-", "ttlConfig:*", NewPageParams(0, ""))
	if err != nil || len(byTTL) != 1 || !strings.HasSuffix(byTTL[0].Name, "/region") {
		t.Errorf("ListFields ttlConfig:* = %+v err=%v", byTTL, err)
	}

	// An omitted update_mask is the implied mask of every populated field
	// (AIP-134, the same contract UpdateDatabaseDef honours since DC20): a
	// maskless patch applies what it carries instead of answering
	// INVALID_ARGUMENT, leaving unpopulated configs untouched.
	maskless, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "score",
		fieldDef{TtlConfig: &fieldTtlConfig{State: "ACTIVE", ExpirationOffset: 2 * time.Hour}}, nil, false)
	if err != nil {
		t.Fatalf("maskless UpdateFieldDef: %v", err)
	}
	if maskless.TtlConfig == nil || maskless.TtlConfig.State != "ACTIVE" || maskless.TtlConfig.ExpirationOffset != 2*time.Hour {
		t.Errorf("maskless patch ttl_config = %+v", maskless.TtlConfig)
	}
	if maskless.IndexConfig != nil {
		t.Errorf("maskless patch invented index_config = %+v", maskless.IndexConfig)
	}

	// The widened `*` arm carries both configs in one maskless patch.
	// Assert on `expiration_offset` (an input field): `ttl_config.state` is
	// OUTPUT_ONLY and derived by real GCP, so a maskless patch must not be
	// pinned on a server-computed value.
	both, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "score",
		fieldDef{
			IndexConfig: &fieldIndexConfig{Indexes: []indexDef{{
				Name:       "projects/proj/databases/(default)/collectionGroups/cities/indexes/ix1",
				QueryScope: "COLLECTION",
				Fields:     []indexField{{FieldPath: "score", Order: "ASCENDING"}},
			}}},
			TtlConfig: &fieldTtlConfig{State: "ACTIVE", ExpirationOffset: 3 * time.Hour},
		}, nil, false)
	if err != nil {
		t.Fatalf("maskless UpdateFieldDef(both configs): %v", err)
	}
	if both.IndexConfig == nil || len(both.IndexConfig.Indexes) != 1 {
		t.Errorf("maskless patch index_config = %+v", both.IndexConfig)
	}
	if both.TtlConfig == nil || both.TtlConfig.ExpirationOffset != 3*time.Hour {
		t.Errorf("maskless patch ttl_config = %+v", both.TtlConfig)
	}
}

func TestDatabaseDeleteProtectionDefaults(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	// Create defaults the field to the documented default.
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "defdb", databaseDef{})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if created.DeleteProtectionState != deleteProtectionDisabled {
		t.Errorf("create default = %q, want %q", created.DeleteProtectionState, deleteProtectionDisabled)
	}

	// An explicit DELETE_PROTECTION_STATE_UNSPECIFIED is documented as
	// equivalent to disabled, not stored verbatim.
	unspec, _, err := p.CreateDatabaseDef(ctx, "proj", "unspecdb", databaseDef{DeleteProtectionState: deleteProtectionUnspecified})
	if err != nil {
		t.Fatalf("CreateDatabaseDef(unspecified): %v", err)
	}
	if unspec.DeleteProtectionState != deleteProtectionDisabled {
		t.Errorf("unspecified create = %q, want %q", unspec.DeleteProtectionState, deleteProtectionDisabled)
	}

	// A record persisted before the default existed (no field on disk) reads
	// back as the documented default rather than as an unknown state.
	legacy := databaseDef{Name: databaseName("proj", "legacydb"), Uid: "legacy", Etag: "legacy-etag"}
	if err := p.putResource(ctx, "proj", rtDatabase, databaseRel("legacydb"), legacy, true); err != nil {
		t.Fatalf("putResource(legacy): %v", err)
	}
	got, err := p.GetDatabase(ctx, "proj", "legacydb")
	if err != nil {
		t.Fatalf("GetDatabase(legacy): %v", err)
	}
	if got.DeleteProtectionState != deleteProtectionDisabled {
		t.Errorf("legacy record read back = %q, want %q", got.DeleteProtectionState, deleteProtectionDisabled)
	}
}

func TestCreateDatabaseValidatesID(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	for _, bad := range []string{"", "ABC", "1bad", "no"} {
		if _, _, err := p.CreateDatabaseDef(ctx, "proj", bad, databaseDef{}); err == nil {
			t.Errorf("CreateDatabaseDef(%q) succeeded, want InvalidArgument", bad)
		}
	}
}

func TestUserCredsLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, err := p.CreateUserCredsDef(ctx, "proj", "(default)", "cred1", userCredsDef{})
	if err != nil {
		t.Fatalf("CreateUserCredsDef: %v", err)
	}
	if created.SecretPassword == "" {
		t.Error("create did not return a secret password")
	}
	if created.State != "ENABLED" {
		t.Errorf("state = %q, want ENABLED", created.State)
	}
	if want := "cred1@proj.iam.gserviceaccount.com"; created.Principal != want {
		t.Errorf("principal = %q, want %q", created.Principal, want)
	}

	got, err := p.GetUserCreds(ctx, "proj", "(default)", "cred1")
	if err != nil {
		t.Fatalf("GetUserCreds: %v", err)
	}
	if got.SecretPassword != "" {
		t.Error("get exposed the secret password")
	}

	list, err := p.ListUserCreds(ctx, "proj", "(default)")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListUserCreds len=%d err=%v", len(list), err)
	}
	if list[0].SecretPassword != "" {
		t.Error("list exposed the secret password")
	}

	dis, err := p.SetUserCredsState(ctx, "proj", "(default)", "cred1", "DISABLED")
	if err != nil || dis.State != "DISABLED" {
		t.Fatalf("disable = %+v err=%v", dis, err)
	}

	reset, err := p.ResetUserPassword(ctx, "proj", "(default)", "cred1")
	if err != nil {
		t.Fatalf("ResetUserPassword: %v", err)
	}
	if reset.SecretPassword == "" || reset.SecretPassword == created.SecretPassword {
		t.Errorf("reset password = %q (old %q)", reset.SecretPassword, created.SecretPassword)
	}

	if err := p.DeleteUserCreds(ctx, "proj", "(default)", "cred1"); err != nil {
		t.Fatalf("DeleteUserCreds: %v", err)
	}
	if _, err := p.GetUserCreds(ctx, "proj", "(default)", "cred1"); !isNotFound(err) {
		t.Errorf("GetUserCreds after delete err = %v, want NotFound", err)
	}
}

func TestBackupScheduleLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	created, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 7 * 24 * time.Hour, Recurrence: "DAILY"})
	if err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	if want := "projects/proj/databases/(default)/backupSchedules/1"; created.Name != want {
		t.Errorf("name = %q, want %q", created.Name, want)
	}

	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}); err == nil {
		t.Error("duplicate schedule accepted, want AlreadyExists")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}

	got, err := p.GetBackupSchedule(ctx, "proj", "(default)", "1")
	if err != nil || got.Retention != 7*24*time.Hour || got.Recurrence != "DAILY" {
		t.Fatalf("GetBackupSchedule = %+v err=%v", got, err)
	}

	upd, err := p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 14 * 24 * time.Hour}, []string{"retention"}, true)
	if err != nil || upd.Retention != 14*24*time.Hour {
		t.Fatalf("UpdateBackupScheduleDef = %+v err=%v", upd, err)
	}

	// The recurrence is immutable (Firestore: "you cannot update its
	// recurrence"): a maskless patch that re-sends the stored daily recurrence
	// applies only the retention, and a change to weekly is INVALID_ARGUMENT.
	maskless, err := p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 2 * 24 * time.Hour, Recurrence: "DAILY"}, nil, false)
	if err != nil {
		t.Fatalf("maskless UpdateBackupScheduleDef: %v", err)
	}
	if maskless.Retention != 2*24*time.Hour || maskless.Recurrence != "DAILY" {
		t.Errorf("maskless update = %+v, want 2d daily", maskless)
	}
	if _, err := p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Recurrence: "WEEKLY", DayOfWeek: 1}, []string{"weekly_recurrence"}, true); err == nil {
		t.Error("a recurrence change succeeded, want InvalidArgument")
	} else {
		assertProviderCode(t, err, "InvalidArgument", 400)
	}

	list, err := p.ListBackupSchedules(ctx, "proj", "(default)")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListBackupSchedules len=%d err=%v", len(list), err)
	}

	if err := p.DeleteBackupSchedule(ctx, "proj", "(default)", "1"); err != nil {
		t.Fatalf("DeleteBackupSchedule: %v", err)
	}
	if _, err := p.GetBackupSchedule(ctx, "proj", "(default)", "1"); !isNotFound(err) {
		t.Errorf("GetBackupSchedule after delete err = %v, want NotFound", err)
	}
}

func TestBackupReadOnly(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	list, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil || len(list) != 0 {
		t.Fatalf("ListBackups len=%d err=%v", len(list), err)
	}
	if _, err := p.GetBackup(ctx, "proj", "us-central1", "b1"); !isNotFound(err) {
		t.Errorf("GetBackup(missing) err = %v, want NotFound", err)
	}
	if err := p.DeleteBackup(ctx, "proj", "us-central1", "b1"); !isNotFound(err) {
		t.Errorf("DeleteBackup(missing) err = %v, want NotFound", err)
	}
}

func TestCreateDatabaseConcurrencyByEdition(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	// database.proto: "If unspecified in a CreateDatabase request, this will
	// default based on the database edition: Optimistic for Enterprise and
	// Pessimistic for all other databases" (DC53).
	cases := []struct{ id, edition, want string }{
		{"eddefault", "", concurrencyPessimistic},
		{"edstandard", databaseEditionStandard, concurrencyPessimistic},
		{"edunset", databaseEditionUnspecified, concurrencyPessimistic},
		{"edenterprise", databaseEditionEnterprise, concurrencyOptimistic},
	}
	for _, tc := range cases {
		created, _, err := p.CreateDatabaseDef(ctx, "proj", tc.id, databaseDef{DatabaseEdition: tc.edition})
		if err != nil {
			t.Fatalf("CreateDatabaseDef(%q): %v", tc.edition, err)
		}
		if created.ConcurrencyMode != tc.want {
			t.Errorf("create(edition %q) concurrency = %q, want %q", tc.edition, created.ConcurrencyMode, tc.want)
		}
	}

	// An explicit mode wins over the edition default, on both editions.
	for _, explicit := range []string{concurrencyPessimistic, concurrencyOptimistic} {
		for _, edition := range []string{databaseEditionStandard, databaseEditionEnterprise} {
			id := "edexplicit-" + strings.ToLower(edition+"-"+explicit)
			created, _, err := p.CreateDatabaseDef(ctx, "proj", id, databaseDef{
				DatabaseEdition: edition,
				ConcurrencyMode: explicit,
			})
			if err != nil {
				t.Fatalf("CreateDatabaseDef(%s/%s): %v", edition, explicit, err)
			}
			if created.ConcurrencyMode != explicit {
				t.Errorf("create(edition %q, mode %q) concurrency = %q, want the explicit value", edition, explicit, created.ConcurrencyMode)
			}
		}
	}
}

func TestDatabaseEarliestVersionTime(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	frozen := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: frozen})
	defer clock.SetGlobalClock(clock.RealClock{})

	// database.proto: earliest_version_time is OUTPUT_ONLY and "populated with
	// `now - version_retention_period`" — 1 hour with PITR off, 7 days with it
	// on (DC54).
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "windowdb", databaseDef{})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if got, want := EarliestVersionTime(created), frozen.Add(-time.Hour); !got.Equal(want) {
		t.Errorf("earliest version time = %v, want %v", got, want)
	}

	enabled, _, err := p.UpdateDatabaseDef(ctx, "proj", "windowdb",
		databaseDef{PointInTimeRecoveryEnablement: pitrEnabled},
		[]string{"point_in_time_recovery_enablement"}, true, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(PITR): %v", err)
	}
	if got, want := EarliestVersionTime(enabled), frozen.Add(-7*24*time.Hour); !got.Equal(want) {
		t.Errorf("earliest version time (PITR) = %v, want %v", got, want)
	}

	// A legacy record with no stored window still derives it from the toggle.
	legacy := databaseDef{Name: databaseName("proj", "legacydb"), PointInTimeRecoveryEnablement: pitrEnabled}
	if got, want := EarliestVersionTime(legacy), frozen.Add(-7*24*time.Hour); !got.Equal(want) {
		t.Errorf("earliest version time (legacy) = %v, want %v", got, want)
	}
}

// TestDatabaseDataAccessModes pins TF6-6: both data-access modes are OPTIONAL
// enums in database.proto with documented edition defaults. A create that omits
// them stores the default (STANDARD: Firestore ENABLED / Mongo-compatible
// DISABLED; ENTERPRISE: Firestore DISABLED / Mongo-compatible ENABLED), an
// explicit value is stored and served, a masked update applies, and a record
// persisted before the fields existed reads back the derived default rather
// than an unspecified mode.
func TestDatabaseDataAccessModes(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	// Create: the documented default applies per edition.
	cases := []struct {
		id, edition, wantFirestore, wantMongo string
	}{
		{"damdefault", "", dataAccessModeEnabled, dataAccessModeDisabled},
		{"damstd", databaseEditionStandard, dataAccessModeEnabled, dataAccessModeDisabled},
		{"dament", databaseEditionEnterprise, dataAccessModeDisabled, dataAccessModeEnabled},
	}
	for _, tc := range cases {
		created, _, err := p.CreateDatabaseDef(ctx, "proj", tc.id, databaseDef{DatabaseEdition: tc.edition})
		if err != nil {
			t.Fatalf("CreateDatabaseDef(%q): %v", tc.edition, err)
		}
		if created.FirestoreDataAccessMode != tc.wantFirestore {
			t.Errorf("create(edition %q) firestore_data_access_mode = %q, want %q", tc.edition, created.FirestoreDataAccessMode, tc.wantFirestore)
		}
		if created.MongodbCompatibleDataAccessMode != tc.wantMongo {
			t.Errorf("create(edition %q) mongodb_compatible_data_access_mode = %q, want %q", tc.edition, created.MongodbCompatibleDataAccessMode, tc.wantMongo)
		}
	}

	// An explicit value is stored and echoed, on both editions.
	created, _, err := p.CreateDatabaseDef(ctx, "proj", "damexplicit", databaseDef{
		DatabaseEdition:                 databaseEditionEnterprise,
		FirestoreDataAccessMode:         dataAccessModeEnabled,
		MongodbCompatibleDataAccessMode: dataAccessModeDisabled,
	})
	if err != nil {
		t.Fatalf("CreateDatabaseDef(explicit modes): %v", err)
	}
	if created.FirestoreDataAccessMode != dataAccessModeEnabled || created.MongodbCompatibleDataAccessMode != dataAccessModeDisabled {
		t.Fatalf("create(explicit modes) = %q/%q, want ENABLED/DISABLED",
			created.FirestoreDataAccessMode, created.MongodbCompatibleDataAccessMode)
	}
	read, err := p.GetDatabase(ctx, "proj", "damexplicit")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if read.FirestoreDataAccessMode != dataAccessModeEnabled || read.MongodbCompatibleDataAccessMode != dataAccessModeDisabled {
		t.Errorf("GetDatabase modes = %q/%q, want the stored values",
			read.FirestoreDataAccessMode, read.MongodbCompatibleDataAccessMode)
	}

	// A masked update applies each mode; a masked write of the zero value clears
	// it back to the documented edition default.
	updated, _, err := p.UpdateDatabaseDef(ctx, "proj", "damexplicit",
		databaseDef{FirestoreDataAccessMode: dataAccessModeDisabled, MongodbCompatibleDataAccessMode: dataAccessModeEnabled},
		[]string{"firestore_data_access_mode", "mongodb_compatible_data_access_mode"}, true, created.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(modes): %v", err)
	}
	if updated.FirestoreDataAccessMode != dataAccessModeDisabled || updated.MongodbCompatibleDataAccessMode != dataAccessModeEnabled {
		t.Fatalf("masked update modes = %q/%q, want DISABLED/ENABLED",
			updated.FirestoreDataAccessMode, updated.MongodbCompatibleDataAccessMode)
	}
	cleared, _, err := p.UpdateDatabaseDef(ctx, "proj", "damexplicit",
		databaseDef{FirestoreDataAccessMode: dataAccessModeUnspecified},
		[]string{"firestore_data_access_mode"}, true, updated.Etag)
	if err != nil {
		t.Fatalf("UpdateDatabaseDef(clear mode): %v", err)
	}
	if cleared.FirestoreDataAccessMode != dataAccessModeDisabled {
		t.Errorf("masked clear = %q, want the ENTERPRISE default DISABLED", cleared.FirestoreDataAccessMode)
	}

	// A record persisted before the fields existed derives both defaults from its
	// stored edition on read.
	if err := p.putResource(ctx, "proj", rtDatabase, databaseRel("damlegacy"), databaseDef{
		Name:            databaseName("proj", "damlegacy"),
		Type:            "FIRESTORE_NATIVE",
		DatabaseEdition: databaseEditionStandard,
	}, false); err != nil {
		t.Fatalf("seed legacy database: %v", err)
	}
	legacy, err := p.GetDatabase(ctx, "proj", "damlegacy")
	if err != nil {
		t.Fatalf("GetDatabase(legacy): %v", err)
	}
	if legacy.FirestoreDataAccessMode != dataAccessModeEnabled || legacy.MongodbCompatibleDataAccessMode != dataAccessModeDisabled {
		t.Errorf("legacy record modes = %q/%q, want the STANDARD defaults ENABLED/DISABLED",
			legacy.FirestoreDataAccessMode, legacy.MongodbCompatibleDataAccessMode)
	}

	// The synthesized (default) database is a STANDARD database and serves the
	// same defaults.
	def, err := p.GetDatabase(ctx, "proj", defaultDatabaseID)
	if err != nil {
		t.Fatalf("GetDatabase(default): %v", err)
	}
	if def.FirestoreDataAccessMode != dataAccessModeEnabled || def.MongodbCompatibleDataAccessMode != dataAccessModeDisabled {
		t.Errorf("(default) modes = %q/%q, want the STANDARD defaults ENABLED/DISABLED",
			def.FirestoreDataAccessMode, def.MongodbCompatibleDataAccessMode)
	}
}

func TestFieldServerOwnedOutputOnly(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()

	// Establish the writable configuration.
	if _, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "location",
		fieldDef{
			IndexConfig: &fieldIndexConfig{Indexes: []indexDef{{
				Name:       "projects/proj/databases/(default)/collectionGroups/cities/indexes/ix1",
				QueryScope: "COLLECTION",
				Fields:     []indexField{{FieldPath: "location", Order: "ASCENDING"}},
			}}},
			TtlConfig: &fieldTtlConfig{ExpirationOffset: time.Hour},
		}, nil, false); err != nil {
		t.Fatalf("UpdateFieldDef: %v", err)
	}

	// index_config.uses_ancestor_config / ancestor_field / reverting and
	// ttl_config.state are OUTPUT_ONLY (DC49): a mask naming them writes
	// nothing, so the writable configuration survives and the derived values are
	// reported.
	wantAncestor := "projects/proj/databases/(default)/collectionGroups/cities/fields/__name__"
	got, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "location",
		fieldDef{
			IndexConfig: &fieldIndexConfig{UsesAncestorConfig: false, AncestorField: "elsewhere", Reverting: true},
			TtlConfig:   &fieldTtlConfig{State: "NEEDS_REPAIR"},
		},
		[]string{"index_config.uses_ancestor_config", "index_config.ancestor_field", "index_config.reverting", "ttl_config.state"}, true)
	if err != nil {
		t.Fatalf("UpdateFieldDef(output-only mask): %v", err)
	}
	ic := got.IndexConfig
	if ic == nil || !ic.UsesAncestorConfig || ic.AncestorField != wantAncestor || ic.Reverting {
		t.Errorf("index config = %+v, want the derived inherited form", ic)
	}
	if len(ic.Indexes) != 1 {
		t.Errorf("output-only mask lost the writable indexes: %+v", ic.Indexes)
	}
	if got.TtlConfig == nil || got.TtlConfig.State != ttlConfigActive || got.TtlConfig.ExpirationOffset != time.Hour {
		t.Errorf("ttl config = %+v, want ACTIVE/1h", got.TtlConfig)
	}

	// DC49's contract is store-side as well as on the wire: the request's
	// output-only values never reach the record, so a request that carried them
	// cannot leak them through any reader.
	var stored fieldDef
	if err := p.getResource(ctx, "proj", rtField, fieldRel("(default)", "cities", "location"), &stored); err != nil {
		t.Fatalf("getResource: %v", err)
	}
	if stored.IndexConfig == nil || stored.IndexConfig.AncestorField != wantAncestor || stored.IndexConfig.Reverting ||
		stored.TtlConfig == nil || stored.TtlConfig.State != ttlConfigActive {
		t.Errorf("stored field = %+v, want the derived output-only members", stored)
	}
}

// rendezvousStore forces the classic lost-update interleaving: every write is
// held until all armed writers have arrived, so each writer's read necessarily
// precedes every write. A load-then-store update loses one of two concurrent
// patches under it; the store's atomic read-modify-write does not.
type rendezvousStore struct {
	store.ResourceStore
	t       *testing.T
	mu      sync.Mutex
	writers int
	arrived int
	release chan struct{}
}

func newRendezvousStore(t *testing.T, inner store.ResourceStore) *rendezvousStore {
	return &rendezvousStore{ResourceStore: inner, t: t}
}

// arm makes the next writers writes rendezvous.
func (r *rendezvousStore) arm(writers int) {
	r.mu.Lock()
	r.writers, r.arrived = writers, 0
	r.release = make(chan struct{})
	r.mu.Unlock()
}

func (r *rendezvousStore) arrive() {
	r.mu.Lock()
	if r.writers == 0 {
		r.mu.Unlock()
		return
	}
	r.arrived++
	if r.arrived == r.writers {
		close(r.release)
	}
	ch := r.release
	r.mu.Unlock()
	// A branch under test that stops writing under the armed count must fail the
	// test, not hang it until the go-test timeout.
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		r.t.Errorf("rendezvousStore: only some of the %d armed writes arrived", r.writers)
	}
}

func (r *rendezvousStore) Upsert(ctx context.Context, account, region string, entry store.ResourceEntry) error {
	r.arrive()
	return r.ResourceStore.Upsert(ctx, account, region, entry)
}

func (r *rendezvousStore) UpsertAtomic(ctx context.Context, account, region, resourceType, id string, mutate func(store.ResourceEntry, bool) (store.ResourceEntry, error)) (store.ResourceEntry, error) {
	r.arrive()
	return r.ResourceStore.UpsertAtomic(ctx, account, region, resourceType, id, mutate)
}

func TestFieldAndScheduleUpdatesAreAtomic(t *testing.T) {
	ctx := context.Background()
	resources := newRendezvousStore(t, store.NewMemoryResourceStore())
	p := newService(nil, resources)

	// Field: the index configuration and the TTL offset are patched through
	// separate masks; under a read-modify-write race one would be lost (DC51).
	fieldPath := "projects/proj/databases/(default)/collectionGroups/cities/fields/location"
	if _, _, err := p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "location",
		fieldDef{TtlConfig: &fieldTtlConfig{ExpirationOffset: time.Hour}}, nil, false); err != nil {
		t.Fatalf("seed UpdateFieldDef: %v", err)
	}
	resources.arm(2)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _, errs[0] = p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "location",
			fieldDef{IndexConfig: &fieldIndexConfig{Indexes: []indexDef{{
				Name:       "projects/proj/databases/(default)/collectionGroups/cities/indexes/ix1",
				QueryScope: "COLLECTION",
				Fields:     []indexField{{FieldPath: "location", Order: "ASCENDING"}},
			}}}}, []string{"index_config.indexes"}, true)
	}()
	go func() {
		defer wg.Done()
		_, _, errs[1] = p.UpdateFieldDef(ctx, "proj", "(default)", "cities", "location",
			fieldDef{TtlConfig: &fieldTtlConfig{ExpirationOffset: 5 * time.Hour}},
			[]string{"ttl_config.expiration_offset"}, true)
	}()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent UpdateFieldDef[%d]: %v", i, err)
		}
	}
	merged, err := p.GetField(ctx, "proj", "(default)", "cities", "location")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if merged.Name != fieldPath {
		t.Fatalf("field name = %q", merged.Name)
	}
	if merged.IndexConfig == nil || len(merged.IndexConfig.Indexes) != 1 {
		t.Errorf("a concurrent patch lost the index config: %+v", merged.IndexConfig)
	}
	if merged.TtlConfig == nil || merged.TtlConfig.ExpirationOffset != 5*time.Hour {
		t.Errorf("a concurrent patch lost the TTL offset: %+v", merged.TtlConfig)
	}

	// Backup schedule: two concurrent masked retention updates must serialize
	// through the atomic primitive (a lost update would leave the older value),
	// and neither may change the immutable recurrence.
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "(default)", "1",
		backupScheduleDef{Retention: 7 * 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	resources.arm(2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "1",
			backupScheduleDef{Retention: 14 * 24 * time.Hour}, []string{"retention"}, true)
	}()
	go func() {
		defer wg.Done()
		_, errs[1] = p.UpdateBackupScheduleDef(ctx, "proj", "(default)", "1",
			backupScheduleDef{Retention: 21 * 24 * time.Hour}, []string{"retention"}, true)
	}()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent UpdateBackupScheduleDef[%d]: %v", i, err)
		}
	}
	bs, err := p.GetBackupSchedule(ctx, "proj", "(default)", "1")
	if err != nil {
		t.Fatalf("GetBackupSchedule: %v", err)
	}
	if bs.Retention != 14*24*time.Hour && bs.Retention != 21*24*time.Hour {
		t.Errorf("concurrent retention updates left %v, want one of 14d/21d", bs.Retention)
	}
	if bs.Recurrence != "DAILY" {
		t.Errorf("concurrent retention updates changed the recurrence to %q", bs.Recurrence)
	}
}

// TestBackupMaterializationFromSchedule covers the read-only backup surface: a
// backup schedule produces backups, so a read materializes the occurrences that
// are due on the emulator clock, the retention window bounds them, and a deleted
// backup is not resurrected by the next read.
func TestBackupMaterializationFromSchedule(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	base := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(clock.RealClock{})

	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "bkpdb", databaseDef{LocationID: "us-central1"}); err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "bkpdb", "1",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}

	// Nothing is due before the first occurrence, and another location never
	// sees this database's backups.
	if list, err := p.ListBackups(ctx, "proj", "us-central1", ""); err != nil || len(list) != 0 {
		t.Fatalf("ListBackups before first occurrence len=%d err=%v", len(list), err)
	}

	// A schedule patched after the materialization reads it must keep its
	// patched fields: advancing the high-water mark is an atomic field update,
	// not a whole-record overwrite.
	if _, err := p.UpdateBackupScheduleDef(ctx, "proj", "bkpdb", "1",
		backupScheduleDef{Retention: 48 * time.Hour}, []string{"retention"}, true); err != nil {
		t.Fatalf("UpdateBackupScheduleDef: %v", err)
	}

	// Cross the first daily occurrence.
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(24*time.Hour + time.Minute)})
	list, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("ListBackups after first occurrence = %d backups, want 1", len(list))
	}
	b := list[0]
	// The id is opaque: schedule id, incarnation, occurrence. Assert the shape,
	// not the emulator's chosen values.
	if wantPrefix := "projects/proj/locations/us-central1/backups/1-"; !strings.HasPrefix(b.Name, wantPrefix) ||
		!strings.HasSuffix(b.Name, "-"+strconv.FormatInt(base.Add(24*time.Hour).Unix(), 10)) {
		t.Errorf("backup name = %q, want %q…%d", b.Name, wantPrefix, base.Add(24*time.Hour).Unix())
	}
	if b.Database != "projects/proj/databases/bkpdb" || b.DatabaseUid == "" {
		t.Errorf("backup database = %q uid %q", b.Database, b.DatabaseUid)
	}
	if !b.SnapshotTime.Equal(base.Add(24 * time.Hour)) {
		t.Errorf("snapshotTime = %v, want %v", b.SnapshotTime, base.Add(24*time.Hour))
	}
	if !b.ExpireTime.Equal(base.Add(72 * time.Hour)) {
		t.Errorf("expireTime = %v, want %v (snapshot + the patched 48h retention)", b.ExpireTime, base.Add(72*time.Hour))
	}
	if b.State != "READY" {
		t.Errorf("state = %q, want READY", b.State)
	}
	sched, err := p.GetBackupSchedule(ctx, "proj", "bkpdb", "1")
	if err != nil {
		t.Fatalf("GetBackupSchedule: %v", err)
	}
	if sched.Retention != 48*time.Hour {
		t.Errorf("patched retention = %v, want 48h preserved across materialization", sched.Retention)
	}
	if !sched.MaterializedThrough.Equal(base.Add(24 * time.Hour)) {
		t.Errorf("materializedThrough = %v, want %v", sched.MaterializedThrough, base.Add(24*time.Hour))
	}

	// A read is idempotent, the documented "-" wildcard sees the backup, and a
	// different location does not.
	if again, err := p.ListBackups(ctx, "proj", "us-central1", ""); err != nil || len(again) != 1 {
		t.Fatalf("second ListBackups len=%d err=%v, want the same single backup", len(again), err)
	}
	if all, err := p.ListBackups(ctx, "proj", "-", ""); err != nil || len(all) != 1 {
		t.Fatalf("ListBackups(-) len=%d err=%v", len(all), err)
	}
	if other, err := p.ListBackups(ctx, "proj", "europe-west1", ""); err != nil || len(other) != 0 {
		t.Fatalf("ListBackups(europe-west1) len=%d err=%v, want none", len(other), err)
	}

	// Deleting it must not make the next read recreate it.
	id := strings.TrimPrefix(b.Name, "projects/proj/locations/us-central1/backups/")
	got, err := p.GetBackup(ctx, "proj", "us-central1", id)
	if err != nil || got.Name != b.Name {
		t.Fatalf("GetBackup = %+v err=%v", got, err)
	}
	if err := p.DeleteBackup(ctx, "proj", "us-central1", id); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}
	if gone, err := p.ListBackups(ctx, "proj", "us-central1", ""); err != nil || len(gone) != 0 {
		t.Fatalf("ListBackups after delete len=%d err=%v, want no resurrection", len(gone), err)
	}

	// The next day's occurrence materializes while the deleted one stays gone.
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(48*time.Hour + time.Minute)})
	next, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil || len(next) != 1 {
		t.Fatalf("ListBackups day 2 len=%d err=%v, want 1", len(next), err)
	}
	if !next[0].SnapshotTime.Equal(base.Add(48 * time.Hour)) {
		t.Errorf("day-2 snapshotTime = %v, want %v", next[0].SnapshotTime, base.Add(48*time.Hour))
	}
}

// TestBackupMaterializationPurgesExpired asserts a backup whose retention window
// has closed is removed on read, as real Firestore does.
func TestBackupMaterializationPurgesExpired(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(clock.RealClock{})

	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "purgedb", databaseDef{LocationID: "us-central1"}); err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	// A weekly schedule with a two-day retention: the occurrence expires before
	// the next one is due.
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "purgedb", "1",
		backupScheduleDef{Retention: 48 * time.Hour, Recurrence: "WEEKLY", DayOfWeek: 1}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: base.Add(7*24*time.Hour + time.Minute)})
	list, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListBackups week 1 len=%d err=%v, want 1", len(list), err)
	}
	// Read again three days later: the backup is past its retention window.
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(10 * 24 * time.Hour)})
	list, err = p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil || len(list) != 0 {
		t.Fatalf("ListBackups after expiry len=%d err=%v, want the expired backup purged", len(list), err)
	}
}

// TestBackupMaterializationStats proves a materialized backup carries the
// database's measured statistics (documents, composite indexes, persisted size)
// rather than an empty stats object.
func TestBackupMaterializationStats(t *testing.T) {
	ctx := context.Background()
	p := newService(firestorestore.NewMemoryStore(), store.NewMemoryResourceStore())
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(clock.RealClock{})

	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "statsdb", databaseDef{LocationID: "us-central1"}); err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	for _, name := range []string{"c/one", "c/two"} {
		doc := firestorestore.Document{Name: "projects/proj/databases/statsdb/documents/" + name}
		if err := p.store.CreateDocument(ctx, doc); err != nil {
			t.Fatalf("CreateDocument(%s): %v", name, err)
		}
	}
	if _, _, err := p.CreateIndexDef(ctx, "proj", "statsdb", "c", indexDef{
		QueryScope: "COLLECTION",
		Fields:     []indexField{{FieldPath: "a", Order: "ASCENDING"}, {FieldPath: "b", Order: "ASCENDING"}},
	}); err != nil {
		t.Fatalf("CreateIndexDef: %v", err)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "statsdb", "1",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: base.Add(24*time.Hour + time.Minute)})
	list, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListBackups len=%d err=%v", len(list), err)
	}
	b := list[0]
	if b.DocumentCount != 2 {
		t.Errorf("documentCount = %d, want 2", b.DocumentCount)
	}
	if b.IndexCount != 1 {
		t.Errorf("indexCount = %d, want the one composite index definition", b.IndexCount)
	}
	if b.SizeBytes <= 0 {
		t.Errorf("sizeBytes = %d, want the persisted document bytes", b.SizeBytes)
	}
}

// TestListBackupsFilter covers the documented filter field (database_uid) and
// rejects an unsupported expression instead of silently matching by name.
func TestListBackupsFilter(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(clock.RealClock{})

	dbrec, _, err := p.CreateDatabaseDef(ctx, "proj", "filterdb", databaseDef{LocationID: "us-central1"})
	if err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "filterdb", "1",
		backupScheduleDef{Retention: 24 * time.Hour, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(24*time.Hour + time.Minute)})

	for _, filter := range []string{`database_uid = "` + dbrec.Uid + `"`, "database_uid:" + dbrec.Uid[:8]} {
		list, err := p.ListBackups(ctx, "proj", "us-central1", filter)
		if err != nil || len(list) != 1 {
			t.Errorf("filter %q: len=%d err=%v, want the backup", filter, len(list), err)
		}
	}
	for _, filter := range []string{`database_uid != "x"`, `name = "x"`, "nonsense"} {
		if _, err := p.ListBackups(ctx, "proj", "us-central1", filter); err == nil {
			t.Errorf("filter %q: expected InvalidArgument", filter)
		}
	}
	if list, err := p.ListBackups(ctx, "proj", "us-central1", `database_uid = "other"`); err != nil || len(list) != 0 {
		t.Errorf("non-matching uid filter: len=%d err=%v, want none", len(list), err)
	}
}

// TestBackupMaterializationClamp proves a large clock jump does not build an
// unbounded backlog of backup records.
func TestBackupMaterializationClamp(t *testing.T) {
	ctx := context.Background()
	p := newAdminService()
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: base})
	defer clock.SetGlobalClock(clock.RealClock{})

	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "clampdb", databaseDef{LocationID: "us-central1"}); err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	// The maximum retention over a year-long jump: the pass is bounded by
	// maxBackupsPerPass rather than materializing 365 records.
	if _, err := p.CreateBackupScheduleDef(ctx, "proj", "clampdb", "1",
		backupScheduleDef{Retention: maxBackupRetention, Recurrence: "DAILY"}); err != nil {
		t.Fatalf("CreateBackupScheduleDef: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: base.Add(365 * 24 * time.Hour)})
	list, err := p.ListBackups(ctx, "proj", "us-central1", "")
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(list) != maxBackupsPerPass {
		t.Fatalf("materialized %d backups, want the %d-per-pass cap", len(list), maxBackupsPerPass)
	}
}
