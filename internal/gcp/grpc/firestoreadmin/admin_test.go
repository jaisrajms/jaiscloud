package firestoreadmin

import (
	"context"
	"testing"
	"time"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"jaiscloud/internal/clock"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	"jaiscloud/internal/store"

	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const testDBParent = "projects/proj"

func TestDatabaseAdminRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     testDBParent,
		DatabaseId: "test-db",
		Database:   &adminpb.Database{LocationId: "nam5"},
	})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if !op.GetDone() {
		t.Fatalf("CreateDatabase done = false, want true")
	}
	created := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(created); err != nil {
		t.Fatalf("CreateDatabase response unmarshal: %v", err)
	}
	if want := "projects/proj/databases/test-db"; created.GetName() != want {
		t.Fatalf("created name = %q, want %q", created.GetName(), want)
	}
	if created.GetType() != adminpb.Database_FIRESTORE_NATIVE {
		t.Errorf("created type = %v, want FIRESTORE_NATIVE", created.GetType())
	}
	// Delete protection is documented as disabled by default.
	if created.GetDeleteProtectionState() != adminpb.Database_DELETE_PROTECTION_DISABLED {
		t.Errorf("created delete protection = %v, want DELETE_PROTECTION_DISABLED", created.GetDeleteProtectionState())
	}

	got, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.GetUid() != created.GetUid() || got.GetLocationId() != "nam5" {
		t.Fatalf("GetDatabase = %+v", got)
	}

	list, err := client.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: testDBParent})
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(list.GetDatabases()) != 2 {
		t.Fatalf("ListDatabases len = %d, want 2 (default + test-db)", len(list.GetDatabases()))
	}

	updOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:            created.GetName(),
			Etag:            created.GetEtag(),
			ConcurrencyMode: adminpb.Database_PESSIMISTIC,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase: %v", err)
	}
	updated := &adminpb.Database{}
	if err := updOp.GetResponse().UnmarshalTo(updated); err != nil {
		t.Fatalf("UpdateDatabase response unmarshal: %v", err)
	}
	if updated.GetConcurrencyMode() != adminpb.Database_PESSIMISTIC {
		t.Fatalf("updated concurrency = %v, want PESSIMISTIC", updated.GetConcurrencyMode())
	}

	// An omitted update_mask is an implied mask of the populated fields
	// (AIP-134): the maskless patch applies instead of failing INVALID_ARGUMENT.
	impliedOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:            created.GetName(),
			Etag:            updated.GetEtag(),
			ConcurrencyMode: adminpb.Database_OPTIMISTIC,
		},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase(omitted mask): %v", err)
	}
	implied := &adminpb.Database{}
	if err := impliedOp.GetResponse().UnmarshalTo(implied); err != nil {
		t.Fatalf("UpdateDatabase(omitted mask) response unmarshal: %v", err)
	}
	if implied.GetConcurrencyMode() != adminpb.Database_OPTIMISTIC {
		t.Fatalf("maskless update concurrency = %v, want OPTIMISTIC", implied.GetConcurrencyMode())
	}
	if implied.GetLocationId() != "nam5" {
		t.Errorf("maskless update changed an unpopulated field: location = %q, want nam5", implied.GetLocationId())
	}
	if implied.GetEtag() == updated.GetEtag() {
		t.Error("etag not rotated on maskless update")
	}

	// Stale etag is rejected.
	if _, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database:   &adminpb.Database{Name: created.GetName(), Etag: created.GetEtag()},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
	}); status.Code(err) != codes.Aborted {
		t.Errorf("stale etag code = %v, want Aborted", status.Code(err))
	}

	// The delete's etag documents a blocked deletion, so a stale one is
	// FAILED_PRECONDITION (not the update's ABORTED) and deletes nothing.
	if _, err := client.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{
		Name: created.GetName(), Etag: created.GetEtag(),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("stale etag delete code = %v, want FailedPrecondition", status.Code(err))
	}
	if _, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: created.GetName()}); err != nil {
		t.Errorf("stale-etag delete removed the database: %v", err)
	}

	delOp, err := client.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: created.GetName(), Etag: implied.GetEtag()})
	if err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
	// operation_info declares response_type "Database": the terminal operation
	// carries the soft-deleted database, so the typed response resolves.
	deleted := &adminpb.Database{}
	if err := delOp.GetResponse().UnmarshalTo(deleted); err != nil {
		t.Fatalf("DeleteDatabase response unmarshal: %v", err)
	}
	if deleted.GetName() != created.GetName() {
		t.Errorf("deleted database name = %q, want %q", deleted.GetName(), created.GetName())
	}
	if deleted.GetDeleteTime() == nil {
		t.Error("deleted database response carries no deleteTime")
	}
	if _, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetDatabase after delete = %v, want NotFound", err)
	}
}

// TestUpdateDatabaseServerOwnedFields pins database.proto's field ownership on
// the gRPC surface: key_prefix (OUTPUT_ONLY) and database_edition (IMMUTABLE)
// are never written by an update, and version_retention_period (OUTPUT_ONLY) is
// the PITR-derived window (7 days enabled, 1 hour otherwise), not the request's.
func TestUpdateDatabaseServerOwnedFields(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     testDBParent,
		DatabaseId: "owned-db",
		Database: &adminpb.Database{
			LocationId:                    "nam5",
			DatabaseEdition:               adminpb.Database_ENTERPRISE,
			KeyPrefix:                     "client-supplied",
			VersionRetentionPeriod:        durationpb.New(2 * time.Hour),
			PointInTimeRecoveryEnablement: adminpb.Database_POINT_IN_TIME_RECOVERY_ENABLED,
		},
	})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	created := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(created); err != nil {
		t.Fatalf("CreateDatabase response unmarshal: %v", err)
	}
	if created.GetDatabaseEdition() != adminpb.Database_ENTERPRISE {
		t.Errorf("create databaseEdition = %v, want ENTERPRISE", created.GetDatabaseEdition())
	}
	if created.GetKeyPrefix() != "" {
		t.Errorf("create stored the OUTPUT_ONLY keyPrefix = %q", created.GetKeyPrefix())
	}
	if got := created.GetVersionRetentionPeriod().AsDuration(); got != 7*24*time.Hour {
		t.Errorf("create retention = %v, want 168h (PITR enabled)", got)
	}

	// The maskless patch (AIP-134 implied mask) ignores the server-owned body
	// fields and re-derives the window from the PITR toggle it sets. The
	// immutable edition is resent unchanged, as a client that read the record
	// would do.
	updOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                          created.GetName(),
			DatabaseEdition:               adminpb.Database_ENTERPRISE,
			KeyPrefix:                     "client-supplied",
			VersionRetentionPeriod:        durationpb.New(30 * time.Minute),
			PointInTimeRecoveryEnablement: adminpb.Database_POINT_IN_TIME_RECOVERY_DISABLED,
		},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase(omitted mask): %v", err)
	}
	updated := &adminpb.Database{}
	if err := updOp.GetResponse().UnmarshalTo(updated); err != nil {
		t.Fatalf("UpdateDatabase response unmarshal: %v", err)
	}
	if updated.GetDatabaseEdition() != adminpb.Database_ENTERPRISE {
		t.Errorf("patch changed the IMMUTABLE databaseEdition = %v", updated.GetDatabaseEdition())
	}
	if updated.GetKeyPrefix() != "" {
		t.Errorf("patch wrote the OUTPUT_ONLY keyPrefix = %q", updated.GetKeyPrefix())
	}
	if got := updated.GetVersionRetentionPeriod().AsDuration(); got != time.Hour {
		t.Errorf("patch retention = %v, want 1h (PITR disabled)", got)
	}

	// An explicit mask that names the OUTPUT_ONLY field writes nothing either.
	explicitOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                   created.GetName(),
			VersionRetentionPeriod: durationpb.New(7 * 24 * time.Hour),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"version_retention_period"}},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase(version_retention_period): %v", err)
	}
	explicit := &adminpb.Database{}
	if err := explicitOp.GetResponse().UnmarshalTo(explicit); err != nil {
		t.Fatalf("UpdateDatabase response unmarshal: %v", err)
	}
	if got := explicit.GetVersionRetentionPeriod().AsDuration(); got != time.Hour {
		t.Errorf("explicit retention patch stored %v, want the derived 1h", got)
	}

	// A masked change to the IMMUTABLE edition is INVALID_ARGUMENT (AIP-203);
	// resending the current value above was accepted.
	if _, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database:   &adminpb.Database{Name: created.GetName(), DatabaseEdition: adminpb.Database_STANDARD},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"database_edition"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("database_edition change code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestDeleteDatabaseProtection verifies the documented delete-protection gate
// on the gRPC surface: a delete of a protection-enabled database is
// FAILED_PRECONDITION and leaves it in place until the protection is patched
// off.
func TestDeleteDatabaseProtection(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     testDBParent,
		DatabaseId: "prot-db",
		Database:   &adminpb.Database{LocationId: "nam5"},
	})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	created := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(created); err != nil {
		t.Fatalf("CreateDatabase response unmarshal: %v", err)
	}

	enableOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                  created.GetName(),
			DeleteProtectionState: adminpb.Database_DELETE_PROTECTION_ENABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_protection_state"}},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase(enable protection): %v", err)
	}
	enabled := &adminpb.Database{}
	if err := enableOp.GetResponse().UnmarshalTo(enabled); err != nil {
		t.Fatalf("UpdateDatabase response unmarshal: %v", err)
	}
	if enabled.GetDeleteProtectionState() != adminpb.Database_DELETE_PROTECTION_ENABLED {
		t.Fatalf("delete protection = %v, want ENABLED", enabled.GetDeleteProtectionState())
	}
	if _, err := client.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{
		Name: created.GetName(), Etag: enabled.GetEtag(),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("protected delete code = %v, want FailedPrecondition", status.Code(err))
	}
	kept, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: created.GetName()})
	if err != nil {
		t.Errorf("blocked delete removed the database: %v", err)
	} else if kept.GetEtag() != enabled.GetEtag() {
		t.Errorf("blocked delete rotated the etag %q -> %q", enabled.GetEtag(), kept.GetEtag())
	}

	disableOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                  created.GetName(),
			Etag:                  enabled.GetEtag(),
			DeleteProtectionState: adminpb.Database_DELETE_PROTECTION_DISABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_protection_state"}},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase(disable protection): %v", err)
	}
	disabled := &adminpb.Database{}
	if err := disableOp.GetResponse().UnmarshalTo(disabled); err != nil {
		t.Fatalf("UpdateDatabase response unmarshal: %v", err)
	}
	if _, err := client.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{
		Name: created.GetName(), Etag: disabled.GetEtag(),
	}); err != nil {
		t.Fatalf("DeleteDatabase after disabling protection: %v", err)
	}
	if _, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetDatabase after delete = %v, want NotFound", err)
	}
}

func TestEmptyUpdateMaskNoop(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	// Database: a present-but-empty FieldMask is AIP-134's no-op, distinct from
	// the nil mask the round-trip test pins as the implied mask. The record
	// comes back unchanged, without rotating etag/update_time.
	op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     testDBParent,
		DatabaseId: "noop-db",
		Database:   &adminpb.Database{LocationId: "nam5"},
	})
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	created := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(created); err != nil {
		t.Fatalf("CreateDatabase response unmarshal: %v", err)
	}
	updOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:            created.GetName(),
			Etag:            created.GetEtag(),
			ConcurrencyMode: adminpb.Database_PESSIMISTIC,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase: %v", err)
	}
	updated := &adminpb.Database{}
	if err := updOp.GetResponse().UnmarshalTo(updated); err != nil {
		t.Fatalf("UpdateDatabase response unmarshal: %v", err)
	}

	noopOp, err := client.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:            created.GetName(),
			Etag:            updated.GetEtag(),
			ConcurrencyMode: adminpb.Database_OPTIMISTIC,
		},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		t.Fatalf("UpdateDatabase(empty mask): %v", err)
	}
	noop := &adminpb.Database{}
	if err := noopOp.GetResponse().UnmarshalTo(noop); err != nil {
		t.Fatalf("UpdateDatabase(empty mask) response unmarshal: %v", err)
	}
	if noop.GetConcurrencyMode() != adminpb.Database_PESSIMISTIC {
		t.Errorf("empty mask applied concurrency = %v, want PESSIMISTIC", noop.GetConcurrencyMode())
	}
	if noop.GetEtag() != updated.GetEtag() {
		t.Errorf("empty mask rotated the etag %q -> %q", updated.GetEtag(), noop.GetEtag())
	}
	if !noop.GetUpdateTime().AsTime().Equal(updated.GetUpdateTime().AsTime()) {
		t.Errorf("empty mask rotated update_time %v -> %v", updated.GetUpdateTime(), noop.GetUpdateTime())
	}

	// Field: nothing is written, and a no-op on a missing field creates nothing.
	fieldName := "projects/proj/databases/(default)/collectionGroups/cities/fields/population"
	if _, err := client.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name: fieldName,
			TtlConfig: &adminpb.Field_TtlConfig{
				State:            adminpb.Field_TtlConfig_ACTIVE,
				ExpirationOffset: durationpb.New(2 * time.Hour),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"ttl_config"}},
	}); err != nil {
		t.Fatalf("UpdateField: %v", err)
	}
	fieldOp, err := client.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name: fieldName,
			TtlConfig: &adminpb.Field_TtlConfig{
				State:            adminpb.Field_TtlConfig_ACTIVE,
				ExpirationOffset: durationpb.New(9 * time.Hour),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		t.Fatalf("UpdateField(empty mask): %v", err)
	}
	field := &adminpb.Field{}
	if err := fieldOp.GetResponse().UnmarshalTo(field); err != nil {
		t.Fatalf("UpdateField(empty mask) response: %v", err)
	}
	if field.GetTtlConfig().GetExpirationOffset().AsDuration() != 2*time.Hour {
		t.Errorf("empty mask applied ttl_config = %+v, want the stored 2h", field.GetTtlConfig())
	}
	ghost := "projects/proj/databases/(default)/collectionGroups/cities/fields/ghost"
	if _, err := client.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field:      &adminpb.Field{Name: ghost, TtlConfig: &adminpb.Field_TtlConfig{State: adminpb.Field_TtlConfig_ACTIVE}},
		UpdateMask: &fieldmaskpb.FieldMask{},
	}); status.Code(err) != codes.NotFound {
		t.Errorf("UpdateField(empty mask, missing field) code = %v, want NotFound", status.Code(err))
	}
	if _, err := client.GetField(ctx, &adminpb.GetFieldRequest{Name: ghost}); status.Code(err) != codes.NotFound {
		t.Errorf("empty mask created a field: GetField code = %v, want NotFound", status.Code(err))
	}

	// Backup schedule: retention and the recurrence oneof stay as stored.
	parent := "projects/proj/databases/(default)"
	schedName := parent + "/backupSchedules/1"
	if _, err := client.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
		Parent: parent,
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      schedName,
			Retention: durationpb.New(7 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_DailyRecurrence{
				DailyRecurrence: &adminpb.DailyRecurrence{},
			},
		},
	}); err != nil {
		t.Fatalf("CreateBackupSchedule: %v", err)
	}
	bs, err := client.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      schedName,
			Retention: durationpb.New(14 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_WeeklyRecurrence{
				WeeklyRecurrence: &adminpb.WeeklyRecurrence{Day: dayofweek.DayOfWeek_MONDAY},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		t.Fatalf("UpdateBackupSchedule(empty mask): %v", err)
	}
	if bs.GetRetention().AsDuration() != 7*24*time.Hour {
		t.Errorf("empty mask applied retention = %v, want 7d", bs.GetRetention())
	}
	if bs.GetWeeklyRecurrence() != nil || bs.GetDailyRecurrence() == nil {
		t.Errorf("empty mask applied the recurrence = %+v, want daily", bs.GetRecurrence())
	}
}

func TestFieldAdminRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	name := "projects/proj/databases/(default)/collectionGroups/cities/fields/population"
	op, err := client.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name: name,
			IndexConfig: &adminpb.Field_IndexConfig{
				UsesAncestorConfig: true,
				AncestorField:      "cities",
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"index_config"}},
	})
	if err != nil {
		t.Fatalf("UpdateField: %v", err)
	}
	field := &adminpb.Field{}
	if err := op.GetResponse().UnmarshalTo(field); err != nil {
		t.Fatalf("UpdateField response unmarshal: %v", err)
	}
	if field.GetName() != name || !field.GetIndexConfig().GetUsesAncestorConfig() {
		t.Fatalf("field = %+v", field)
	}

	wantAncestor := "projects/proj/databases/(default)/collectionGroups/cities/fields/__name__"
	got, err := client.GetField(ctx, &adminpb.GetFieldRequest{Name: name})
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if got.GetIndexConfig().GetAncestorField() != wantAncestor {
		t.Fatalf("GetField = %+v, want the derived __name__ ancestor", got)
	}

	list, err := client.ListFields(ctx, &adminpb.ListFieldsRequest{
		Parent: "projects/proj/databases/(default)/collectionGroups/-",
	})
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	if len(list.GetFields()) != 1 {
		t.Fatalf("ListFields len = %d, want 1", len(list.GetFields()))
	}

	// A nil UpdateMask is the implied mask of every populated field (AIP-134,
	// the same contract UpdateDatabase honours since DC20): the maskless patch
	// applies both configs instead of answering INVALID_ARGUMENT.
	op, err = client.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name: name,
			IndexConfig: &adminpb.Field_IndexConfig{
				UsesAncestorConfig: true,
				AncestorField:      "cities",
			},
			TtlConfig: &adminpb.Field_TtlConfig{
				State:            adminpb.Field_TtlConfig_ACTIVE,
				ExpirationOffset: durationpb.New(2 * time.Hour),
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateField(maskless): %v", err)
	}
	maskless := &adminpb.Field{}
	if err := op.GetResponse().UnmarshalTo(maskless); err != nil {
		t.Fatalf("UpdateField(maskless) response: %v", err)
	}
	if maskless.GetTtlConfig().GetState() != adminpb.Field_TtlConfig_ACTIVE ||
		maskless.GetTtlConfig().GetExpirationOffset().AsDuration() != 2*time.Hour {
		t.Errorf("maskless ttl_config = %+v", maskless.GetTtlConfig())
	}
	if maskless.GetIndexConfig().GetAncestorField() != wantAncestor {
		t.Errorf("maskless patch dropped index_config = %+v", maskless.GetIndexConfig())
	}
}

func TestUserCredsAdminRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	parent := "projects/proj/databases/(default)"
	created, err := client.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
		Parent:      parent,
		UserCredsId: "cred1",
	})
	if err != nil {
		t.Fatalf("CreateUserCreds: %v", err)
	}
	if created.GetSecurePassword() == "" {
		t.Fatal("CreateUserCreds did not return a secure_password")
	}
	if created.GetResourceIdentity().GetPrincipal() == "" {
		t.Error("CreateUserCreds principal empty")
	}

	got, err := client.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("GetUserCreds: %v", err)
	}
	if got.GetSecurePassword() != "" {
		t.Error("GetUserCreds exposed secure_password")
	}

	list, err := client.ListUserCreds(ctx, &adminpb.ListUserCredsRequest{Parent: parent})
	if err != nil || len(list.GetUserCreds()) != 1 {
		t.Fatalf("ListUserCreds len=%d err=%v", len(list.GetUserCreds()), err)
	}

	dis, err := client.DisableUserCreds(ctx, &adminpb.DisableUserCredsRequest{Name: created.GetName()})
	if err != nil || dis.GetState() != adminpb.UserCreds_DISABLED {
		t.Fatalf("DisableUserCreds = %+v err=%v", dis, err)
	}
	en, err := client.EnableUserCreds(ctx, &adminpb.EnableUserCredsRequest{Name: created.GetName()})
	if err != nil || en.GetState() != adminpb.UserCreds_ENABLED {
		t.Fatalf("EnableUserCreds = %+v err=%v", en, err)
	}

	reset, err := client.ResetUserPassword(ctx, &adminpb.ResetUserPasswordRequest{Name: created.GetName()})
	if err != nil || reset.GetSecurePassword() == "" {
		t.Fatalf("ResetUserPassword = %+v err=%v", reset, err)
	}

	if _, err := client.DeleteUserCreds(ctx, &adminpb.DeleteUserCredsRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("DeleteUserCreds: %v", err)
	}
	if _, err := client.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetUserCreds after delete = %v, want NotFound", err)
	}
}

func TestBackupScheduleAdminRoundTrip(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	parent := "projects/proj/databases/(default)"
	schedName := parent + "/backupSchedules/1"
	created, err := client.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
		Parent: parent,
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      schedName,
			Retention: durationpb.New(7 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_DailyRecurrence{
				DailyRecurrence: &adminpb.DailyRecurrence{},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateBackupSchedule: %v", err)
	}
	if created.GetName() != schedName || created.GetDailyRecurrence() == nil {
		t.Fatalf("created schedule = %+v", created)
	}

	got, err := client.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: schedName})
	if err != nil || got.GetRetention() == nil {
		t.Fatalf("GetBackupSchedule = %+v err=%v", got, err)
	}

	list, err := client.ListBackupSchedules(ctx, &adminpb.ListBackupSchedulesRequest{Parent: parent})
	if err != nil || len(list.GetBackupSchedules()) != 1 {
		t.Fatalf("ListBackupSchedules len=%d err=%v", len(list.GetBackupSchedules()), err)
	}

	upd, err := client.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      schedName,
			Retention: durationpb.New(14 * 24 * time.Hour),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retention"}},
	})
	if err != nil || upd.GetRetention().AsDuration() != 14*24*time.Hour {
		t.Fatalf("UpdateBackupSchedule = %+v err=%v", upd, err)
	}

	// A nil UpdateMask applies every populated field — both the retention and
	// the recurrence oneof, which the old `*` arm silently dropped.
	upd, err = client.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      schedName,
			Retention: durationpb.New(21 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_WeeklyRecurrence{
				WeeklyRecurrence: &adminpb.WeeklyRecurrence{Day: dayofweek.DayOfWeek_WEDNESDAY},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateBackupSchedule(maskless): %v", err)
	}
	if upd.GetRetention().AsDuration() != 21*24*time.Hour {
		t.Errorf("maskless retention = %v, want 21d", upd.GetRetention())
	}
	if upd.GetWeeklyRecurrence().GetDay() != dayofweek.DayOfWeek_WEDNESDAY {
		t.Errorf("maskless recurrence = %+v, want weekly Wednesday", upd.GetRecurrence())
	}

	if _, err := client.DeleteBackupSchedule(ctx, &adminpb.DeleteBackupScheduleRequest{Name: schedName}); err != nil {
		t.Fatalf("DeleteBackupSchedule: %v", err)
	}
	if _, err := client.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: schedName}); status.Code(err) != codes.NotFound {
		t.Errorf("GetBackupSchedule after delete = %v, want NotFound", err)
	}
}

func TestBackupReadOnlyRPCs(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()

	list, err := client.ListBackups(ctx, &adminpb.ListBackupsRequest{Parent: "projects/proj/locations/us-central1"})
	if err != nil || len(list.GetBackups()) != 0 {
		t.Fatalf("ListBackups len=%d err=%v", len(list.GetBackups()), err)
	}
	if _, err := client.GetBackup(ctx, &adminpb.GetBackupRequest{
		Name: "projects/proj/locations/us-central1/backups/b1",
	}); status.Code(err) != codes.NotFound {
		t.Errorf("GetBackup(missing) = %v, want NotFound", err)
	}
	if _, err := client.DeleteBackup(ctx, &adminpb.DeleteBackupRequest{
		Name: "projects/proj/locations/us-central1/backups/b1",
	}); status.Code(err) != codes.NotFound {
		t.Errorf("DeleteBackup(missing) = %v, want NotFound", err)
	}
}

// TestDatabaseInvalidName checks malformed resource names are InvalidArgument.
func TestDatabaseInvalidName(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	if _, err := client.GetDatabase(context.Background(), &adminpb.GetDatabaseRequest{Name: "not-a-name"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetDatabase(bad name) = %v, want InvalidArgument", err)
	}
}

// TestDatabaseOutputFields pins the Database fields the emulator derives rather
// than stores: the edition-dependent concurrency default (DC53), the
// continuously updated earliest_version_time (DC54) and the implicit (default)
// database's real uid/timestamps/etag (DC46).
func TestDatabaseOutputFields(t *testing.T) {
	client, cleanup := newAdminClient(t, store.NewMemoryResourceStore())
	defer cleanup()
	ctx := context.Background()
	frozen := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: frozen})
	defer clock.SetGlobalClock(clock.RealClock{})

	// A create that omits concurrency_mode defaults by edition: Pessimistic for
	// STANDARD, Optimistic for ENTERPRISE.
	for _, tc := range []struct {
		id      string
		edition adminpb.Database_DatabaseEdition
		want    adminpb.Database_ConcurrencyMode
	}{
		{"editionstd", adminpb.Database_STANDARD, adminpb.Database_PESSIMISTIC},
		{"editionent", adminpb.Database_ENTERPRISE, adminpb.Database_OPTIMISTIC},
	} {
		op, err := client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
			Parent:     testDBParent,
			DatabaseId: tc.id,
			Database:   &adminpb.Database{DatabaseEdition: tc.edition},
		})
		if err != nil {
			t.Fatalf("CreateDatabase(%v): %v", tc.edition, err)
		}
		db := &adminpb.Database{}
		if err := op.GetResponse().UnmarshalTo(db); err != nil {
			t.Fatalf("CreateDatabase response: %v", err)
		}
		if db.GetConcurrencyMode() != tc.want {
			t.Errorf("create(%v) concurrency = %v, want %v", tc.edition, db.GetConcurrencyMode(), tc.want)
		}
		// earliest_version_time is populated with now - version_retention_period
		// (PITR is off, so 1 hour).
		if got := db.GetEarliestVersionTime().AsTime(); !got.Equal(frozen.Add(-time.Hour)) {
			t.Errorf("create(%v) earliest_version_time = %v, want %v", tc.edition, got, frozen.Add(-time.Hour))
		}
	}

	// The implicit (default) database carries the documented output fields, and
	// the etag it reports is a usable delete precondition.
	def, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: "projects/proj/databases/(default)"})
	if err != nil {
		t.Fatalf("GetDatabase(default): %v", err)
	}
	if def.GetUid() == "" || def.GetUid() == "default" {
		t.Errorf("default uid = %q, want a generated value", def.GetUid())
	}
	if def.GetEtag() == "" || def.GetCreateTime() == nil || def.GetUpdateTime() == nil {
		t.Errorf("default database = %+v, want etag + timestamps", def)
	}
	if got := def.GetEarliestVersionTime().AsTime(); !got.Equal(frozen.Add(-time.Hour)) {
		t.Errorf("default earliest_version_time = %v, want %v", got, frozen.Add(-time.Hour))
	}
	again, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: def.GetName()})
	if err != nil {
		t.Fatalf("GetDatabase(default) again: %v", err)
	}
	if again.GetEtag() != def.GetEtag() {
		t.Errorf("default etag is not stable across reads: %q -> %q", def.GetEtag(), again.GetEtag())
	}
	if _, err := client.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{
		Name: def.GetName(), Etag: "stale-etag",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("stale-etag delete of (default) code = %v, want FailedPrecondition", status.Code(err))
	}
	if _, err := client.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{
		Name: def.GetName(), Etag: def.GetEtag(),
	}); err != nil {
		t.Errorf("DeleteDatabase(default) with the read etag: %v", err)
	}
	if _, err := client.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: def.GetName()}); status.Code(err) != codes.NotFound {
		t.Errorf("deleted (default) read code = %v, want NotFound", status.Code(err))
	}
}

// TestBackupToProtoStats locks the Backup.stats rendering: a READY backup
// carries the statistics message (the data becomes available once the backup is
// materialized, and an empty database legitimately measures zero), while a
// backup that is still being created has none.
func TestBackupToProtoStats(t *testing.T) {
	ready := backupToProto(firestoreprovider.BackupDef{
		Name: "projects/p/locations/l/backups/b", State: "READY",
	})
	if ready.GetStats() == nil {
		t.Fatal("a READY backup must carry Stats even with zero counts")
	}
	if ready.GetStats().GetDocumentCount() != 0 || ready.GetStats().GetIndexCount() != 0 || ready.GetStats().GetSizeBytes() != 0 {
		t.Errorf("zero stats = %+v", ready.GetStats())
	}

	creating := backupToProto(firestoreprovider.BackupDef{
		Name: "projects/p/locations/l/backups/b", State: "CREATING",
	})
	if creating.GetStats() != nil {
		t.Errorf("a CREATING backup must not carry Stats, got %+v", creating.GetStats())
	}

	counted := backupToProto(firestoreprovider.BackupDef{
		Name: "projects/p/locations/l/backups/b", State: "READY",
		DocumentCount: 3, IndexCount: 2, SizeBytes: 99,
	})
	if got := counted.GetStats(); got.GetDocumentCount() != 3 || got.GetIndexCount() != 2 || got.GetSizeBytes() != 99 {
		t.Errorf("stats = %+v, want 3/2/99", got)
	}
}
