// firestoreadmin_depth_test.go is the Firestore Admin service's first
// official-client behavioural suite, and closes DC8
// (plan_docs/gcp-depth-closure-wave-plan.md W2.6): the service had no
// official-client suite at all, so its p1 lifecycle was unproven and every
// p2-p5 depth cell was a day-one exemption.
//
// The suite drives the official Firestore Admin gRPC client
// (cloud.google.com/go/firestore/apiv1/admin, the google.firestore.admin.v1
// wire service) through the admin control plane this service *is* — databases,
// composite indexes, collection-group fields, backup schedules, user
// credentials and the read-only backups surface — and asserts the four depth
// classes with the shared depth-test kit (tests/integration/gcp/testdepth,
// DC2).
//
//	TestSDKFirestoreAdminLifecycle    p1  create -> get -> list -> update -> delete
//	TestSDKFirestoreAdminConcurrency  p2  same-name create races + non-idempotent delete
//	TestSDKFirestoreAdminErrors       p3  the typed envelope on the documented negatives
//	TestSDKFirestoreAdminListing      p4  pageSize/pageToken over indexes.list
//	TestSDKFirestoreAdminScale        p5  the many-indexes / many-user-creds boundary
//
// Two Discovery-document constraints shape the suite rather than being faked:
//
//   - Among the admin resources this suite drives, only
//     `projects.databases.collectionGroups.indexes.list` (and `.fields.list`)
//     carries pageSize/pageToken; `projects.databases.list` and
//     `projects.locations.backups.list` declare no cursor at all, so they are
//     asserted as complete single responses, never forced into a page walk.
//   - Backups are read-only in the Admin API: the Discovery document declares
//     only get/list/delete (a backup is produced by a schedule, not by a client),
//     so the suite asserts the read-only contract — an empty list and
//     NOT_FOUND on get/delete — rather than inventing a create path.
//
// The terminal DeleteDatabase operation carries the soft-deleted Database (the
// proto's operation_info declares response_type "Database"), so the suite waits
// on it through the official client's DeleteDatabaseOperation.Wait and asserts
// the returned record, then the resource's absence. A maskless UpdateDatabase
// (AIP-134's implied mask of the populated fields) is asserted in the lifecycle's
// Databases case — DC20 — and the same implied-mask form is asserted for
// UpdateField and UpdateBackupSchedule (dotted through the official client),
// which must apply every populated field including the recurrence oneof — DC36.
// The ServerOwnedFields case pins database.proto's field ownership — DC38:
// key_prefix/version_retention_period are OUTPUT_ONLY and database_edition is
// IMMUTABLE, so a patch never stores them and the retention window is derived
// from the PITR toggle.
//
// The concurrency cases run under -race (the module's integration step adds it);
// asserting a contention property in a binary without the race detector is a
// failure, not a silent downgrade (testdepth.AssertRaceEnabled).
package firestoreadmin_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	admin "cloud.google.com/go/firestore/apiv1/admin"
	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	testdepth "jaiscloud-gcp-testdepth"
)

// TestSDKFirestoreAdminLifecycle is the p1 baseline lifecycle the §9 contract
// requires and the suite's reason to exist: each Admin resource is created,
// read back, listed, updated and deleted through the official client.
func TestSDKFirestoreAdminLifecycle(t *testing.T) {
	ctx := context.Background()
	c := newAdmin(t)

	t.Run("Databases", func(t *testing.T) {
		id := unique("depthdb")
		created := createDatabase(t, c, id)
		require.Equal(t, adminpb.Database_FIRESTORE_NATIVE, created.GetType())
		require.NotEmpty(t, created.GetUid())
		require.NotEmpty(t, created.GetEtag())
		require.Equal(t, adminpb.Database_DELETE_PROTECTION_DISABLED, created.GetDeleteProtectionState(),
			"delete protection is documented as disabled by default")
		// database.proto: a create that omits concurrency_mode defaults by
		// edition — Pessimistic for STANDARD (DC53) — and earliest_version_time
		// is `now - version_retention_period` (DC54).
		require.Equal(t, adminpb.Database_PESSIMISTIC, created.GetConcurrencyMode(),
			"STANDARD is the create default, so the concurrency default is Pessimistic")
		require.WithinDuration(t, time.Now().Add(-time.Hour), created.GetEarliestVersionTime().AsTime(), 2*time.Minute,
			"PITR is off by default, so the readable window is the last hour")

		got, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: databaseName(id)})
		require.NoError(t, err)
		require.Equal(t, created.GetUid(), got.GetUid())

		list, err := c.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: projectName()})
		require.NoError(t, err)
		names := make([]string, 0, len(list.GetDatabases()))
		for _, d := range list.GetDatabases() {
			names = append(names, d.GetName())
		}
		require.Contains(t, names, databaseName(id))
		require.Contains(t, names, databaseName("(default)"), "the implicit default database is always listed")

		updOp, err := c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database:   &adminpb.Database{Name: databaseName(id), ConcurrencyMode: adminpb.Database_PESSIMISTIC},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
		})
		require.NoError(t, err)
		updated, err := updOp.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, adminpb.Database_PESSIMISTIC, updated.GetConcurrencyMode())

		// An omitted update_mask is an implied mask of the populated fields
		// (AIP-134, DC20): a maskless patch applies instead of
		// failing INVALID_ARGUMENT.
		impliedOp, err := c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database: &adminpb.Database{Name: databaseName(id), ConcurrencyMode: adminpb.Database_OPTIMISTIC},
		})
		require.NoError(t, err)
		implied, err := impliedOp.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, adminpb.Database_OPTIMISTIC, implied.GetConcurrencyMode())
		require.Equal(t, created.GetLocationId(), implied.GetLocationId(),
			"a maskless patch must not clear an unpopulated field")
		require.NotEqual(t, updated.GetEtag(), implied.GetEtag(), "a maskless update rotates the etag")

		// Delete returns a terminal operation carrying the soft-deleted
		// database (operation_info response_type "Database"), so the typed
		// Wait resolves instead of failing on an empty result.
		delOp, err := c.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: databaseName(id)})
		require.NoError(t, err)
		require.True(t, delOp.Done(), "the control plane is synchronous: the operation is already terminal")
		require.NotEmpty(t, delOp.Name())
		deleted, err := delOp.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, databaseName(id), deleted.GetName())
		require.NotNil(t, deleted.GetDeleteTime(), "the operation response is the soft-deleted database")
		testdepth.AssertError(t, getDatabaseErr(ctx, c, id), testdepth.SpecNotFound())
	})

	// database.proto marks key_prefix and version_retention_period OUTPUT_ONLY and
	// database_edition IMMUTABLE (DC38): the official client can carry them in a
	// patch, but the emulator must not store the output-only ones, the retention
	// window is the PITR-derived one — 7 days enabled, 1 hour otherwise — and a
	// change to the immutable edition is INVALID_ARGUMENT.
	t.Run("ServerOwnedFields", func(t *testing.T) {
		id := unique("owneddb")
		created := createDatabase(t, c, id)
		require.Equal(t, adminpb.Database_STANDARD, created.GetDatabaseEdition(),
			"database.proto: STANDARD is the default when a create omits the edition")
		require.Equal(t, time.Hour, created.GetVersionRetentionPeriod().AsDuration(),
			"PITR is disabled by default, so retention is 1 hour")

		enabledOp, err := c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database: &adminpb.Database{
				Name:                          databaseName(id),
				DatabaseEdition:               created.GetDatabaseEdition(),
				KeyPrefix:                     "client-supplied",
				VersionRetentionPeriod:        durationpb.New(30 * time.Minute),
				PointInTimeRecoveryEnablement: adminpb.Database_POINT_IN_TIME_RECOVERY_ENABLED,
			},
		})
		require.NoError(t, err)
		enabled, err := enabledOp.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, 7*24*time.Hour, enabled.GetVersionRetentionPeriod().AsDuration(),
			"enabling PITR reports the documented 7-day window, not the request's")
		require.Empty(t, enabled.GetKeyPrefix(), "key_prefix is OUTPUT_ONLY")
		require.Equal(t, created.GetDatabaseEdition(), enabled.GetDatabaseEdition(),
			"database_edition is IMMUTABLE and a resent value is preserved")
		require.WithinDuration(t, time.Now().Add(-7*24*time.Hour), enabled.GetEarliestVersionTime().AsTime(), 2*time.Minute,
			"enabling PITR widens the earliest readable version to 7 days back")

		// A masked change to the IMMUTABLE edition is INVALID_ARGUMENT (AIP-203);
		// the patch above resent the created value, which is accepted. The
		// control plane rejects it up front, so the error arrives on the call.
		_, err = c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database:   &adminpb.Database{Name: databaseName(id), DatabaseEdition: adminpb.Database_ENTERPRISE},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"database_edition"}},
		})
		testdepth.AssertError(t, err, testdepth.SpecInvalidArgument())
	})

	// database.proto: a create that omits concurrency_mode defaults by edition.
	// STANDARD is covered above (Pessimistic); an Enterprise create defaults to
	// Optimistic.
	t.Run("CreateConcurrencyDefaultByEdition", func(t *testing.T) {
		id := unique("entdb")
		op, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
			Parent:     projectName(),
			DatabaseId: id,
			Database:   &adminpb.Database{DatabaseEdition: adminpb.Database_ENTERPRISE},
		})
		require.NoError(t, err)
		db, err := op.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, adminpb.Database_ENTERPRISE, db.GetDatabaseEdition())
		require.Equal(t, adminpb.Database_OPTIMISTIC, db.GetConcurrencyMode(),
			"an Enterprise create that omits concurrency_mode defaults to Optimistic")
		deleteDatabaseOnCleanup(t, c, id)
	})

	t.Run("DefaultDatabaseOutputFields", func(t *testing.T) {
		// The implicit (default) database is a real resource on real GCP: it
		// reports a uid, create/update timestamps and an etag, and that etag is
		// the documented delete precondition (DC46). It is not deleted here
		// because the other subtests use it as a parent.
		def, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: databaseName("(default)")})
		require.NoError(t, err)
		require.Equal(t, adminpb.Database_FIRESTORE_NATIVE, def.GetType())
		require.NotEmpty(t, def.GetUid())
		require.NotEqual(t, "default", def.GetUid(), "uid is a system-generated UUID4, not a placeholder")
		require.NotEmpty(t, def.GetEtag())
		require.NotNil(t, def.GetCreateTime())
		require.NotNil(t, def.GetUpdateTime())
		require.NotNil(t, def.GetEarliestVersionTime())

		again, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: def.GetName()})
		require.NoError(t, err)
		require.Equal(t, def.GetEtag(), again.GetEtag(), "the default database's output fields are stable across reads")
		require.Equal(t, def.GetUid(), again.GetUid())

		_, err = c.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: def.GetName(), Etag: "stale-etag"})
		testdepth.AssertError(t, err, testdepth.SpecPreconditionFailed())
		kept, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: def.GetName()})
		require.NoError(t, err, "a stale-etag delete must leave the default database in place")
		require.Equal(t, def.GetEtag(), kept.GetEtag())
	})

	// A present-but-empty update_mask is AIP-134's no-op, distinct from the
	// omitted (nil) mask asserted above as the implied mask: the request must
	// return the stored resource unmodified, without rotating etag/update_time,
	// and must not create a resource for a missing field (DC37).
	t.Run("EmptyUpdateMaskIsNoop", func(t *testing.T) {
		id := unique("noopdb")
		created := createDatabase(t, c, id)
		updOp, err := c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database:   &adminpb.Database{Name: databaseName(id), ConcurrencyMode: adminpb.Database_PESSIMISTIC},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
		})
		require.NoError(t, err)
		updated, err := updOp.Wait(ctx)
		require.NoError(t, err)
		require.NotEqual(t, created.GetEtag(), updated.GetEtag())

		noopOp, err := c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
			Database:   &adminpb.Database{Name: databaseName(id), ConcurrencyMode: adminpb.Database_OPTIMISTIC},
			UpdateMask: &fieldmaskpb.FieldMask{},
		})
		require.NoError(t, err)
		noop, err := noopOp.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, adminpb.Database_PESSIMISTIC, noop.GetConcurrencyMode(),
			"an empty mask must not apply the patch body")
		require.Equal(t, updated.GetEtag(), noop.GetEtag(), "an empty mask must not rotate the etag")
		require.Equal(t, updated.GetUpdateTime().AsTime(), noop.GetUpdateTime().AsTime(),
			"an empty mask must not rotate update_time")

		cg := unique("noopcg")
		fname := fieldName("(default)", cg, "f")
		fieldOp, err := c.UpdateField(ctx, &adminpb.UpdateFieldRequest{
			Field: &adminpb.Field{Name: fname, TtlConfig: &adminpb.Field_TtlConfig{
				State: adminpb.Field_TtlConfig_ACTIVE, ExpirationOffset: durationpb.New(2 * time.Hour),
			}},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"ttl_config"}},
		})
		require.NoError(t, err)
		_, err = fieldOp.Wait(ctx)
		require.NoError(t, err)
		fieldOp, err = c.UpdateField(ctx, &adminpb.UpdateFieldRequest{
			Field: &adminpb.Field{Name: fname, TtlConfig: &adminpb.Field_TtlConfig{
				State: adminpb.Field_TtlConfig_ACTIVE, ExpirationOffset: durationpb.New(9 * time.Hour),
			}},
			UpdateMask: &fieldmaskpb.FieldMask{},
		})
		require.NoError(t, err)
		field, err := fieldOp.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, 2*time.Hour, field.GetTtlConfig().GetExpirationOffset().AsDuration(),
			"an empty mask must leave the stored ttl_config alone")
		ghost := fieldName("(default)", cg, "ghost")
		_, err = c.UpdateField(ctx, &adminpb.UpdateFieldRequest{
			Field:      &adminpb.Field{Name: ghost, TtlConfig: &adminpb.Field_TtlConfig{State: adminpb.Field_TtlConfig_ACTIVE}},
			UpdateMask: &fieldmaskpb.FieldMask{},
		})
		testdepth.AssertError(t, err, testdepth.SpecNotFound())
		_, err = c.GetField(ctx, &adminpb.GetFieldRequest{Name: ghost})
		testdepth.AssertError(t, err, testdepth.SpecNotFound())

		schedParent := databaseName("(default)")
		schedName := schedParent + "/backupSchedules/noop"
		_, err = c.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
			Parent: schedParent,
			BackupSchedule: &adminpb.BackupSchedule{
				Name:       schedName,
				Retention:  durationpb.New(7 * 24 * time.Hour),
				Recurrence: &adminpb.BackupSchedule_DailyRecurrence{DailyRecurrence: &adminpb.DailyRecurrence{}},
			},
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = c.DeleteBackupSchedule(context.Background(), &adminpb.DeleteBackupScheduleRequest{Name: schedName})
		})
		bs, err := c.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
			BackupSchedule: &adminpb.BackupSchedule{
				Name:      schedName,
				Retention: durationpb.New(14 * 24 * time.Hour),
				Recurrence: &adminpb.BackupSchedule_WeeklyRecurrence{
					WeeklyRecurrence: &adminpb.WeeklyRecurrence{Day: dayofweek.DayOfWeek_MONDAY},
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{},
		})
		require.NoError(t, err)
		require.Equal(t, 7*24*time.Hour, bs.GetRetention().AsDuration(), "an empty mask must leave retention alone")
		require.NotNil(t, bs.GetDailyRecurrence(), "an empty mask must leave the recurrence oneof alone")
	})

	t.Run("CompositeIndexes", func(t *testing.T) {
		cg := unique("cg")
		parent := collectionGroupParent("(default)", cg)
		created := createIndex(t, c, parent, "a", "b")
		require.Equal(t, adminpb.Index_READY, created.GetState())
		require.Len(t, created.GetFields(), 2)

		got, err := c.GetIndex(ctx, &adminpb.GetIndexRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Equal(t, created.GetName(), got.GetName())

		list, err := listAllIndexes(ctx, c, parent)
		require.NoError(t, err)
		require.Len(t, list, 1, "a fresh collection group carries exactly its created index")
		require.Equal(t, created.GetName(), list[0].GetName())

		err = c.DeleteIndex(ctx, &adminpb.DeleteIndexRequest{Name: created.GetName()})
		require.NoError(t, err)
		_, err = c.GetIndex(ctx, &adminpb.GetIndexRequest{Name: created.GetName()})
		testdepth.AssertError(t, err, testdepth.SpecNotFound())
	})

	t.Run("CollectionGroupFields", func(t *testing.T) {
		cg := unique("cg")
		parent := collectionGroupParent("(default)", cg)
		name := fieldName("(default)", cg, "weight")

		op, err := c.UpdateField(ctx, &adminpb.UpdateFieldRequest{
			Field: &adminpb.Field{
				Name:      name,
				TtlConfig: &adminpb.Field_TtlConfig{State: adminpb.Field_TtlConfig_ACTIVE},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"ttl_config"}},
		})
		require.NoError(t, err)
		field, err := op.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, adminpb.Field_TtlConfig_ACTIVE, field.GetTtlConfig().GetState())

		got, err := c.GetField(ctx, &adminpb.GetFieldRequest{Name: name})
		require.NoError(t, err)
		require.Equal(t, adminpb.Field_TtlConfig_ACTIVE, got.GetTtlConfig().GetState())

		fields, err := listAllFields(ctx, c, parent)
		require.NoError(t, err)
		require.Len(t, fields, 1)
		require.Equal(t, name, fields[0].GetName())

		// A nil UpdateMask is the implied mask of the populated fields
		// (AIP-134): both configs apply with no mask, instead of INVALID_ARGUMENT.
		op, err = c.UpdateField(ctx, &adminpb.UpdateFieldRequest{
			Field: &adminpb.Field{
				Name:        name,
				IndexConfig: &adminpb.Field_IndexConfig{UsesAncestorConfig: true, AncestorField: cg},
				TtlConfig: &adminpb.Field_TtlConfig{
					State:            adminpb.Field_TtlConfig_ACTIVE,
					ExpirationOffset: durationpb.New(2 * time.Hour),
				},
			},
		})
		require.NoError(t, err)
		field, err = op.Wait(ctx)
		require.NoError(t, err)
		require.Equal(t, adminpb.Field_TtlConfig_ACTIVE, field.GetTtlConfig().GetState())
		require.Equal(t, 2*time.Hour, field.GetTtlConfig().GetExpirationOffset().AsDuration())
		require.True(t, field.GetIndexConfig().GetUsesAncestorConfig(), "the implied mask must carry index_config too")
	})

	t.Run("BackupSchedules", func(t *testing.T) {
		dbID := unique("depthbs")
		createDatabase(t, c, dbID)

		created, err := c.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
			Parent: databaseName(dbID),
			BackupSchedule: &adminpb.BackupSchedule{
				Retention:  durationpb.New(7 * 24 * time.Hour),
				Recurrence: &adminpb.BackupSchedule_DailyRecurrence{DailyRecurrence: &adminpb.DailyRecurrence{}},
			},
		})
		require.NoError(t, err)
		// The backup-schedule id is server-assigned (the proto marks name
		// OUTPUT_ONLY and CreateBackupScheduleRequest carries no id), so only the
		// parent and a non-empty child id are asserted — never the emulator's
		// chosen value.
		require.NotEmpty(t, strings.TrimPrefix(created.GetName(), databaseName(dbID)+"/backupSchedules/"))
		require.True(t, strings.HasPrefix(created.GetName(), databaseName(dbID)+"/backupSchedules/"), "name %q must be a backup schedule of the database", created.GetName())
		require.Equal(t, 7*24*time.Hour, created.GetRetention().AsDuration())

		got, err := c.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Equal(t, created.GetName(), got.GetName())

		list, err := c.ListBackupSchedules(ctx, &adminpb.ListBackupSchedulesRequest{Parent: databaseName(dbID)})
		require.NoError(t, err)
		require.Len(t, list.GetBackupSchedules(), 1)

		updated, err := c.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
			BackupSchedule: &adminpb.BackupSchedule{
				Name:      created.GetName(),
				Retention: durationpb.New(14 * 24 * time.Hour),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retention"}},
		})
		require.NoError(t, err)
		require.Equal(t, 14*24*time.Hour, updated.GetRetention().AsDuration())

		// A nil UpdateMask applies every populated field — retention and the
		// recurrence oneof, which the old `*` arm silently dropped.
		updated, err = c.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
			BackupSchedule: &adminpb.BackupSchedule{
				Name:      created.GetName(),
				Retention: durationpb.New(21 * 24 * time.Hour),
				Recurrence: &adminpb.BackupSchedule_WeeklyRecurrence{
					WeeklyRecurrence: &adminpb.WeeklyRecurrence{Day: dayofweek.DayOfWeek_WEDNESDAY},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 21*24*time.Hour, updated.GetRetention().AsDuration())
		require.Equal(t, dayofweek.DayOfWeek_WEDNESDAY, updated.GetWeeklyRecurrence().GetDay())

		err = c.DeleteBackupSchedule(ctx, &adminpb.DeleteBackupScheduleRequest{Name: created.GetName()})
		require.NoError(t, err)
		_, err = c.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: created.GetName()})
		testdepth.AssertError(t, err, testdepth.SpecNotFound())
	})

	t.Run("UserCreds", func(t *testing.T) {
		dbID := unique("depthuc")
		createDatabase(t, c, dbID)

		created, err := c.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
			Parent:      databaseName(dbID),
			UserCredsId: unique("uc"),
			UserCreds:   &adminpb.UserCreds{},
		})
		require.NoError(t, err)
		require.Equal(t, adminpb.UserCreds_ENABLED, created.GetState())
		require.NotEmpty(t, created.GetSecurePassword(), "the secret is surfaced once, on create")
		identity, ok := created.GetUserCredsIdentity().(*adminpb.UserCreds_ResourceIdentity_)
		require.True(t, ok, "the creds carry a resource identity")
		require.NotEmpty(t, identity.ResourceIdentity.GetPrincipal())

		got, err := c.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Empty(t, got.GetSecurePassword(), "get never returns the secret")

		list, err := c.ListUserCreds(ctx, &adminpb.ListUserCredsRequest{Parent: databaseName(dbID)})
		require.NoError(t, err)
		require.Len(t, list.GetUserCreds(), 1)
		require.Empty(t, list.GetUserCreds()[0].GetSecurePassword(), "list never returns the secret")

		disabled, err := c.DisableUserCreds(ctx, &adminpb.DisableUserCredsRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Equal(t, adminpb.UserCreds_DISABLED, disabled.GetState())

		enabled, err := c.EnableUserCreds(ctx, &adminpb.EnableUserCredsRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.Equal(t, adminpb.UserCreds_ENABLED, enabled.GetState())

		reset, err := c.ResetUserPassword(ctx, &adminpb.ResetUserPasswordRequest{Name: created.GetName()})
		require.NoError(t, err)
		require.NotEmpty(t, reset.GetSecurePassword(), "reset rotates and returns the secret once")
		require.NotEqual(t, created.GetSecurePassword(), reset.GetSecurePassword())

		err = c.DeleteUserCreds(ctx, &adminpb.DeleteUserCredsRequest{Name: created.GetName()})
		require.NoError(t, err)
		_, err = c.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: created.GetName()})
		testdepth.AssertError(t, err, testdepth.SpecNotFound())
	})

	t.Run("BackupsAreReadOnly", func(t *testing.T) {
		list, err := c.ListBackups(ctx, &adminpb.ListBackupsRequest{Parent: backupParent("us-central1")})
		require.NoError(t, err)
		require.Empty(t, list.GetBackups(), "the Discovery document declares no client create path, so a location has no backups")
	})
}

// getDatabaseErr issues the GetDatabase call the lifecycle's delete check
// asserts (a helper keeps the subtest readable).
func getDatabaseErr(ctx context.Context, c *admin.FirestoreAdminClient, id string) error {
	_, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: databaseName(id)})
	return err
}

// listAllIndexes drains the ListIndexes iterator (the gapic client pages it
// internally, unlike the databases/user-creds lists which return one response).
func listAllIndexes(ctx context.Context, c *admin.FirestoreAdminClient, parent string) ([]*adminpb.Index, error) {
	it := c.ListIndexes(ctx, &adminpb.ListIndexesRequest{Parent: parent})
	var out []*adminpb.Index
	for {
		item, err := it.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
}

// listAllFields drains the ListFields iterator.
func listAllFields(ctx context.Context, c *admin.FirestoreAdminClient, parent string) ([]*adminpb.Field, error) {
	it := c.ListFields(ctx, &adminpb.ListFieldsRequest{Parent: parent})
	var out []*adminpb.Field
	for {
		item, err := it.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
}

// TestSDKFirestoreAdminConcurrency is p2: concurrent control-plane mutations
// under -race. A same-name create is arbitered by the store's atomic create
// (exactly one winner, every loser AlreadyExists), distinct creates each land,
// and a delete is not idempotent (one winner, every loser NotFound) — matching
// real Firestore, whose delete of an already-gone resource is NOT_FOUND.
func TestSDKFirestoreAdminConcurrency(t *testing.T) {
	ctx := context.Background()
	c := newAdmin(t)
	testdepth.AssertRaceEnabled(t)

	t.Run("SameNameDatabaseCreateRace", func(t *testing.T) {
		id := unique("racedb")
		deleteDatabaseOnCleanup(t, c, id)
		errs := testdepth.Race(6, func(int) error {
			op, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
				Parent:     projectName(),
				DatabaseId: id,
				Database:   &adminpb.Database{LocationId: "us-central1", Type: adminpb.Database_FIRESTORE_NATIVE},
			})
			if err != nil {
				return err
			}
			_, err = op.Wait(ctx)
			return err
		})
		testdepth.AssertOneWinner(t, errs, conflictStatus)

		got, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: databaseName(id)})
		require.NoError(t, err)
		require.Equal(t, databaseName(id), got.GetName())
	})

	t.Run("DistinctDatabasesAllLand", func(t *testing.T) {
		const creators = 6
		ids := make([]string, creators)
		for i := range ids {
			ids[i] = unique(fmt.Sprintf("distdb%d", i))
			deleteDatabaseOnCleanup(t, c, ids[i])
		}
		errs := testdepth.Race(creators, func(i int) error {
			op, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
				Parent:     projectName(),
				DatabaseId: ids[i],
				Database:   &adminpb.Database{LocationId: "us-central1", Type: adminpb.Database_FIRESTORE_NATIVE},
			})
			if err != nil {
				return err
			}
			_, err = op.Wait(ctx)
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "create %d", i)
		}
		for i, id := range ids {
			got, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: databaseName(id)})
			require.NoError(t, err, "get %d", i)
			require.Equal(t, databaseName(id), got.GetName())
		}
	})

	t.Run("SameIdUserCredsCreateRace", func(t *testing.T) {
		dbID := unique("raceuc")
		createDatabase(t, c, dbID)
		ucID := unique("uc")
		deleteUserCredsOnCleanup(t, c, databaseName(dbID)+"/userCreds/"+ucID)

		errs := testdepth.Race(6, func(int) error {
			_, err := c.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
				Parent:      databaseName(dbID),
				UserCredsId: ucID,
				UserCreds:   &adminpb.UserCreds{},
			})
			return err
		})
		testdepth.AssertOneWinner(t, errs, conflictStatus)

		got, err := c.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: databaseName(dbID) + "/userCreds/" + ucID})
		require.NoError(t, err)
		require.Equal(t, databaseName(dbID)+"/userCreds/"+ucID, got.GetName())
	})

	t.Run("ConcurrentUserCredsDeleteIsNotIdempotent", func(t *testing.T) {
		dbID := unique("deluc")
		createDatabase(t, c, dbID)
		created, err := c.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
			Parent:      databaseName(dbID),
			UserCredsId: unique("uc"),
			UserCreds:   &adminpb.UserCreds{},
		})
		require.NoError(t, err)

		errs := testdepth.Race(6, func(int) error {
			return c.DeleteUserCreds(ctx, &adminpb.DeleteUserCredsRequest{Name: created.GetName()})
		})
		// Exactly one delete wins; every loser sees the creds are already gone.
		testdepth.AssertOneWinner(t, errs, goneStatus)

		_, err = c.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: created.GetName()})
		testdepth.AssertError(t, err, testdepth.SpecNotFound())
	})
}

// TestSDKFirestoreAdminErrors is p3: the official client's typed error envelope
// on the documented negatives, asserted through the kit's catalogue.
//
// A maskless UpdateDatabase is not a negative: AIP-134 makes an omitted mask an
// implied mask of the populated fields, and that positive contract is asserted
// in the lifecycle's Databases case (DC20).
func TestSDKFirestoreAdminErrors(t *testing.T) {
	ctx := context.Background()
	c := newAdmin(t)

	dupDB := unique("dupdb")
	createDatabase(t, c, dupDB)

	ucDB := unique("dupuc")
	createDatabase(t, c, ucDB)
	etagDB := unique("etagdb")
	createDatabase(t, c, etagDB)
	protDB := unique("protdb")
	createDatabase(t, c, protDB)
	enableOp, err := c.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                  databaseName(protDB),
			DeleteProtectionState: adminpb.Database_DELETE_PROTECTION_ENABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_protection_state"}},
	})
	require.NoError(t, err)
	_, err = enableOp.Wait(ctx)
	require.NoError(t, err)
	createdCreds, err := c.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
		Parent:      databaseName(ucDB),
		UserCredsId: unique("uc"),
		UserCreds:   &adminpb.UserCreds{},
	})
	require.NoError(t, err)

	singleFieldIndex := &adminpb.Index{
		QueryScope: adminpb.Index_COLLECTION,
		Fields: []*adminpb.Index_IndexField{
			{FieldPath: "only", ValueMode: &adminpb.Index_IndexField_Order_{Order: adminpb.Index_IndexField_ASCENDING}},
		},
	}

	testdepth.RunNegatives(t, []testdepth.NegativeCase{
		{
			Name: "InvalidDatabaseIdIsInvalidArgument",
			Want: testdepth.SpecInvalidArgument(),
			Run: func() error {
				_, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
					Parent:     projectName(),
					DatabaseId: "X",
					Database:   &adminpb.Database{LocationId: "us-central1"},
				})
				return err
			},
		},
		{
			Name: "CreateDefaultDatabaseIsAlreadyExists",
			Want: testdepth.SpecAlreadyExists(),
			Run: func() error {
				_, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
					Parent:     projectName(),
					DatabaseId: "(default)",
					Database:   &adminpb.Database{LocationId: "us-central1"},
				})
				return err
			},
		},
		{
			Name: "DuplicateDatabaseIsAlreadyExists",
			Want: testdepth.SpecAlreadyExists(),
			Run: func() error {
				_, err := c.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
					Parent:     projectName(),
					DatabaseId: dupDB,
					Database:   &adminpb.Database{LocationId: "us-central1"},
				})
				return err
			},
		},
		{
			Name: "GetMissingDatabaseIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				_, err := c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: databaseName(unique("nope"))})
				return err
			},
		},
		{
			Name: "SingleFieldCompositeIndexIsInvalidArgument",
			Want: testdepth.SpecInvalidArgument(),
			Run: func() error {
				_, err := c.CreateIndex(ctx, &adminpb.CreateIndexRequest{
					Parent: collectionGroupParent("(default)", unique("cgerr")),
					Index:  singleFieldIndex,
				})
				return err
			},
		},
		{
			Name: "GetMissingIndexIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				_, err := c.GetIndex(ctx, &adminpb.GetIndexRequest{
					Name: collectionGroupParent("(default)", unique("cgerr")) + "/indexes/" + unique("idx"),
				})
				return err
			},
		},
		{
			Name: "DeleteMissingIndexIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				err := c.DeleteIndex(ctx, &adminpb.DeleteIndexRequest{
					Name: collectionGroupParent("(default)", unique("cgerr")) + "/indexes/" + unique("idx"),
				})
				return err
			},
		},
		{
			Name: "DuplicateUserCredsIsAlreadyExists",
			Want: testdepth.SpecAlreadyExists(),
			Run: func() error {
				_, err := c.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
					Parent:      databaseName(ucDB),
					UserCredsId: leafID(createdCreds.GetName()),
					UserCreds:   &adminpb.UserCreds{},
				})
				return err
			},
		},
		{
			Name: "DeleteDatabaseWithStaleEtagIsFailedPrecondition",
			Want: testdepth.SpecPreconditionFailed(),
			Run: func() error {
				// DeleteDatabaseRequest.etag documents that a mismatch blocks
				// the deletion with FAILED_PRECONDITION (not an update's
				// ABORTED), so nothing may be deleted.
				_, err := c.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{
					Name: databaseName(etagDB), Etag: "stale-etag",
				})
				return err
			},
		},
		{
			Name: "DeleteDatabaseWithDeleteProtectionIsFailedPrecondition",
			Want: testdepth.SpecPreconditionFailed(),
			Run: func() error {
				// Firestore: you cannot delete a database with delete
				// protection enabled until you disable it.
				_, err := c.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: databaseName(protDB)})
				return err
			},
		},
		{
			Name: "GetMissingUserCredsIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				_, err := c.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{
					Name: databaseName(ucDB) + "/userCreds/" + unique("nope"),
				})
				return err
			},
		},
		{
			Name: "GetMissingBackupIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				_, err := c.GetBackup(ctx, &adminpb.GetBackupRequest{Name: backupName("us-central1", unique("bk"))})
				return err
			},
		},
		{
			Name: "DeleteMissingBackupIsNotFound",
			Want: testdepth.SpecNotFound(),
			Run: func() error {
				return c.DeleteBackup(ctx, &adminpb.DeleteBackupRequest{Name: backupName("us-central1", unique("bk"))})
			},
		},
	})

	// The blocked delete left the database intact.
	_, err = c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: databaseName(etagDB)})
	require.NoError(t, err, "a stale-etag delete must not delete the database")
	_, err = c.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: databaseName(protDB)})
	require.NoError(t, err, "a delete-protected delete must not delete the database")
}

// leafID returns the final segment of a resource name (the userCreds id).
func leafID(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' {
			return name[i+1:]
		}
	}
	return name
}

// TestSDKFirestoreAdminListing is p4: projects.databases.collectionGroups.
// indexes.list pages through pageSize/pageToken without dropping, duplicating
// or reordering an index across a page boundary. Among the admin lists this
// suite drives, only the composite-index list carries a cursor in the Discovery
// document; the databases and backups lists declare none, so they are asserted
// as complete single responses elsewhere, never faked into a page walk. The
// Discovery document specifies no order, so the walk is compared as a set.
func TestSDKFirestoreAdminListing(t *testing.T) {
	c := newAdmin(t)

	cg := unique("depthpage")
	parent := collectionGroupParent("(default)", cg)
	want := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		idx := createIndex(t, c, parent, fmt.Sprintf("f%d", i), "name")
		deleteIndexOnCleanup(t, c, idx.GetName())
		want = append(want, idx.GetName())
	}

	got := walkIndexNames(t, c, parent, testdepth.Paging[*adminpb.Index]{
		Name:                 func(i *adminpb.Index) string { return i.GetName() },
		PageSize:             2,
		Order:                testdepth.OrderUnspecified,
		RequireNonEmptyPages: true, // the emulator fills every non-terminal page; AIP-158 permits a zero-result mid-collection page, hence the explicit opt-in
		Expect:               want,
	})
	require.Subset(t, got, want, "the paged walk must return every created index")
}

// walkIndexNames walks ListIndexes through the kit, one page per RPC via the
// generated iterator's InternalFetch seam, and returns the index names in walk
// order.
func walkIndexNames(t *testing.T, c *admin.FirestoreAdminClient, parent string, cfg testdepth.Paging[*adminpb.Index]) []string {
	t.Helper()
	idx := testdepth.AssertPaging(t, func() testdepth.Lister[*adminpb.Index] {
		it := c.ListIndexes(context.Background(), &adminpb.ListIndexesRequest{Parent: parent})
		return func(req testdepth.ListRequest) (testdepth.Page[*adminpb.Index], error) {
			items, next, err := it.InternalFetch(int(req.PageSize), req.PageToken)
			return testdepth.Page[*adminpb.Index]{Items: items, NextPageToken: next}, err
		}
	}, cfg)
	return testdepth.Names(idx, cfg)
}

// TestSDKFirestoreAdminScale is p5: the scale/boundary class. A twenty-five-index
// collection group is walked at a small page size, so the boundary is the whole
// cursor chain over a large collection rather than a single response; and a
// twenty-five-user-creds database is returned whole by the unpaged
// ListUserCreds, proving the unpaged list is not truncated.
func TestSDKFirestoreAdminScale(t *testing.T) {
	ctx := context.Background()
	c := newAdmin(t)

	const many = 25

	t.Run("ManyIndexesPageWalk", func(t *testing.T) {
		cg := unique("depthscale")
		parent := collectionGroupParent("(default)", cg)
		want := make([]string, 0, many)
		for i := 0; i < many; i++ {
			idx := createIndex(t, c, parent, fmt.Sprintf("pf%d", i), "name")
			deleteIndexOnCleanup(t, c, idx.GetName())
			want = append(want, idx.GetName())
		}

		got := walkIndexNames(t, c, parent, testdepth.Paging[*adminpb.Index]{
			Name:                 func(i *adminpb.Index) string { return i.GetName() },
			PageSize:             4,
			Order:                testdepth.OrderUnspecified,
			RequireNonEmptyPages: true, // the emulator fills every non-terminal page; AIP-158 permits a zero-result mid-collection page, hence the explicit opt-in
			Expect:               want,
		})
		require.Subset(t, got, want, "every one of the %d indexes must survive the page walk", many)
	})

	t.Run("ManyUserCredsOneResponse", func(t *testing.T) {
		dbID := unique("scaleuc")
		createDatabase(t, c, dbID)
		want := make([]string, 0, many)
		for i := 0; i < many; i++ {
			uc, err := c.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
				Parent:      databaseName(dbID),
				UserCredsId: fmt.Sprintf("scaleuc-%d-%d", i, time.Now().UnixNano()),
				UserCreds:   &adminpb.UserCreds{},
			})
			require.NoError(t, err)
			deleteUserCredsOnCleanup(t, c, uc.GetName())
			want = append(want, uc.GetName())
		}

		list, err := c.ListUserCreds(ctx, &adminpb.ListUserCredsRequest{Parent: databaseName(dbID)})
		require.NoError(t, err)
		got := make([]string, 0, len(list.GetUserCreds()))
		for _, uc := range list.GetUserCreds() {
			got = append(got, uc.GetName())
		}
		require.Subset(t, got, want, "the unpaged user-creds list must return every creds")
	})
}
