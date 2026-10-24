package grpcconformance

// FirestoreAdmin control-plane probes (databases, fields, user creds, backup
// schedules, backups) over the official admin client / generated stub. These
// are the RPCs beyond the composite-index CRUD covered in
// checks_firestore_admin.go. Every LRO returned by the emulator is terminal
// (done=true), so the probes unmarshal the inline response rather than polling
// operations.get. Names are run-unique via cfg.Suffix.

import (
	"context"
	"fmt"
	"time"

	adminpb "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	"google.golang.org/genproto/googleapis/type/dayofweek"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// firestoreAdminControlChecks returns the control-plane probes in execution
// order. They share the emulator's state: the database probes operate on a
// run-unique named database, all other probes on (default).
func firestoreAdminControlChecks() []Check {
	return []Check{
		{Service: "firestoreadmin", RPC: "CreateDatabase", KeyField: "operation done=true; database name; edition-default concurrency_mode; earliest_version_time about now-1h", Run: checkFsAdminCreateDatabase},
		{Service: "firestoreadmin", RPC: "GetDatabase", KeyField: "database name/uid/timestamps/etag + (default) materialized with stable output fields", Run: checkFsAdminGetDatabase},
		{Service: "firestoreadmin", RPC: "ListDatabases", KeyField: "created database present + (default) with output fields", Run: checkFsAdminListDatabases},
		{Service: "firestoreadmin", RPC: "UpdateDatabase", KeyField: "concurrency_mode under mask + omitted mask applies populated fields + empty mask is a no-op + OUTPUT_ONLY fields ignored, retention derived from PITR, IMMUTABLE edition change rejected", Run: checkFsAdminUpdateDatabase},
		{Service: "firestoreadmin", RPC: "DeleteDatabase", KeyField: "stale etag -> FailedPrecondition; delete protection -> FailedPrecondition; operation response carries database; absent after delete", Run: checkFsAdminDeleteDatabase},
		{Service: "firestoreadmin", RPC: "UpdateField", Method: "UpdateField", KeyField: "operation done=true; index_config under mask with OUTPUT_ONLY members derived + index_config/ttl_config under an omitted mask + empty mask is a no-op", Run: checkFsAdminUpdateField},
		{Service: "firestoreadmin", RPC: "GetField", Method: "GetField", KeyField: "index_config round-trip with derived ancestor/uses_ancestor_config + ttl state ACTIVE", Run: checkFsAdminGetField},
		{Service: "firestoreadmin", RPC: "ListFields", Method: "ListFields", KeyField: "field present under wildcard group", Run: checkFsAdminListFields},
		{Service: "firestoreadmin", RPC: "CreateUserCreds", Method: "CreateUserCreds", KeyField: "secure_password returned once", Run: checkFsAdminCreateUserCreds},
		{Service: "firestoreadmin", RPC: "GetUserCreds", Method: "GetUserCreds", KeyField: "name; no secure_password", Run: checkFsAdminGetUserCreds},
		{Service: "firestoreadmin", RPC: "ListUserCreds", Method: "ListUserCreds", KeyField: "user creds present", Run: checkFsAdminListUserCreds},
		{Service: "firestoreadmin", RPC: "DisableUserCreds", Method: "DisableUserCreds", KeyField: "state DISABLED", Run: checkFsAdminDisableUserCreds},
		{Service: "firestoreadmin", RPC: "EnableUserCreds", Method: "EnableUserCreds", KeyField: "state ENABLED", Run: checkFsAdminEnableUserCreds},
		{Service: "firestoreadmin", RPC: "ResetUserPassword", Method: "ResetUserPassword", KeyField: "new secure_password", Run: checkFsAdminResetUserPassword},
		{Service: "firestoreadmin", RPC: "DeleteUserCreds", Method: "DeleteUserCreds", KeyField: "user creds absent after delete", Run: checkFsAdminDeleteUserCreds},
		{Service: "firestoreadmin", RPC: "CreateBackupSchedule", Method: "CreateBackupSchedule", KeyField: "schedule name/retention", Run: checkFsAdminCreateBackupSchedule},
		{Service: "firestoreadmin", RPC: "GetBackupSchedule", Method: "GetBackupSchedule", KeyField: "retention round-trip", Run: checkFsAdminGetBackupSchedule},
		{Service: "firestoreadmin", RPC: "ListBackupSchedules", Method: "ListBackupSchedules", KeyField: "schedule present", Run: checkFsAdminListBackupSchedules},
		{Service: "firestoreadmin", RPC: "UpdateBackupSchedule", Method: "UpdateBackupSchedule", KeyField: "retention under mask + retention/recurrence under an omitted mask + empty mask is a no-op", Run: checkFsAdminUpdateBackupSchedule},
		{Service: "firestoreadmin", RPC: "DeleteBackupSchedule", Method: "DeleteBackupSchedule", KeyField: "schedule absent after delete", Run: checkFsAdminDeleteBackupSchedule},
		{Service: "firestoreadmin", RPC: "ListBackups", Method: "ListBackups", KeyField: "empty list (no create path)", Run: checkFsAdminListBackups},
		{Service: "firestoreadmin", RPC: "GetBackup", Method: "GetBackup", KeyField: "missing backup -> NotFound", Run: checkFsAdminGetBackupMissing},
		{Service: "firestoreadmin", RPC: "DeleteBackup", Method: "DeleteBackup", KeyField: "missing backup -> NotFound", Run: checkFsAdminDeleteBackupMissing},
	}
}

// ─── naming helpers ───────────────────────────────────────────────────────────

func fsAdminDBName(cfg Config, id string) string {
	return fmt.Sprintf("projects/%s/databases/%s", cfg.Project, id)
}

func fsAdminDefaultDB(cfg Config) string { return fsAdminDBName(cfg, "(default)") }

func fsAdminDBID(cfg Config) string { return cfg.ResourceName("gcpc-fsdb") }

func fsAdminField(cfg Config) string { return cfg.ResourceName("fsaf") }

func fsAdminFieldCG(cfg Config) string { return cfg.ResourceName("gcpc-fsaf") }

func fsAdminFieldName(cfg Config) string {
	return fmt.Sprintf("%s/collectionGroups/%s/fields/%s", fsAdminDefaultDB(cfg), fsAdminFieldCG(cfg), fsAdminField(cfg))
}

// fsAdminFieldAncestor is the collection group's __name__ field, the ancestor a
// field's single-field index configuration is inherited from.
func fsAdminFieldAncestor(cfg Config) string {
	return fmt.Sprintf("%s/collectionGroups/%s/fields/__name__", fsAdminDefaultDB(cfg), fsAdminFieldCG(cfg))
}

func fsAdminUserCredsID(cfg Config) string { return cfg.ResourceName("fscred") }

func fsAdminUserCredsName(cfg Config) string {
	return fmt.Sprintf("%s/userCreds/%s", fsAdminDefaultDB(cfg), fsAdminUserCredsID(cfg))
}

func fsAdminScheduleID(cfg Config) string { return cfg.ResourceName("fssched") }

func fsAdminScheduleName(cfg Config) string {
	return fmt.Sprintf("%s/backupSchedules/%s", fsAdminDefaultDB(cfg), fsAdminScheduleID(cfg))
}

func fsAdminBackupParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/us-central1", cfg.Project)
}

func fsAdminBackupName(cfg Config) string {
	return fsAdminBackupParent(cfg) + "/backups/" + cfg.ResourceName("fsbackup")
}

// ─── database probes ──────────────────────────────────────────────────────────

func checkFsAdminCreateDatabase(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	op, err := stub.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:     fmt.Sprintf("projects/%s", cfg.Project),
		DatabaseId: fsAdminDBID(cfg),
		Database:   &adminpb.Database{LocationId: "nam5"},
	})
	if err != nil {
		return fmt.Errorf("CreateDatabase: %w", err)
	}
	if !op.GetDone() || op.GetResponse() == nil {
		return fmt.Errorf("CreateDatabase operation = done:%t response:%v, want terminal response", op.GetDone(), op.GetResponse())
	}
	db := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(db); err != nil {
		return fmt.Errorf("CreateDatabase response: %w", err)
	}
	if want := fsAdminDBName(cfg, fsAdminDBID(cfg)); db.GetName() != want {
		return fmt.Errorf("CreateDatabase name = %q, want %q", db.GetName(), want)
	}
	if db.GetType() != adminpb.Database_FIRESTORE_NATIVE {
		return fmt.Errorf("CreateDatabase type = %v, want FIRESTORE_NATIVE", db.GetType())
	}
	// The create omitted concurrency_mode and the edition, so both defaults apply:
	// STANDARD, and Pessimistic ("If unspecified in a CreateDatabase request, this
	// will default based on the database edition: Optimistic for Enterprise and
	// Pessimistic for all other databases").
	if db.GetConcurrencyMode() != adminpb.Database_PESSIMISTIC {
		return fmt.Errorf("CreateDatabase concurrency_mode = %v, want PESSIMISTIC for the STANDARD default", db.GetConcurrencyMode())
	}
	// earliest_version_time is `now - version_retention_period`; PITR is off, so
	// it trails the response by about an hour.
	if db.GetEarliestVersionTime() == nil || db.GetVersionRetentionPeriod() == nil {
		return fmt.Errorf("CreateDatabase = %+v, want earliest_version_time + version_retention_period", db)
	}
	if got := db.GetEarliestVersionTime().AsTime(); got.After(time.Now().Add(-time.Hour)) || got.Before(time.Now().Add(-2*time.Hour)) {
		return fmt.Errorf("CreateDatabase earliest_version_time = %v, want about now-1h", got)
	}
	return nil
}

func checkFsAdminGetDatabase(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: fsAdminDBName(cfg, fsAdminDBID(cfg))})
	if err != nil {
		return fmt.Errorf("GetDatabase: %w", err)
	}
	if got.GetUid() == "" || got.GetLocationId() != "nam5" {
		return fmt.Errorf("GetDatabase = %+v, want uid + location nam5", got)
	}
	if got.GetCreateTime() == nil || got.GetUpdateTime() == nil || got.GetEtag() == "" {
		return fmt.Errorf("GetDatabase = %+v, want create/update time + etag", got)
	}

	// The implicit (default) database is a real resource: its output fields are
	// present and its etag is the delete precondition a client would send.
	def, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: fsAdminDefaultDB(cfg)})
	if err != nil {
		return fmt.Errorf("GetDatabase(default): %w", err)
	}
	if def.GetUid() == "" || def.GetUid() == "default" || def.GetEtag() == "" ||
		def.GetCreateTime() == nil || def.GetUpdateTime() == nil || def.GetEarliestVersionTime() == nil {
		return fmt.Errorf("GetDatabase(default) = %+v, want uid/timestamps/etag/earliest_version_time", def)
	}
	if again, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: def.GetName()}); err != nil {
		return fmt.Errorf("GetDatabase(default) again: %w", err)
	} else if again.GetEtag() != def.GetEtag() || again.GetUid() != def.GetUid() {
		return fmt.Errorf("GetDatabase(default) output fields are not stable: %+v -> %+v", def, again)
	}
	return nil
}

func checkFsAdminListDatabases(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := stub.ListDatabases(ctx, &adminpb.ListDatabasesRequest{Parent: fmt.Sprintf("projects/%s", cfg.Project)})
	if err != nil {
		return fmt.Errorf("ListDatabases: %w", err)
	}
	want := fsAdminDBName(cfg, fsAdminDBID(cfg))
	var haveCreated, haveDefault bool
	for _, d := range resp.GetDatabases() {
		if d.GetName() == want {
			haveCreated = true
		}
		if d.GetName() == fsAdminDefaultDB(cfg) {
			haveDefault = true
			if d.GetUid() == "" || d.GetEtag() == "" || d.GetCreateTime() == nil || d.GetEarliestVersionTime() == nil {
				return fmt.Errorf("ListDatabases (default) = %+v, want the materialized resource's output fields", d)
			}
		}
	}
	if !haveCreated {
		return fmt.Errorf("ListDatabases did not include %q (%d databases)", want, len(resp.GetDatabases()))
	}
	if !haveDefault {
		return fmt.Errorf("ListDatabases did not include the (default) database")
	}
	return nil
}

func checkFsAdminUpdateDatabase(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	name := fsAdminDBName(cfg, fsAdminDBID(cfg))
	cur, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetDatabase: %w", err)
	}
	op, err := stub.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database:   &adminpb.Database{Name: name, Etag: cur.GetEtag(), ConcurrencyMode: adminpb.Database_PESSIMISTIC},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"concurrency_mode"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateDatabase: %w", err)
	}
	upd := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(upd); err != nil {
		return fmt.Errorf("UpdateDatabase response: %w", err)
	}
	if upd.GetConcurrencyMode() != adminpb.Database_PESSIMISTIC {
		return fmt.Errorf("UpdateDatabase concurrency = %v, want PESSIMISTIC", upd.GetConcurrencyMode())
	}
	// AIP-134: an omitted update_mask is an implied mask of the populated
	// fields, so the maskless patch must apply.
	op, err = stub.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{Name: name, Etag: upd.GetEtag(), ConcurrencyMode: adminpb.Database_OPTIMISTIC},
	})
	if err != nil {
		return fmt.Errorf("UpdateDatabase(omitted mask): %w", err)
	}
	implied := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(implied); err != nil {
		return fmt.Errorf("UpdateDatabase(omitted mask) response: %w", err)
	}
	if implied.GetConcurrencyMode() != adminpb.Database_OPTIMISTIC {
		return fmt.Errorf("UpdateDatabase(omitted mask) concurrency = %v, want OPTIMISTIC", implied.GetConcurrencyMode())
	}
	// A present-but-empty mask is AIP-134's no-op — distinct from the nil mask
	// above: the record must come back unchanged, with no etag rotation.
	op, err = stub.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database:   &adminpb.Database{Name: name, Etag: implied.GetEtag(), ConcurrencyMode: adminpb.Database_PESSIMISTIC},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		return fmt.Errorf("UpdateDatabase(empty mask): %w", err)
	}
	unchanged := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(unchanged); err != nil {
		return fmt.Errorf("UpdateDatabase(empty mask) response: %w", err)
	}
	if unchanged.GetConcurrencyMode() != adminpb.Database_OPTIMISTIC {
		return fmt.Errorf("UpdateDatabase(empty mask) applied concurrency = %v, want OPTIMISTIC", unchanged.GetConcurrencyMode())
	}
	if unchanged.GetEtag() != implied.GetEtag() {
		return fmt.Errorf("UpdateDatabase(empty mask) rotated the etag %q -> %q", implied.GetEtag(), unchanged.GetEtag())
	}
	// database.proto marks key_prefix and version_retention_period OUTPUT_ONLY
	// and database_edition IMMUTABLE: a patch that carries changed values must
	// not store them, and the retention window is the PITR-derived one (7 days
	// enabled, 1 hour otherwise), never the request's.
	op, err = stub.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                          name,
			Etag:                          unchanged.GetEtag(),
			KeyPrefix:                     "client-supplied",
			DatabaseEdition:               adminpb.Database_STANDARD,
			VersionRetentionPeriod:        durationpb.New(30 * time.Minute),
			PointInTimeRecoveryEnablement: adminpb.Database_POINT_IN_TIME_RECOVERY_ENABLED,
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateDatabase(server-owned fields): %w", err)
	}
	owned := &adminpb.Database{}
	if err := op.GetResponse().UnmarshalTo(owned); err != nil {
		return fmt.Errorf("UpdateDatabase(server-owned fields) response: %w", err)
	}
	if owned.GetKeyPrefix() != "" {
		return fmt.Errorf("UpdateDatabase stored the OUTPUT_ONLY key_prefix = %q", owned.GetKeyPrefix())
	}
	if owned.GetDatabaseEdition() != unchanged.GetDatabaseEdition() {
		return fmt.Errorf("UpdateDatabase wrote the IMMUTABLE database_edition %v -> %v", unchanged.GetDatabaseEdition(), owned.GetDatabaseEdition())
	}
	if got := owned.GetVersionRetentionPeriod().AsDuration(); got != 7*24*time.Hour {
		return fmt.Errorf("UpdateDatabase retention = %v, want 168h (PITR enabled)", got)
	}
	// A masked change to the IMMUTABLE edition is INVALID_ARGUMENT (AIP-203); the
	// patch above resent the created value, which is accepted.
	if _, err := stub.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database:   &adminpb.Database{Name: name, DatabaseEdition: adminpb.Database_ENTERPRISE},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"database_edition"}},
	}); status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("UpdateDatabase(database_edition change) code = %v, want InvalidArgument", status.Code(err))
	}
	return nil
}

func checkFsAdminDeleteDatabase(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	name := fsAdminDBName(cfg, fsAdminDBID(cfg))
	cur, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetDatabase: %w", err)
	}
	// DeleteDatabaseRequest.etag documents that a mismatch blocks the deletion
	// with FAILED_PRECONDITION (not an update's ABORTED), so a stale token must
	// leave the database in place.
	if _, err := stub.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: name, Etag: "stale-etag"}); status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("DeleteDatabase(stale etag) code = %v, want FailedPrecondition", status.Code(err))
	}
	unchanged, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetDatabase after stale-etag delete: %w", err)
	}
	if unchanged.GetEtag() != cur.GetEtag() {
		return fmt.Errorf("stale-etag delete changed the database etag %q -> %q", cur.GetEtag(), unchanged.GetEtag())
	}
	// Delete protection blocks the delete while enabled (Firestore: a database
	// with delete protection enabled cannot be deleted until it is disabled).
	protOp, err := stub.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                  name,
			Etag:                  cur.GetEtag(),
			DeleteProtectionState: adminpb.Database_DELETE_PROTECTION_ENABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_protection_state"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateDatabase(enable delete protection): %w", err)
	}
	protected := &adminpb.Database{}
	if err := protOp.GetResponse().UnmarshalTo(protected); err != nil {
		return fmt.Errorf("UpdateDatabase response: %w", err)
	}
	if _, err := stub.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: name, Etag: protected.GetEtag()}); status.Code(err) != codes.FailedPrecondition {
		return fmt.Errorf("DeleteDatabase(delete protection) code = %v, want FailedPrecondition", status.Code(err))
	}
	kept, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetDatabase after protected delete: %w", err)
	}
	if kept.GetDeleteProtectionState() != adminpb.Database_DELETE_PROTECTION_ENABLED {
		return fmt.Errorf("protected delete changed the database state = %v", kept.GetDeleteProtectionState())
	}
	if kept.GetEtag() != protected.GetEtag() {
		return fmt.Errorf("protected delete rotated the database etag %q -> %q", protected.GetEtag(), kept.GetEtag())
	}
	unprotOp, err := stub.UpdateDatabase(ctx, &adminpb.UpdateDatabaseRequest{
		Database: &adminpb.Database{
			Name:                  name,
			Etag:                  kept.GetEtag(),
			DeleteProtectionState: adminpb.Database_DELETE_PROTECTION_DISABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"delete_protection_state"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateDatabase(disable delete protection): %w", err)
	}
	unprotected := &adminpb.Database{}
	if err := unprotOp.GetResponse().UnmarshalTo(unprotected); err != nil {
		return fmt.Errorf("UpdateDatabase response: %w", err)
	}
	delOp, err := stub.DeleteDatabase(ctx, &adminpb.DeleteDatabaseRequest{Name: name, Etag: unprotected.GetEtag()})
	if err != nil {
		return fmt.Errorf("DeleteDatabase: %w", err)
	}
	// operation_info declares response_type "Database": the terminal operation
	// must carry the soft-deleted database (a client's Operation.Wait errors on
	// an empty typed response).
	deleted := &adminpb.Database{}
	if err := delOp.GetResponse().UnmarshalTo(deleted); err != nil {
		return fmt.Errorf("DeleteDatabase response: %w", err)
	}
	if deleted.GetName() != name || deleted.GetDeleteTime() == nil {
		return fmt.Errorf("DeleteDatabase response name=%q delete_time=%v, want the soft-deleted database",
			deleted.GetName(), deleted.GetDeleteTime())
	}
	if _, err := stub.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetDatabase after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── field probes ─────────────────────────────────────────────────────────────

func checkFsAdminUpdateField(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	op, err := stub.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name: fsAdminFieldName(cfg),
			// The index-config members are OUTPUT_ONLY: values a request carries
			// are ignored and the inherited (collection-group __name__) form is
			// reported instead.
			IndexConfig: &adminpb.Field_IndexConfig{
				UsesAncestorConfig: false,
				AncestorField:      "ignored",
				Reverting:          true,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"index_config"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateField: %w", err)
	}
	field := &adminpb.Field{}
	if err := op.GetResponse().UnmarshalTo(field); err != nil {
		return fmt.Errorf("UpdateField response: %w", err)
	}
	if field.GetName() != fsAdminFieldName(cfg) || !field.GetIndexConfig().GetUsesAncestorConfig() {
		return fmt.Errorf("UpdateField = %+v", field)
	}
	if got := field.GetIndexConfig(); got.GetAncestorField() != fsAdminFieldAncestor(cfg) || got.GetReverting() {
		return fmt.Errorf("UpdateField index_config = %+v, want the derived ancestor %q and reverting=false",
			got, fsAdminFieldAncestor(cfg))
	}

	// A nil UpdateMask is the implied mask of the populated fields (AIP-134):
	// both configs must apply, including ttl_config (the old `*` arm dropped it).
	op, err = stub.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name:        fsAdminFieldName(cfg),
			IndexConfig: &adminpb.Field_IndexConfig{UsesAncestorConfig: true, AncestorField: fsAdminFieldCG(cfg)},
			TtlConfig: &adminpb.Field_TtlConfig{
				State:            adminpb.Field_TtlConfig_ACTIVE,
				ExpirationOffset: durationpb.New(2 * time.Hour),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateField (omitted mask): %w", err)
	}
	field = &adminpb.Field{}
	if err := op.GetResponse().UnmarshalTo(field); err != nil {
		return fmt.Errorf("UpdateField (omitted mask) response: %w", err)
	}
	if field.GetTtlConfig().GetState() != adminpb.Field_TtlConfig_ACTIVE ||
		field.GetTtlConfig().GetExpirationOffset().AsDuration() != 2*time.Hour {
		return fmt.Errorf("UpdateField (omitted mask) ttl_config = %+v", field.GetTtlConfig())
	}
	if !field.GetIndexConfig().GetUsesAncestorConfig() {
		return fmt.Errorf("UpdateField (omitted mask) dropped index_config = %+v", field.GetIndexConfig())
	}
	// A present-but-empty mask is AIP-134's no-op: nothing is applied.
	op, err = stub.UpdateField(ctx, &adminpb.UpdateFieldRequest{
		Field: &adminpb.Field{
			Name: fsAdminFieldName(cfg),
			TtlConfig: &adminpb.Field_TtlConfig{
				State:            adminpb.Field_TtlConfig_ACTIVE,
				ExpirationOffset: durationpb.New(9 * time.Hour),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		return fmt.Errorf("UpdateField(empty mask): %w", err)
	}
	field = &adminpb.Field{}
	if err := op.GetResponse().UnmarshalTo(field); err != nil {
		return fmt.Errorf("UpdateField(empty mask) response: %w", err)
	}
	if field.GetTtlConfig().GetExpirationOffset().AsDuration() != 2*time.Hour {
		return fmt.Errorf("UpdateField(empty mask) applied ttl_config = %+v", field.GetTtlConfig())
	}
	return nil
}

func checkFsAdminGetField(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.GetField(ctx, &adminpb.GetFieldRequest{Name: fsAdminFieldName(cfg)})
	if err != nil {
		return fmt.Errorf("GetField: %w", err)
	}
	if got.GetIndexConfig().GetAncestorField() != fsAdminFieldAncestor(cfg) ||
		!got.GetIndexConfig().GetUsesAncestorConfig() || got.GetIndexConfig().GetReverting() {
		return fmt.Errorf("GetField index_config = %+v, want the derived inherited form", got.GetIndexConfig())
	}
	if got.GetTtlConfig().GetState() != adminpb.Field_TtlConfig_ACTIVE {
		return fmt.Errorf("GetField ttl_config = %+v, want state ACTIVE", got.GetTtlConfig())
	}
	return nil
}

func checkFsAdminListFields(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	want := fsAdminFieldName(cfg)
	token := ""
	for {
		resp, err := stub.ListFields(ctx, &adminpb.ListFieldsRequest{
			Parent:    fsAdminDefaultDB(cfg) + "/collectionGroups/-",
			PageSize:  100,
			PageToken: token,
		})
		if err != nil {
			return fmt.Errorf("ListFields: %w", err)
		}
		for _, f := range resp.GetFields() {
			if f.GetName() == want {
				return nil
			}
		}
		token = resp.GetNextPageToken()
		if token == "" {
			return fmt.Errorf("ListFields did not include %q", want)
		}
	}
}

// ─── user creds probes ────────────────────────────────────────────────────────

func checkFsAdminCreateUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	uc, err := stub.CreateUserCreds(ctx, &adminpb.CreateUserCredsRequest{
		Parent:      fsAdminDefaultDB(cfg),
		UserCredsId: fsAdminUserCredsID(cfg),
	})
	if err != nil {
		return fmt.Errorf("CreateUserCreds: %w", err)
	}
	if uc.GetName() != fsAdminUserCredsName(cfg) {
		return fmt.Errorf("CreateUserCreds name = %q, want %q", uc.GetName(), fsAdminUserCredsName(cfg))
	}
	if uc.GetSecurePassword() == "" {
		return fmt.Errorf("CreateUserCreds did not return a secure_password")
	}
	if uc.GetState() != adminpb.UserCreds_ENABLED {
		return fmt.Errorf("CreateUserCreds state = %v, want ENABLED", uc.GetState())
	}
	return nil
}

func checkFsAdminGetUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: fsAdminUserCredsName(cfg)})
	if err != nil {
		return fmt.Errorf("GetUserCreds: %w", err)
	}
	if got.GetName() != fsAdminUserCredsName(cfg) {
		return fmt.Errorf("GetUserCreds name = %q", got.GetName())
	}
	if got.GetSecurePassword() != "" {
		return fmt.Errorf("GetUserCreds exposed secure_password")
	}
	return nil
}

func checkFsAdminListUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := stub.ListUserCreds(ctx, &adminpb.ListUserCredsRequest{Parent: fsAdminDefaultDB(cfg)})
	if err != nil {
		return fmt.Errorf("ListUserCreds: %w", err)
	}
	for _, uc := range resp.GetUserCreds() {
		if uc.GetName() == fsAdminUserCredsName(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListUserCreds did not include %q", fsAdminUserCredsName(cfg))
}

func checkFsAdminDisableUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	uc, err := stub.DisableUserCreds(ctx, &adminpb.DisableUserCredsRequest{Name: fsAdminUserCredsName(cfg)})
	if err != nil {
		return fmt.Errorf("DisableUserCreds: %w", err)
	}
	if uc.GetState() != adminpb.UserCreds_DISABLED {
		return fmt.Errorf("DisableUserCreds state = %v, want DISABLED", uc.GetState())
	}
	return nil
}

func checkFsAdminEnableUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	uc, err := stub.EnableUserCreds(ctx, &adminpb.EnableUserCredsRequest{Name: fsAdminUserCredsName(cfg)})
	if err != nil {
		return fmt.Errorf("EnableUserCreds: %w", err)
	}
	if uc.GetState() != adminpb.UserCreds_ENABLED {
		return fmt.Errorf("EnableUserCreds state = %v, want ENABLED", uc.GetState())
	}
	return nil
}

func checkFsAdminResetUserPassword(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	uc, err := stub.ResetUserPassword(ctx, &adminpb.ResetUserPasswordRequest{Name: fsAdminUserCredsName(cfg)})
	if err != nil {
		return fmt.Errorf("ResetUserPassword: %w", err)
	}
	if uc.GetSecurePassword() == "" {
		return fmt.Errorf("ResetUserPassword did not return a secure_password")
	}
	return nil
}

func checkFsAdminDeleteUserCreds(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := stub.DeleteUserCreds(ctx, &adminpb.DeleteUserCredsRequest{Name: fsAdminUserCredsName(cfg)}); err != nil {
		return fmt.Errorf("DeleteUserCreds: %w", err)
	}
	if _, err := stub.GetUserCreds(ctx, &adminpb.GetUserCredsRequest{Name: fsAdminUserCredsName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetUserCreds after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── backup schedule probes ───────────────────────────────────────────────────

func checkFsAdminCreateBackupSchedule(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	bs, err := stub.CreateBackupSchedule(ctx, &adminpb.CreateBackupScheduleRequest{
		Parent: fsAdminDefaultDB(cfg),
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      fsAdminScheduleName(cfg),
			Retention: durationpb.New(7 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_DailyRecurrence{
				DailyRecurrence: &adminpb.DailyRecurrence{},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateBackupSchedule: %w", err)
	}
	if bs.GetName() != fsAdminScheduleName(cfg) || bs.GetDailyRecurrence() == nil {
		return fmt.Errorf("CreateBackupSchedule = %+v", bs)
	}
	return nil
}

func checkFsAdminGetBackupSchedule(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: fsAdminScheduleName(cfg)})
	if err != nil {
		return fmt.Errorf("GetBackupSchedule: %w", err)
	}
	if got.GetRetention().AsDuration() != 7*24*time.Hour {
		return fmt.Errorf("GetBackupSchedule retention = %v, want 7d", got.GetRetention())
	}
	return nil
}

func checkFsAdminListBackupSchedules(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := stub.ListBackupSchedules(ctx, &adminpb.ListBackupSchedulesRequest{Parent: fsAdminDefaultDB(cfg)})
	if err != nil {
		return fmt.Errorf("ListBackupSchedules: %w", err)
	}
	for _, bs := range resp.GetBackupSchedules() {
		if bs.GetName() == fsAdminScheduleName(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListBackupSchedules did not include %q", fsAdminScheduleName(cfg))
}

func checkFsAdminUpdateBackupSchedule(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	got, err := stub.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      fsAdminScheduleName(cfg),
			Retention: durationpb.New(14 * 24 * time.Hour),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retention"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateBackupSchedule: %w", err)
	}
	if got.GetRetention().AsDuration() != 14*24*time.Hour {
		return fmt.Errorf("UpdateBackupSchedule retention = %v, want 14d", got.GetRetention())
	}

	// A nil UpdateMask applies every populated field — retention and the
	// recurrence oneof, which the old `*` arm silently dropped.
	got, err = stub.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      fsAdminScheduleName(cfg),
			Retention: durationpb.New(21 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_WeeklyRecurrence{
				WeeklyRecurrence: &adminpb.WeeklyRecurrence{Day: dayofweek.DayOfWeek_WEDNESDAY},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateBackupSchedule (omitted mask): %w", err)
	}
	if got.GetRetention().AsDuration() != 21*24*time.Hour {
		return fmt.Errorf("UpdateBackupSchedule (omitted mask) retention = %v, want 21d", got.GetRetention())
	}
	if got.GetWeeklyRecurrence().GetDay() != dayofweek.DayOfWeek_WEDNESDAY {
		return fmt.Errorf("UpdateBackupSchedule (omitted mask) recurrence = %+v, want weekly Wednesday", got.GetRecurrence())
	}
	// A present-but-empty mask is AIP-134's no-op: retention and the recurrence
	// oneof stay as stored.
	got, err = stub.UpdateBackupSchedule(ctx, &adminpb.UpdateBackupScheduleRequest{
		BackupSchedule: &adminpb.BackupSchedule{
			Name:      fsAdminScheduleName(cfg),
			Retention: durationpb.New(28 * 24 * time.Hour),
			Recurrence: &adminpb.BackupSchedule_WeeklyRecurrence{
				WeeklyRecurrence: &adminpb.WeeklyRecurrence{Day: dayofweek.DayOfWeek_TUESDAY},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{},
	})
	if err != nil {
		return fmt.Errorf("UpdateBackupSchedule(empty mask): %w", err)
	}
	if got.GetRetention().AsDuration() != 21*24*time.Hour {
		return fmt.Errorf("UpdateBackupSchedule(empty mask) applied retention = %v, want 21d", got.GetRetention())
	}
	if got.GetWeeklyRecurrence().GetDay() != dayofweek.DayOfWeek_WEDNESDAY {
		return fmt.Errorf("UpdateBackupSchedule(empty mask) applied recurrence = %+v, want weekly Wednesday", got.GetRecurrence())
	}
	return nil
}

func checkFsAdminDeleteBackupSchedule(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := stub.DeleteBackupSchedule(ctx, &adminpb.DeleteBackupScheduleRequest{Name: fsAdminScheduleName(cfg)}); err != nil {
		return fmt.Errorf("DeleteBackupSchedule: %w", err)
	}
	if _, err := stub.GetBackupSchedule(ctx, &adminpb.GetBackupScheduleRequest{Name: fsAdminScheduleName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetBackupSchedule after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── backup probes ────────────────────────────────────────────────────────────

func checkFsAdminListBackups(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := stub.ListBackups(ctx, &adminpb.ListBackupsRequest{Parent: fsAdminBackupParent(cfg)})
	if err != nil {
		return fmt.Errorf("ListBackups: %w", err)
	}
	if len(resp.GetBackups()) != 0 {
		return fmt.Errorf("ListBackups returned %d backups, want 0 (no create path)", len(resp.GetBackups()))
	}
	return nil
}

func checkFsAdminGetBackupMissing(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := stub.GetBackup(ctx, &adminpb.GetBackupRequest{Name: fsAdminBackupName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetBackup(missing) = %v, want NotFound", err)
	}
	return nil
}

func checkFsAdminDeleteBackupMissing(ctx context.Context, cfg Config) error {
	stub, conn, err := newFirestoreAdminStub(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := stub.DeleteBackup(ctx, &adminpb.DeleteBackupRequest{Name: fsAdminBackupName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("DeleteBackup(missing) = %v, want NotFound", err)
	}
	return nil
}
