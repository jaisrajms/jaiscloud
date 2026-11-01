package firestore

// Firestore Admin control-plane domain logic (google.firestore.admin.v1) beyond
// the composite-index CRUD in indexes.go: databases, collection-group fields,
// backup schedules, backups and user creds. Every method here is
// transport-agnostic — the gRPC FirestoreAdmin service
// (internal/gcp/grpc/firestoreadmin) is the only caller today — and stores its
// records in the shared ResourceStore, so memory and --dsn (Postgres) backends
// behave identically. Long-running operations are born terminal (done=true),
// matching the index surface: there is no asynchronous control plane to poll.

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Resource-store types for the Firestore Admin control plane.
const (
	rtDatabase       = "firestore_database"
	rtField          = "firestore_field"
	rtUserCreds      = "firestore_user_creds"
	rtBackupSchedule = "firestore_backup_schedule"
	rtBackup         = "firestore_backup"
)

// defaultDatabaseID is the implicit database every Firestore project has.
const defaultDatabaseID = "(default)"

// Database.deleteProtectionState values. Protection is disabled by default and
// blocks projects.databases.delete while enabled. DELETE_PROTECTION_STATE_
// UNSPECIFIED is documented as equivalent to disabled, so requests carrying it
// are normalized rather than stored verbatim (the zero value must not read back
// as "unknown" on the wire).
const (
	deleteProtectionDisabled    = "DELETE_PROTECTION_DISABLED"
	deleteProtectionEnabled     = "DELETE_PROTECTION_ENABLED"
	deleteProtectionUnspecified = "DELETE_PROTECTION_STATE_UNSPECIFIED"
)

// normalizeDeleteProtection maps the documented zero-value equivalence onto
// DISABLED; every other value (including an unrecognized one) is left alone.
func normalizeDeleteProtection(state string) string {
	if state == "" || state == deleteProtectionUnspecified {
		return deleteProtectionDisabled
	}
	return state
}

// Database.pointInTimeRecoveryEnablement values that drive the OUTPUT_ONLY
// version_retention_period (7 days enabled, 1 hour otherwise).
const (
	pitrEnabled  = "POINT_IN_TIME_RECOVERY_ENABLED"
	pitrDisabled = "POINT_IN_TIME_RECOVERY_DISABLED"
)

// appEngineIntegrationDisabled is the documented create-time default for
// Database.app_engine_integration_mode, restored when an explicit `*` patch
// omits the field.
const appEngineIntegrationDisabled = "DISABLED"

// Database.databaseEdition values. STANDARD is the documented default when a
// create omits the field ("This is the default setting if not specified").
const (
	databaseEditionUnspecified = "DATABASE_EDITION_UNSPECIFIED"
	databaseEditionStandard    = "STANDARD"
	databaseEditionEnterprise  = "ENTERPRISE"
)

// Database.concurrency_mode values. A create that omits the mode defaults by
// edition: Optimistic for Enterprise and Pessimistic for every other database
// (database.proto).
const (
	concurrencyOptimistic  = "OPTIMISTIC"
	concurrencyPessimistic = "PESSIMISTIC"
)

// ttlConfigActive is the terminal Field.TtlConfig.State real GCP reports for a
// configured TTL policy. The field is OUTPUT_ONLY, so it is derived rather than
// stored from a request (DC49).
const ttlConfigActive = "ACTIVE"

// passwordAlphabet is the character set for a synthesized user-creds password.
const passwordAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// databaseDef is the persisted google.firestore.admin.v1.Database (the mutable
// subset the emulator models). Fields the emulator has no source for
// (cmek_config, source_info, ...) are omitted rather than fabricated;
// earliest_version_time is derived per response (EarliestVersionTime), never
// stored, because the proto documents it as continuously updated.
type databaseDef struct {
	Name                          string        `json:"name,omitempty"`
	Uid                           string        `json:"uid,omitempty"`
	CreateTime                    time.Time     `json:"createTime,omitempty"`
	UpdateTime                    time.Time     `json:"updateTime,omitempty"`
	DeleteTime                    time.Time     `json:"deleteTime,omitempty"`
	LocationID                    string        `json:"locationId,omitempty"`
	Type                          string        `json:"type,omitempty"`
	ConcurrencyMode               string        `json:"concurrencyMode,omitempty"`
	AppEngineIntegrationMode      string        `json:"appEngineIntegrationMode,omitempty"`
	PointInTimeRecoveryEnablement string        `json:"pointInTimeRecoveryEnablement,omitempty"`
	DeleteProtectionState         string        `json:"deleteProtectionState,omitempty"`
	DatabaseEdition               string        `json:"databaseEdition,omitempty"`
	KeyPrefix                     string        `json:"keyPrefix,omitempty"`
	VersionRetentionPeriod        time.Duration `json:"versionRetentionPeriod,omitempty"`
	Etag                          string        `json:"etag,omitempty"`
	Deleted                       bool          `json:"deleted,omitempty"`
}

// fieldDef is the persisted google.firestore.admin.v1.Field.
type fieldDef struct {
	Name        string            `json:"name,omitempty"`
	IndexConfig *fieldIndexConfig `json:"indexConfig,omitempty"`
	TtlConfig   *fieldTtlConfig   `json:"ttlConfig,omitempty"`
}

// fieldIndexConfig mirrors google.firestore.admin.v1.Field.IndexConfig.
type fieldIndexConfig struct {
	Indexes            []indexDef `json:"indexes,omitempty"`
	UsesAncestorConfig bool       `json:"usesAncestorConfig,omitempty"`
	AncestorField      string     `json:"ancestorField,omitempty"`
	Reverting          bool       `json:"reverting,omitempty"`
}

// fieldTtlConfig mirrors google.firestore.admin.v1.Field.TtlConfig.
type fieldTtlConfig struct {
	State            string        `json:"state,omitempty"`
	ExpirationOffset time.Duration `json:"expirationOffset,omitempty"`
}

// userCredsDef is the persisted google.firestore.admin.v1.UserCreds. The secret
// password is stored so a later reset can rotate it, but it is only ever
// surfaced on the create/reset responses (never on get/list/enable/disable), as
// real Firestore does.
type userCredsDef struct {
	Name           string    `json:"name,omitempty"`
	CreateTime     time.Time `json:"createTime,omitempty"`
	UpdateTime     time.Time `json:"updateTime,omitempty"`
	State          string    `json:"state,omitempty"`
	Principal      string    `json:"principal,omitempty"`
	SecretPassword string    `json:"secretPassword,omitempty"`
}

// backupScheduleDef is the persisted google.firestore.admin.v1.BackupSchedule.
// Recurrence is "DAILY" or "WEEKLY"; DayOfWeek is only meaningful for WEEKLY.
type backupScheduleDef struct {
	Name       string        `json:"name,omitempty"`
	CreateTime time.Time     `json:"createTime,omitempty"`
	UpdateTime time.Time     `json:"updateTime,omitempty"`
	Retention  time.Duration `json:"retention,omitempty"`
	Recurrence string        `json:"recurrence,omitempty"`
	DayOfWeek  int           `json:"dayOfWeek,omitempty"`
	// MaterializedThrough is the latest scheduled occurrence this emulator has
	// already turned into a Backup record (or skipped because it had aged out
	// of the retention window). It makes the lazy materialization idempotent
	// and stops a deleted backup from reappearing on the next read.
	MaterializedThrough time.Time `json:"materializedThrough,omitempty"`
}

// backupDef is the persisted google.firestore.admin.v1.Backup. Backups are
// read-only in the Admin API (a schedule produces them), so the emulator only
// supports get/list/delete over records; there is no create path.
type backupDef struct {
	Name          string    `json:"name,omitempty"`
	Database      string    `json:"database,omitempty"`
	DatabaseUid   string    `json:"databaseUid,omitempty"`
	SnapshotTime  time.Time `json:"snapshotTime,omitempty"`
	ExpireTime    time.Time `json:"expireTime,omitempty"`
	State         string    `json:"state,omitempty"`
	DocumentCount int64     `json:"documentCount,omitempty"`
	IndexCount    int64     `json:"indexCount,omitempty"`
	SizeBytes     int64     `json:"sizeBytes,omitempty"`
}

// ─── resource-name helpers ────────────────────────────────────────────────────

func databaseName(project, db string) string {
	return resource.ResourceID(project)("firestore-database", db)
}

func fieldName(project, db, cg, fieldPath string) string {
	return resource.ResourceID(project)("firestore-field", db+"/"+cg+"/"+fieldPath)
}

func userCredsName(project, db, id string) string {
	return resource.ResourceID(project)("firestore-user-creds", db+"/"+id)
}

func backupScheduleName(project, db, id string) string {
	return resource.ResourceID(project)("firestore-backup-schedule", db+"/"+id)
}

func backupName(project, loc, id string) string {
	return resource.ResourceID(project)("firestore-backup", loc+"/"+id)
}

func databaseOperationName(project, db, op string) string {
	return resource.ResourceID(project)("firestore-operation", db+"/"+op)
}

// resource-name store keys (relative to the project).

func databaseRel(db string) string { return "databases/" + db }

func fieldRel(db, cg, fieldPath string) string {
	return "databases/" + db + "/collectionGroups/" + cg + "/fields/" + fieldPath
}

func userCredsRel(db, id string) string { return "databases/" + db + "/userCreds/" + id }

func backupScheduleRel(db, id string) string {
	return "databases/" + db + "/backupSchedules/" + id
}

func backupRel(loc, id string) string { return "locations/" + loc + "/backups/" + id }

// ─── shared store helpers ─────────────────────────────────────────────────────

func gcpNotFound(what string) error {
	return model.NewProviderError("NotFound", what+" not found", 404)
}

func gcpAlreadyExists(what string) error {
	return model.NewProviderError("AlreadyExists", what+" already exists", 409)
}

func gcpInvalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func gcpAborted(msg string) error {
	return model.NewProviderError("Aborted", msg, 409)
}

// gcpFailedPrecondition is the documented precondition failure (HTTP 400 with
// the FAILED_PRECONDITION google.rpc status). It is deliberately separate from
// gcpAborted: AIP-154 etag mismatches on an update are ABORTED, but a method
// whose request etag documents a blocked operation (DeleteDatabase) answers
// FAILED_PRECONDITION.
func gcpFailedPrecondition(msg string) error {
	return model.NewProviderError("FailedPrecondition", msg, 400)
}

// putResource marshals v and creates (create=true) or upserts the store entry.
func (s *Service) putResource(ctx context.Context, project, rt, id string, v any, create bool) error {
	if s.resources == nil {
		return nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return model.NewProviderError("Internal", err.Error(), 500)
	}
	entry := store.ResourceEntry{Type: rt, ID: id, Data: data}
	if create {
		if err := s.resources.Create(ctx, project, store.GlobalRegion, entry); err != nil {
			if errors.Is(err, store.ErrAlreadyExists) {
				return gcpAlreadyExists(strings.TrimPrefix(rt, "firestore_"))
			}
			return err
		}
		return nil
	}
	return s.resources.Upsert(ctx, project, store.GlobalRegion, entry)
}

// getResource loads and unmarshals a store entry into v.
func (s *Service) getResource(ctx context.Context, project, rt, id string, v any) error {
	if s.resources == nil {
		return gcpNotFound(strings.TrimPrefix(rt, "firestore_"))
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rt, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return gcpNotFound(strings.TrimPrefix(rt, "firestore_"))
		}
		return err
	}
	if err := json.Unmarshal(e.Data, v); err != nil {
		return model.NewProviderError("Internal", err.Error(), 500)
	}
	return nil
}

// listResources lists all entries of a type under a prefix, unmarshalling each
// as an independent item with decode.
func listResources[T any](ctx context.Context, s *Service, project, rt, prefix string) ([]T, error) {
	var out []T
	if s.resources == nil {
		return out, nil
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rt, prefix)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		var v T
		if json.Unmarshal(e.Data, &v) == nil {
			out = append(out, v)
		}
	}
	return out, nil
}

// randomPassword returns a fresh user-creds password.
func randomPassword() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", 24)
	}
	out := make([]byte, len(b))
	for i, c := range b {
		out[i] = passwordAlphabet[int(c)%len(passwordAlphabet)]
	}
	return string(out)
}

// randomUUID4 returns a random UUID4, the documented shape of Database.uid
// ("The system-generated UUID4 for this Database").
func randomUUID4() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return randomHex(32)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// databaseRetentionPeriod returns the documented version-retention window for a
// database. version_retention_period is OUTPUT_ONLY
// (google/firestore/admin/v1/database.proto): real GCP derives it from the PITR
// toggle — 7 days when PITR is enabled, 1 hour otherwise — and never accepts a
// client-supplied value, so every write path recomputes it instead of storing
// whatever a request body carried.
func databaseRetentionPeriod(pitrEnablement string) time.Duration {
	if pitrEnablement == pitrEnabled {
		return 7 * 24 * time.Hour
	}
	return time.Hour
}

// defaultConcurrencyMode is the documented create-time concurrency_mode default
// for an edition: "Optimistic for Enterprise and Pessimistic for all other
// databases" (database.proto).
func defaultConcurrencyMode(edition string) string {
	if edition == databaseEditionEnterprise {
		return concurrencyOptimistic
	}
	return concurrencyPessimistic
}

// EarliestVersionTime is the documented read window for past versions:
// "populated with `now - version_retention_period`" (database.proto). The field
// is OUTPUT_ONLY and continuously updated, so it is derived per response rather
// than persisted.
func EarliestVersionTime(d DatabaseDef) time.Time {
	retention := d.VersionRetentionPeriod
	if retention <= 0 {
		retention = databaseRetentionPeriod(d.PointInTimeRecoveryEnablement)
	}
	return clock.Now().Add(-retention)
}

// normalizeDatabase applies the documented zero-value equivalences and the
// OUTPUT_ONLY fields to a stored record, also repairing records written by an
// older build (a pre-DC38 record may carry a client-supplied retention window).
func normalizeDatabase(rec databaseDef) databaseDef {
	rec.DeleteProtectionState = normalizeDeleteProtection(rec.DeleteProtectionState)
	rec.VersionRetentionPeriod = databaseRetentionPeriod(rec.PointInTimeRecoveryEnablement)
	return rec
}

// newDefaultDatabase builds the implicit (default) database every Firestore
// project has. It is shaped like a named database — a real UUID4 uid, create and
// update timestamps and an etag — so the output fields are present and the etag
// is a usable delete precondition (DC46).
func newDefaultDatabase(project string) databaseDef {
	now := clock.Now()
	return databaseDef{
		Name:                          databaseName(project, defaultDatabaseID),
		Uid:                           randomUUID4(),
		CreateTime:                    now,
		UpdateTime:                    now,
		Type:                          "FIRESTORE_NATIVE",
		ConcurrencyMode:               defaultConcurrencyMode(databaseEditionStandard),
		VersionRetentionPeriod:        databaseRetentionPeriod(pitrDisabled),
		PointInTimeRecoveryEnablement: pitrDisabled,
		DeleteProtectionState:         deleteProtectionDisabled,
		DatabaseEdition:               databaseEditionStandard,
		Etag:                          randomHex(16),
	}
}

// ─── Databases ────────────────────────────────────────────────────────────────

// databaseIDRe mirrors real Firestore's database-id constraint:
// 4-63 characters, starting with a lowercase letter, lowercase letters/digits/
// hyphens.
var databaseIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]{3,62}$`)

// CreateDatabaseDef creates a database metadata record and returns it with the
// name of the terminal long-running operation wrapping it.
func (s *Service) CreateDatabaseDef(ctx context.Context, project, dbID string, db databaseDef) (databaseDef, string, error) {
	if dbID == "" {
		return databaseDef{}, "", gcpInvalidArgument("database id is required")
	}
	if dbID == defaultDatabaseID {
		return databaseDef{}, "", gcpAlreadyExists("database")
	}
	if !databaseIDRe.MatchString(dbID) {
		return databaseDef{}, "", gcpInvalidArgument("database id must be 4-63 chars, start with a lowercase letter, and contain only lowercase letters, digits, or hyphens")
	}
	db.Name = databaseName(project, dbID)
	db.Uid = randomUUID4()
	now := clock.Now()
	db.CreateTime, db.UpdateTime = now, now
	if db.Type == "" {
		db.Type = "FIRESTORE_NATIVE"
	}
	if db.DatabaseEdition == "" || db.DatabaseEdition == databaseEditionUnspecified {
		// database.proto: STANDARD "is the default setting if not specified".
		db.DatabaseEdition = databaseEditionStandard
	}
	if db.ConcurrencyMode == "" {
		// database.proto: "If unspecified in a CreateDatabase request, this will
		// default based on the database edition: Optimistic for Enterprise and
		// Pessimistic for all other databases."
		db.ConcurrencyMode = defaultConcurrencyMode(db.DatabaseEdition)
	}
	// version_retention_period is OUTPUT_ONLY: derive it from the requested PITR
	// toggle rather than accepting the value in the create body. key_prefix is
	// OUTPUT_ONLY as well, so a supplied value is not stored either.
	db.VersionRetentionPeriod = databaseRetentionPeriod(db.PointInTimeRecoveryEnablement)
	db.KeyPrefix = ""
	db.DeleteProtectionState = normalizeDeleteProtection(db.DeleteProtectionState)
	db.Etag = randomHex(16)
	if err := s.putResource(ctx, project, rtDatabase, databaseRel(dbID), db, true); err != nil {
		return databaseDef{}, "", err
	}
	return db, databaseOperationName(project, dbID, randomHex(12)), nil
}

// GetDatabase returns a database record. The implicit (default) database is
// materialized on first access (see defaultDatabase).
func (s *Service) GetDatabase(ctx context.Context, project, db string) (databaseDef, error) {
	if db == defaultDatabaseID {
		return s.defaultDatabase(ctx, project)
	}
	var rec databaseDef
	if s.resources == nil {
		return databaseDef{}, gcpNotFound("database")
	}
	err := s.getResource(ctx, project, rtDatabase, databaseRel(db), &rec)
	switch {
	case err == nil:
		if rec.Deleted {
			return databaseDef{}, gcpNotFound("database")
		}
		return normalizeDatabase(rec), nil
	case !isNotFound(err):
		return databaseDef{}, err
	}
	return databaseDef{}, gcpNotFound("database")
}

// defaultDatabase returns the implicit (default) database, materializing it on
// first access. Real GCP's (default) database is a real resource carrying a uid,
// timestamps and an etag, so the record must be stable across reads: a
// per-call synthesized record would change those OUTPUT_ONLY fields every time
// and could never satisfy the documented delete etag precondition (DC46). A
// tombstoned (default) is NotFound, like any deleted database.
func (s *Service) defaultDatabase(ctx context.Context, project string) (databaseDef, error) {
	if s.resources == nil {
		return newDefaultDatabase(project), nil
	}
	// The steady state is a plain read: materializing on every GET would turn the
	// most common admin call into a write.
	var rec databaseDef
	switch err := s.getResource(ctx, project, rtDatabase, databaseRel(defaultDatabaseID), &rec); {
	case err == nil:
		if rec.Deleted {
			return databaseDef{}, gcpNotFound("database")
		}
		if !needsDefaultDatabaseRepair(rec) {
			return normalizeDatabase(rec), nil
		}
	case !isNotFound(err):
		return databaseDef{}, err
	}
	// First access, or a record whose output fields a pre-DC46 build left empty:
	// create or repair it atomically, so two concurrent readers agree on one
	// uid/etag.
	var out databaseDef
	_, err := s.resources.UpsertAtomic(ctx, project, store.GlobalRegion, rtDatabase, databaseRel(defaultDatabaseID),
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			var created databaseDef
			if exists {
				if err := json.Unmarshal(current.Data, &created); err != nil {
					return current, model.NewProviderError("Internal", err.Error(), 500)
				}
				if created.Deleted {
					return current, gcpNotFound("database")
				}
				if !needsDefaultDatabaseRepair(created) {
					// Another reader materialized it between our read and here.
					out = normalizeDatabase(created)
					return current, nil
				}
				repairDefaultDatabase(&created)
			} else {
				created = newDefaultDatabase(project)
			}
			data, err := json.Marshal(created)
			if err != nil {
				return current, model.NewProviderError("Internal", err.Error(), 500)
			}
			out = normalizeDatabase(created)
			return store.ResourceEntry{Type: rtDatabase, ID: databaseRel(defaultDatabaseID), Data: data}, nil
		})
	if err != nil {
		return databaseDef{}, err
	}
	return out, nil
}

// needsDefaultDatabaseRepair reports whether a persisted (default) record was
// written by a build that synthesized it without output fields (a record is only
// persisted on an update/delete there): the placeholder uid and the missing
// timestamps/etag are the markers (DC46).
func needsDefaultDatabaseRepair(rec databaseDef) bool {
	return rec.Uid == "" || rec.Uid == "default" ||
		rec.CreateTime.IsZero() || rec.UpdateTime.IsZero() || rec.Etag == ""
}

// repairDefaultDatabase fills the output fields a legacy synthesized (default)
// record lacks, so the uid/etag a client reads are stable and the etag is a
// usable delete precondition. concurrency_mode is left as stored: a client may
// legitimately have patched the legacy record's mode.
func repairDefaultDatabase(rec *databaseDef) {
	if rec.Uid == "" || rec.Uid == "default" {
		rec.Uid = randomUUID4()
	}
	now := clock.Now()
	if rec.CreateTime.IsZero() {
		rec.CreateTime = now
	}
	if rec.UpdateTime.IsZero() {
		rec.UpdateTime = rec.CreateTime
	}
	if rec.Etag == "" {
		rec.Etag = randomHex(16)
	}
}

// ListDatabases returns every database in the project, including the implicit
// (default) database. Deleted records are only included when showDeleted is set.
func (s *Service) ListDatabases(ctx context.Context, project string, showDeleted bool) ([]databaseDef, error) {
	recs, err := listResources[databaseDef](ctx, s, project, rtDatabase, "databases/")
	if err != nil {
		return nil, err
	}
	out := make([]databaseDef, 0, len(recs)+1)
	haveDefault := false
	for _, r := range recs {
		if r.Name == databaseName(project, defaultDatabaseID) {
			haveDefault = true
		}
		if r.Deleted && !showDeleted {
			continue
		}
		out = append(out, normalizeDatabase(r))
	}
	if !haveDefault {
		// Materializing the default is the one write a list performs. If it is
		// deleted between the list and this call, report the remaining databases
		// rather than failing the LIST with NOT_FOUND.
		def, err := s.defaultDatabase(ctx, project)
		switch {
		case err == nil:
			out = append(out, def)
		case !isNotFound(err):
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// impliedMask applies AIP-134's omitted-field-mask rule: a request that carries
// no update_mask has an implied mask of every populated field. Neither the
// Firestore admin request protos nor the Discovery document marks any
// Update*Request.update_mask REQUIRED, so a maskless patch must apply the
// populated fields instead of being rejected.
//
// A *present-but-empty* mask is a different state: AIP-134 says the service
// "should treat the request as a no-op and return the existing resource
// unmodified". The transports distinguish the two (proto message presence /
// REST query-parameter presence) and hand the distinction in as maskPresent;
// an empty result here means the no-op, which the callers below honour by not
// applying any field and not rotating etag/update_time.
//
// This is populated-field semantics, not the full replacement AIP-134 assigns to
// an explicit `*`: the `*` arm serves both the implied and the explicit mask
// (an explicit `*` keeps unpopulated fields) because the transports cannot tell
// them apart. An explicit `*` meaning replacement is recorded as DC50.
func impliedMask(mask []string, maskPresent bool) []string {
	if !maskPresent {
		return []string{"*"}
	}
	return mask
}

// replacesResource reports whether the request carried an explicit `*` mask.
// An omitted mask becomes `*` through impliedMask but keeps maskPresent false,
// and AIP-134 gives the two different meanings: implied `*` is "every populated
// field" (a merge), an explicit `*` is a full replacement (unlisted mutable
// fields are cleared). The transports pass presence (proto oneof / REST
// query-parameter presence), so this is decidable here (DC37).
func replacesResource(mask []string, maskPresent bool) bool {
	return maskPresent && len(mask) == 1 && mask[0] == "*"
}

// applyDatabaseReplace implements an explicit `*` patch: every mutable field
// takes the request value, falling back to its documented default when the
// request omits it. Output-only fields (name, uid, times, etag, key_prefix,
// version_retention_period) and immutable ones (location_id, type,
// database_edition) stay under the server's control.
func applyDatabaseReplace(cur *databaseDef, patch databaseDef) {
	cur.ConcurrencyMode = patch.ConcurrencyMode
	if cur.ConcurrencyMode == "" {
		cur.ConcurrencyMode = defaultConcurrencyMode(cur.DatabaseEdition)
	}
	cur.AppEngineIntegrationMode = patch.AppEngineIntegrationMode
	if cur.AppEngineIntegrationMode == "" {
		cur.AppEngineIntegrationMode = appEngineIntegrationDisabled
	}
	cur.PointInTimeRecoveryEnablement = patch.PointInTimeRecoveryEnablement
	if cur.PointInTimeRecoveryEnablement == "" {
		cur.PointInTimeRecoveryEnablement = pitrDisabled
	}
	cur.DeleteProtectionState = normalizeDeleteProtection(patch.DeleteProtectionState)
}

// UpdateDatabaseDef applies patch to the database record under mask (field
// paths, or "*" for all), honoring an optional etag for optimistic concurrency.
// The read-check-write runs inside the store's atomic UpsertAtomic so two
// concurrent updates cannot lose one another. A present-but-empty mask is a
// no-op: the record is returned unchanged (no etag/update_time rotation), while
// an etag precondition still applies to the request.
//
// The server-owned fields follow their proto annotations: an OUTPUT_ONLY field
// (key_prefix, version_retention_period) in the mask is ignored as AIP-161
// requires, but a masked write that would *change* the IMMUTABLE database_edition
// is INVALID_ARGUMENT (AIP-203); resending the current value is a no-op.
func (s *Service) UpdateDatabaseDef(ctx context.Context, project, db string, patch databaseDef, mask []string, maskPresent bool, etag string) (databaseDef, string, error) {
	replace := replacesResource(mask, maskPresent)
	mask = impliedMask(mask, maskPresent)
	if db == "" {
		return databaseDef{}, "", gcpInvalidArgument("database name is required")
	}
	// An explicit mask only covers the fields it names, so a body value outside
	// it is irrelevant (AIP-134); `*` covers every populated field.
	editionMasked := false
	for _, path := range mask {
		if path == "*" || path == "database_edition" {
			editionMasked = true
			break
		}
	}
	updated, err := s.mutateDatabase(ctx, project, db, func(cur databaseDef) (databaseDef, error) {
		if etag != "" && etag != cur.Etag {
			return databaseDef{}, gcpAborted("etag mismatch")
		}
		if len(mask) == 0 {
			return cur, nil
		}
		if editionMasked && patch.DatabaseEdition != "" && patch.DatabaseEdition != cur.DatabaseEdition {
			return databaseDef{}, gcpInvalidArgument("database_edition is immutable and cannot be changed")
		}
		if replace {
			applyDatabaseReplace(&cur, patch)
		} else {
			for _, path := range mask {
				applyDatabaseField(&cur, path, patch)
			}
		}
		// version_retention_period is OUTPUT_ONLY and derived from the effective
		// PITR toggle, never from the request: real GCP reports 7 days once PITR
		// is enabled and 1 hour otherwise.
		cur.VersionRetentionPeriod = databaseRetentionPeriod(cur.PointInTimeRecoveryEnablement)
		cur.UpdateTime = clock.Now()
		cur.Etag = randomHex(16)
		cur.Deleted = false
		return cur, nil
	})
	if err != nil {
		return databaseDef{}, "", err
	}
	return updated, databaseOperationName(project, db, randomHex(12)), nil
}

// mutateDatabase runs load → mutate → store for a database inside the store's
// atomic read-modify-write, so etag/CAS checks cannot race. A deleted record is
// reported NotFound; the implicit (default) database is synthesized when it has
// no record. When no store is attached (unit tests) the mutation runs against
// the synthesized/served record without persisting.
func (s *Service) mutateDatabase(ctx context.Context, project, db string, mutate func(databaseDef) (databaseDef, error)) (databaseDef, error) {
	if s.resources == nil {
		cur, err := s.GetDatabase(ctx, project, db)
		if err != nil {
			return databaseDef{}, err
		}
		return mutate(cur)
	}
	var result databaseDef
	_, err := s.resources.UpsertAtomic(ctx, project, store.GlobalRegion, rtDatabase, databaseRel(db),
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			var cur databaseDef
			switch {
			case exists:
				if err := json.Unmarshal(current.Data, &cur); err != nil {
					return current, model.NewProviderError("Internal", err.Error(), 500)
				}
				if cur.Deleted {
					return current, gcpNotFound("database")
				}
				cur = normalizeDatabase(cur)
			case db == defaultDatabaseID:
				cur = normalizeDatabase(newDefaultDatabase(project))
			default:
				return current, gcpNotFound("database")
			}
			next, err := mutate(cur)
			if err != nil {
				return current, err
			}
			data, err := json.Marshal(next)
			if err != nil {
				return current, model.NewProviderError("Internal", err.Error(), 500)
			}
			result = next
			return store.ResourceEntry{Type: rtDatabase, ID: databaseRel(db), Data: data}, nil
		})
	if err != nil {
		return databaseDef{}, err
	}
	return result, nil
}

// mutateOne runs load → mutate → store for one record inside the store's atomic
// read-modify-write, so two concurrent patches cannot lose one another (DC51) —
// the same guarantee mutateDatabase gives the database resource. create, when
// non-nil, supplies the record for a missing entry (the Firestore Admin field
// patch upserts, the backup-schedule patch does not); mutate reports whether the
// record changed, so a present-but-empty mask can return the stored record
// unmodified instead of rewriting its data.
func mutateOne[T any](ctx context.Context, s *Service, project, rt, id string, create func() T, mutate func(cur T, exists bool) (T, bool, error)) (T, error) {
	var zero T
	what := strings.TrimPrefix(rt, "firestore_")
	if s.resources == nil {
		if create == nil {
			return zero, gcpNotFound(what)
		}
		next, _, err := mutate(create(), false)
		return next, err
	}
	var out T
	_, err := s.resources.UpsertAtomic(ctx, project, store.GlobalRegion, rt, id,
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			var cur T
			switch {
			case exists:
				if err := json.Unmarshal(current.Data, &cur); err != nil {
					return current, model.NewProviderError("Internal", err.Error(), 500)
				}
			case create != nil:
				cur = create()
			default:
				return current, gcpNotFound(what)
			}
			next, write, err := mutate(cur, exists)
			if err != nil {
				return current, err
			}
			out = next
			if !write {
				return current, nil
			}
			data, err := json.Marshal(next)
			if err != nil {
				return current, model.NewProviderError("Internal", err.Error(), 500)
			}
			return store.ResourceEntry{Type: rt, ID: id, Data: data}, nil
		})
	if err != nil {
		return zero, err
	}
	return out, nil
}

// applyDatabaseField copies one mutable Database field (by snake_case proto
// field path) from patch into cur. Unknown paths are ignored so a mask carrying
// a field the emulator does not model does not fail the update.
//
// The server-owned fields are deliberately absent: key_prefix and
// version_retention_period are OUTPUT_ONLY and database_edition is IMMUTABLE
// (database.proto), so a request that carries one never writes it. The
// retention window is re-derived from the PITR toggle by the caller.
func applyDatabaseField(cur *databaseDef, path string, patch databaseDef) {
	switch path {
	case "*":
		// Apply the mutable fields only; the output-only fields (name, uid,
		// create/update/delete time, etag, key_prefix, version_retention_period)
		// and the immutable database_edition stay under the server's control.
		if patch.LocationID != "" {
			cur.LocationID = patch.LocationID
		}
		if patch.Type != "" {
			cur.Type = patch.Type
		}
		if patch.ConcurrencyMode != "" {
			cur.ConcurrencyMode = patch.ConcurrencyMode
		}
		if patch.AppEngineIntegrationMode != "" {
			cur.AppEngineIntegrationMode = patch.AppEngineIntegrationMode
		}
		if patch.PointInTimeRecoveryEnablement != "" {
			cur.PointInTimeRecoveryEnablement = patch.PointInTimeRecoveryEnablement
		}
		if patch.DeleteProtectionState != "" {
			cur.DeleteProtectionState = normalizeDeleteProtection(patch.DeleteProtectionState)
		}
	case "concurrency_mode":
		cur.ConcurrencyMode = patch.ConcurrencyMode
	case "app_engine_integration_mode":
		cur.AppEngineIntegrationMode = patch.AppEngineIntegrationMode
	case "point_in_time_recovery_enablement":
		cur.PointInTimeRecoveryEnablement = patch.PointInTimeRecoveryEnablement
	case "delete_protection_state":
		cur.DeleteProtectionState = normalizeDeleteProtection(patch.DeleteProtectionState)
	case "location_id":
		cur.LocationID = patch.LocationID
	case "type":
		cur.Type = patch.Type
	}
}

// DeleteDatabaseDef soft-deletes a database record (or tombstones the implicit
// default) and returns the soft-deleted record plus the terminal operation
// name. The record is the operation's documented response_type "Database"; real
// GCP returns it (deleteTime set) so the official client's
// DeleteDatabaseOperation.Wait resolves.
//
// DeleteDatabaseRequest.etag documents that a mismatch blocks the delete and
// returns FAILED_PRECONDITION (the proto and the Discovery document both say
// so), unlike an update's AIP-154 ABORTED.
//
// Delete protection is also a FAILED_PRECONDITION: Firestore documents that a
// database with delete_protection_state DELETE_PROTECTION_ENABLED "cannot be
// deleted ... until you disable delete protection", so the record is left
// intact until the protection is patched off.
func (s *Service) DeleteDatabaseDef(ctx context.Context, project, db, etag string) (databaseDef, string, error) {
	deleted, err := s.mutateDatabase(ctx, project, db, func(cur databaseDef) (databaseDef, error) {
		if etag != "" && etag != cur.Etag {
			return databaseDef{}, gcpFailedPrecondition("etag mismatch: the database changed since the etag was read")
		}
		if cur.DeleteProtectionState == deleteProtectionEnabled {
			return databaseDef{}, gcpFailedPrecondition("database has delete protection enabled: disable delete protection before deleting")
		}
		cur.Deleted = true
		cur.DeleteTime = clock.Now()
		cur.UpdateTime = cur.DeleteTime
		cur.Etag = randomHex(16)
		// The soft-deleted record is the operation's response_type "Database",
		// so it carries the same derived OUTPUT_ONLY retention window a read
		// returns (a pre-fix persisted record may hold a client-supplied one).
		cur.VersionRetentionPeriod = databaseRetentionPeriod(cur.PointInTimeRecoveryEnablement)
		return cur, nil
	})
	if err != nil {
		return databaseDef{}, "", err
	}
	return deleted, databaseOperationName(project, db, randomHex(12)), nil
}

// ─── Fields ───────────────────────────────────────────────────────────────────

// GetField returns a collection-group field record.
func (s *Service) GetField(ctx context.Context, project, db, cg, fieldPath string) (fieldDef, error) {
	if fieldPath == "" {
		return fieldDef{}, gcpInvalidArgument("field path is required")
	}
	var f fieldDef
	if err := s.getResource(ctx, project, rtField, fieldRel(db, cg, fieldPath), &f); err != nil {
		return fieldDef{}, err
	}
	deriveFieldServerFields(project, db, cg, &f)
	return f, nil
}

// deriveFieldServerFields fills the Field members the proto marks OUTPUT_ONLY
// from what the collection-group configuration implies, rather than from a
// request (DC49):
//
//   - uses_ancestor_config / ancestor_field: a field inherits its single-field
//     index configuration from its collection group's __name__ field. The
//     emulator does not model single-field overrides, so every field reports the
//     inherited form, and ancestor_field names the __name__ field (the proto
//     populates it in both forms). One documented form is therefore not
//     distinguishable: "to explicitly remove all indexes for this field, specify
//     an index config with an empty list of indexes" (Field.index_config) would
//     read back as the inherited form rather than as an explicit empty config.
//   - reverting: only true while a revert is in progress; the synchronous
//     control plane never runs one.
//   - ttl_config.state: a configured TTL policy is ACTIVE (CREATING and
//     NEEDS_REPAIR are transitional states the emulator has no source for).
func deriveFieldServerFields(project, db, cg string, f *fieldDef) {
	if f.IndexConfig != nil {
		f.IndexConfig.UsesAncestorConfig = true
		f.IndexConfig.AncestorField = fieldName(project, db, cg, "__name__")
		f.IndexConfig.Reverting = false
	}
	if f.TtlConfig != nil {
		f.TtlConfig.State = ttlConfigActive
	}
}

// collectionGroupOf extracts the collection group from a stored field resource
// name ("projects/{p}/databases/{db}/collectionGroups/{cg}/fields/{path}").
func collectionGroupOf(name string) string {
	parts := strings.Split(name, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "collectionGroups" {
			return parts[i+1]
		}
	}
	return ""
}

// ListFields returns the configured collection-group fields for a database,
// optionally scoped to one collection group (cg == "-" is the wildcard) and
// filtered by a substring of the field name, honoring page_size/page_token.
func (s *Service) ListFields(ctx context.Context, project, db, cg, filter string, page pageParams) ([]fieldDef, string, error) {
	prefix := "databases/" + db + "/collectionGroups/"
	if cg != "" && cg != "-" {
		prefix = "databases/" + db + "/collectionGroups/" + cg + "/fields/"
	}
	fields, err := listResources[fieldDef](ctx, s, project, rtField, prefix)
	if err != nil {
		return nil, "", err
	}
	for i := range fields {
		deriveFieldServerFields(project, db, collectionGroupOf(fields[i].Name), &fields[i])
	}
	if filter != "" {
		kept := fields[:0]
		for _, f := range fields {
			if fieldMatchesFilter(f, filter) {
				kept = append(kept, f)
			}
		}
		fields = kept
	}
	pageFields, nextToken, err := paging.Page(fields, func(f fieldDef) string { return f.Name }, page.params())
	if err != nil {
		return nil, "", err
	}
	return pageFields, nextToken, nil
}

// fieldMatchesFilter evaluates the subset of the documented ListFields filter
// expressions the emulator models — conjunctive (" AND ") clauses over
// indexConfig / ttlConfig — and falls back to a substring match on the field
// name for an unrecognized clause.
func fieldMatchesFilter(f fieldDef, filter string) bool {
	for _, clause := range strings.Split(filter, " AND ") {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		switch {
		case strings.HasPrefix(clause, "ttlConfig"):
			if f.TtlConfig == nil {
				return false
			}
		case strings.Contains(clause, "usesAncestorConfig:false"):
			// The emulator derives uses_ancestor_config=true for every field (it
			// models no single-field index overrides), so this arm matches
			// nothing today; it is kept because the filter is documented and a
			// future override surface would populate the false form.
			if f.IndexConfig == nil || f.IndexConfig.UsesAncestorConfig {
				return false
			}
		case strings.Contains(clause, "usesAncestorConfig:true"):
			if f.IndexConfig == nil || !f.IndexConfig.UsesAncestorConfig {
				return false
			}
		default:
			if !strings.Contains(f.Name, clause) {
				return false
			}
		}
	}
	return true
}

// UpdateFieldDef creates or updates a field's index/ttl configuration under
// mask, returning the stored field and the terminal operation name. A
// present-but-empty mask is a no-op and does not write the record. The
// read-modify-write runs inside the store's atomic primitive so two concurrent
// patches cannot lose one another (DC51), and the OUTPUT_ONLY config members are
// re-derived from the collection-group configuration (DC49).
func (s *Service) UpdateFieldDef(ctx context.Context, project, db, cg, fieldPath string, patch fieldDef, mask []string, maskPresent bool) (fieldDef, string, error) {
	if fieldPath == "" {
		return fieldDef{}, "", gcpInvalidArgument("field path is required")
	}
	// An omitted mask is the implied mask of every populated field (AIP-134).
	replace := replacesResource(mask, maskPresent)
	mask = impliedMask(mask, maskPresent)
	name := fieldName(project, db, cg, fieldPath)
	opName := databaseOperationName(project, db, randomHex(12))
	cur, err := mutateOne(ctx, s, project, rtField, fieldRel(db, cg, fieldPath),
		func() fieldDef { return fieldDef{Name: name} },
		func(cur fieldDef, exists bool) (fieldDef, bool, error) {
			cur.Name = name
			if len(mask) == 0 {
				// Present-but-empty mask: no-op (AIP-134), no write. A populated
				// patch upserts a missing field, but a no-op cannot fabricate
				// one: report the miss instead of returning a field that was
				// never stored. The response is still shaped like a read, so a
				// legacy record does not answer differently here than on GET.
				if !exists {
					return fieldDef{}, false, gcpNotFound("field")
				}
				deriveFieldServerFields(project, db, cg, &cur)
				return cur, false, nil
			}
			if replace {
				applyFieldReplace(&cur, patch)
			} else {
				for _, path := range mask {
					applyFieldMask(&cur, path, patch)
				}
			}
			deriveFieldServerFields(project, db, cg, &cur)
			return cur, true, nil
		})
	if err != nil {
		return fieldDef{}, "", err
	}
	return cur, opName, nil
}

// applyFieldMask copies the writable Field configuration named by a proto field
// path. The OUTPUT_ONLY members — index_config.uses_ancestor_config,
// ancestor_field, reverting and ttl_config.state — have no arm: a mask naming one
// is ignored, as AIP-161 requires, and the values are derived by
// deriveFieldServerFields from the collection-group configuration (DC49).
func applyFieldMask(cur *fieldDef, path string, patch fieldDef) {
	switch path {
	case "*":
		// The `*` arm is the populated-field mask: it must carry both configs,
		// not only index_config.
		if patch.IndexConfig != nil {
			cur.IndexConfig = patch.IndexConfig
		}
		if patch.TtlConfig != nil {
			cur.TtlConfig = patch.TtlConfig
		}
	case "index_config":
		if patch.IndexConfig != nil {
			cur.IndexConfig = patch.IndexConfig
		}
	case "ttl_config":
		if patch.TtlConfig != nil {
			cur.TtlConfig = patch.TtlConfig
		}
	case "index_config.indexes":
		if patch.IndexConfig != nil {
			cur.IndexConfig = &fieldIndexConfig{Indexes: patch.IndexConfig.Indexes}
		}
	case "ttl_config.expiration_offset":
		if patch.TtlConfig != nil {
			ensureTtlConfig(cur)
			cur.TtlConfig.ExpirationOffset = patch.TtlConfig.ExpirationOffset
		}
	}
}

// applyFieldReplace implements an explicit `*` patch: both writable configs
// take the request value, so a `*` patch that omits ttl_config clears a stored
// TTL policy instead of keeping it (AIP-134 replace). Server-derived members
// (uses_ancestor_config, ancestor_field, reverting, ttl_config.state) are
// recomputed afterwards by deriveFieldServerFields.
func applyFieldReplace(cur *fieldDef, patch fieldDef) {
	cur.IndexConfig = patch.IndexConfig
	cur.TtlConfig = patch.TtlConfig
}

func ensureTtlConfig(f *fieldDef) {
	if f.TtlConfig == nil {
		f.TtlConfig = &fieldTtlConfig{}
	}
}

// ─── User creds ───────────────────────────────────────────────────────────────

// CreateUserCredsDef creates a user creds record and returns it with a
// freshly-minted secret password.
func (s *Service) CreateUserCredsDef(ctx context.Context, project, db, id string, uc userCredsDef) (userCredsDef, error) {
	if id == "" {
		id = randomHex(20)
	}
	uc.Name = userCredsName(project, db, id)
	now := clock.Now()
	uc.CreateTime, uc.UpdateTime = now, now
	if uc.State == "" {
		uc.State = "ENABLED"
	}
	uc.Principal = id + "@" + project + ".iam.gserviceaccount.com"
	uc.SecretPassword = randomPassword()
	if err := s.putResource(ctx, project, rtUserCreds, userCredsRel(db, id), uc, true); err != nil {
		return userCredsDef{}, err
	}
	return uc, nil
}

// GetUserCreds returns a user creds record without its secret password.
func (s *Service) GetUserCreds(ctx context.Context, project, db, id string) (userCredsDef, error) {
	var uc userCredsDef
	if err := s.getResource(ctx, project, rtUserCreds, userCredsRel(db, id), &uc); err != nil {
		return userCredsDef{}, err
	}
	uc.SecretPassword = ""
	return uc, nil
}

// ListUserCreds returns every user creds record in a database, without secrets.
func (s *Service) ListUserCreds(ctx context.Context, project, db string) ([]userCredsDef, error) {
	ucs, err := listResources[userCredsDef](ctx, s, project, rtUserCreds, "databases/"+db+"/userCreds/")
	if err != nil {
		return nil, err
	}
	for i := range ucs {
		ucs[i].SecretPassword = ""
	}
	sort.Slice(ucs, func(i, j int) bool { return ucs[i].Name < ucs[j].Name })
	return ucs, nil
}

// SetUserCredsState enables or disables a user creds record.
func (s *Service) SetUserCredsState(ctx context.Context, project, db, id, state string) (userCredsDef, error) {
	var uc userCredsDef
	if err := s.getResource(ctx, project, rtUserCreds, userCredsRel(db, id), &uc); err != nil {
		return userCredsDef{}, err
	}
	uc.State = state
	uc.UpdateTime = clock.Now()
	if err := s.putResource(ctx, project, rtUserCreds, userCredsRel(db, id), uc, false); err != nil {
		return userCredsDef{}, err
	}
	uc.SecretPassword = ""
	return uc, nil
}

// ResetUserPassword rotates a user creds secret and returns it once.
func (s *Service) ResetUserPassword(ctx context.Context, project, db, id string) (userCredsDef, error) {
	var uc userCredsDef
	if err := s.getResource(ctx, project, rtUserCreds, userCredsRel(db, id), &uc); err != nil {
		return userCredsDef{}, err
	}
	uc.SecretPassword = randomPassword()
	uc.UpdateTime = clock.Now()
	if err := s.putResource(ctx, project, rtUserCreds, userCredsRel(db, id), uc, false); err != nil {
		return userCredsDef{}, err
	}
	return uc, nil
}

// DeleteUserCreds removes a user creds record.
func (s *Service) DeleteUserCreds(ctx context.Context, project, db, id string) error {
	if s.resources == nil {
		return nil
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtUserCreds, userCredsRel(db, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return gcpNotFound("user creds")
		}
		return err
	}
	return nil
}

// ─── Backup schedules ─────────────────────────────────────────────────────────

// CreateBackupScheduleDef creates a backup schedule record.
func (s *Service) CreateBackupScheduleDef(ctx context.Context, project, db, id string, bs backupScheduleDef) (backupScheduleDef, error) {
	if id == "" {
		id = "1"
	}
	bs.Name = backupScheduleName(project, db, id)
	now := clock.Now()
	bs.CreateTime, bs.UpdateTime = now, now
	if err := s.putResource(ctx, project, rtBackupSchedule, backupScheduleRel(db, id), bs, true); err != nil {
		return backupScheduleDef{}, err
	}
	return bs, nil
}

// GetBackupSchedule returns a backup schedule record.
func (s *Service) GetBackupSchedule(ctx context.Context, project, db, id string) (backupScheduleDef, error) {
	var bs backupScheduleDef
	if err := s.getResource(ctx, project, rtBackupSchedule, backupScheduleRel(db, id), &bs); err != nil {
		return backupScheduleDef{}, err
	}
	return bs, nil
}

// ListBackupSchedules returns every backup schedule for a database.
func (s *Service) ListBackupSchedules(ctx context.Context, project, db string) ([]backupScheduleDef, error) {
	bss, err := listResources[backupScheduleDef](ctx, s, project, rtBackupSchedule, "databases/"+db+"/backupSchedules/")
	if err != nil {
		return nil, err
	}
	sort.Slice(bss, func(i, j int) bool { return bss[i].Name < bss[j].Name })
	return bss, nil
}

// UpdateBackupScheduleDef applies patch to a backup schedule under mask. A
// present-but-empty mask is a no-op and does not write the record. The
// read-modify-write runs inside the store's atomic primitive so two concurrent
// patches cannot lose one another (DC51).
func (s *Service) UpdateBackupScheduleDef(ctx context.Context, project, db, id string, patch backupScheduleDef, mask []string, maskPresent bool) (backupScheduleDef, error) {
	// An omitted mask is the implied mask of every populated field (AIP-134),
	// exactly as UpdateFieldDef documents above.
	replace := replacesResource(mask, maskPresent)
	mask = impliedMask(mask, maskPresent)
	return mutateOne(ctx, s, project, rtBackupSchedule, backupScheduleRel(db, id), nil,
		func(cur backupScheduleDef, _ bool) (backupScheduleDef, bool, error) {
			if len(mask) == 0 {
				// Present-but-empty mask: no-op (AIP-134), no write.
				return cur, false, nil
			}
			if replace {
				// Explicit `*` is a full replacement, and both retention and the
				// recurrence oneof are REQUIRED on the resource: a replace that
				// omits either is the same INVALID_ARGUMENT real GCP returns,
				// rather than silently keeping the stored value.
				if patch.Retention <= 0 {
					return cur, false, gcpInvalidArgument("retention is required for a full replacement")
				}
				switch patch.Recurrence {
				case "DAILY":
					cur.Retention, cur.Recurrence, cur.DayOfWeek = patch.Retention, "DAILY", 0
				case "WEEKLY":
					if patch.DayOfWeek < 1 || patch.DayOfWeek > 7 {
						return cur, false, gcpInvalidArgument("day_of_week must be 1-7 for a weekly recurrence")
					}
					cur.Retention, cur.Recurrence, cur.DayOfWeek = patch.Retention, "WEEKLY", patch.DayOfWeek
				default:
					return cur, false, gcpInvalidArgument("a daily or weekly recurrence is required for a full replacement")
				}
				cur.UpdateTime = clock.Now()
				return cur, true, nil
			}
			for _, path := range mask {
				switch path {
				case "*":
					// Every populated field, including both halves of the
					// recurrence oneof — a maskless patch carrying a weekly
					// recurrence must not be silently dropped.
					if patch.Retention > 0 {
						cur.Retention = patch.Retention
					}
					switch patch.Recurrence {
					case "DAILY":
						cur.Recurrence, cur.DayOfWeek = "DAILY", 0
					case "WEEKLY":
						cur.Recurrence, cur.DayOfWeek = "WEEKLY", patch.DayOfWeek
					}
				case "retention":
					if patch.Retention > 0 {
						cur.Retention = patch.Retention
					}
				case "daily_recurrence":
					if patch.Recurrence == "DAILY" {
						cur.Recurrence, cur.DayOfWeek = "DAILY", 0
					}
				case "weekly_recurrence":
					if patch.Recurrence == "WEEKLY" {
						cur.Recurrence, cur.DayOfWeek = "WEEKLY", patch.DayOfWeek
					}
				}
			}
			cur.UpdateTime = clock.Now()
			return cur, true, nil
		})
}

// DeleteBackupSchedule removes a backup schedule record.
func (s *Service) DeleteBackupSchedule(ctx context.Context, project, db, id string) error {
	if s.resources == nil {
		return nil
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtBackupSchedule, backupScheduleRel(db, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return gcpNotFound("backup schedule")
		}
		return err
	}
	return nil
}

// ─── Backups ──────────────────────────────────────────────────────────────────

const (
	// defaultBackupRetention is the fallback retention when a schedule omits the
	// field (it is required in the API; the emulator stays lenient).
	defaultBackupRetention = 7 * 24 * time.Hour
	// maxBackupsPerPass bounds one lazy materialization pass so a large clock
	// jump cannot build an unbounded list of records.
	maxBackupsPerPass = 32
)

// backupScheduleInterval returns a schedule's recurrence period. The emulator
// anchors occurrences to the schedule's create time: a WEEKLY schedule with a
// day_of_week naming a different weekday still fires on the create-time weekday
// (the Discovery document models the day but the emulator does not shift the
// anchor).
func backupScheduleInterval(bs backupScheduleDef) time.Duration {
	if bs.Recurrence == "WEEKLY" {
		return 7 * 24 * time.Hour
	}
	return 24 * time.Hour
}

// parseBackupScheduleName splits a schedule name
// ("projects/{p}/databases/{db}/backupSchedules/{id}").
func parseBackupScheduleName(name string) (db, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "databases" || parts[4] != "backupSchedules" {
		return "", "", false
	}
	return parts[3], parts[5], true
}

// backupStats is the Backup.stats payload, measured from the database the backup
// was taken from. documentCount counts documents and indexCount counts tracked
// composite index definitions — the emulator does not model per-document index
// entries — and sizeBytes sums the persisted document records.
type backupStats struct {
	documentCount int64
	indexCount    int64
	sizeBytes     int64
}

// backupStatsFor measures a database's current document/index state. Store
// errors are returned so a materialized backup never carries silently wrong
// statistics.
func (s *Service) backupStatsFor(ctx context.Context, project, db string) (backupStats, error) {
	var st backupStats
	if s.store != nil {
		docs, err := s.store.ListDocuments(ctx, project, db)
		if err != nil {
			return st, err
		}
		st.documentCount = int64(len(docs))
		for _, d := range docs {
			if data, err := json.Marshal(d); err == nil {
				st.sizeBytes += int64(len(data))
			}
		}
	}
	indexes, err := listResources[indexDef](ctx, s, project, rtIndex, "databases/"+db+"/collectionGroups/")
	if err != nil {
		return st, err
	}
	st.indexCount = int64(len(indexes))
	return st, nil
}

// materializeBackups creates the Backup records a due backup schedule implies.
// Backups are read-only in the Admin API — a backup schedule produces them — so
// the emulator derives them lazily on every backup read/list from the schedule
// and the emulator clock (which the admin control plane can advance), and drops
// the ones whose retention window has closed, as real Firestore does.
//
// The work is proportional to the project's schedules (one GetDatabase and, when
// an occurrence is due, one document scan per schedule), plus one list of the
// location's backups for the expiry pass. That is fine at emulator scale, but it
// means a single GetBackup is not a pure read: it can materialize and purge.
func (s *Service) materializeBackups(ctx context.Context, project, loc string) error {
	if s.resources == nil {
		return nil
	}
	now := clock.Now().UTC()
	schedules, err := listResources[backupScheduleDef](ctx, s, project, rtBackupSchedule, "databases/")
	if err != nil {
		return err
	}
	for _, bs := range schedules {
		db, _, ok := parseBackupScheduleName(bs.Name)
		if !ok {
			continue
		}
		dbrec, err := s.GetDatabase(ctx, project, db)
		if err != nil {
			// A deleted database stops producing backups; its surviving backups
			// stay readable until they expire.
			continue
		}
		backupLoc := dbrec.LocationID
		if backupLoc == "" && loc != "-" {
			backupLoc = loc
		}
		// A backup lives in its database's location; a database without one (the
		// implicit "(default)" record) has nowhere to place them.
		if backupLoc == "" || (loc != "-" && backupLoc != loc) {
			continue
		}
		if err := s.materializeSchedule(ctx, project, db, backupLoc, dbrec, bs, now); err != nil {
			return err
		}
	}
	return s.purgeExpiredBackups(ctx, project, loc, now)
}

// materializeSchedule materializes the occurrences of one schedule that are due
// and not yet recorded. Occurrence k happens at createTime + k*interval; the
// schedule's create time is not itself a backup. Occurrences older than the
// retention window are skipped (they would be purged immediately), and the
// schedule's MaterializedThrough high-water mark advances past every occurrence
// considered, so a deleted backup is not recreated by the next read.
func (s *Service) materializeSchedule(ctx context.Context, project, db, backupLoc string, dbrec databaseDef, bs backupScheduleDef, now time.Time) error {
	start := bs.CreateTime.UTC()
	if start.IsZero() || now.Before(start) {
		return nil
	}
	interval := backupScheduleInterval(bs)
	retention := bs.Retention
	if retention <= 0 {
		retention = defaultBackupRetention
	}
	last := int64(now.Sub(start) / interval)
	if last < 1 {
		return nil
	}
	first := int64(1)
	if from := bs.MaterializedThrough.UTC(); !from.IsZero() {
		first = int64(from.Sub(start)/interval) + 1
	}
	// Drop occurrences that already fell out of the retention window.
	earliest := now.Add(-retention)
	if k := int64(earliest.Sub(start)/interval) + 1; k > first {
		first = k
	}
	if first < last-maxBackupsPerPass+1 {
		first = last - maxBackupsPerPass + 1
	}
	if first > last {
		return nil
	}

	_, schedID, _ := parseBackupScheduleName(bs.Name)
	if schedID == "" {
		return nil
	}
	stats, err := s.backupStatsFor(ctx, project, db)
	if err != nil {
		return err
	}
	for k := first; k <= last; k++ {
		snapshot := start.Add(time.Duration(k) * interval)
		id := fmt.Sprintf("%s-%d", schedID, snapshot.Unix())
		b := backupDef{
			Name:          backupName(project, backupLoc, id),
			Database:      databaseName(project, db),
			DatabaseUid:   dbrec.Uid,
			SnapshotTime:  snapshot,
			ExpireTime:    snapshot.Add(retention),
			State:         "READY",
			DocumentCount: stats.documentCount,
			IndexCount:    stats.indexCount,
			SizeBytes:     stats.sizeBytes,
		}
		if err := s.putResource(ctx, project, rtBackup, backupRel(backupLoc, id), b, false); err != nil {
			return err
		}
	}
	// Advance the high-water mark atomically: a read-side write must not
	// overwrite a concurrent schedule patch (nor resurrect a deleted schedule).
	if _, err := mutateOne(ctx, s, project, rtBackupSchedule, backupScheduleRel(db, schedID), nil,
		func(cur backupScheduleDef, _ bool) (backupScheduleDef, bool, error) {
			mark := start.Add(time.Duration(last) * interval)
			if !cur.MaterializedThrough.Before(mark) {
				return cur, false, nil
			}
			cur.MaterializedThrough = mark
			return cur, true, nil
		}); err != nil {
		if isNotFound(err) {
			return nil // the schedule was deleted while this read was in flight
		}
		return err
	}
	return nil
}

// purgeExpiredBackups removes backup records whose retention window has closed.
// Real Firestore deletes expired backups; the emulator does it lazily on read.
func (s *Service) purgeExpiredBackups(ctx context.Context, project, loc string, now time.Time) error {
	prefix := "locations/" + loc + "/backups/"
	if loc == "-" {
		prefix = "locations/"
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtBackup, prefix)
	if err != nil {
		return err
	}
	for _, e := range entries {
		var b backupDef
		if json.Unmarshal(e.Data, &b) != nil {
			continue
		}
		if b.ExpireTime.IsZero() || b.ExpireTime.After(now) {
			continue
		}
		if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtBackup, e.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
	}
	return nil
}

// GetBackup returns a backup record produced by a due backup schedule.
func (s *Service) GetBackup(ctx context.Context, project, loc, id string) (backupDef, error) {
	if err := s.materializeBackups(ctx, project, loc); err != nil {
		return backupDef{}, err
	}
	var b backupDef
	if err := s.getResource(ctx, project, rtBackup, backupRel(loc, id), &b); err != nil {
		return backupDef{}, err
	}
	return b, nil
}

// ListBackups returns the backups in a location (or, for the documented "-"
// wildcard, in every location), filtered by a substring of the backup resource
// name. Due backup-schedule occurrences are materialized first.
func (s *Service) ListBackups(ctx context.Context, project, loc, filter string) ([]backupDef, error) {
	if err := s.materializeBackups(ctx, project, loc); err != nil {
		return nil, err
	}
	prefix := "locations/" + loc + "/backups/"
	if loc == "-" {
		prefix = "locations/"
	}
	backups, err := listResources[backupDef](ctx, s, project, rtBackup, prefix)
	if err != nil {
		return nil, err
	}
	if filter != "" {
		kept := make([]backupDef, 0, len(backups))
		for _, b := range backups {
			match, err := backupMatchesFilter(b, filter)
			if err != nil {
				return nil, err
			}
			if match {
				kept = append(kept, b)
			}
		}
		backups = kept
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].Name < backups[j].Name })
	return backups, nil
}

// backupMatchesFilter evaluates a ListBackups filter expression. The Discovery
// document declares one eligible field — database_uid — compared with `=`
// (equality) or the generic `:` contains operator. An unsupported field or a
// malformed expression is InvalidArgument, as real Firestore answers.
func backupMatchesFilter(b backupDef, filter string) (bool, error) {
	expr := strings.TrimSpace(filter)
	idx := strings.IndexAny(expr, "=:")
	if idx < 0 {
		return false, model.NewProviderError("InvalidArgument",
			"invalid backup filter "+strconv.Quote(filter)+": expected <field> <operator> <value>", 400)
	}
	field := strings.ToLower(strings.TrimSpace(expr[:idx]))
	op := expr[idx]
	value := strings.TrimSpace(expr[idx+1:])
	if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
		value = value[1 : len(value)-1]
	}
	if field != "database_uid" {
		return false, model.NewProviderError("InvalidArgument",
			"invalid backup filter "+strconv.Quote(filter)+": the only supported field is database_uid", 400)
	}
	switch op {
	case '=':
		return b.DatabaseUid == value, nil
	default: // ':'
		return strings.Contains(b.DatabaseUid, value), nil
	}
}

// DeleteBackup removes a backup record.
func (s *Service) DeleteBackup(ctx context.Context, project, loc, id string) error {
	if s.resources == nil {
		return gcpNotFound("backup")
	}
	if err := s.resources.Delete(ctx, project, store.GlobalRegion, rtBackup, backupRel(loc, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return gcpNotFound("backup")
		}
		return err
	}
	return nil
}

// isNotFound reports whether err is the provider NotFound error.
func isNotFound(err error) bool {
	var perr *model.ProviderError
	if errors.As(err, &perr) {
		return perr.Code == "NotFound" || perr.HTTPStatus == 404
	}
	return false
}
