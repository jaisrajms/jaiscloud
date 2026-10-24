package firestore

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newAdminProvider() *Provider { return New(nil, store.NewMemoryResourceStore()) }

func adminNR(name string, extra map[string]any) *model.NormalizedRequest {
	params := map[string]any{"name": name}
	for k, v := range extra {
		params[k] = v
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params}
}

func TestRESTEmptyUpdateMaskNoop(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	// An explicit empty updateMask= query parameter is AIP-134's no-op; only a
	// missing parameter is the implied mask.
	if _, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "noop-db",
		"body":       map[string]any{"locationId": "nam5"},
	})); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if _, err := p.UpdateDatabase(ctx, adminNR("databases/noop-db", map[string]any{
		"updateMask": "concurrencyMode",
		"body":       map[string]any{"concurrencyMode": "PESSIMISTIC"},
	})); err != nil {
		t.Fatalf("UpdateDatabase: %v", err)
	}
	before, err := p.GetDatabase(ctx, adminNR("databases/noop-db", nil))
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	noop, err := p.UpdateDatabase(ctx, adminNR("databases/noop-db", map[string]any{
		"updateMask": "",
		"body":       map[string]any{"concurrencyMode": "OPTIMISTIC"},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase(empty updateMask): %v", err)
	}
	resp, _ := noop.Data["response"].(map[string]any)
	if resp["concurrencyMode"] != "PESSIMISTIC" {
		t.Errorf("empty updateMask applied concurrencyMode = %v, want PESSIMISTIC", resp["concurrencyMode"])
	}
	if resp["etag"] != before.Data["etag"] {
		t.Errorf("empty updateMask rotated the etag %v -> %v", before.Data["etag"], resp["etag"])
	}

	// Field: nothing is written either, and the terminal operation still
	// returns the stored field.
	if _, err := p.UpdateField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", map[string]any{
		"updateMask": "ttlConfig",
		"body":       map[string]any{"ttlConfig": map[string]any{"state": "ACTIVE", "expirationOffset": "3600s"}},
	})); err != nil {
		t.Fatalf("UpdateField: %v", err)
	}
	fieldNoop, err := p.UpdateField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", map[string]any{
		"updateMask": "",
		"body":       map[string]any{"ttlConfig": map[string]any{"state": "ACTIVE", "expirationOffset": "9000s"}},
	}))
	if err != nil {
		t.Fatalf("UpdateField(empty updateMask): %v", err)
	}
	fieldResp, _ := fieldNoop.Data["response"].(map[string]any)
	if tc, _ := fieldResp["ttlConfig"].(map[string]any); tc["expirationOffset"] != "3600s" {
		t.Errorf("empty updateMask applied ttlConfig = %v, want 3600s", fieldResp["ttlConfig"])
	}

	// Backup schedule: retention and recurrence stay as stored.
	if _, err := p.CreateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules", map[string]any{
		"backupScheduleId": "1",
		"body":             map[string]any{"retention": "604800s", "dailyRecurrence": map[string]any{}},
	})); err != nil {
		t.Fatalf("CreateBackupSchedule: %v", err)
	}
	bsNoop, err := p.UpdateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", map[string]any{
		"updateMask": "",
		"body": map[string]any{
			"retention":        "1209600s",
			"weeklyRecurrence": map[string]any{"day": "MONDAY"},
		},
	}))
	if err != nil {
		t.Fatalf("UpdateBackupSchedule(empty updateMask): %v", err)
	}
	if bsNoop.Data["retention"] != "604800s" {
		t.Errorf("empty updateMask applied retention = %v, want 604800s", bsNoop.Data["retention"])
	}
	if _, daily := bsNoop.Data["dailyRecurrence"]; !daily {
		t.Errorf("empty updateMask cleared dailyRecurrence = %v", bsNoop.Data["recurrence"])
	}
	if _, weekly := bsNoop.Data["weeklyRecurrence"]; weekly {
		t.Errorf("empty updateMask applied weeklyRecurrence = %v", bsNoop.Data["weeklyRecurrence"])
	}

	// updateTime is part of the no-op contract: it must not rotate.
	if bsNoop.Data["updateTime"] == nil || bsNoop.Data["updateTime"] == "" {
		t.Error("empty updateMask response carries no updateTime")
	}
}

func TestRESTDatabaseLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	// create -> terminal operation wrapping the Database.
	resp, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "test-db",
		"body":       map[string]any{"locationId": "nam5"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	op := resp.Data
	if op["done"] != true {
		t.Errorf("create op done = %v, want true", op["done"])
	}
	inner, _ := op["response"].(map[string]any)
	if inner["name"] != "projects/proj/databases/test-db" {
		t.Errorf("created db name = %v", inner["name"])
	}
	if inner["@type"] != "type.googleapis.com/google.firestore.admin.v1.Database" {
		t.Errorf("create op @type = %v", inner["@type"])
	}

	// get round-trips the mutable fields, with the protobuf-duration string form.
	got, err := p.GetDatabase(ctx, adminNR("databases/test-db", nil))
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if got.Data["locationId"] != "nam5" {
		t.Errorf("locationId = %v, want nam5", got.Data["locationId"])
	}
	if got.Data["versionRetentionPeriod"] != "3600s" {
		t.Errorf("versionRetentionPeriod = %v, want 3600s", got.Data["versionRetentionPeriod"])
	}

	// list includes the implicit (default) database.
	list, err := p.ListDatabases(ctx, adminNR("databases", nil))
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	dbs, _ := list.Data["databases"].([]any)
	if len(dbs) != 2 {
		t.Errorf("ListDatabases len = %d, want 2", len(dbs))
	}

	// patch applies updateMask=concurrencyMode.
	upd, err := p.UpdateDatabase(ctx, adminNR("databases/test-db", map[string]any{
		"updateMask": "concurrencyMode",
		"body":       map[string]any{"concurrencyMode": "PESSIMISTIC"},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase: %v", err)
	}
	uin, _ := upd.Data["response"].(map[string]any)
	if uin["concurrencyMode"] != "PESSIMISTIC" {
		t.Errorf("concurrencyMode = %v, want PESSIMISTIC", uin["concurrencyMode"])
	}

	// An omitted updateMask is an implied mask of the populated fields
	// (AIP-134), so a maskless patch is accepted.
	implied, err := p.UpdateDatabase(ctx, adminNR("databases/test-db", map[string]any{
		"body": map[string]any{"appEngineIntegrationMode": "DISABLED"},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase(no updateMask): %v", err)
	}
	iin, _ := implied.Data["response"].(map[string]any)
	if iin["appEngineIntegrationMode"] != "DISABLED" {
		t.Errorf("appEngineIntegrationMode = %v, want DISABLED", iin["appEngineIntegrationMode"])
	}
	if iin["concurrencyMode"] != "PESSIMISTIC" {
		t.Errorf("maskless patch changed an unpopulated field: concurrencyMode = %v", iin["concurrencyMode"])
	}

	// A stale etag on the documented delete query param blocks the delete with
	// FAILED_PRECONDITION/400 and leaves the database in place.
	currentEtag, _ := iin["etag"].(string)
	if currentEtag == "" {
		t.Fatal("maskless patch response carries no etag")
	}
	if _, err := p.DeleteDatabase(ctx, adminNR("databases/test-db", map[string]any{"etag": currentEtag + "-stale"})); err == nil {
		t.Error("DeleteDatabase(stale etag) succeeded, want FailedPrecondition")
	} else {
		assertDeletePrecondition(t, err, "stale-etag")
	}
	if _, err := p.GetDatabase(ctx, adminNR("databases/test-db", nil)); err != nil {
		t.Errorf("stale-etag delete removed the database: %v", err)
	}

	// delete returns a terminal operation carrying the soft-deleted database
	// (operation_info response_type "Database"). The current etag (the
	// documented query parameter) is accepted.
	del, err := p.DeleteDatabase(ctx, adminNR("databases/test-db", map[string]any{"etag": currentEtag}))
	if err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
	if del.Data["done"] != true {
		t.Errorf("delete op done = %v", del.Data["done"])
	}
	dresp, _ := del.Data["response"].(map[string]any)
	if dresp["@type"] != "type.googleapis.com/google.firestore.admin.v1.Database" {
		t.Errorf("delete op response @type = %v", dresp["@type"])
	}
	if dresp["name"] != "projects/proj/databases/test-db" {
		t.Errorf("delete op response name = %v", dresp["name"])
	}
	if dresp["deleteTime"] == nil {
		t.Error("delete op response carries no deleteTime")
	}
	if _, err := p.GetDatabase(ctx, adminNR("databases/test-db", nil)); err == nil {
		t.Error("GetDatabase after delete succeeded, want NotFound")
	}

	// data-plane verbs are rejected before dispatch (invalid path here).
	if _, err := p.CreateDatabase(ctx, adminNR("databases/test-db", nil)); err == nil {
		t.Error("CreateDatabase on a database resource succeeded, want InvalidArgument")
	}
}

// TestRESTDatabaseServerOwnedFields pins the wire view of database.proto's field
// ownership: a REST create/patch carrying key_prefix (OUTPUT_ONLY) or a changed
// database_edition (IMMUTABLE) does not store them, and versionRetentionPeriod is
// the PITR-derived window (7 days enabled, 1 hour otherwise), never the body's.
func TestRESTDatabaseServerOwnedFields(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	created, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "owned-db",
		"body": map[string]any{
			"locationId":                    "nam5",
			"databaseEdition":               "ENTERPRISE",
			"keyPrefix":                     "client-supplied",
			"versionRetentionPeriod":        "7200s",
			"pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_ENABLED",
		},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	cresp, _ := created.Data["response"].(map[string]any)
	if cresp["databaseEdition"] != "ENTERPRISE" {
		t.Errorf("create databaseEdition = %v, want ENTERPRISE (IMMUTABLE is settable on create)", cresp["databaseEdition"])
	}
	if _, ok := cresp["keyPrefix"]; ok {
		t.Errorf("create stored the OUTPUT_ONLY keyPrefix = %v", cresp["keyPrefix"])
	}
	if cresp["versionRetentionPeriod"] != "604800s" {
		t.Errorf("create versionRetentionPeriod = %v, want 604800s (PITR enabled)", cresp["versionRetentionPeriod"])
	}

	// The documented maskless patch: server-owned fields in the body are still
	// ignored, and disabling PITR re-derives the window to 1 hour. The immutable
	// edition is resent unchanged, as a client that read the record would do.
	upd, err := p.UpdateDatabase(ctx, adminNR("databases/owned-db", map[string]any{
		"body": map[string]any{
			"databaseEdition":               "ENTERPRISE",
			"keyPrefix":                     "client-supplied",
			"versionRetentionPeriod":        "1800s",
			"pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_DISABLED",
		},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase(no updateMask): %v", err)
	}
	uresp, _ := upd.Data["response"].(map[string]any)
	if uresp["databaseEdition"] != "ENTERPRISE" {
		t.Errorf("patch changed the IMMUTABLE databaseEdition = %v", uresp["databaseEdition"])
	}
	if _, ok := uresp["keyPrefix"]; ok {
		t.Errorf("patch wrote the OUTPUT_ONLY keyPrefix = %v", uresp["keyPrefix"])
	}
	if uresp["versionRetentionPeriod"] != "3600s" {
		t.Errorf("patch versionRetentionPeriod = %v, want 3600s (PITR disabled)", uresp["versionRetentionPeriod"])
	}

	// An explicit mask naming the OUTPUT_ONLY fields writes neither of them
	// (AIP-161), including the camelCase spellings the REST mask uses.
	explicit, err := p.UpdateDatabase(ctx, adminNR("databases/owned-db", map[string]any{
		"updateMask": "versionRetentionPeriod,keyPrefix",
		"body": map[string]any{
			"keyPrefix":              "client-supplied",
			"versionRetentionPeriod": "604800s",
		},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase(updateMask=versionRetentionPeriod,keyPrefix): %v", err)
	}
	xresp, _ := explicit.Data["response"].(map[string]any)
	if _, ok := xresp["keyPrefix"]; ok {
		t.Errorf("explicit mask wrote the OUTPUT_ONLY keyPrefix = %v", xresp["keyPrefix"])
	}
	if xresp["versionRetentionPeriod"] != "3600s" {
		t.Errorf("explicit retention patch = %v, want the derived 3600s", xresp["versionRetentionPeriod"])
	}

	// A masked change to the IMMUTABLE edition is INVALID_ARGUMENT (AIP-203).
	if _, err := p.UpdateDatabase(ctx, adminNR("databases/owned-db", map[string]any{
		"updateMask": "databaseEdition",
		"body":       map[string]any{"databaseEdition": "STANDARD"},
	})); err == nil {
		t.Error("UpdateDatabase(databaseEdition change) succeeded, want InvalidArgument")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
			t.Errorf("databaseEdition change = %v, want InvalidArgument/400", err)
		}
	}
}

func TestRESTDeleteProtectionBlock(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	created, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "prot-db",
		"body":       map[string]any{"locationId": "nam5"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	createResp, _ := created.Data["response"].(map[string]any)
	if got := createResp["deleteProtectionState"]; got != deleteProtectionDisabled {
		t.Fatalf("create deleteProtectionState = %v, want %s", got, deleteProtectionDisabled)
	}

	// Enabling protection through projects.databases.patch blocks the delete.
	enabled, err := p.UpdateDatabase(ctx, adminNR("databases/prot-db", map[string]any{
		"updateMask": "deleteProtectionState",
		"body":       map[string]any{"deleteProtectionState": deleteProtectionEnabled},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase(enable protection): %v", err)
	}
	enabledResp, _ := enabled.Data["response"].(map[string]any)
	if enabledResp["deleteProtectionState"] != deleteProtectionEnabled {
		t.Fatalf("deleteProtectionState = %v, want %s", enabledResp["deleteProtectionState"], deleteProtectionEnabled)
	}
	etag, _ := enabledResp["etag"].(string)
	if _, err := p.DeleteDatabase(ctx, adminNR("databases/prot-db", map[string]any{"etag": etag})); err == nil {
		t.Error("DeleteDatabase succeeded with delete protection enabled, want FailedPrecondition")
	} else {
		assertDeletePrecondition(t, err, "delete-protected")
	}
	if kept, err := p.GetDatabase(ctx, adminNR("databases/prot-db", nil)); err != nil {
		t.Errorf("blocked delete removed the database: %v", err)
	} else if kept.Data["etag"] != etag {
		t.Errorf("blocked delete rotated the etag %v -> %v", etag, kept.Data["etag"])
	}

	// Disabling protection admits the delete.
	disabled, err := p.UpdateDatabase(ctx, adminNR("databases/prot-db", map[string]any{
		"updateMask": "deleteProtectionState",
		"body":       map[string]any{"deleteProtectionState": deleteProtectionDisabled},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase(disable protection): %v", err)
	}
	disabledResp, _ := disabled.Data["response"].(map[string]any)
	etag, _ = disabledResp["etag"].(string)
	if _, err := p.DeleteDatabase(ctx, adminNR("databases/prot-db", map[string]any{"etag": etag})); err != nil {
		t.Fatalf("DeleteDatabase after disabling protection: %v", err)
	}
	if _, err := p.GetDatabase(ctx, adminNR("databases/prot-db", nil)); err == nil {
		t.Error("GetDatabase after delete succeeded, want NotFound")
	}
}

func TestRESTBackupScheduleLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	created, err := p.CreateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules", map[string]any{
		"body": map[string]any{"retention": "3600s", "weeklyRecurrence": map[string]any{"day": "MONDAY"}},
	}))
	if err != nil {
		t.Fatalf("CreateBackupSchedule: %v", err)
	}
	if created.Data["name"] != "projects/proj/databases/(default)/backupSchedules/1" {
		t.Errorf("schedule name = %v", created.Data["name"])
	}
	if created.Data["retention"] != "3600s" {
		t.Errorf("retention = %v, want 3600s", created.Data["retention"])
	}
	wr, _ := created.Data["weeklyRecurrence"].(map[string]any)
	if wr["day"] != "MONDAY" {
		t.Errorf("weeklyRecurrence.day = %v, want MONDAY", wr["day"])
	}

	got, err := p.GetBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", nil))
	if err != nil {
		t.Fatalf("GetBackupSchedule: %v", err)
	}
	if got.Data["name"] != created.Data["name"] {
		t.Errorf("GetBackupSchedule name = %v", got.Data["name"])
	}

	list, err := p.ListBackupSchedules(ctx, adminNR("databases/(default)/backupSchedules", nil))
	if err != nil {
		t.Fatalf("ListBackupSchedules: %v", err)
	}
	if bss, _ := list.Data["backupSchedules"].([]any); len(bss) != 1 {
		t.Errorf("ListBackupSchedules len = %d, want 1", len(bss))
	}

	upd, err := p.UpdateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", map[string]any{
		"updateMask": "retention",
		"body":       map[string]any{"retention": "7200s"},
	}))
	if err != nil {
		t.Fatalf("UpdateBackupSchedule: %v", err)
	}
	if upd.Data["retention"] != "7200s" {
		t.Errorf("retention after update = %v, want 7200s", upd.Data["retention"])
	}

	// An omitted updateMask is the implied mask of the populated fields
	// (AIP-134): the PATCH applies the retention *and* the recurrence oneof,
	// which the old `*` arm silently dropped.
	maskless, err := p.UpdateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", map[string]any{
		"body": map[string]any{
			"retention":        "10800s",
			"weeklyRecurrence": map[string]any{"day": "WEDNESDAY"},
		},
	}))
	if err != nil {
		t.Fatalf("UpdateBackupSchedule(maskless): %v", err)
	}
	if maskless.Data["retention"] != "10800s" {
		t.Errorf("maskless retention = %v, want 10800s", maskless.Data["retention"])
	}
	// WEDNESDAY differs from the create's MONDAY, so the recurrence can only
	// come from the maskless patch (not from the stored record).
	if wr, _ := maskless.Data["weeklyRecurrence"].(map[string]any); wr["day"] != "WEDNESDAY" {
		t.Errorf("maskless weeklyRecurrence = %v, want WEDNESDAY", maskless.Data["weeklyRecurrence"])
	}

	if _, err := p.DeleteBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", nil)); err != nil {
		t.Fatalf("DeleteBackupSchedule: %v", err)
	}
	if _, err := p.GetBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", nil)); err == nil {
		t.Error("GetBackupSchedule after delete succeeded, want NotFound")
	}
}

func TestRESTUserCredsLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	// create surfaces the secret password once.
	created, err := p.CreateUserCreds(ctx, adminNR("databases/(default)/userCreds", map[string]any{"userCredsId": "uc1"}))
	if err != nil {
		t.Fatalf("CreateUserCreds: %v", err)
	}
	if created.Data["name"] != "projects/proj/databases/(default)/userCreds/uc1" {
		t.Errorf("user creds name = %v", created.Data["name"])
	}
	secret, _ := created.Data["securePassword"].(string)
	if secret == "" {
		t.Error("CreateUserCreds returned no securePassword")
	}
	if created.Data["state"] != "ENABLED" {
		t.Errorf("state = %v, want ENABLED", created.Data["state"])
	}
	ri, _ := created.Data["resourceIdentity"].(map[string]any)
	if ri["principal"] != "uc1@proj.iam.gserviceaccount.com" {
		t.Errorf("principal = %v", ri["principal"])
	}

	// get/list never surface the secret.
	got, err := p.GetUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil))
	if err != nil {
		t.Fatalf("GetUserCreds: %v", err)
	}
	if _, ok := got.Data["securePassword"]; ok {
		t.Error("GetUserCreds surfaced securePassword")
	}
	list, err := p.ListUserCreds(ctx, adminNR("databases/(default)/userCreds", nil))
	if err != nil {
		t.Fatalf("ListUserCreds: %v", err)
	}
	if ucs, _ := list.Data["userCreds"].([]any); len(ucs) != 1 {
		t.Errorf("ListUserCreds len = %d, want 1", len(ucs))
	}

	dis, err := p.DisableUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil))
	if err != nil {
		t.Fatalf("DisableUserCreds: %v", err)
	}
	if dis.Data["state"] != "DISABLED" {
		t.Errorf("state after disable = %v", dis.Data["state"])
	}
	en, err := p.EnableUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil))
	if err != nil {
		t.Fatalf("EnableUserCreds: %v", err)
	}
	if en.Data["state"] != "ENABLED" {
		t.Errorf("state after enable = %v", en.Data["state"])
	}

	reset, err := p.ResetUserPassword(ctx, adminNR("databases/(default)/userCreds/uc1", nil))
	if err != nil {
		t.Fatalf("ResetUserPassword: %v", err)
	}
	if reset.Data["securePassword"] == secret {
		t.Error("ResetUserPassword did not rotate the password")
	}

	if _, err := p.DeleteUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil)); err != nil {
		t.Fatalf("DeleteUserCreds: %v", err)
	}
	if _, err := p.GetUserCreds(ctx, adminNR("databases/(default)/userCreds/uc1", nil)); err == nil {
		t.Error("GetUserCreds after delete succeeded, want NotFound")
	}
}

func TestRESTFieldUpdateAndGet(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	upd, err := p.UpdateField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", map[string]any{
		"updateMask": "indexConfig",
		"body": map[string]any{
			"indexConfig": map[string]any{
				"indexes": []any{
					map[string]any{"fieldPath": "f", "order": "ASCENDING"},
				},
			},
		},
	}))
	if err != nil {
		t.Fatalf("UpdateField: %v", err)
	}
	respObj, _ := upd.Data["response"].(map[string]any)
	if respObj["name"] != "projects/proj/databases/(default)/collectionGroups/cg/fields/f" {
		t.Errorf("field name = %v", respObj["name"])
	}
	ic, _ := respObj["indexConfig"].(map[string]any)
	if _, ok := ic["indexes"].([]any); !ok {
		t.Errorf("field indexConfig.indexes missing: %v", respObj)
	}

	got, err := p.GetField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", nil))
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if got.Data["name"] != "projects/proj/databases/(default)/collectionGroups/cg/fields/f" {
		t.Errorf("GetField name = %v", got.Data["name"])
	}

	list, err := p.ListFields(ctx, adminNR("databases/(default)/collectionGroups/cg/fields", nil))
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
	if fields, _ := list.Data["fields"].([]any); len(fields) != 1 {
		t.Errorf("ListFields len = %d, want 1", len(fields))
	}

	// An omitted updateMask is the implied mask of the populated fields
	// (AIP-134): the PATCH applies both configs with no query parameter, and
	// the widened `*` arm carries ttl_config as well as index_config.
	maskless, err := p.UpdateField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", map[string]any{
		"body": map[string]any{
			"indexConfig": map[string]any{"usesAncestorConfig": false, "ancestorField": "cg", "reverting": true},
			"ttlConfig":   map[string]any{"state": "NEEDS_REPAIR", "expirationOffset": "7200s"},
		},
	}))
	if err != nil {
		t.Fatalf("UpdateField(maskless): %v", err)
	}
	respObj, _ = maskless.Data["response"].(map[string]any)
	// The three index-config members and the TTL state are OUTPUT_ONLY: the
	// patch's values are ignored and the derived ones are reported (DC49).
	wantAncestor := "projects/proj/databases/(default)/collectionGroups/cg/fields/__name__"
	if mic, _ := respObj["indexConfig"].(map[string]any); mic["ancestorField"] != wantAncestor ||
		mic["usesAncestorConfig"] != true || mic["reverting"] != false {
		t.Errorf("maskless indexConfig = %v, want the derived inherited form", respObj["indexConfig"])
	}
	if tc, _ := respObj["ttlConfig"].(map[string]any); tc["state"] != "ACTIVE" || tc["expirationOffset"] != "7200s" {
		t.Errorf("maskless ttlConfig = %v, want ACTIVE/7200s", respObj["ttlConfig"])
	}
}

func TestRESTBackupsReadOnly(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	list, err := p.ListBackups(ctx, adminNR("locations/nam5/backups", nil))
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if backups, _ := list.Data["backups"].([]any); len(backups) != 0 {
		t.Errorf("ListBackups len = %d, want 0", len(backups))
	}
	if _, err := p.GetBackup(ctx, adminNR("locations/nam5/backups/b1", nil)); err == nil {
		t.Error("GetBackup(missing) succeeded, want NotFound")
	}
	if _, err := p.DeleteBackup(ctx, adminNR("locations/nam5/backups/b1", nil)); err == nil {
		t.Error("DeleteBackup(missing) succeeded, want NotFound")
	}
	// The list/get survive a stored record.
	if err := p.Service.putResource(ctx, "proj", rtBackup, backupRel("nam5", "b1"), backupDef{
		Name: backupName("proj", "nam5", "b1"), Database: "projects/proj/databases/(default)",
		SizeBytes: 42, SnapshotTime: time.Unix(0, 0),
	}, false); err != nil {
		t.Fatalf("putResource: %v", err)
	}
	b, err := p.GetBackup(ctx, adminNR("locations/nam5/backups/b1", nil))
	if err != nil {
		t.Fatalf("GetBackup(stored): %v", err)
	}
	stats, _ := b.Data["stats"].(map[string]any)
	if stats["sizeBytes"] != "42" {
		t.Errorf("stats.sizeBytes = %v, want \"42\"", stats["sizeBytes"])
	}
}

func TestRESTDatabaseOperations(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	// get reports the canonical name terminal.
	got, err := p.GetOperation(ctx, adminNR("databases/gcprobe/operations/op1", nil))
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if got.Data["name"] != "projects/proj/databases/gcprobe/operations/op1" {
		t.Errorf("operation name = %v", got.Data["name"])
	}
	if got.Data["done"] != true {
		t.Errorf("operation done = %v, want true", got.Data["done"])
	}

	// list is empty (no operation records are persisted).
	list, err := p.ListOperations(ctx, adminNR("databases/gcprobe/operations", nil))
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if ops, _ := list.Data["operations"].([]any); len(ops) != 0 {
		t.Errorf("ListOperations len = %d, want 0", len(ops))
	}

	// delete/cancel are no-op successes.
	if _, err := p.DeleteOperation(ctx, adminNR("databases/gcprobe/operations/op1", nil)); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}
	if _, err := p.CancelOperation(ctx, adminNR("databases/gcprobe/operations/op1", nil)); err != nil {
		t.Fatalf("CancelOperation: %v", err)
	}

	// A collection path is not an operation resource.
	if _, err := p.GetOperation(ctx, adminNR("databases/gcprobe/operations", nil)); err == nil {
		t.Error("GetOperation on the collection path succeeded, want InvalidArgument")
	}
}

func TestAdminMaskForms(t *testing.T) { // Plain csv updateMask is normalized to snake_case proto paths.
	nr := adminNR("databases/db", map[string]any{"updateMask": "concurrencyMode,locationId"})
	if got, present := adminMask(nr); !present || len(got) != 2 || got[0] != "concurrency_mode" || got[1] != "location_id" {
		t.Errorf("adminMask(plain) = %v present=%v", got, present)
	}
	// Repeated updateMask.fieldPaths (older clients).
	nr = adminNR("databases/db", map[string]any{"updateMask.fieldPaths": "concurrencyMode"})
	if got, present := adminMask(nr); !present || len(got) != 1 || got[0] != "concurrency_mode" {
		t.Errorf("adminMask(fieldPaths) = %v present=%v", got, present)
	}
	// Nested JSON path and the wildcard.
	nr = adminNR("databases/db", map[string]any{"updateMask": "indexConfig.indexes,*"})
	if got, present := adminMask(nr); !present || len(got) != 2 || got[0] != "index_config.indexes" || got[1] != "*" {
		t.Errorf("adminMask(nested) = %v present=%v", got, present)
	}
	// An omitted mask is not present; an explicit `updateMask=` is present but
	// empty — AIP-134's no-op, not the implied mask.
	nr = adminNR("databases/db", nil)
	if got, present := adminMask(nr); present || len(got) != 0 {
		t.Errorf("adminMask(omitted) = %v present=%v, want absent", got, present)
	}
	nr = adminNR("databases/db", map[string]any{"updateMask": ""})
	if got, present := adminMask(nr); !present || len(got) != 0 {
		t.Errorf("adminMask(empty) = %v present=%v, want present-but-empty", got, present)
	}
	nr = adminNR("databases/db", map[string]any{"updateMask.fieldPaths": ""})
	if _, present := adminMask(nr); !present {
		t.Error("adminMask(empty fieldPaths) absent, want present-but-empty")
	}
	// A request whose query is only on Raw (no decoded Params) reads the value
	// from the URL, not just its presence.
	nr = &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{},
		Raw: httptest.NewRequest("PATCH", "/v1/projects/proj/databases/db?updateMask=concurrencyMode", nil)}
	if got, present := adminMask(nr); !present || len(got) != 1 || got[0] != "concurrency_mode" {
		t.Errorf("adminMask(raw) = %v present=%v", got, present)
	}
}

func TestRESTDatabaseOutputFields(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()
	frozen := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: frozen})
	defer clock.SetGlobalClock(clock.RealClock{})

	// The implicit (default) database is materialized with real output fields,
	// and earliest_version_time is derived per response (DC46/DC54).
	def, err := p.GetDatabase(ctx, adminNR("databases/(default)", nil))
	if err != nil {
		t.Fatalf("GetDatabase(default): %v", err)
	}
	if def.Data["uid"] == nil || def.Data["uid"] == "" || def.Data["uid"] == "default" {
		t.Errorf("default uid = %v", def.Data["uid"])
	}
	if def.Data["etag"] == nil || def.Data["createTime"] == nil || def.Data["updateTime"] == nil {
		t.Errorf("default database = %v, want etag + timestamps", def.Data)
	}
	if want := restRFC3339(frozen.Add(-time.Hour)); def.Data["earliestVersionTime"] != want {
		t.Errorf("default earliestVersionTime = %v, want %v", def.Data["earliestVersionTime"], want)
	}
	if def.Data["concurrencyMode"] != concurrencyPessimistic {
		t.Errorf("default concurrencyMode = %v, want %s (STANDARD edition)", def.Data["concurrencyMode"], concurrencyPessimistic)
	}
	// The materialized record is stable across reads: a per-call synthesis would
	// rotate the etag/uid and never satisfy the delete precondition.
	defAgain, err := p.GetDatabase(ctx, adminNR("databases/(default)", nil))
	if err != nil {
		t.Fatalf("GetDatabase(default) again: %v", err)
	}
	if defAgain.Data["etag"] != def.Data["etag"] || defAgain.Data["uid"] != def.Data["uid"] {
		t.Errorf("default output fields are not stable: %v -> %v", def.Data, defAgain.Data)
	}

	// A create that omits concurrencyMode defaults by edition (DC53), and the
	// PITR-off retention window drives earliest_version_time.
	resp, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{"databaseId": "outp-db"}))
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	created, _ := resp.Data["response"].(map[string]any)
	if created["concurrencyMode"] != concurrencyPessimistic {
		t.Errorf("create concurrencyMode = %v, want %s", created["concurrencyMode"], concurrencyPessimistic)
	}
	if want := restRFC3339(frozen.Add(-time.Hour)); created["earliestVersionTime"] != want {
		t.Errorf("create earliestVersionTime = %v, want %v", created["earliestVersionTime"], want)
	}

	enterprise, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "outp-ent",
		"body":       map[string]any{"databaseEdition": "ENTERPRISE"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase(ENTERPRISE): %v", err)
	}
	entObj, _ := enterprise.Data["response"].(map[string]any)
	if entObj["concurrencyMode"] != concurrencyOptimistic {
		t.Errorf("create(ENTERPRISE) concurrencyMode = %v, want %s", entObj["concurrencyMode"], concurrencyOptimistic)
	}
}
