package firestore

import (
	"context"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
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

// TestRESTDatabaseDefaultCreate pins projects.databases.create for the implicit
// (default) database: a project with no live default accepts the create (the
// Firebase Terraform shape, google_firestore_database { name = "(default)" }),
// the terminal operation's Database carries the requested locationId, a
// following GET echoes it, and a duplicate is ALREADY_EXISTS.
func TestRESTDatabaseDefaultCreate(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	resp, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "(default)",
		"body": map[string]any{
			"locationId":            "us-central1",
			"deleteProtectionState": "DELETE_PROTECTION_DISABLED",
		},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase(default): %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("create op done = %v, want true", resp.Data["done"])
	}
	inner, _ := resp.Data["response"].(map[string]any)
	if want := "projects/proj/databases/(default)"; inner["name"] != want {
		t.Errorf("created default name = %v, want %s", inner["name"], want)
	}
	if inner["@type"] != "type.googleapis.com/google.firestore.admin.v1.Database" {
		t.Errorf("create op @type = %v", inner["@type"])
	}
	if inner["locationId"] != "us-central1" {
		t.Errorf("created default locationId = %v, want us-central1", inner["locationId"])
	}

	got, err := p.GetDatabase(ctx, adminNR("databases/(default)", nil))
	if err != nil {
		t.Fatalf("GetDatabase(default): %v", err)
	}
	if got.Data["locationId"] != "us-central1" || got.Data["name"] != inner["name"] {
		t.Errorf("GetDatabase(default) = %v, want the created record", got.Data)
	}

	// The default's create has the same operation envelope and field set as a
	// named database's; only the reserved id differs.
	named, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "named-db",
		"body":       map[string]any{"locationId": "us-central1"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase(named): %v", err)
	}
	namedInner, _ := named.Data["response"].(map[string]any)
	for _, field := range []string{
		"name", "uid", "createTime", "updateTime", "etag", "type", "concurrencyMode",
		"deleteProtectionState", "databaseEdition", "versionRetentionPeriod",
		"earliestVersionTime", "locationId",
	} {
		_, haveDefault := inner[field]
		_, haveNamed := namedInner[field]
		if haveDefault != haveNamed {
			t.Errorf("%s present on default=%v named=%v, want the same shape", field, haveDefault, haveNamed)
		}
	}

	// A live default is ALREADY_EXISTS, just like a named duplicate.
	if _, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "(default)",
		"body":       map[string]any{"locationId": "nam5"},
	})); err == nil {
		t.Error("duplicate CreateDatabase(default) succeeded, want AlreadyExists")
	} else {
		assertProviderCode(t, err, "AlreadyExists", 409)
	}

	// After a delete the create is accepted again (the tombstone is absent).
	deleted, err := p.DeleteDatabase(ctx, adminNR("databases/(default)", map[string]any{"etag": inner["etag"]}))
	if err != nil {
		t.Fatalf("DeleteDatabase(default): %v", err)
	}
	if dresp, _ := deleted.Data["response"].(map[string]any); dresp["deleteTime"] == nil {
		t.Error("delete op response carries no deleteTime")
	}
	recreated, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "(default)",
		"body":       map[string]any{"locationId": "nam5"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase(default after delete): %v", err)
	}
	rein, _ := recreated.Data["response"].(map[string]any)
	if rein["locationId"] != "nam5" {
		t.Errorf("recreated default locationId = %v, want nam5", rein["locationId"])
	}
}

// TestRESTDatabaseRecreateAfterDelete pins immediate id reuse on the JSON wire:
// DELETE then POST with the same databaseId succeeds at once (the tombstone is
// absent for create), and the tombstone remains listed under showDeleted until
// the create replaces it. Real GCP blocks reuse for about five minutes; the
// emulator deliberately does not model that cooldown (TGM13).
func TestRESTDatabaseRecreateAfterDelete(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	created, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "reuse-db",
		"body":       map[string]any{"locationId": "us-central1"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	cresp, _ := created.Data["response"].(map[string]any)

	deleted, err := p.DeleteDatabase(ctx, adminNR("databases/reuse-db", map[string]any{"etag": cresp["etag"]}))
	if err != nil {
		t.Fatalf("DeleteDatabase: %v", err)
	}
	if dresp, _ := deleted.Data["response"].(map[string]any); dresp["deleteTime"] == nil {
		t.Error("delete op response carries no deleteTime")
	}
	if _, err := p.GetDatabase(ctx, adminNR("databases/reuse-db", nil)); !isNotFound(err) {
		t.Fatalf("GetDatabase(deleted) = %v, want NotFound", err)
	}

	// showDeleted still lists the tombstone.
	listed, err := p.ListDatabases(ctx, adminNR("databases", map[string]any{"showDeleted": true}))
	if err != nil {
		t.Fatalf("ListDatabases(showDeleted): %v", err)
	}
	items, _ := listed.Data["databases"].([]any)
	tombstoned := false
	for _, it := range items {
		db, _ := it.(map[string]any)
		if db["name"] == "projects/proj/databases/reuse-db" {
			tombstoned = db["deleteTime"] != nil
		}
	}
	if !tombstoned {
		t.Error("ListDatabases(showDeleted) omitted the tombstone")
	}

	// The same databaseId is available at once and the operation response is the
	// new record; the create replaces the tombstone rather than adding one.
	recreated, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "reuse-db",
		"body":       map[string]any{"locationId": "nam5"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase(after delete): %v", err)
	}
	rein, _ := recreated.Data["response"].(map[string]any)
	if rein["locationId"] != "nam5" {
		t.Errorf("recreated locationId = %v, want nam5", rein["locationId"])
	}
	if rein["uid"] == cresp["uid"] {
		t.Errorf("recreated uid = %v, want a fresh one", rein["uid"])
	}
	if _, has := rein["deleteTime"]; has {
		t.Errorf("recreated record carries deleteTime: %v", rein["deleteTime"])
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

// TestRESTDatabaseCmekConfig pins Database.cmek_config on the JSON wire (TGM11).
// The create body carries the block, and every response that renders a Database
// (the terminal operation's response, get, list, patch) echoes it. A plain
// create has no cmekConfig member at all — presence is the enablement signal, so
// an empty/fabricated block would wrongly claim CMEK. The block is create-only:
// mask `cmekConfig` resending the key is a no-op, a different key is
// INVALID_ARGUMENT, `*` keeps a stored block, and a body-supplied
// activeKeyVersion is never read as authoritative state (OUTPUT_ONLY).
func TestRESTDatabaseCmekConfig(t *testing.T) {
	const key = "projects/proj/locations/us-central1/keyRings/r/cryptoKeys/k"
	ctx := context.Background()
	p := newAdminProvider()

	// Create with the block: the terminal operation's response is the Database
	// the provider reads its state from after the apply.
	created, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "cmek-db",
		"body": map[string]any{
			"locationId": "us-central1",
			"cmekConfig": map[string]any{
				"kmsKeyName": key,
				// OUTPUT_ONLY; a client-supplied list is not stored.
				"activeKeyVersion": []any{"1"},
			},
		},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase(cmekConfig): %v", err)
	}
	cresp, _ := created.Data["response"].(map[string]any)
	crespCmek, _ := cresp["cmekConfig"].(map[string]any)
	if crespCmek["kmsKeyName"] != key {
		t.Errorf("create response cmekConfig = %v, want kmsKeyName %q", cresp["cmekConfig"], key)
	}
	if _, ok := crespCmek["activeKeyVersion"]; ok {
		t.Errorf("create response echoed the OUTPUT_ONLY activeKeyVersion = %v", crespCmek["activeKeyVersion"])
	}

	// GET echoes the stored block.
	got, err := p.GetDatabase(ctx, adminNR("databases/cmek-db", nil))
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	gotCmek, _ := got.Data["cmekConfig"].(map[string]any)
	if gotCmek["kmsKeyName"] != key {
		t.Errorf("GET cmekConfig = %v, want kmsKeyName %q", got.Data["cmekConfig"], key)
	}
	if _, ok := gotCmek["activeKeyVersion"]; ok {
		t.Errorf("GET echoed activeKeyVersion = %v, want it omitted", gotCmek["activeKeyVersion"])
	}

	// LIST echoes it, and only for the CMEK database.
	list, err := p.ListDatabases(ctx, adminNR("databases", nil))
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	dbs, _ := list.Data["databases"].([]any)
	var seen bool
	for _, raw := range dbs {
		db, _ := raw.(map[string]any)
		cmek, hasCmek := db["cmekConfig"].(map[string]any)
		switch db["name"] {
		case "projects/proj/databases/cmek-db":
			seen = true
			if !hasCmek || cmek["kmsKeyName"] != key {
				t.Errorf("list cmek-db cmekConfig = %v, want kmsKeyName %q", db["cmekConfig"], key)
			}
		default:
			if hasCmek {
				t.Errorf("list fabricated a cmekConfig for %v: %v", db["name"], db["cmekConfig"])
			}
		}
	}
	if !seen {
		t.Fatal("ListDatabases omitted cmek-db")
	}

	// A plain database has no cmekConfig member in any response.
	plain, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "plain-db",
		"body":       map[string]any{"locationId": "nam5"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase(plain): %v", err)
	}
	plainResp, _ := plain.Data["response"].(map[string]any)
	if _, ok := plainResp["cmekConfig"]; ok {
		t.Errorf("plain create response carries cmekConfig = %v", plainResp["cmekConfig"])
	}

	// The camelCase mask path arrives from the provider as `cmekConfig` and is
	// normalized to the proto path; resending the key is a no-op.
	same, err := p.UpdateDatabase(ctx, adminNR("databases/cmek-db", map[string]any{
		"updateMask": "cmekConfig",
		"body":       map[string]any{"cmekConfig": map[string]any{"kmsKeyName": key}},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase(cmekConfig, same key): %v", err)
	}
	sameResp, _ := same.Data["response"].(map[string]any)
	if cmek, _ := sameResp["cmekConfig"].(map[string]any); cmek["kmsKeyName"] != key {
		t.Errorf("resend changed cmekConfig = %v", sameResp["cmekConfig"])
	}

	// The nested path is the same field and behaves the same.
	if _, err := p.UpdateDatabase(ctx, adminNR("databases/cmek-db", map[string]any{
		"updateMask": "cmekConfig.kmsKeyName",
		"body":       map[string]any{"cmekConfig": map[string]any{"kmsKeyName": key}},
	})); err != nil {
		t.Fatalf("UpdateDatabase(cmekConfig.kmsKeyName, same key): %v", err)
	}

	// A different key is INVALID_ARGUMENT: the key of a CMEK-protected database
	// is fixed at creation.
	if _, err := p.UpdateDatabase(ctx, adminNR("databases/cmek-db", map[string]any{
		"updateMask": "cmekConfig",
		"body": map[string]any{
			"cmekConfig": map[string]any{
				"kmsKeyName": "projects/proj/locations/us-central1/keyRings/r/cryptoKeys/other",
			},
		},
	})); err != nil {
		assertRESTInvalidArgument(t, "cmek key change", err)
	} else {
		t.Error("cmek key change succeeded, want InvalidArgument")
	}

	// A present block without kms_key_name is how the gcloud CLI serializes an
	// unset CmekConfig (`"cmekConfig": {}` on every `firestore databases
	// create`). Real GCP treats it as "no CMEK": the create succeeds and the
	// operation response carries no block.
	empty, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "empty-cmek",
		"body": map[string]any{
			"type":       "FIRESTORE_NATIVE",
			"locationId": "nam5",
			"cmekConfig": map[string]any{},
		},
	}))
	if err != nil {
		t.Fatalf("create with an empty cmekConfig: %v", err)
	}
	emptyResp, _ := empty.Data["response"].(map[string]any)
	if block, has := emptyResp["cmekConfig"]; has {
		t.Errorf("create with an empty cmekConfig echoed %v, want no block", block)
	}
	// A non-object value is malformed JSON for the field.
	if _, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "bad-cmek",
		"body":       map[string]any{"cmekConfig": "projects/p/key"},
	})); err != nil {
		assertRESTInvalidArgument(t, "create with a non-object cmekConfig", err)
	} else {
		t.Error("create with a non-object cmekConfig succeeded, want InvalidArgument")
	}

	// An explicit `*` replace whose body omits cmekConfig preserves the block.
	starred, err := p.UpdateDatabase(ctx, adminNR("databases/cmek-db", map[string]any{
		"updateMask": "*",
		"body":       map[string]any{"pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_ENABLED"},
	}))
	if err != nil {
		t.Fatalf("UpdateDatabase(*): %v", err)
	}
	starredResp, _ := starred.Data["response"].(map[string]any)
	if cmek, _ := starredResp["cmekConfig"].(map[string]any); cmek["kmsKeyName"] != key {
		t.Errorf("`*` patch cleared cmekConfig = %v", starredResp["cmekConfig"])
	}
	if starredResp["pointInTimeRecoveryEnablement"] != "POINT_IN_TIME_RECOVERY_ENABLED" {
		t.Errorf("`*` patch did not apply pointInTimeRecoveryEnablement = %v", starredResp["pointInTimeRecoveryEnablement"])
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

// ─── Change streams ──────────────────────────────────────────────────────────

// assertProviderCode asserts the wire code a REST handler answers.
func assertProviderCode(t *testing.T, err error, wantCode string, wantStatus int) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.Code != wantCode || pe.HTTPStatus != wantStatus {
		t.Fatalf("error = %s/%d (%v), want %s/%d", pe.Code, pe.HTTPStatus, err, wantCode, wantStatus)
	}
}

func TestRESTChangeStreamLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	base := "databases/(default)/changeStreams"

	created, err := p.CreateChangeStream(ctx, adminNR(base, map[string]any{
		"changeStreamId": "orders-stream",
		"body": map[string]any{
			"retentionPeriod":      "172800s",
			"collectionGroupScope": map[string]any{"collectionGroupId": "orders"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateChangeStream: %v", err)
	}
	if want := "projects/proj/databases/(default)/changeStreams/orders-stream"; created.Data["name"] != want {
		t.Errorf("name = %v, want %s", created.Data["name"], want)
	}
	if created.Data["retentionPeriod"] != "172800s" {
		t.Errorf("retentionPeriod = %v, want 172800s", created.Data["retentionPeriod"])
	}
	if cg, _ := created.Data["collectionGroupScope"].(map[string]any); cg["collectionGroupId"] != "orders" {
		t.Errorf("collectionGroupScope = %v, want orders", created.Data["collectionGroupScope"])
	}
	// Server-owned fields: an etag for the delete precondition, timestamps for
	// the record's lifetime; start_time is deliberately absent (no feed).
	for _, field := range []string{"etag", "createTime", "updateTime"} {
		if v, _ := created.Data[field].(string); v == "" {
			t.Errorf("%s is empty on create, want a server-assigned value", field)
		}
	}
	if _, ok := created.Data["startTime"]; ok {
		t.Errorf("startTime = %v, want unset: the emulator records no events", created.Data["startTime"])
	}

	got, err := p.GetChangeStream(ctx, adminNR(base+"/orders-stream", nil))
	if err != nil {
		t.Fatalf("GetChangeStream: %v", err)
	}
	if got.Data["name"] != created.Data["name"] || got.Data["etag"] != created.Data["etag"] {
		t.Errorf("GetChangeStream = %v, want the record created", got.Data)
	}

	list, err := p.ListChangeStreams(ctx, adminNR(base, nil))
	if err != nil {
		t.Fatalf("ListChangeStreams: %v", err)
	}
	items, _ := list.Data["changeStreams"].([]any)
	if len(items) != 1 {
		t.Fatalf("ListChangeStreams len = %d, want 1", len(items))
	}

	// A second database-scoped stream is documented as disallowed.
	if _, err := p.CreateChangeStream(ctx, adminNR(base, map[string]any{
		"changeStreamId": "db-scope-one",
		"body":           map[string]any{"retentionPeriod": "86400s", "databaseScope": map[string]any{}},
	})); err != nil {
		t.Fatalf("CreateChangeStream(databaseScope): %v", err)
	}
	_, err = p.CreateChangeStream(ctx, adminNR(base, map[string]any{
		"changeStreamId": "db-scope-two",
		"body":           map[string]any{"retentionPeriod": "86400s", "databaseScope": map[string]any{}},
	}))
	assertProviderCode(t, err, "AlreadyExists", 409)

	// Deterministic listing: sorted by name.
	list, err = p.ListChangeStreams(ctx, adminNR(base, nil))
	if err != nil {
		t.Fatalf("ListChangeStreams: %v", err)
	}
	items, _ = list.Data["changeStreams"].([]any)
	if len(items) != 2 {
		t.Fatalf("ListChangeStreams len = %d, want 2", len(items))
	}
	first, _ := items[0].(map[string]any)
	if first["name"] != "projects/proj/databases/(default)/changeStreams/db-scope-one" {
		t.Errorf("first list item = %v, want the name-sorted db-scope-one", first["name"])
	}

	// The delete etag is a precondition.
	_, err = p.DeleteChangeStream(ctx, adminNR(base+"/orders-stream", map[string]any{"etag": "stale"}))
	assertDeletePrecondition(t, err, "stale-etag change stream")

	if _, err := p.DeleteChangeStream(ctx, adminNR(base+"/orders-stream", map[string]any{
		"etag": created.Data["etag"],
	})); err != nil {
		t.Fatalf("DeleteChangeStream: %v", err)
	}
	if _, err := p.GetChangeStream(ctx, adminNR(base+"/orders-stream", nil)); err == nil {
		t.Error("GetChangeStream after delete succeeded, want NotFound")
	} else {
		assertProviderCode(t, err, "NotFound", 404)
	}
	if _, err := p.DeleteChangeStream(ctx, adminNR(base+"/orders-stream", nil)); err == nil {
		t.Error("DeleteChangeStream after delete succeeded, want NotFound")
	} else {
		assertProviderCode(t, err, "NotFound", 404)
	}
}

func TestRESTChangeStreamDuplicateID(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()
	base := "databases/(default)/changeStreams"

	if _, err := p.CreateChangeStream(ctx, adminNR(base, map[string]any{
		"changeStreamId": "dup-stream",
		"body":           map[string]any{"retentionPeriod": "86400s", "databaseScope": map[string]any{}},
	})); err != nil {
		t.Fatalf("CreateChangeStream: %v", err)
	}
	// Same id again, with a different scope so the per-scope uniqueness rule is
	// not what fires: the record's existence is.
	_, err := p.CreateChangeStream(ctx, adminNR(base, map[string]any{
		"changeStreamId": "dup-stream",
		"body": map[string]any{
			"retentionPeriod":      "86400s",
			"collectionGroupScope": map[string]any{"collectionGroupId": "orders"},
		},
	}))
	assertProviderCode(t, err, "AlreadyExists", 409)
}

func TestRESTChangeStreamValidation(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()
	base := "databases/(default)/changeStreams"
	valid := map[string]any{"retentionPeriod": "86400s", "databaseScope": map[string]any{}}

	for _, id := range []string{
		"",                 // required
		"abc",              // 3 chars, below the 4-char floor
		"UPPER-case",       // uppercase
		"-leading-hyphen",  // first character must be a letter
		"trailing-hyphen-", // last character must be a letter or digit
		"has_underscore",   // underscore is not in the alphabet
	} {
		_, err := p.CreateChangeStream(ctx, adminNR(base, map[string]any{"changeStreamId": id, "body": valid}))
		if id == "" {
			// An absent changeStreamId with no body name is invalid, not a
			// generated id: the Discovery document marks it Required.
			assertProviderCode(t, err, "InvalidArgument", 400)
			continue
		}
		assertProviderCode(t, err, "InvalidArgument", 400)
	}

	// A 63-character id is the documented maximum and is accepted.
	longID := "a" + strings.Repeat("b", 61) + "c"
	if _, err := p.CreateChangeStream(ctx, adminNR(base, map[string]any{
		"changeStreamId": longID,
		"body":           valid,
	})); err != nil {
		t.Fatalf("CreateChangeStream(63-char id): %v", err)
	}

	// Retention period: required, 1-7 days, whole days.
	for _, body := range []map[string]any{
		{"databaseScope": map[string]any{}},                               // missing
		{"retentionPeriod": "3600s", "databaseScope": map[string]any{}},   // under a day
		{"retentionPeriod": "691200s", "databaseScope": map[string]any{}}, // over 7 days
		{"retentionPeriod": "129600s", "databaseScope": map[string]any{}}, // 1.5 days
		{"retentionPeriod": "86400s"},                                     // no scope
		{"retentionPeriod": "86400s", "databaseScope": map[string]any{},
			"collectionGroupScope": map[string]any{"collectionGroupId": "orders"}}, // both scopes
		{"retentionPeriod": "86400s",
			"collectionGroupScope": map[string]any{}}, // empty collection group
	} {
		_, err := p.CreateChangeStream(ctx, adminNR(base, map[string]any{
			"changeStreamId": "validated-" + strconv.Itoa(len(body)),
			"body":           body,
		}))
		if err == nil {
			t.Errorf("CreateChangeStream(%v) succeeded, want InvalidArgument", body)
			continue
		}
		assertProviderCode(t, err, "InvalidArgument", 400)
	}

	// A malformed resource name is InvalidArgument on every single-resource verb.
	for _, name := range []string{
		"databases/(default)/changeStreams",
		"databases/(default)/changeStreams/x/extra",
		"databases/(default)/userCreds/orders-stream",
	} {
		if _, err := p.GetChangeStream(ctx, adminNR(name, nil)); err == nil {
			t.Errorf("GetChangeStream(%q) succeeded, want InvalidArgument", name)
		} else {
			assertProviderCode(t, err, "InvalidArgument", 400)
		}
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

// TestRESTExplicitStarReplaces covers DC50: an explicit updateMask=* is AIP-134
// full replacement, so mutable fields the request omits are cleared rather than
// left at their stored value. The implied mask (no updateMask parameter) keeps
// its populated-field merge semantics.
func TestRESTExplicitStarReplaces(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	if _, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "star-db",
		"body":       map[string]any{"locationId": "nam5"},
	})); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if _, err := p.UpdateDatabase(ctx, adminNR("databases/star-db", map[string]any{
		"updateMask": "concurrencyMode,appEngineIntegrationMode",
		"body":       map[string]any{"concurrencyMode": "PESSIMISTIC", "appEngineIntegrationMode": "ENABLED"},
	})); err != nil {
		t.Fatalf("UpdateDatabase(seed): %v", err)
	}
	if _, err := p.UpdateDatabase(ctx, adminNR("databases/star-db", map[string]any{
		"updateMask": "*",
		"body":       map[string]any{"concurrencyMode": "OPTIMISTIC"},
	})); err != nil {
		t.Fatalf("UpdateDatabase(*): %v", err)
	}
	db, err := p.GetDatabase(ctx, adminNR("databases/star-db", nil))
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if db.Data["concurrencyMode"] != "OPTIMISTIC" {
		t.Errorf("concurrencyMode = %v, want OPTIMISTIC (supplied field)", db.Data["concurrencyMode"])
	}
	if db.Data["appEngineIntegrationMode"] != "DISABLED" {
		t.Errorf("appEngineIntegrationMode = %v, want DISABLED (omitted mutable field cleared)", db.Data["appEngineIntegrationMode"])
	}

	if _, err := p.UpdateField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", map[string]any{
		"updateMask": "ttlConfig",
		"body":       map[string]any{"ttlConfig": map[string]any{"expirationOffset": "3600s"}},
	})); err != nil {
		t.Fatalf("UpdateField(seed ttl): %v", err)
	}
	if _, err := p.UpdateField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", map[string]any{
		"updateMask": "*",
		"body":       map[string]any{"indexConfig": map[string]any{}},
	})); err != nil {
		t.Fatalf("UpdateField(*): %v", err)
	}
	field, err := p.GetField(ctx, adminNR("databases/(default)/collectionGroups/cg/fields/f", nil))
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if _, stored := field.Data["ttlConfig"]; stored {
		t.Errorf("ttlConfig = %v, want cleared by an explicit * replace", field.Data["ttlConfig"])
	}

	if _, err := p.CreateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules", map[string]any{
		"backupScheduleId": "1",
		"body":             map[string]any{"retention": "604800s", "dailyRecurrence": map[string]any{}},
	})); err != nil {
		t.Fatalf("CreateBackupSchedule: %v", err)
	}
	if _, err := p.UpdateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", map[string]any{
		"updateMask": "*",
		"body":       map[string]any{"retention": "1209600s"},
	})); err == nil {
		t.Error("UpdateBackupSchedule(*) without a recurrence succeeded, want InvalidArgument")
	}
	bs, err := p.UpdateBackupSchedule(ctx, adminNR("databases/(default)/backupSchedules/1", map[string]any{
		"updateMask": "*",
		"body":       map[string]any{"retention": "1209600s", "weeklyRecurrence": map[string]any{"day": "WEDNESDAY"}},
	}))
	if err != nil {
		t.Fatalf("UpdateBackupSchedule(*): %v", err)
	}
	if bs.Data["retention"] != "1209600s" {
		t.Errorf("retention = %v, want 1209600s", bs.Data["retention"])
	}
	if _, daily := bs.Data["dailyRecurrence"]; daily {
		t.Errorf("dailyRecurrence survived an explicit * replace: %v", bs.Data["dailyRecurrence"])
	}
	if _, weekly := bs.Data["weeklyRecurrence"]; !weekly {
		t.Errorf("weeklyRecurrence = %v, want present", bs.Data["weeklyRecurrence"])
	}
}

// TestRESTDatabaseEnumValidation pins DC48: every enum-typed Database field is
// checked against the Discovery enum set at the REST boundary, so an unknown
// proto3 JSON enum name is INVALID_ARGUMENT instead of being stored/echoed.
func TestRESTDatabaseEnumValidation(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	cases := []struct{ field, value string }{
		{"type", "BOGUS"},
		{"concurrencyMode", "BOGUS"},
		{"appEngineIntegrationMode", "BOGUS"},
		{"pointInTimeRecoveryEnablement", "BOGUS"},
		{"deleteProtectionState", "BOGUS"},
		{"databaseEdition", "BOGUS"},
	}
	for _, tc := range cases {
		// Create rejects before the record is written.
		_, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
			"databaseId": "enum-db",
			"body":       map[string]any{tc.field: tc.value},
		}))
		assertRESTInvalidArgument(t, "CreateDatabase."+tc.field, err)

		// Patch rejects too, before the mask/record is applied.
		_, err = p.UpdateDatabase(ctx, adminNR("databases/(default)", map[string]any{
			"updateMask": tc.field,
			"body":       map[string]any{tc.field: tc.value},
		}))
		assertRESTInvalidArgument(t, "UpdateDatabase."+tc.field, err)
	}

	// Enum names are case-sensitive: a near-miss is still INVALID_ARGUMENT.
	if _, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "enum-db",
		"body": map[string]any{
			"pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_DISABLEd",
		},
	})); err == nil {
		t.Fatal("CreateDatabase accepted a misspelled enum name (DISABLEd), want InvalidArgument")
	}

	// The documented enum names (including the unspecified zero values) are
	// accepted.
	if _, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "enum-db",
		"body": map[string]any{
			"type":                          "DATABASE_TYPE_UNSPECIFIED",
			"concurrencyMode":               "OPTIMISTIC_WITH_ENTITY_GROUPS",
			"appEngineIntegrationMode":      "APP_ENGINE_INTEGRATION_MODE_UNSPECIFIED",
			"pointInTimeRecoveryEnablement": "POINT_IN_TIME_RECOVERY_DISABLED",
			"deleteProtectionState":         "DELETE_PROTECTION_STATE_UNSPECIFIED",
			"databaseEdition":               "DATABASE_EDITION_UNSPECIFIED",
		},
	})); err != nil {
		t.Fatalf("CreateDatabase with documented enum names: %v", err)
	}
}

func assertRESTInvalidArgument(t *testing.T, label string, err error) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
		t.Errorf("%s error = %v, want InvalidArgument/400", label, err)
	}
}

// TestRESTDeleteDatabaseRequiresQueryEtag pins DC47: DeleteDatabaseRequest.etag
// is a query parameter in the Discovery HTTP binding, so a body-only etag must
// not satisfy the precondition (or delete the database).
func TestRESTDeleteDatabaseRequiresQueryEtag(t *testing.T) {
	ctx := context.Background()
	p := newAdminProvider()

	created, err := p.CreateDatabase(ctx, adminNR("databases", map[string]any{
		"databaseId": "etag-db",
		"body":       map[string]any{"locationId": "nam5"},
	}))
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	resp, _ := created.Data["response"].(map[string]any)
	etag, _ := resp["etag"].(string)
	if etag == "" {
		t.Fatal("created database carries no etag")
	}

	// Body etag, no query parameter: the non-compliant request fails and the
	// database survives.
	_, err = p.DeleteDatabase(ctx, adminNR("databases/etag-db", map[string]any{
		"body": map[string]any{"etag": etag},
	}))
	assertRESTInvalidArgument(t, "DeleteDatabase(body etag)", err)
	if _, err := p.GetDatabase(ctx, adminNR("databases/etag-db", nil)); err != nil {
		t.Errorf("body-etag delete removed the database: %v", err)
	}

	// The documented query parameter form succeeds.
	if _, err := p.DeleteDatabase(ctx, adminNR("databases/etag-db", map[string]any{"etag": etag})); err != nil {
		t.Fatalf("DeleteDatabase(query etag): %v", err)
	}
	if _, err := p.GetDatabase(ctx, adminNR("databases/etag-db", nil)); err == nil {
		t.Error("GetDatabase after delete succeeded, want NotFound")
	}
}
