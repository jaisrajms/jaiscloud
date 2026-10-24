package firestore

// Firestore Admin control-plane REST adapter
// (firestore.googleapis.com/v1/projects/{p}/databases/...): databases,
// collection-group fields, backup schedules, backups and user creds. All
// domain logic lives in the transport-neutral Service (admin.go); this file is
// the JSON wire adapter plus the REST path parsing, mirroring the composite-index
// handlers in firestore.go and the gRPC transcoder in
// internal/gcp/grpc/firestoreadmin.
//
// The REST surface is served under the `firestore` wire service (real Firestore
// serves the admin control plane on firestore.googleapis.com); the distinct
// google.firestore.admin.v1.FirestoreAdmin gRPC service is served by
// internal/gcp/grpc/firestoreadmin. Both share this package's Service, so the
// two transports see one resource store.

import (
	"context"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// ─── REST path parsing (relative to projects/{project}) ───────────────────────

// splitDatabasePath parses "databases" (collection) or "databases/{db}".
func splitDatabasePath(name string) (db string, isCollection, ok bool) {
	parts := strings.Split(name, "/")
	switch {
	case len(parts) == 1 && parts[0] == "databases":
		return "", true, true
	case len(parts) == 2 && parts[0] == "databases" && parts[1] != "":
		return parts[1], false, true
	}
	return "", false, false
}

// splitBackupSchedulePath parses
// "databases/{db}/backupSchedules" | "databases/{db}/backupSchedules/{id}".
func splitBackupSchedulePath(name string) (db, id string, isCollection, ok bool) {
	parts := strings.Split(name, "/")
	switch {
	case len(parts) == 3 && parts[0] == "databases" && parts[2] == "backupSchedules":
		return parts[1], "", true, true
	case len(parts) == 4 && parts[0] == "databases" && parts[2] == "backupSchedules" && parts[3] != "":
		return parts[1], parts[3], false, true
	}
	return "", "", false, false
}

// splitUserCredsPath parses
// "databases/{db}/userCreds" | "databases/{db}/userCreds/{id}".
func splitUserCredsPath(name string) (db, id string, isCollection, ok bool) {
	parts := strings.Split(name, "/")
	switch {
	case len(parts) == 3 && parts[0] == "databases" && parts[2] == "userCreds":
		return parts[1], "", true, true
	case len(parts) == 4 && parts[0] == "databases" && parts[2] == "userCreds" && parts[3] != "":
		return parts[1], parts[3], false, true
	}
	return "", "", false, false
}

// splitFieldPath parses
// "databases/{db}/collectionGroups/{cg}/fields" (collection) or ".../fields/{fieldPath}".
func splitFieldPath(name string) (db, cg, fieldPath string, isCollection, ok bool) {
	parts := strings.Split(name, "/")
	switch {
	case len(parts) == 5 && parts[0] == "databases" && parts[2] == "collectionGroups" && parts[4] == "fields":
		return parts[1], parts[3], "", true, true
	case len(parts) == 6 && parts[0] == "databases" && parts[2] == "collectionGroups" &&
		parts[4] == "fields" && parts[5] != "":
		return parts[1], parts[3], parts[5], false, true
	}
	return "", "", "", false, false
}

// splitBackupPath parses "locations/{loc}/backups" | "locations/{loc}/backups/{id}".
func splitBackupPath(name string) (loc, id string, isCollection, ok bool) {
	parts := strings.Split(name, "/")
	switch {
	case len(parts) == 3 && parts[0] == "locations" && parts[2] == "backups":
		return parts[1], "", true, true
	case len(parts) == 4 && parts[0] == "locations" && parts[2] == "backups" && parts[3] != "":
		return parts[1], parts[3], false, true
	}
	return "", "", false, false
}

// splitDatabaseOperationPath parses
// "databases/{db}/operations" | "databases/{db}/operations/{op}".
func splitDatabaseOperationPath(name string) (db, op string, isCollection, ok bool) {
	parts := strings.Split(name, "/")
	switch {
	case len(parts) == 3 && parts[0] == "databases" && parts[2] == "operations":
		return parts[1], "", true, true
	case len(parts) == 4 && parts[0] == "databases" && parts[2] == "operations" && parts[3] != "":
		return parts[1], parts[3], false, true
	}
	return "", "", false, false
}

// ─── query/body helpers ───────────────────────────────────────────────────────

// adminMask returns the update_mask field paths, normalized to the snake_case
// proto paths the domain's apply*Mask functions key on, plus whether the mask
// was present at all. Google's HTTP transcoding (and the conformance probe)
// send the FieldMask as a single comma-separated `updateMask` query parameter
// using the JSON (lowerCamelCase) field names; older clients use repeated
// `updateMask.fieldPaths`. Both forms are accepted and converted. An
// `updateMask=` query parameter (or an empty `updateMask.fieldPaths`) counts as
// present-but-empty, which the domain treats as AIP-134's no-op rather than as
// the omitted mask.
func adminMask(nr *model.NormalizedRequest) (mask []string, present bool) {
	raw := splitMaskValue(firstParam(nr, "updateMask"))
	if len(raw) == 0 {
		raw = updateMaskPaths(nr)
	}
	if maskParamPresent(nr, "updateMask") || maskParamPresent(nr, "updateMask.fieldPaths") {
		present = true
	}
	out := make([]string, 0, len(raw))
	for _, p := range raw {
		out = append(out, protoMaskPath(p))
	}
	return out, present
}

// firstParam returns a query parameter's first value, preferring the parsed
// request URL over the decoded Params map so the two agree on repeated and
// empty values (mirroring queryFieldPaths).
func firstParam(nr *model.NormalizedRequest, key string) string {
	if nr.Raw != nil {
		if vs := nr.Raw.URL.Query()[key]; len(vs) > 0 {
			return vs[0]
		}
	}
	s, _ := nr.Params[key].(string)
	return s
}

// maskParamPresent reports whether a query parameter was supplied at all,
// including with an empty value (`?updateMask=`).
func maskParamPresent(nr *model.NormalizedRequest, key string) bool {
	if _, ok := nr.Params[key]; ok {
		return true
	}
	if nr.Raw != nil {
		_, ok := nr.Raw.URL.Query()[key]
		return ok
	}
	return false
}

// splitMaskValue splits a comma-separated updateMask query value.
func splitMaskValue(v string) []string {
	var paths []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			paths = append(paths, part)
		}
	}
	return paths
}

// protoMaskPath converts a JSON (lowerCamelCase) field-mask path to its
// snake_case proto form ("concurrencyMode" -> "concurrency_mode",
// "indexConfig.indexes" -> "index_config.indexes"). The wildcard "*" and
// already-snake paths are returned unchanged.
func protoMaskPath(p string) string {
	if p == "*" {
		return p
	}
	return strings.Join(mapSegments(strings.Split(p, "."), camelToSnake), ".")
}

func mapSegments(segs []string, f func(string) string) []string {
	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = f(s)
	}
	return out
}

// camelToSnake lowercases a lowerCamelCase segment and inserts an underscore
// before each interior uppercase letter.
func camelToSnake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r + ('a' - 'A'))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// strParam reads a string parameter (query or otherwise).
func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// bodyMap returns the decoded JSON request body as a map (empty when absent).
func bodyMap(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

// restDuration renders a duration as a google.protobuf.Duration string ("3600s").
func restDuration(d time.Duration) string {
	if d == 0 {
		return ""
	}
	secs := d / time.Second
	nanos := d % time.Second
	if nanos == 0 {
		return strconv.FormatInt(int64(secs), 10) + "s"
	}
	return strconv.FormatInt(int64(secs), 10) + "." + strings.TrimRight(padNanos(int64(nanos)), "0") + "s"
}

func padNanos(n int64) string {
	s := strconv.FormatInt(n, 10)
	for len(s) < 9 {
		s = "0" + s
	}
	return s
}

// parseRestDuration parses a google.protobuf.Duration string ("3600s"). An empty
// or unparseable value yields 0 (the domain treats 0 as "unset").
func parseRestDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

// restRFC3339 renders a timestamp in the protojson spelling of a
// google-datetime field (UTC, 0/3/6-digit fraction groups, microseconds).
func restRFC3339(t time.Time) string { return timestamp.Canonical(t) }

// lroResponse renders a terminal google.longrunning.Operation carrying a typed
// response, matching the grpc-gateway transcode (an Any with @type).
func lroResponse(opName, typeURL string, resource map[string]any) map[string]any {
	resp := map[string]any{"@type": "type.googleapis.com/" + typeURL}
	for k, v := range resource {
		resp[k] = v
	}
	return map[string]any{"name": opName, "done": true, "response": resp}
}

// emptyOK is the 200 Empty body the REST deletes return.
func emptyOK() *model.ProviderResponse {
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}
}

// ─── Database wire mapping ────────────────────────────────────────────────────

func databaseMap(d databaseDef) map[string]any {
	m := map[string]any{"name": d.Name}
	if d.Uid != "" {
		m["uid"] = d.Uid
	}
	if !d.CreateTime.IsZero() {
		m["createTime"] = restRFC3339(d.CreateTime)
	}
	if !d.UpdateTime.IsZero() {
		m["updateTime"] = restRFC3339(d.UpdateTime)
	}
	if !d.DeleteTime.IsZero() {
		m["deleteTime"] = restRFC3339(d.DeleteTime)
	}
	if d.LocationID != "" {
		m["locationId"] = d.LocationID
	}
	if d.Type != "" {
		m["type"] = d.Type
	}
	if d.ConcurrencyMode != "" {
		m["concurrencyMode"] = d.ConcurrencyMode
	}
	if d.AppEngineIntegrationMode != "" {
		m["appEngineIntegrationMode"] = d.AppEngineIntegrationMode
	}
	if d.PointInTimeRecoveryEnablement != "" {
		m["pointInTimeRecoveryEnablement"] = d.PointInTimeRecoveryEnablement
	}
	if d.DeleteProtectionState != "" {
		m["deleteProtectionState"] = d.DeleteProtectionState
	}
	if d.DatabaseEdition != "" {
		m["databaseEdition"] = d.DatabaseEdition
	}
	if d.KeyPrefix != "" {
		m["keyPrefix"] = d.KeyPrefix
	}
	if d.VersionRetentionPeriod > 0 {
		m["versionRetentionPeriod"] = restDuration(d.VersionRetentionPeriod)
	}
	// earliest_version_time is OUTPUT_ONLY and continuously updated
	// (database.proto: "populated with `now - version_retention_period`"), so it
	// is derived per response instead of being stored (DC54).
	m["earliestVersionTime"] = restRFC3339(EarliestVersionTime(d))
	if d.Etag != "" {
		m["etag"] = d.Etag
	}
	return m
}

// databaseFromREST reads the mutable Database fields from a request body.
func databaseFromREST(body map[string]any) databaseDef {
	d := databaseDef{}
	if s, _ := body["locationId"].(string); s != "" {
		d.LocationID = s
	}
	if s, _ := body["type"].(string); s != "" {
		d.Type = s
	}
	if s, _ := body["concurrencyMode"].(string); s != "" {
		d.ConcurrencyMode = s
	}
	if s, _ := body["appEngineIntegrationMode"].(string); s != "" {
		d.AppEngineIntegrationMode = s
	}
	if s, _ := body["pointInTimeRecoveryEnablement"].(string); s != "" {
		d.PointInTimeRecoveryEnablement = s
	}
	if s, _ := body["deleteProtectionState"].(string); s != "" {
		d.DeleteProtectionState = s
	}
	if s, _ := body["databaseEdition"].(string); s != "" {
		d.DatabaseEdition = s
	}
	if s, _ := body["keyPrefix"].(string); s != "" {
		d.KeyPrefix = s
	}
	if s, _ := body["etag"].(string); s != "" {
		d.Etag = s
	}
	if s, _ := body["versionRetentionPeriod"].(string); s != "" {
		d.VersionRetentionPeriod = parseRestDuration(s)
	}
	return d
}

// ─── Database handlers ────────────────────────────────────────────────────────

// CreateDatabase implements projects.databases.create.
func (p *Provider) CreateDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, isCollection, ok := splitDatabasePath(strParam(nr, "name"))
	if !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database parent path", 400)
	}
	db, opName, err := p.Service.CreateDatabaseDef(ctx, nr.AccountID, strParam(nr, "databaseId"), databaseFromREST(bodyMap(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(lroResponse(opName, "google.firestore.admin.v1.Database", databaseMap(db))), nil
}

// ListDatabases implements projects.databases.list.
func (p *Provider) ListDatabases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, isCollection, ok := splitDatabasePath(strParam(nr, "name"))
	if !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database parent path", 400)
	}
	dbs, err := p.Service.ListDatabases(ctx, nr.AccountID, boolParam(nr, "showDeleted"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(dbs))
	for _, d := range dbs {
		items = append(items, databaseMap(d))
	}
	return provider.OK(map[string]any{"databases": items}), nil
}

// GetDatabase implements projects.databases.get.
func (p *Provider) GetDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, isCollection, ok := splitDatabasePath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database resource name", 400)
	}
	rec, err := p.Service.GetDatabase(ctx, nr.AccountID, db)
	if err != nil {
		return nil, err
	}
	return provider.OK(databaseMap(rec)), nil
}

// UpdateDatabase implements projects.databases.patch.
func (p *Provider) UpdateDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, isCollection, ok := splitDatabasePath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database resource name", 400)
	}
	patch := databaseFromREST(bodyMap(nr))
	mask, maskPresent := adminMask(nr)
	rec, opName, err := p.Service.UpdateDatabaseDef(ctx, nr.AccountID, db, patch, mask, maskPresent, patch.Etag)
	if err != nil {
		return nil, err
	}
	return provider.OK(lroResponse(opName, "google.firestore.admin.v1.Database", databaseMap(rec))), nil
}

// DeleteDatabase implements projects.databases.delete.
func (p *Provider) DeleteDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, isCollection, ok := splitDatabasePath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database resource name", 400)
	}
	etag := strParam(nr, "etag")
	if etag == "" {
		if s, _ := bodyMap(nr)["etag"].(string); s != "" {
			etag = s
		}
	}
	deleted, opName, err := p.Service.DeleteDatabaseDef(ctx, nr.AccountID, db, etag)
	if err != nil {
		return nil, err
	}
	return provider.OK(lroResponse(opName, "google.firestore.admin.v1.Database", databaseMap(deleted))), nil
}

// ─── Field wire mapping ───────────────────────────────────────────────────────

func fieldMap(f fieldDef) map[string]any {
	m := map[string]any{"name": f.Name}
	if f.IndexConfig != nil {
		// protojson omits an empty repeated field, so a field carrying no
		// indexes sends no `indexes` member rather than an empty array.
		ic := map[string]any{
			"usesAncestorConfig": f.IndexConfig.UsesAncestorConfig,
			"reverting":          f.IndexConfig.Reverting,
			"ancestorField":      f.IndexConfig.AncestorField,
		}
		if len(f.IndexConfig.Indexes) > 0 {
			indexes := make([]any, 0, len(f.IndexConfig.Indexes))
			for _, ix := range f.IndexConfig.Indexes {
				indexes = append(indexes, indexMap(ix))
			}
			ic["indexes"] = indexes
		}
		m["indexConfig"] = ic
	}
	if f.TtlConfig != nil {
		tc := map[string]any{}
		if f.TtlConfig.State != "" {
			tc["state"] = f.TtlConfig.State
		}
		if f.TtlConfig.ExpirationOffset > 0 {
			tc["expirationOffset"] = restDuration(f.TtlConfig.ExpirationOffset)
		}
		m["ttlConfig"] = tc
	}
	return m
}

// fieldFromREST reads a Field's index/ttl configuration from a request body.
func fieldFromREST(body map[string]any) fieldDef {
	f := fieldDef{}
	if ic, ok := body["indexConfig"].(map[string]any); ok && ic != nil {
		cfg := &fieldIndexConfig{}
		if b, ok := ic["usesAncestorConfig"].(bool); ok {
			cfg.UsesAncestorConfig = b
		}
		if s, _ := ic["ancestorField"].(string); s != "" {
			cfg.AncestorField = s
		}
		if b, ok := ic["reverting"].(bool); ok {
			cfg.Reverting = b
		}
		if arr, ok := ic["indexes"].([]any); ok {
			for _, raw := range arr {
				if im, ok := raw.(map[string]any); ok {
					cfg.Indexes = append(cfg.Indexes, indexFromREST(im))
				}
			}
		}
		f.IndexConfig = cfg
	}
	if tc, ok := body["ttlConfig"].(map[string]any); ok && tc != nil {
		cfg := &fieldTtlConfig{}
		if s, _ := tc["state"].(string); s != "" {
			cfg.State = s
		}
		if s, _ := tc["expirationOffset"].(string); s != "" {
			cfg.ExpirationOffset = parseRestDuration(s)
		}
		f.TtlConfig = cfg
	}
	return f
}

// indexFromREST reads one composite index from its REST object.
func indexFromREST(m map[string]any) indexDef {
	idx := indexDef{}
	if s, _ := m["queryScope"].(string); s != "" {
		idx.QueryScope = s
	}
	if arr, ok := m["fields"].([]any); ok {
		for _, raw := range arr {
			fm, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			field := indexField{}
			field.FieldPath, _ = fm["fieldPath"].(string)
			field.Order, _ = fm["order"].(string)
			field.ArrayConfig, _ = fm["arrayConfig"].(string)
			idx.Fields = append(idx.Fields, field)
		}
	}
	return idx
}

// ─── Field handlers ───────────────────────────────────────────────────────────

// ListFields implements projects.databases.collectionGroups.fields.list.
func (p *Provider) ListFields(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, cg, _, isCollection, ok := splitFieldPath(strParam(nr, "name"))
	if !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid collection group parent", 400)
	}
	page, err := pageFromNR(nr)
	if err != nil {
		return nil, err
	}
	fields, nextToken, err := p.Service.ListFields(ctx, nr.AccountID, db, cg, strParam(nr, "filter"), page)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(fields))
	for _, f := range fields {
		items = append(items, fieldMap(f))
	}
	resp := map[string]any{"fields": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

// GetField implements projects.databases.collectionGroups.fields.get.
func (p *Provider) GetField(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, cg, fieldPath, isCollection, ok := splitFieldPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid field resource name", 400)
	}
	f, err := p.Service.GetField(ctx, nr.AccountID, db, cg, fieldPath)
	if err != nil {
		return nil, err
	}
	return provider.OK(fieldMap(f)), nil
}

// UpdateField implements projects.databases.collectionGroups.fields.patch.
func (p *Provider) UpdateField(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, cg, fieldPath, isCollection, ok := splitFieldPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid field resource name", 400)
	}
	mask, maskPresent := adminMask(nr)
	f, opName, err := p.Service.UpdateFieldDef(ctx, nr.AccountID, db, cg, fieldPath, fieldFromREST(bodyMap(nr)), mask, maskPresent)
	if err != nil {
		return nil, err
	}
	return provider.OK(lroResponse(opName, "google.firestore.admin.v1.Field", fieldMap(f))), nil
}

// ─── User creds wire mapping ──────────────────────────────────────────────────

func userCredsMap(u userCredsDef) map[string]any {
	m := map[string]any{"name": u.Name}
	if !u.CreateTime.IsZero() {
		m["createTime"] = restRFC3339(u.CreateTime)
	}
	if !u.UpdateTime.IsZero() {
		m["updateTime"] = restRFC3339(u.UpdateTime)
	}
	if u.State != "" {
		m["state"] = u.State
	}
	if u.Principal != "" {
		m["resourceIdentity"] = map[string]any{"principal": u.Principal}
	}
	// The secret password is only ever surfaced on create/reset (the domain
	// clears it on get/list/enable/disable).
	if u.SecretPassword != "" {
		m["securePassword"] = u.SecretPassword
	}
	return m
}

// ─── User creds handlers ──────────────────────────────────────────────────────

// CreateUserCreds implements projects.databases.userCreds.create.
func (p *Provider) CreateUserCreds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, _, isCollection, ok := splitUserCredsPath(strParam(nr, "name"))
	if !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database parent path", 400)
	}
	uc, err := p.Service.CreateUserCredsDef(ctx, nr.AccountID, db, strParam(nr, "userCredsId"), UserCredsDef{})
	if err != nil {
		return nil, err
	}
	return provider.OK(userCredsMap(uc)), nil
}

// ListUserCreds implements projects.databases.userCreds.list.
func (p *Provider) ListUserCreds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, _, isCollection, ok := splitUserCredsPath(strParam(nr, "name"))
	if !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database parent path", 400)
	}
	ucs, err := p.Service.ListUserCreds(ctx, nr.AccountID, db)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(ucs))
	for _, uc := range ucs {
		items = append(items, userCredsMap(uc))
	}
	return provider.OK(map[string]any{"userCreds": items}), nil
}

// GetUserCreds implements projects.databases.userCreds.get.
func (p *Provider) GetUserCreds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, id, isCollection, ok := splitUserCredsPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid user creds resource name", 400)
	}
	uc, err := p.Service.GetUserCreds(ctx, nr.AccountID, db, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(userCredsMap(uc)), nil
}

// EnableUserCreds implements projects.databases.userCreds.enable.
func (p *Provider) EnableUserCreds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setUserCredsState(ctx, nr, "ENABLED")
}

// DisableUserCreds implements projects.databases.userCreds.disable.
func (p *Provider) DisableUserCreds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setUserCredsState(ctx, nr, "DISABLED")
}

func (p *Provider) setUserCredsState(ctx context.Context, nr *model.NormalizedRequest, state string) (*model.ProviderResponse, error) {
	db, id, isCollection, ok := splitUserCredsPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid user creds resource name", 400)
	}
	uc, err := p.Service.SetUserCredsState(ctx, nr.AccountID, db, id, state)
	if err != nil {
		return nil, err
	}
	return provider.OK(userCredsMap(uc)), nil
}

// ResetUserPassword implements projects.databases.userCreds.resetPassword.
func (p *Provider) ResetUserPassword(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, id, isCollection, ok := splitUserCredsPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid user creds resource name", 400)
	}
	uc, err := p.Service.ResetUserPassword(ctx, nr.AccountID, db, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(userCredsMap(uc)), nil
}

// DeleteUserCreds implements projects.databases.userCreds.delete.
func (p *Provider) DeleteUserCreds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, id, isCollection, ok := splitUserCredsPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid user creds resource name", 400)
	}
	if err := p.Service.DeleteUserCreds(ctx, nr.AccountID, db, id); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

// ─── Backup schedule wire mapping ─────────────────────────────────────────────

func backupScheduleMap(bs backupScheduleDef) map[string]any {
	m := map[string]any{"name": bs.Name}
	if !bs.CreateTime.IsZero() {
		m["createTime"] = restRFC3339(bs.CreateTime)
	}
	if !bs.UpdateTime.IsZero() {
		m["updateTime"] = restRFC3339(bs.UpdateTime)
	}
	if bs.Retention > 0 {
		m["retention"] = restDuration(bs.Retention)
	}
	switch bs.Recurrence {
	case "DAILY":
		m["dailyRecurrence"] = map[string]any{}
	case "WEEKLY":
		m["weeklyRecurrence"] = map[string]any{"day": dayOfWeekName(bs.DayOfWeek)}
	}
	return m
}

// dayOfWeekName maps an int to the google.type.DayOfWeek enum name.
func dayOfWeekName(v int) string {
	names := [...]string{"DAY_OF_WEEK_UNSPECIFIED", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"}
	if v >= 0 && v < len(names) {
		return names[v]
	}
	return "DAY_OF_WEEK_UNSPECIFIED"
}

// dayOfWeekNumber is the inverse of dayOfWeekName.
func dayOfWeekNumber(name string) int {
	switch name {
	case "MONDAY":
		return 1
	case "TUESDAY":
		return 2
	case "WEDNESDAY":
		return 3
	case "THURSDAY":
		return 4
	case "FRIDAY":
		return 5
	case "SATURDAY":
		return 6
	case "SUNDAY":
		return 7
	}
	return 0
}

// backupScheduleFromREST reads a backup schedule's mutable fields from a body.
func backupScheduleFromREST(body map[string]any) backupScheduleDef {
	bs := backupScheduleDef{}
	if s, _ := body["retention"].(string); s != "" {
		bs.Retention = parseRestDuration(s)
	}
	if _, ok := body["dailyRecurrence"].(map[string]any); ok {
		bs.Recurrence = "DAILY"
	}
	if wr, ok := body["weeklyRecurrence"].(map[string]any); ok {
		bs.Recurrence = "WEEKLY"
		if s, _ := wr["day"].(string); s != "" {
			bs.DayOfWeek = dayOfWeekNumber(s)
		}
	}
	return bs
}

// ─── Backup schedule handlers ─────────────────────────────────────────────────

// CreateBackupSchedule implements projects.databases.backupSchedules.create.
func (p *Provider) CreateBackupSchedule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, _, isCollection, ok := splitBackupSchedulePath(strParam(nr, "name"))
	if !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database parent path", 400)
	}
	bs, err := p.Service.CreateBackupScheduleDef(ctx, nr.AccountID, db, "1", backupScheduleFromREST(bodyMap(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(backupScheduleMap(bs)), nil
}

// ListBackupSchedules implements projects.databases.backupSchedules.list.
func (p *Provider) ListBackupSchedules(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, _, isCollection, ok := splitBackupSchedulePath(strParam(nr, "name"))
	if !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid database parent path", 400)
	}
	bss, err := p.Service.ListBackupSchedules(ctx, nr.AccountID, db)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(bss))
	for _, bs := range bss {
		items = append(items, backupScheduleMap(bs))
	}
	return provider.OK(map[string]any{"backupSchedules": items}), nil
}

// GetBackupSchedule implements projects.databases.backupSchedules.get.
func (p *Provider) GetBackupSchedule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, id, isCollection, ok := splitBackupSchedulePath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid backup schedule resource name", 400)
	}
	bs, err := p.Service.GetBackupSchedule(ctx, nr.AccountID, db, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(backupScheduleMap(bs)), nil
}

// UpdateBackupSchedule implements projects.databases.backupSchedules.patch.
func (p *Provider) UpdateBackupSchedule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, id, isCollection, ok := splitBackupSchedulePath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid backup schedule resource name", 400)
	}
	mask, maskPresent := adminMask(nr)
	bs, err := p.Service.UpdateBackupScheduleDef(ctx, nr.AccountID, db, id, backupScheduleFromREST(bodyMap(nr)), mask, maskPresent)
	if err != nil {
		return nil, err
	}
	return provider.OK(backupScheduleMap(bs)), nil
}

// DeleteBackupSchedule implements projects.databases.backupSchedules.delete.
func (p *Provider) DeleteBackupSchedule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, id, isCollection, ok := splitBackupSchedulePath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid backup schedule resource name", 400)
	}
	if err := p.Service.DeleteBackupSchedule(ctx, nr.AccountID, db, id); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

// ─── Backup wire mapping ──────────────────────────────────────────────────────

func backupMap(b backupDef) map[string]any {
	m := map[string]any{"name": b.Name}
	if b.Database != "" {
		m["database"] = b.Database
	}
	if b.DatabaseUid != "" {
		m["databaseUid"] = b.DatabaseUid
	}
	if !b.SnapshotTime.IsZero() {
		m["snapshotTime"] = restRFC3339(b.SnapshotTime)
	}
	if !b.ExpireTime.IsZero() {
		m["expireTime"] = restRFC3339(b.ExpireTime)
	}
	if b.State != "" {
		m["state"] = b.State
	}
	// A READY backup carries measured statistics, including zero counts for a
	// database that holds nothing yet.
	if b.State == "READY" || b.DocumentCount != 0 || b.IndexCount != 0 || b.SizeBytes != 0 {
		m["stats"] = map[string]any{
			"sizeBytes":     strconv.FormatInt(b.SizeBytes, 10),
			"documentCount": strconv.FormatInt(b.DocumentCount, 10),
			"indexCount":    strconv.FormatInt(b.IndexCount, 10),
		}
	}
	return m
}

// ─── Backup handlers ──────────────────────────────────────────────────────────

// ListBackups implements projects.locations.backups.list.
func (p *Provider) ListBackups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, _, isCollection, ok := splitBackupPath(strParam(nr, "name"))
	if !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid backup parent path", 400)
	}
	backups, err := p.Service.ListBackups(ctx, nr.AccountID, loc, strParam(nr, "filter"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(backups))
	for _, b := range backups {
		items = append(items, backupMap(b))
	}
	return provider.OK(map[string]any{"backups": items}), nil
}

// GetBackup implements projects.locations.backups.get.
func (p *Provider) GetBackup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, id, isCollection, ok := splitBackupPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid backup resource name", 400)
	}
	b, err := p.Service.GetBackup(ctx, nr.AccountID, loc, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(backupMap(b)), nil
}

// DeleteBackup implements projects.locations.backups.delete.
func (p *Provider) DeleteBackup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, id, isCollection, ok := splitBackupPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid backup resource name", 400)
	}
	if err := p.Service.DeleteBackup(ctx, nr.AccountID, loc, id); err != nil {
		return nil, err
	}
	return emptyOK(), nil
}

// ─── Long-running operations ──────────────────────────────────────────────────
//
// The Firestore Admin control plane is synchronous: every mutation returns a
// terminal (done=true) operation with its typed response inline, and no
// operation record is persisted. The projects.databases.operations family is
// therefore served statelessly — Get reports the requested name done, List is
// empty, and Delete/Cancel are no-ops — matching the shared
// google.longrunning.Operations lenient contract (and enough for a client that
// polls the operation name to terminate cleanly; real Terraform/gcloud
// firestore apply flows do).

// GetOperation implements projects.databases.operations.get.
func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	db, op, isCollection, ok := splitDatabaseOperationPath(strParam(nr, "name"))
	if !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid operation resource name", 400)
	}
	// Reconstruct the canonical operation name and report it terminal. There is
	// no stored body: the caller already received the typed response inline.
	return provider.OK(map[string]any{"name": databaseOperationName(nr.AccountID, db, op), "done": true}), nil
}

// ListOperations implements projects.databases.operations.list.
func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, _, isCollection, ok := splitDatabaseOperationPath(strParam(nr, "name")); !ok || !isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid operations parent path", 400)
	}
	return provider.OK(map[string]any{"operations": []any{}}), nil
}

// DeleteOperation implements projects.databases.operations.delete.
func (p *Provider) DeleteOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, _, isCollection, ok := splitDatabaseOperationPath(strParam(nr, "name")); !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid operation resource name", 400)
	}
	return emptyOK(), nil
}

// CancelOperation implements projects.databases.operations.cancel.
func (p *Provider) CancelOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, _, isCollection, ok := splitDatabaseOperationPath(strParam(nr, "name")); !ok || isCollection {
		return nil, model.NewProviderError("InvalidArgument", "invalid operation resource name", 400)
	}
	return emptyOK(), nil
}
