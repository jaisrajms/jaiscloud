package logging

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
)

func errNotFound(kind string) error { return errors.New(kind) }
func errExists(kind string) error   { return errors.New(kind) }
func sortStrings(s []string)        { sort.Strings(s) }

// This file adds the Cloud Logging Admin v2 registries — log buckets, views,
// BigQuery links, log scopes, and the per-scope Settings/CMEK records — to the
// logging store.
//
// The emulator has no per-bucket entry data plane, so every one of these kinds
// is a metadata record. Rather than a dedicated table plus a hand-written scan
// per kind, each kind is persisted as one JSON document keyed by
// (collection, scope, id); the four admin* primitives are the per-backend seam
// (memory map / shared Postgres table) and adminRecords implements the typed
// registry semantics on top of them. MemoryStore and PostgresStore embed
// adminRecords, so both backends share one implementation.
//
// scope is the owning resource parent — a location parent
// ("projects/p/locations/l") for buckets and log scopes, a full bucket name for
// views and links, and a container parent ("projects/p", "organizations/o",
// "folders/f", "billingAccounts/b") for settings/CMEK. id is the short
// client-assigned identifier; settings/CMEK are singletons and use a fixed id.

// Collections stored in the shared admin-record namespace.
const (
	collBuckets   = "buckets"
	collViews     = "views"
	collLinks     = "links"
	collLogScopes = "logScopes"
	collSettings  = "settings"
	collCmek      = "cmekSettings"
)

// Settings/cmek singleton ids (one record per scope).
const (
	settingsID = "_settings"
	cmekID     = "_cmek"
)

// Sentinel errors for the admin registries.
var (
	ErrBucketNotFound   = errNotFound("BucketNotFound")
	ErrBucketExists     = errExists("BucketExists")
	ErrViewNotFound     = errNotFound("ViewNotFound")
	ErrViewExists       = errExists("ViewExists")
	ErrLinkNotFound     = errNotFound("LinkNotFound")
	ErrLinkExists       = errExists("LinkExists")
	ErrLogScopeNotFound = errNotFound("LogScopeNotFound")
	ErrLogScopeExists   = errExists("LogScopeExists")
)

// adminRecord is one stored admin document. CreateTime/UpdateTime mirror the
// record's own server-assigned timestamps so a backend can order/list without
// decoding Data.
type adminRecord struct {
	Data       json.RawMessage `json:"data"`
	CreateTime int64           `json:"createTime,omitempty"`
	UpdateTime int64           `json:"updateTime,omitempty"`
}

// adminBackend is the per-backend persistence seam for admin records.
type adminBackend interface {
	putAdmin(ctx context.Context, collection, scope, id string, rec adminRecord) error
	getAdmin(ctx context.Context, collection, scope, id string) (adminRecord, bool, error)
	listAdmin(ctx context.Context, collection, scope string) (map[string]adminRecord, error)
	deleteAdmin(ctx context.Context, collection, scope, id string) (bool, error)
}

// ─── neutral record types ─────────────────────────────────────────────────────

// LogBucket is a stored Cloud Logging log bucket. Name is the short
// client-assigned bucket id; the service builds the full resource name.
type LogBucket struct {
	Name             string           `json:"name"`
	Description      string           `json:"description,omitempty"`
	RetentionDays    int32            `json:"retentionDays,omitempty"`
	Locked           bool             `json:"locked,omitempty"`
	LifecycleState   string           `json:"lifecycleState,omitempty"`
	AnalyticsEnabled bool             `json:"analyticsEnabled,omitempty"`
	RestrictedFields []string         `json:"restrictedFields,omitempty"`
	IndexConfigs     []LogIndexConfig `json:"indexConfigs,omitempty"`
	Cmek             *LogCmekSettings `json:"cmek,omitempty"`
	CreateTime       int64            `json:"createTime,omitempty"`
	UpdateTime       int64            `json:"updateTime,omitempty"`
}

// LogIndexConfig is one indexed field path of a log bucket.
type LogIndexConfig struct {
	FieldPath  string `json:"fieldPath,omitempty"`
	Type       string `json:"type,omitempty"` // INDEX_TYPE_STRING | INDEX_TYPE_INTEGER
	CreateTime int64  `json:"createTime,omitempty"`
}

// LogView is a stored Cloud Logging log view. Name is the short view id.
type LogView struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Filter      string `json:"filter,omitempty"`
	CreateTime  int64  `json:"createTime,omitempty"`
	UpdateTime  int64  `json:"updateTime,omitempty"`
}

// LogLink is a stored Cloud Logging BigQuery link. Name is the short link id.
type LogLink struct {
	Name              string `json:"name"`
	Description       string `json:"description,omitempty"`
	BigQueryDatasetID string `json:"bigqueryDatasetId,omitempty"`
	LifecycleState    string `json:"lifecycleState,omitempty"`
	CreateTime        int64  `json:"createTime,omitempty"`
}

// LogScope is a stored Cloud Logging log scope. Name is the short scope id.
type LogScope struct {
	Name          string   `json:"name"`
	Description   string   `json:"description,omitempty"`
	ResourceNames []string `json:"resourceNames,omitempty"`
	CreateTime    int64    `json:"createTime,omitempty"`
	UpdateTime    int64    `json:"updateTime,omitempty"`
}

// LogSettings is a stored Cloud Logging Settings record (singleton per scope).
type LogSettings struct {
	KmsKeyName          string          `json:"kmsKeyName,omitempty"`
	KmsServiceAccountID string          `json:"kmsServiceAccountId,omitempty"`
	StorageLocation     string          `json:"storageLocation,omitempty"`
	DisableDefaultSink  bool            `json:"disableDefaultSink,omitempty"`
	DefaultSinkConfig   json.RawMessage `json:"defaultSinkConfig,omitempty"`
}

// LogCmekSettings is a stored Cloud Logging CMEK settings record (singleton per
// scope).
type LogCmekSettings struct {
	KmsKeyName        string `json:"kmsKeyName,omitempty"`
	KmsKeyVersionName string `json:"kmsKeyVersionName,omitempty"`
	ServiceAccountID  string `json:"serviceAccountId,omitempty"`
}

// ─── implementation over a backend ─────────────────────────────────────────────

// adminRecords implements the admin registries over an adminBackend. It is
// embedded by MemoryStore and PostgresStore.
type adminRecords struct {
	backend adminBackend
}

func encodeAdmin(v any) (json.RawMessage, error) { return json.Marshal(v) }

func decodeAdmin[T any](raw json.RawMessage) (T, error) {
	var v T
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &v); err != nil {
			return v, err
		}
	}
	return v, nil
}

// create writes a new record, returning existsErr when the id is taken.
func (a *adminRecords) create(ctx context.Context, collection, scope, id string, v any, existsErr error) error {
	if _, ok, err := a.backend.getAdmin(ctx, collection, scope, id); err != nil {
		return err
	} else if ok {
		return existsErr
	}
	return a.put(ctx, collection, scope, id, v)
}

func (a *adminRecords) put(ctx context.Context, collection, scope, id string, v any) error {
	raw, err := encodeAdmin(v)
	if err != nil {
		return err
	}
	return a.backend.putAdmin(ctx, collection, scope, id, adminRecord{Data: raw})
}

// get decodes the record with the given id, returning notFoundErr when absent.
func (a *adminRecords) get(ctx context.Context, collection, scope, id string, notFoundErr error) (adminRecord, error) {
	rec, ok, err := a.backend.getAdmin(ctx, collection, scope, id)
	if err != nil {
		return adminRecord{}, err
	}
	if !ok {
		return adminRecord{}, notFoundErr
	}
	return rec, nil
}

// list decodes every record in a scope, ordered by id.
func (a *adminRecords) list(ctx context.Context, collection, scope string) ([]adminRecord, error) {
	m, err := a.backend.listAdmin(ctx, collection, scope)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sortStrings(ids)
	out := make([]adminRecord, 0, len(ids))
	for _, id := range ids {
		out = append(out, m[id])
	}
	return out, nil
}

// delete removes a record, returning notFoundErr when absent.
func (a *adminRecords) delete(ctx context.Context, collection, scope, id string, notFoundErr error) error {
	ok, err := a.backend.deleteAdmin(ctx, collection, scope, id)
	if err != nil {
		return err
	}
	if !ok {
		return notFoundErr
	}
	return nil
}

// ─── buckets ──────────────────────────────────────────────────────────────────

func (a *adminRecords) CreateBucket(ctx context.Context, scope string, b LogBucket) error {
	return a.create(ctx, collBuckets, scope, b.Name, b, ErrBucketExists)
}

func (a *adminRecords) GetBucket(ctx context.Context, scope, id string) (LogBucket, error) {
	rec, err := a.get(ctx, collBuckets, scope, id, ErrBucketNotFound)
	if err != nil {
		return LogBucket{}, err
	}
	return decodeAdmin[LogBucket](rec.Data)
}

func (a *adminRecords) ListBuckets(ctx context.Context, scope string) ([]LogBucket, error) {
	recs, err := a.list(ctx, collBuckets, scope)
	if err != nil {
		return nil, err
	}
	out := make([]LogBucket, 0, len(recs))
	for _, rec := range recs {
		b, derr := decodeAdmin[LogBucket](rec.Data)
		if derr != nil {
			return nil, derr
		}
		out = append(out, b)
	}
	return out, nil
}

func (a *adminRecords) UpdateBucket(ctx context.Context, scope string, b LogBucket) error {
	if _, err := a.get(ctx, collBuckets, scope, b.Name, ErrBucketNotFound); err != nil {
		return err
	}
	return a.put(ctx, collBuckets, scope, b.Name, b)
}

func (a *adminRecords) DeleteBucket(ctx context.Context, scope, id string) error {
	return a.delete(ctx, collBuckets, scope, id, ErrBucketNotFound)
}

// ─── views ────────────────────────────────────────────────────────────────────

func (a *adminRecords) CreateView(ctx context.Context, scope string, v LogView) error {
	return a.create(ctx, collViews, scope, v.Name, v, ErrViewExists)
}

func (a *adminRecords) GetView(ctx context.Context, scope, id string) (LogView, error) {
	rec, err := a.get(ctx, collViews, scope, id, ErrViewNotFound)
	if err != nil {
		return LogView{}, err
	}
	return decodeAdmin[LogView](rec.Data)
}

func (a *adminRecords) ListViews(ctx context.Context, scope string) ([]LogView, error) {
	recs, err := a.list(ctx, collViews, scope)
	if err != nil {
		return nil, err
	}
	out := make([]LogView, 0, len(recs))
	for _, rec := range recs {
		v, derr := decodeAdmin[LogView](rec.Data)
		if derr != nil {
			return nil, derr
		}
		out = append(out, v)
	}
	return out, nil
}

func (a *adminRecords) UpdateView(ctx context.Context, scope string, v LogView) error {
	if _, err := a.get(ctx, collViews, scope, v.Name, ErrViewNotFound); err != nil {
		return err
	}
	return a.put(ctx, collViews, scope, v.Name, v)
}

func (a *adminRecords) DeleteView(ctx context.Context, scope, id string) error {
	return a.delete(ctx, collViews, scope, id, ErrViewNotFound)
}

// ─── links ────────────────────────────────────────────────────────────────────

func (a *adminRecords) CreateLink(ctx context.Context, scope string, l LogLink) error {
	return a.create(ctx, collLinks, scope, l.Name, l, ErrLinkExists)
}

func (a *adminRecords) GetLink(ctx context.Context, scope, id string) (LogLink, error) {
	rec, err := a.get(ctx, collLinks, scope, id, ErrLinkNotFound)
	if err != nil {
		return LogLink{}, err
	}
	return decodeAdmin[LogLink](rec.Data)
}

func (a *adminRecords) ListLinks(ctx context.Context, scope string) ([]LogLink, error) {
	recs, err := a.list(ctx, collLinks, scope)
	if err != nil {
		return nil, err
	}
	out := make([]LogLink, 0, len(recs))
	for _, rec := range recs {
		l, derr := decodeAdmin[LogLink](rec.Data)
		if derr != nil {
			return nil, derr
		}
		out = append(out, l)
	}
	return out, nil
}

func (a *adminRecords) DeleteLink(ctx context.Context, scope, id string) error {
	return a.delete(ctx, collLinks, scope, id, ErrLinkNotFound)
}

// ─── log scopes ───────────────────────────────────────────────────────────────

func (a *adminRecords) CreateLogScope(ctx context.Context, scope string, ls LogScope) error {
	return a.create(ctx, collLogScopes, scope, ls.Name, ls, ErrLogScopeExists)
}

func (a *adminRecords) GetLogScope(ctx context.Context, scope, id string) (LogScope, error) {
	rec, err := a.get(ctx, collLogScopes, scope, id, ErrLogScopeNotFound)
	if err != nil {
		return LogScope{}, err
	}
	return decodeAdmin[LogScope](rec.Data)
}

func (a *adminRecords) ListLogScopes(ctx context.Context, scope string) ([]LogScope, error) {
	recs, err := a.list(ctx, collLogScopes, scope)
	if err != nil {
		return nil, err
	}
	out := make([]LogScope, 0, len(recs))
	for _, rec := range recs {
		ls, derr := decodeAdmin[LogScope](rec.Data)
		if derr != nil {
			return nil, derr
		}
		out = append(out, ls)
	}
	return out, nil
}

func (a *adminRecords) UpdateLogScope(ctx context.Context, scope string, ls LogScope) error {
	if _, err := a.get(ctx, collLogScopes, scope, ls.Name, ErrLogScopeNotFound); err != nil {
		return err
	}
	return a.put(ctx, collLogScopes, scope, ls.Name, ls)
}

func (a *adminRecords) DeleteLogScope(ctx context.Context, scope, id string) error {
	return a.delete(ctx, collLogScopes, scope, id, ErrLogScopeNotFound)
}

// ─── settings / cmek (singletons) ─────────────────────────────────────────────

func (a *adminRecords) GetSettings(ctx context.Context, scope string) (LogSettings, bool, error) {
	rec, ok, err := a.backend.getAdmin(ctx, collSettings, scope, settingsID)
	if err != nil || !ok {
		return LogSettings{}, false, err
	}
	s, derr := decodeAdmin[LogSettings](rec.Data)
	return s, true, derr
}

func (a *adminRecords) SetSettings(ctx context.Context, scope string, s LogSettings) error {
	return a.put(ctx, collSettings, scope, settingsID, s)
}

func (a *adminRecords) GetCmekSettings(ctx context.Context, scope string) (LogCmekSettings, bool, error) {
	rec, ok, err := a.backend.getAdmin(ctx, collCmek, scope, cmekID)
	if err != nil || !ok {
		return LogCmekSettings{}, false, err
	}
	c, derr := decodeAdmin[LogCmekSettings](rec.Data)
	return c, true, derr
}

func (a *adminRecords) SetCmekSettings(ctx context.Context, scope string, c LogCmekSettings) error {
	return a.put(ctx, collCmek, scope, cmekID, c)
}
