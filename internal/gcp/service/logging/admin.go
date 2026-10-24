package logging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/policy"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/model"
)

// This file adds the Cloud Logging Admin v2 surface to the transport-neutral
// core: log buckets, views (and their IAM policy), BigQuery links, log scopes,
// and the per-scope Settings/CMEK records. Every one is a metadata record — the
// emulator has no per-bucket entry data plane — layered over the logging store.
//
// Resource names follow Logging v2:
//
//	{container}/{id}/locations/{location}/buckets/{BUCKET_ID}
//	{...}/buckets/{BUCKET_ID}/views/{VIEW_ID}
//	{...}/buckets/{BUCKET_ID}/links/{LINK_ID}
//	{...}/locations/{location}/logScopes/{SCOPE_ID}
//	{container}/{id}[/locations/{location}]/settings
//	{container}/{id}[/locations/{location}]/cmekSettings
//
// The neutral records carry short ids; each transport builds the wire resource
// name from the request's parent (or re-parses the request name).

// rtViewPolicy is the store.ResourceStore resource type under which a log
// view's IAM policy is kept (see internal/gcp/policy).
const rtViewPolicy = "gcp_logging_view_policy"

// ─── helpers ──────────────────────────────────────────────────────────────────

func nowNanos() int64 { return clock.Now().UnixNano() }

func failedPrecondition(msg string) error {
	return model.NewProviderError("FailedPrecondition", msg, 400)
}

// splitName splits a resource name into non-empty segments after stripping a
// leading slash.
func splitName(name string) []string {
	return strings.Split(strings.TrimPrefix(name, "/"), "/")
}

func lastSegment(name string) string {
	parts := splitName(name)
	return parts[len(parts)-1]
}

// containerOf returns the two-segment container ("projects/p",
// "organizations/o", ...) of a location parent or any nested resource name.
func containerOf(name string) string {
	parts := splitName(name)
	if len(parts) >= 2 {
		return parts[0] + "/" + parts[1]
	}
	return name
}

// validBucketID reports whether id is a legal log-bucket id: 1–100 characters
// of [a-z0-9-], starting and ending alphanumeric.
func validBucketID(id string) bool {
	if id == "" || len(id) > 100 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-':
			if i == 0 || i == len(id)-1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// loggingServiceAgent synthesizes the Google-managed service agent Cloud
// Logging reports for a scope (used for Settings/CMEK service account ids).
func loggingServiceAgent(scope string) string {
	return "service-" + sanitizeIdentity(scope) + "@gcp-sa-logging.iam.gserviceaccount.com"
}

func mapAdminStoreError(err error) error {
	switch {
	case errors.Is(err, loggingstore.ErrBucketNotFound):
		return notFound("bucket not found")
	case errors.Is(err, loggingstore.ErrBucketExists):
		return alreadyExists("bucket already exists")
	case errors.Is(err, loggingstore.ErrViewNotFound):
		return notFound("view not found")
	case errors.Is(err, loggingstore.ErrViewExists):
		return alreadyExists("view already exists")
	case errors.Is(err, loggingstore.ErrLinkNotFound):
		return notFound("link not found")
	case errors.Is(err, loggingstore.ErrLinkExists):
		return alreadyExists("link already exists")
	case errors.Is(err, loggingstore.ErrLogScopeNotFound):
		return notFound("log scope not found")
	case errors.Is(err, loggingstore.ErrLogScopeExists):
		return alreadyExists("log scope already exists")
	}
	return err
}

// ─── resource names ───────────────────────────────────────────────────────────

// ParseLocationParent parses "{container}/{id}/locations/{location}" and
// returns the canonical location parent. The location may be "global" or a
// region; the segments must be non-empty.
func ParseLocationParent(name string) (string, error) {
	parts := splitName(name)
	if len(parts) != 4 || !IsLogScope(parts[0]) || parts[1] == "" || parts[2] != "locations" || parts[3] == "" {
		return "", invalidParent(name)
	}
	return parts[0] + "/" + parts[1] + "/locations/" + parts[3], nil
}

// BucketResourceName builds the full bucket name from a location parent and id.
func BucketResourceName(locationParent, id string) string {
	return locationParent + "/buckets/" + id
}

// ParseBucketName parses a full bucket name into its location parent and id.
func ParseBucketName(name string) (locationParent, id string, err error) {
	parts := splitName(name)
	if len(parts) != 6 || !IsLogScope(parts[0]) || parts[1] == "" || parts[2] != "locations" ||
		parts[3] == "" || parts[4] != "buckets" || parts[5] == "" {
		return "", "", invalidParent(name)
	}
	return parts[0] + "/" + parts[1] + "/locations/" + parts[3], parts[5], nil
}

// ParseViewName parses a full view name into its bucket name and short id.
func ParseViewName(name string) (bucketName, id string, err error) {
	return parseNestedName(name, "views")
}

// ParseLinkName parses a full link name into its bucket name and short id.
func ParseLinkName(name string) (bucketName, id string, err error) {
	return parseNestedName(name, "links")
}

func parseNestedName(name, collection string) (bucketName, id string, err error) {
	parts := splitName(name)
	if len(parts) != 8 || !IsLogScope(parts[0]) || parts[1] == "" || parts[2] != "locations" ||
		parts[3] == "" || parts[4] != "buckets" || parts[5] == "" || parts[6] != collection || parts[7] == "" {
		return "", "", invalidParent(name)
	}
	bucket := parts[0] + "/" + parts[1] + "/locations/" + parts[3] + "/buckets/" + parts[5]
	return bucket, parts[7], nil
}

// ViewResourceName builds a view's full resource name.
func ViewResourceName(bucketName, id string) string { return bucketName + "/views/" + id }

// LinkResourceName builds a link's full resource name.
func LinkResourceName(bucketName, id string) string { return bucketName + "/links/" + id }

// LogScopeResourceName builds a log scope's full resource name.
func LogScopeResourceName(locationParent, id string) string {
	return locationParent + "/logScopes/" + id
}

// ParseLogScopeName parses a full log-scope name into its location parent and id.
func ParseLogScopeName(name string) (locationParent, id string, err error) {
	parts := splitName(name)
	if len(parts) != 6 || !IsLogScope(parts[0]) || parts[1] == "" || parts[2] != "locations" ||
		parts[3] == "" || parts[4] != "logScopes" || parts[5] == "" {
		return "", "", invalidParent(name)
	}
	return parts[0] + "/" + parts[1] + "/locations/" + parts[3], parts[5], nil
}

// ParseSettingsName validates a Settings/CMEK resource name: a container
// ("projects/p") or a location parent ("projects/p/locations/l"). It returns the
// canonical scope key.
func ParseSettingsName(name string) (string, error) {
	trimmed := strings.TrimPrefix(name, "/")
	parts := strings.Split(trimmed, "/")
	switch len(parts) {
	case 2:
		if _, ok := logScopes[parts[0]]; ok && parts[1] != "" {
			return trimmed, nil
		}
	case 4:
		if _, ok := logScopes[parts[0]]; ok && parts[1] != "" && parts[2] == "locations" && parts[3] != "" {
			return trimmed, nil
		}
	}
	return "", invalidParent(name)
}

// ─── buckets ──────────────────────────────────────────────────────────────────

// CreateBucket creates a log bucket under a location parent. id is the
// client-assigned bucket id; when empty it is taken from in.Name's last
// segment. The returned record carries the server-assigned timestamps and an
// ACTIVE lifecycle state.
func (s *Service) CreateBucket(ctx context.Context, parent, id string, in loggingstore.LogBucket) (loggingstore.LogBucket, error) {
	locationParent, err := ParseLocationParent(parent)
	if err != nil {
		return loggingstore.LogBucket{}, err
	}
	if id == "" {
		id = lastSegment(in.Name)
	}
	if !validBucketID(id) {
		return loggingstore.LogBucket{}, invalidArgument("invalid bucket id: " + id)
	}
	in.Name = id
	if in.LifecycleState == "" {
		in.LifecycleState = "ACTIVE"
	}
	if in.RetentionDays == 0 {
		in.RetentionDays = 30
	}
	now := nowNanos()
	in.CreateTime, in.UpdateTime = now, now
	for i := range in.IndexConfigs {
		if in.IndexConfigs[i].CreateTime == 0 {
			in.IndexConfigs[i].CreateTime = now
		}
	}
	if in.Cmek != nil && in.Cmek.ServiceAccountID == "" {
		in.Cmek.ServiceAccountID = loggingServiceAgent(locationParent)
	}
	if err := s.store.CreateBucket(ctx, locationParent, in); err != nil {
		return loggingstore.LogBucket{}, mapAdminStoreError(err)
	}
	return in, nil
}

// GetBucket fetches a bucket by full resource name.
func (s *Service) GetBucket(ctx context.Context, name string) (loggingstore.LogBucket, error) {
	locationParent, id, err := ParseBucketName(name)
	if err != nil {
		return loggingstore.LogBucket{}, err
	}
	b, err := s.store.GetBucket(ctx, locationParent, id)
	if err != nil {
		return loggingstore.LogBucket{}, mapAdminStoreError(err)
	}
	return b, nil
}

// ListBuckets returns a page of the location parent's buckets ordered by id.
func (s *Service) ListBuckets(ctx context.Context, parent string, pageSize int, pageToken string) ([]loggingstore.LogBucket, string, error) {
	locationParent, err := ParseLocationParent(parent)
	if err != nil {
		return nil, "", err
	}
	buckets, err := s.store.ListBuckets(ctx, locationParent)
	if err != nil {
		return nil, "", mapAdminStoreError(err)
	}
	page, next := paginateConfig(buckets, pageSize, pageToken)
	return page, next, nil
}

// UpdateBucket merges a bucket by full resource name. A non-empty mask is
// required (the proto marks update_mask required) and names the fields to merge.
func (s *Service) UpdateBucket(ctx context.Context, name string, in loggingstore.LogBucket, updateMask []string) (loggingstore.LogBucket, error) {
	locationParent, id, err := ParseBucketName(name)
	if err != nil {
		return loggingstore.LogBucket{}, err
	}
	if len(updateMask) == 0 {
		return loggingstore.LogBucket{}, invalidArgument("update_mask is required for a bucket update")
	}
	stored, err := s.store.GetBucket(ctx, locationParent, id)
	if err != nil {
		return loggingstore.LogBucket{}, mapAdminStoreError(err)
	}
	if stored.LifecycleState == "DELETE_REQUESTED" {
		return loggingstore.LogBucket{}, failedPrecondition("bucket is pending deletion")
	}
	merged, merr := applyBucketMask(stored, in, updateMask)
	if merr != nil {
		return loggingstore.LogBucket{}, merr
	}
	merged.Name = id
	merged.UpdateTime = nowNanos()
	if err := s.store.UpdateBucket(ctx, locationParent, merged); err != nil {
		return loggingstore.LogBucket{}, mapAdminStoreError(err)
	}
	return merged, nil
}

// DeleteBucket marks a bucket for deletion (DELETE_REQUESTED), matching real
// Cloud Logging's soft delete/undelete window.
func (s *Service) DeleteBucket(ctx context.Context, name string) error {
	locationParent, id, err := ParseBucketName(name)
	if err != nil {
		return err
	}
	stored, err := s.store.GetBucket(ctx, locationParent, id)
	if err != nil {
		return mapAdminStoreError(err)
	}
	if stored.LifecycleState == "DELETE_REQUESTED" {
		return failedPrecondition("bucket is already pending deletion")
	}
	stored.LifecycleState = "DELETE_REQUESTED"
	stored.UpdateTime = nowNanos()
	return mapAdminStoreError(s.store.UpdateBucket(ctx, locationParent, stored))
}

// UndeleteBucket restores a bucket that is pending deletion.
func (s *Service) UndeleteBucket(ctx context.Context, name string) error {
	locationParent, id, err := ParseBucketName(name)
	if err != nil {
		return err
	}
	stored, err := s.store.GetBucket(ctx, locationParent, id)
	if err != nil {
		return mapAdminStoreError(err)
	}
	stored.LifecycleState = "ACTIVE"
	stored.UpdateTime = nowNanos()
	return mapAdminStoreError(s.store.UpdateBucket(ctx, locationParent, stored))
}

// bucketWritableMaskPaths is the writable field set of a LogBucket (Discovery
// schema google.logging.v2.LogBucket). name/lifecycleState/createTime/updateTime
// are read-only; cmekSettings.kmsKeyName is the only writable subfield of the
// bucket's CMEK settings. The AIP-134 `*` wildcard expands to this set.
var bucketWritableMaskPaths = []string{
	"description", "retention_days", "locked", "analytics_enabled",
	"restricted_fields", "index_configs", "cmek_settings.kms_key_name",
}

func applyBucketMask(stored, incoming loggingstore.LogBucket, updateMask []string) (loggingstore.LogBucket, error) {
	for _, raw := range expandConfigMask(updateMask, bucketWritableMaskPaths) {
		switch normalizeConfigMaskPath(raw) {
		case "description":
			stored.Description = incoming.Description
		case "retention_days":
			stored.RetentionDays = incoming.RetentionDays
		case "locked":
			stored.Locked = incoming.Locked
		case "analytics_enabled":
			stored.AnalyticsEnabled = incoming.AnalyticsEnabled
		case "restricted_fields":
			stored.RestrictedFields = incoming.RestrictedFields
		case "index_configs":
			stored.IndexConfigs = incoming.IndexConfigs
		case "cmek_settings", "cmek_settings.kms_key_name":
			// `cmekSettings` (the message) and its nested writable leaf both
			// apply only `kmsKeyName`.
			stored.Cmek = mergeBucketCmek(stored.Cmek, incoming.Cmek)
		default:
			return stored, invalidMaskPath(raw)
		}
	}
	return stored, nil
}

// mergeBucketCmek applies the writable `kmsKeyName` of a bucket's nested
// cmekSettings message (named by either the message path or its nested leaf) and
// preserves the read-only service account id and key version name, which real
// Logging ignores on write.
func mergeBucketCmek(stored, incoming *loggingstore.LogCmekSettings) *loggingstore.LogCmekSettings {
	if stored == nil && incoming == nil {
		return nil
	}
	out := &loggingstore.LogCmekSettings{}
	if stored != nil {
		out.KmsKeyVersionName = stored.KmsKeyVersionName
		out.ServiceAccountID = stored.ServiceAccountID
	}
	if incoming != nil {
		out.KmsKeyName = incoming.KmsKeyName
	}
	return out
}

// ─── views ────────────────────────────────────────────────────────────────────

// requireBucket ensures the bucket named by a full bucket name exists.
func (s *Service) requireBucket(ctx context.Context, bucketName string) error {
	locationParent, id, err := ParseBucketName(bucketName)
	if err != nil {
		return err
	}
	if _, err := s.store.GetBucket(ctx, locationParent, id); err != nil {
		return mapAdminStoreError(err)
	}
	return nil
}

// bucketScope canonicalizes a full bucket name used as a store scope.
func bucketScope(parent string) (string, error) {
	locationParent, id, err := ParseBucketName(parent)
	if err != nil {
		return "", err
	}
	return BucketResourceName(locationParent, id), nil
}

// CreateView creates a view under a bucket. id is the client-assigned view id;
// when empty it is taken from in.Name's last segment. The bucket must exist.
func (s *Service) CreateView(ctx context.Context, parent, id string, in loggingstore.LogView) (loggingstore.LogView, error) {
	bucketName, err := bucketScope(parent)
	if err != nil {
		return loggingstore.LogView{}, err
	}
	if err := s.requireBucket(ctx, bucketName); err != nil {
		return loggingstore.LogView{}, err
	}
	if id == "" {
		id = lastSegment(in.Name)
	}
	if !validConfigID(id) {
		return loggingstore.LogView{}, invalidArgument("invalid view id: " + id)
	}
	in.Name = id
	now := nowNanos()
	in.CreateTime, in.UpdateTime = now, now
	if err := s.store.CreateView(ctx, bucketName, in); err != nil {
		return loggingstore.LogView{}, mapAdminStoreError(err)
	}
	return in, nil
}

// GetView fetches a view by full resource name.
func (s *Service) GetView(ctx context.Context, name string) (loggingstore.LogView, error) {
	bucketName, id, err := ParseViewName(name)
	if err != nil {
		return loggingstore.LogView{}, err
	}
	v, err := s.store.GetView(ctx, bucketName, id)
	if err != nil {
		return loggingstore.LogView{}, mapAdminStoreError(err)
	}
	return v, nil
}

// ListViews returns a page of a bucket's views ordered by id.
func (s *Service) ListViews(ctx context.Context, parent string, pageSize int, pageToken string) ([]loggingstore.LogView, string, error) {
	bucketName, err := bucketScope(parent)
	if err != nil {
		return nil, "", err
	}
	views, err := s.store.ListViews(ctx, bucketName)
	if err != nil {
		return nil, "", mapAdminStoreError(err)
	}
	page, next := paginateConfig(views, pageSize, pageToken)
	return page, next, nil
}

// viewWritableMaskPaths is the writable field set of a LogView (Discovery schema
// google.logging.v2.LogView); name/createTime/updateTime are read-only. The
// AIP-134 `*` wildcard expands to this set.
var viewWritableMaskPaths = []string{"description", "filter"}

// UpdateView merges a view by full resource name. A non-empty mask is required.
func (s *Service) UpdateView(ctx context.Context, name string, in loggingstore.LogView, updateMask []string) (loggingstore.LogView, error) {
	bucketName, id, err := ParseViewName(name)
	if err != nil {
		return loggingstore.LogView{}, err
	}
	if len(updateMask) == 0 {
		return loggingstore.LogView{}, invalidArgument("update_mask is required for a view update")
	}
	stored, err := s.store.GetView(ctx, bucketName, id)
	if err != nil {
		return loggingstore.LogView{}, mapAdminStoreError(err)
	}
	for _, raw := range expandConfigMask(updateMask, viewWritableMaskPaths) {
		switch normalizeConfigMaskPath(raw) {
		case "description":
			stored.Description = in.Description
		case "filter":
			stored.Filter = in.Filter
		default:
			return loggingstore.LogView{}, invalidMaskPath(raw)
		}
	}
	stored.Name = id
	stored.UpdateTime = nowNanos()
	if err := s.store.UpdateView(ctx, bucketName, stored); err != nil {
		return loggingstore.LogView{}, mapAdminStoreError(err)
	}
	return stored, nil
}

// DeleteView removes a view by full resource name.
func (s *Service) DeleteView(ctx context.Context, name string) error {
	bucketName, id, err := ParseViewName(name)
	if err != nil {
		return err
	}
	return mapAdminStoreError(s.store.DeleteView(ctx, bucketName, id))
}

// ─── view IAM ─────────────────────────────────────────────────────────────────

// ViewGetIamPolicy returns a view's IAM policy (empty default when unset).
func (s *Service) ViewGetIamPolicy(ctx context.Context, name string) (policy.Policy, error) {
	bucketName, id, err := ParseViewName(name)
	if err != nil {
		return policy.Policy{}, err
	}
	if _, err := s.store.GetView(ctx, bucketName, id); err != nil {
		return policy.Policy{}, mapAdminStoreError(err)
	}
	return policy.Load(ctx, s.resources, containerOf(bucketName), rtViewPolicy, name), nil
}

// ViewSetIamPolicy stores a view's IAM policy (etag OCC enforced by policy.Set).
func (s *Service) ViewSetIamPolicy(ctx context.Context, name string, body map[string]any) (policy.Policy, error) {
	bucketName, id, err := ParseViewName(name)
	if err != nil {
		return policy.Policy{}, err
	}
	if _, err := s.store.GetView(ctx, bucketName, id); err != nil {
		return policy.Policy{}, mapAdminStoreError(err)
	}
	return policy.Set(ctx, s.resources, containerOf(bucketName), rtViewPolicy, name, body)
}

// ViewTestIamPermissions grants every requested permission (the emulator does
// not enforce IAM).
func (s *Service) ViewTestIamPermissions(ctx context.Context, name string, permissions []string) ([]string, error) {
	bucketName, id, err := ParseViewName(name)
	if err != nil {
		return nil, err
	}
	if _, err := s.store.GetView(ctx, bucketName, id); err != nil {
		return nil, mapAdminStoreError(err)
	}
	return policy.TestPermissions(permissions), nil
}

// ─── links ────────────────────────────────────────────────────────────────────

// CreateLink creates a BigQuery link under a bucket. id is the client-assigned
// link id; when empty it is taken from in.Name's last segment.
func (s *Service) CreateLink(ctx context.Context, parent, id string, in loggingstore.LogLink) (loggingstore.LogLink, error) {
	bucketName, err := bucketScope(parent)
	if err != nil {
		return loggingstore.LogLink{}, err
	}
	if err := s.requireBucket(ctx, bucketName); err != nil {
		return loggingstore.LogLink{}, err
	}
	if id == "" {
		id = lastSegment(in.Name)
	}
	if !validConfigID(id) {
		return loggingstore.LogLink{}, invalidArgument("invalid link id: " + id)
	}
	if in.BigQueryDatasetID == "" {
		return loggingstore.LogLink{}, invalidArgument("bigquery_dataset.dataset_id is required")
	}
	in.Name = id
	in.LifecycleState = "ACTIVE"
	in.CreateTime = nowNanos()
	if err := s.store.CreateLink(ctx, bucketName, in); err != nil {
		return loggingstore.LogLink{}, mapAdminStoreError(err)
	}
	return in, nil
}

// GetLink fetches a link by full resource name.
func (s *Service) GetLink(ctx context.Context, name string) (loggingstore.LogLink, error) {
	bucketName, id, err := ParseLinkName(name)
	if err != nil {
		return loggingstore.LogLink{}, err
	}
	l, err := s.store.GetLink(ctx, bucketName, id)
	if err != nil {
		return loggingstore.LogLink{}, mapAdminStoreError(err)
	}
	return l, nil
}

// ListLinks returns a page of a bucket's links ordered by id.
func (s *Service) ListLinks(ctx context.Context, parent string, pageSize int, pageToken string) ([]loggingstore.LogLink, string, error) {
	// links.list is the one config list whose Discovery document and proto do
	// not document the "non-positive values are ignored" exception, so a
	// negative size keeps AIP-158's InvalidArgument — validated before the
	// parent lookup and store read so the client error is not masked by them.
	if pageSize < 0 {
		return nil, "", invalidArgument("pageSize must not be negative")
	}
	bucketName, err := bucketScope(parent)
	if err != nil {
		return nil, "", err
	}
	links, err := s.store.ListLinks(ctx, bucketName)
	if err != nil {
		return nil, "", mapAdminStoreError(err)
	}
	page, next := paginateConfig(links, pageSize, pageToken)
	return page, next, nil
}

// DeleteLink removes a link by full resource name.
func (s *Service) DeleteLink(ctx context.Context, name string) error {
	bucketName, id, err := ParseLinkName(name)
	if err != nil {
		return err
	}
	return mapAdminStoreError(s.store.DeleteLink(ctx, bucketName, id))
}

// ─── log scopes ───────────────────────────────────────────────────────────────

// CreateLogScope creates a log scope under a location parent. id is the
// client-assigned scope id; when empty it is taken from in.Name's last segment.
func (s *Service) CreateLogScope(ctx context.Context, parent, id string, in loggingstore.LogScope) (loggingstore.LogScope, error) {
	locationParent, err := ParseLocationParent(parent)
	if err != nil {
		return loggingstore.LogScope{}, err
	}
	if id == "" {
		id = lastSegment(in.Name)
	}
	if !validConfigID(id) {
		return loggingstore.LogScope{}, invalidArgument("invalid log scope id: " + id)
	}
	if len(in.ResourceNames) == 0 {
		return loggingstore.LogScope{}, invalidArgument("resource_names is required")
	}
	in.Name = id
	now := nowNanos()
	in.CreateTime, in.UpdateTime = now, now
	if err := s.store.CreateLogScope(ctx, locationParent, in); err != nil {
		return loggingstore.LogScope{}, mapAdminStoreError(err)
	}
	return in, nil
}

// GetLogScope fetches a log scope by full resource name.
func (s *Service) GetLogScope(ctx context.Context, name string) (loggingstore.LogScope, error) {
	locationParent, id, err := ParseLogScopeName(name)
	if err != nil {
		return loggingstore.LogScope{}, err
	}
	ls, err := s.store.GetLogScope(ctx, locationParent, id)
	if err != nil {
		return loggingstore.LogScope{}, mapAdminStoreError(err)
	}
	return ls, nil
}

// ListLogScopes returns a page of a location parent's log scopes ordered by id.
func (s *Service) ListLogScopes(ctx context.Context, parent string, pageSize int, pageToken string) ([]loggingstore.LogScope, string, error) {
	locationParent, err := ParseLocationParent(parent)
	if err != nil {
		return nil, "", err
	}
	scopes, err := s.store.ListLogScopes(ctx, locationParent)
	if err != nil {
		return nil, "", mapAdminStoreError(err)
	}
	page, next := paginateConfig(scopes, pageSize, pageToken)
	return page, next, nil
}

// scopeWritableMaskPaths is the writable field set of a LogScope (Discovery
// schema google.logging.v2.LogScope); name/createTime/updateTime are read-only.
// The AIP-134 `*` wildcard expands to this set.
var scopeWritableMaskPaths = []string{"description", "resource_names"}

// UpdateLogScope merges a log scope by full resource name. A non-empty mask is
// required.
func (s *Service) UpdateLogScope(ctx context.Context, name string, in loggingstore.LogScope, updateMask []string) (loggingstore.LogScope, error) {
	locationParent, id, err := ParseLogScopeName(name)
	if err != nil {
		return loggingstore.LogScope{}, err
	}
	if len(updateMask) == 0 {
		return loggingstore.LogScope{}, invalidArgument("update_mask is required for a log scope update")
	}
	stored, err := s.store.GetLogScope(ctx, locationParent, id)
	if err != nil {
		return loggingstore.LogScope{}, mapAdminStoreError(err)
	}
	for _, raw := range expandConfigMask(updateMask, scopeWritableMaskPaths) {
		switch normalizeConfigMaskPath(raw) {
		case "description":
			stored.Description = in.Description
		case "resource_names":
			stored.ResourceNames = in.ResourceNames
		default:
			return loggingstore.LogScope{}, invalidMaskPath(raw)
		}
	}
	stored.Name = id
	stored.UpdateTime = nowNanos()
	if err := s.store.UpdateLogScope(ctx, locationParent, stored); err != nil {
		return loggingstore.LogScope{}, mapAdminStoreError(err)
	}
	return stored, nil
}

// DeleteLogScope removes a log scope by full resource name.
func (s *Service) DeleteLogScope(ctx context.Context, name string) error {
	locationParent, id, err := ParseLogScopeName(name)
	if err != nil {
		return err
	}
	return mapAdminStoreError(s.store.DeleteLogScope(ctx, locationParent, id))
}

// ─── settings / cmek ──────────────────────────────────────────────────────────

// GetSettings returns the Settings record for a scope, synthesizing the
// default (with the Google-managed service account id) when none is stored.
func (s *Service) GetSettings(ctx context.Context, name string) (loggingstore.LogSettings, error) {
	scope, err := ParseSettingsName(name)
	if err != nil {
		return loggingstore.LogSettings{}, err
	}
	st, ok, err := s.store.GetSettings(ctx, scope)
	if err != nil {
		return loggingstore.LogSettings{}, err
	}
	if !ok {
		st = loggingstore.LogSettings{}
	}
	if st.KmsServiceAccountID == "" {
		st.KmsServiceAccountID = loggingServiceAgent(scope)
	}
	return st, nil
}

// settingsWritableMaskPaths is the writable field set of a Settings record
// (Discovery schema google.logging.v2.Settings); name/kmsServiceAccountId/
// loggingServiceAccountId are read-only. The AIP-134 `*` wildcard expands to
// this set. The nested leaves of `default_sink_config` (filter/mode/exclusions)
// are accepted in applySettingsMask but are not part of the expansion set,
// because replacing the whole message already writes every writable leaf.
var settingsWritableMaskPaths = []string{
	"kms_key_name", "storage_location", "disable_default_sink", "default_sink_config",
}

// UpdateSettings merges the Settings record for a scope. A non-empty mask is
// required; the synthesized service account id is preserved.
func (s *Service) UpdateSettings(ctx context.Context, name string, in loggingstore.LogSettings, updateMask []string) (loggingstore.LogSettings, error) {
	scope, err := ParseSettingsName(name)
	if err != nil {
		return loggingstore.LogSettings{}, err
	}
	if len(updateMask) == 0 {
		return loggingstore.LogSettings{}, invalidArgument("update_mask is required for a settings update")
	}
	stored, _, err := s.store.GetSettings(ctx, scope)
	if err != nil {
		return loggingstore.LogSettings{}, err
	}
	if stored.KmsServiceAccountID == "" {
		stored.KmsServiceAccountID = loggingServiceAgent(scope)
	}
	merged, merr := applySettingsMask(stored, in, updateMask)
	if merr != nil {
		return loggingstore.LogSettings{}, merr
	}
	stored = merged
	if err := s.store.SetSettings(ctx, scope, stored); err != nil {
		return loggingstore.LogSettings{}, err
	}
	return stored, nil
}

// applySettingsMask merges the incoming Settings into the stored one for the
// named paths. A nested `default_sink_config.<leaf>` path (filter/mode/
// exclusions) updates only that leaf of the stored DefaultSinkConfig object
// (AIP-134/AIP-161), while the `default_sink_config` message path replaces the
// whole object. A path that names no writable field is a 400 InvalidArgument;
// element paths of the repeated `exclusions` field cannot be addressed by a
// field mask and stay 400.
func applySettingsMask(stored, incoming loggingstore.LogSettings, updateMask []string) (loggingstore.LogSettings, error) {
	for _, raw := range expandConfigMask(updateMask, settingsWritableMaskPaths) {
		p := normalizeConfigMaskPath(raw)
		switch p {
		case "kms_key_name":
			stored.KmsKeyName = incoming.KmsKeyName
		case "storage_location":
			stored.StorageLocation = incoming.StorageLocation
		case "disable_default_sink":
			stored.DisableDefaultSink = incoming.DisableDefaultSink
		case defaultSinkConfigPath:
			stored.DefaultSinkConfig = incoming.DefaultSinkConfig
		case defaultSinkConfigPath + ".filter", defaultSinkConfigPath + ".mode", defaultSinkConfigPath + ".exclusions":
			merged, merr := mergeDefaultSinkConfig(stored.DefaultSinkConfig, incoming.DefaultSinkConfig, strings.TrimPrefix(p, defaultSinkConfigPath+"."))
			if merr != nil {
				return stored, merr
			}
			stored.DefaultSinkConfig = merged
		default:
			return stored, invalidMaskPath(raw)
		}
	}
	return stored, nil
}

// defaultSinkConfigPath is the normalized proto path of Settings'
// default_sink_config message.
const defaultSinkConfigPath = "default_sink_config"

// mergeDefaultSinkConfig merges one writable leaf (filter/mode/exclusions) of
// the DefaultSinkConfig message into the stored object. The object is kept as
// opaque JSON so an `exclusions` element round-trips exactly as the client sent
// it (its createTime/updateTime are output-only and would otherwise be
// re-serialized as zero values). A leaf absent from the request clears to its
// zero value, matching a field mask that names the field.
func mergeDefaultSinkConfig(stored, incoming json.RawMessage, leaf string) (json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	if len(stored) > 0 {
		if err := json.Unmarshal(stored, &out); err != nil {
			return nil, invalidArgument("stored default_sink_config is not an object")
		}
	}
	in := map[string]json.RawMessage{}
	if len(incoming) > 0 {
		if err := json.Unmarshal(incoming, &in); err != nil {
			return nil, invalidArgument("default_sink_config must be an object")
		}
	}
	v, ok := in[leaf]
	if !ok {
		v = defaultSinkConfigZeroLeaf(leaf)
	}
	out[leaf] = v
	merged, err := json.Marshal(out)
	if err != nil {
		return nil, invalidArgument("default_sink_config could not be encoded")
	}
	return merged, nil
}

// defaultSinkConfigZeroLeaf is the JSON zero value of a DefaultSinkConfig leaf,
// applied when a mask names the leaf but the request omits it: an empty array
// for the repeated exclusions, the unspecified enum for mode, and an empty
// string for filter.
func defaultSinkConfigZeroLeaf(leafKey string) json.RawMessage {
	switch leafKey {
	case "exclusions":
		return json.RawMessage("[]")
	case "mode":
		return json.RawMessage(`"FILTER_WRITE_MODE_UNSPECIFIED"`)
	default:
		return json.RawMessage(`""`)
	}
}

// GetCmekSettings returns the CMEK record for a scope, synthesizing the default
// (with the Google-managed service account id) when none is stored.
func (s *Service) GetCmekSettings(ctx context.Context, name string) (loggingstore.LogCmekSettings, error) {
	scope, err := ParseSettingsName(name)
	if err != nil {
		return loggingstore.LogCmekSettings{}, err
	}
	st, ok, err := s.store.GetCmekSettings(ctx, scope)
	if err != nil {
		return loggingstore.LogCmekSettings{}, err
	}
	if !ok {
		st = loggingstore.LogCmekSettings{}
	}
	if st.ServiceAccountID == "" {
		st.ServiceAccountID = loggingServiceAgent(scope)
	}
	return st, nil
}

// cmekWritableMaskPaths is the writable field set of CmekSettings (Discovery
// schema google.logging.v2.CmekSettings). name, kmsKeyVersionName and
// serviceAccountId are read-only, so only kmsKeyName is writable; the AIP-134
// `*` wildcard expands to this set.
var cmekWritableMaskPaths = []string{"kms_key_name"}

// UpdateCmekSettings merges the CMEK record for a scope. A non-empty mask and a
// kms_key_name are required; the synthesized service account id is preserved.
func (s *Service) UpdateCmekSettings(ctx context.Context, name string, in loggingstore.LogCmekSettings, updateMask []string) (loggingstore.LogCmekSettings, error) {
	scope, err := ParseSettingsName(name)
	if err != nil {
		return loggingstore.LogCmekSettings{}, err
	}
	if len(updateMask) == 0 {
		return loggingstore.LogCmekSettings{}, invalidArgument("update_mask is required for a cmek settings update")
	}
	if in.KmsKeyName == "" {
		return loggingstore.LogCmekSettings{}, invalidArgument("kms_key_name is required")
	}
	stored, _, err := s.store.GetCmekSettings(ctx, scope)
	if err != nil {
		return loggingstore.LogCmekSettings{}, err
	}
	if stored.ServiceAccountID == "" {
		stored.ServiceAccountID = loggingServiceAgent(scope)
	}
	for _, raw := range expandConfigMask(updateMask, cmekWritableMaskPaths) {
		switch normalizeConfigMaskPath(raw) {
		case "kms_key_name":
			stored.KmsKeyName = in.KmsKeyName
		default:
			return loggingstore.LogCmekSettings{}, invalidMaskPath(raw)
		}
	}
	if err := s.store.SetCmekSettings(ctx, scope, stored); err != nil {
		return loggingstore.LogCmekSettings{}, err
	}
	return stored, nil
}
