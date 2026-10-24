package logging

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// This file adds the Cloud Logging Admin v2 REST surface: log buckets, views
// (with their IAM policy), BigQuery links, log scopes, and the per-scope
// Settings/CMEK records. It is a thin adapter over the shared core, exactly like
// the sink/exclusion/metric handlers in provider.go.

// ─── decoding ─────────────────────────────────────────────────────────────────

// decodeAdminPath recognises the Admin v2 paths and sets the action + resource
// params on nr. It returns false when rest is not an admin path (or uses an
// unsupported method on one), so Decode can fall through.
func decodeAdminPath(r *http.Request, rest string, nr *model.NormalizedRequest) bool {
	parts := strings.Split(rest, "/")
	if len(parts) < 3 || !core.IsLogScope(parts[0]) || parts[1] == "" {
		return false
	}
	// Settings / CMEK: {container}/{id}[/locations/{loc}]/settings|cmekSettings.
	n := len(parts)
	if last := parts[n-1]; last == "settings" || last == "cmekSettings" {
		if n != 3 && !(n == 5 && parts[2] == "locations" && parts[3] != "") {
			return false
		}
		name := strings.Join(parts[:n-1], "/")
		switch last {
		case "settings":
			switch r.Method {
			case http.MethodGet:
				nr.Action, nr.Params["name"] = "SettingsGet", name
			case http.MethodPatch:
				nr.Action, nr.Params["name"] = "SettingsUpdate", name
			default:
				return false
			}
		case "cmekSettings":
			switch r.Method {
			case http.MethodGet:
				nr.Action, nr.Params["name"] = "CmekGet", name
			case http.MethodPatch:
				nr.Action, nr.Params["name"] = "CmekUpdate", name
			default:
				return false
			}
		}
		return true
	}
	if parts[2] != "locations" || n < 5 || parts[3] == "" {
		return false
	}
	locationParent := strings.Join(parts[:4], "/")
	switch parts[4] {
	case "buckets", "buckets:createAsync":
		return decodeBucketPath(r, parts[4:], locationParent, nr)
	case "logScopes":
		return decodeLogScopePath(r, parts[4:], locationParent, nr)
	default:
		return false
	}
}

// decodeBucketPath handles the buckets collection and everything nested under a
// bucket (views, IAM custom methods, links). sub[0] is "buckets" (optionally
// "buckets:createAsync").
func decodeBucketPath(r *http.Request, sub []string, locationParent string, nr *model.NormalizedRequest) bool {
	collection := sub[0]
	if strings.HasSuffix(collection, ":createAsync") {
		if r.Method != http.MethodPost {
			return false
		}
		nr.Action, nr.Params["parent"] = "BucketCreateAsync", locationParent
		return true
	}
	if collection != "buckets" {
		return false
	}
	if len(sub) == 1 {
		switch r.Method {
		case http.MethodGet:
			nr.Action, nr.Params["parent"] = "BucketList", locationParent
		case http.MethodPost:
			nr.Action, nr.Params["parent"] = "BucketCreate", locationParent
		default:
			return false
		}
		return true
	}
	bucketID := sub[1]
	bucketName := locationParent + "/buckets/" + bucketID
	if len(sub) == 2 {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(bucketID, ":updateAsync"):
			nr.Action, nr.Params["name"] = "BucketUpdateAsync", locationParent+"/buckets/"+strings.TrimSuffix(bucketID, ":updateAsync")
			return true
		case r.Method == http.MethodPost && strings.HasSuffix(bucketID, ":undelete"):
			nr.Action, nr.Params["name"] = "BucketUndelete", locationParent+"/buckets/"+strings.TrimSuffix(bucketID, ":undelete")
			return true
		}
		switch r.Method {
		case http.MethodGet:
			nr.Action, nr.Params["name"] = "BucketGet", bucketName
		case http.MethodPatch:
			nr.Action, nr.Params["name"] = "BucketUpdate", bucketName
		case http.MethodDelete:
			nr.Action, nr.Params["name"] = "BucketDelete", bucketName
		default:
			return false
		}
		return true
	}
	// Nested under a bucket: {bucket}/views[/{id}] or {bucket}/links[/{id}].
	switch sub[2] {
	case "views":
		return decodeViewPath(r, sub, bucketName, nr)
	case "links":
		return decodeLinkPath(r, sub, bucketName, nr)
	default:
		return false
	}
}

func decodeViewPath(r *http.Request, sub []string, bucketName string, nr *model.NormalizedRequest) bool {
	if len(sub) == 3 {
		switch r.Method {
		case http.MethodGet:
			nr.Action, nr.Params["parent"] = "ViewList", bucketName
		case http.MethodPost:
			nr.Action, nr.Params["parent"] = "ViewCreate", bucketName
		default:
			return false
		}
		return true
	}
	viewID := sub[3]
	for _, suffix := range []struct{ name, action string }{
		{":getIamPolicy", "ViewGetIamPolicy"},
		{":setIamPolicy", "ViewSetIamPolicy"},
		{":testIamPermissions", "ViewTestIamPermissions"},
	} {
		if strings.HasSuffix(viewID, suffix.name) {
			if r.Method != http.MethodPost {
				return false
			}
			id := strings.TrimSuffix(viewID, suffix.name)
			nr.Action, nr.Params["name"] = suffix.action, bucketName+"/views/"+id
			return true
		}
	}
	name := bucketName + "/views/" + viewID
	switch r.Method {
	case http.MethodGet:
		nr.Action, nr.Params["name"] = "ViewGet", name
	case http.MethodPatch:
		nr.Action, nr.Params["name"] = "ViewUpdate", name
	case http.MethodDelete:
		nr.Action, nr.Params["name"] = "ViewDelete", name
	default:
		return false
	}
	return true
}

func decodeLinkPath(r *http.Request, sub []string, bucketName string, nr *model.NormalizedRequest) bool {
	if len(sub) == 3 {
		switch r.Method {
		case http.MethodGet:
			nr.Action, nr.Params["parent"] = "LinkList", bucketName
		case http.MethodPost:
			nr.Action, nr.Params["parent"] = "LinkCreate", bucketName
		default:
			return false
		}
		return true
	}
	name := bucketName + "/links/" + sub[3]
	switch r.Method {
	case http.MethodGet:
		nr.Action, nr.Params["name"] = "LinkGet", name
	case http.MethodDelete:
		nr.Action, nr.Params["name"] = "LinkDelete", name
	default:
		return false
	}
	return true
}

func decodeLogScopePath(r *http.Request, sub []string, locationParent string, nr *model.NormalizedRequest) bool {
	if len(sub) == 1 {
		switch r.Method {
		case http.MethodGet:
			nr.Action, nr.Params["parent"] = "LogScopeList", locationParent
		case http.MethodPost:
			nr.Action, nr.Params["parent"] = "LogScopeCreate", locationParent
		default:
			return false
		}
		return true
	}
	name := locationParent + "/logScopes/" + sub[1]
	switch r.Method {
	case http.MethodGet:
		nr.Action, nr.Params["name"] = "LogScopeGet", name
	case http.MethodPatch:
		nr.Action, nr.Params["name"] = "LogScopeUpdate", name
	case http.MethodDelete:
		nr.Action, nr.Params["name"] = "LogScopeDelete", name
	default:
		return false
	}
	return true
}

// ─── handlers ─────────────────────────────────────────────────────────────────

// resourceBody returns the resource payload of a REST request body. The
// Discovery documents declare the request body as the resource itself (with the
// id as a query parameter); a body wrapped in the resource field name (the
// grpc-gateway spelling) is also accepted.
func resourceBody(body map[string]any, wrapper string) any {
	if v, ok := body[wrapper]; ok {
		return v
	}
	return body
}

func (p *Provider) BucketCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	id := strParam(nr, "bucketId")
	b, err := p.core.CreateBucket(ctx, parent, id, bucketFromWire(resourceBody(bodyOf(nr), "bucket")))
	if err != nil {
		return nil, err
	}
	return provider.OK(bucketToWire(parent, b)), nil
}

func (p *Provider) BucketCreateAsync(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	id := strParam(nr, "bucketId")
	b, err := p.core.CreateBucket(ctx, parent, id, bucketFromWire(resourceBody(bodyOf(nr), "bucket")))
	if err != nil {
		return nil, err
	}
	op := parent + "/operations/create-bucket-" + b.Name
	return provider.OK(operationToWire(op, "google.logging.v2.LogBucket", bucketToWire(parent, b))), nil
}

func (p *Provider) BucketGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	b, err := p.core.GetBucket(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(bucketToWire(bucketParent(name), b)), nil
}

func (p *Provider) BucketList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	pageSize, err := intParam(nr, "pageSize")
	if err != nil {
		return nil, err
	}
	buckets, next, err := p.core.ListBuckets(ctx, parent, pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(buckets) > 0 {
		list := make([]any, 0, len(buckets))
		for _, b := range buckets {
			list = append(list, bucketToWire(parent, b))
		}
		out["buckets"] = list
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) BucketUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	b, err := p.core.UpdateBucket(ctx, name, bucketFromWire(resourceBody(bodyOf(nr), "bucket")), updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(bucketToWire(bucketParent(name), b)), nil
}

func (p *Provider) BucketUpdateAsync(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	b, err := p.core.UpdateBucket(ctx, name, bucketFromWire(resourceBody(bodyOf(nr), "bucket")), updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	op := bucketParent(name) + "/operations/update-bucket-" + b.Name
	return provider.OK(operationToWire(op, "google.logging.v2.LogBucket", bucketToWire(bucketParent(name), b))), nil
}

func (p *Provider) BucketDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteBucket(ctx, strParam(nr, "name")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) BucketUndelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.UndeleteBucket(ctx, strParam(nr, "name")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ViewCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	v, err := p.core.CreateView(ctx, parent, strParam(nr, "viewId"), viewFromWire(resourceBody(bodyOf(nr), "view")))
	if err != nil {
		return nil, err
	}
	return provider.OK(viewToWire(parent, v)), nil
}

func (p *Provider) ViewGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	v, err := p.core.GetView(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(viewToWire(viewParent(name), v)), nil
}

func (p *Provider) ViewList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	pageSize, err := intParam(nr, "pageSize")
	if err != nil {
		return nil, err
	}
	views, next, err := p.core.ListViews(ctx, parent, pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(views) > 0 {
		list := make([]any, 0, len(views))
		for _, v := range views {
			list = append(list, viewToWire(parent, v))
		}
		out["views"] = list
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) ViewUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	v, err := p.core.UpdateView(ctx, name, viewFromWire(resourceBody(bodyOf(nr), "view")), updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(viewToWire(viewParent(name), v)), nil
}

func (p *Provider) ViewDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteView(ctx, strParam(nr, "name")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) ViewGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pol, err := p.core.ViewGetIamPolicy(ctx, strParam(nr, "name"))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) ViewSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pol, err := p.core.ViewSetIamPolicy(ctx, strParam(nr, "name"), bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) ViewTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	granted, err := p.core.ViewTestIamPermissions(ctx, strParam(nr, "name"), policy.Permissions(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": granted}), nil
}

func (p *Provider) LinkCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	l, err := p.core.CreateLink(ctx, parent, strParam(nr, "linkId"), linkFromWire(resourceBody(bodyOf(nr), "link")))
	if err != nil {
		return nil, err
	}
	op := linkOperationLocation(parent) + "/operations/create-link-" + l.Name
	return provider.OK(operationToWire(op, "google.logging.v2.Link", linkToWire(parent, l))), nil
}

func (p *Provider) LinkGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	l, err := p.core.GetLink(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(linkToWire(linkParent(name), l)), nil
}

func (p *Provider) LinkList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	pageSize, err := intParam(nr, "pageSize")
	if err != nil {
		return nil, err
	}
	links, next, err := p.core.ListLinks(ctx, parent, pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(links) > 0 {
		list := make([]any, 0, len(links))
		for _, l := range links {
			list = append(list, linkToWire(parent, l))
		}
		out["links"] = list
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) LinkDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	if err := p.core.DeleteLink(ctx, name); err != nil {
		return nil, err
	}
	op := linkOperationLocation(linkParent(name)) + "/operations/delete-link-" + name
	return provider.OK(operationToWire(op, "google.protobuf.Empty", map[string]any{})), nil
}

func (p *Provider) LogScopeCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	ls, err := p.core.CreateLogScope(ctx, parent, strParam(nr, "logScopeId"), logScopeFromWire(resourceBody(bodyOf(nr), "logScope")))
	if err != nil {
		return nil, err
	}
	return provider.OK(logScopeToWire(parent, ls)), nil
}

func (p *Provider) LogScopeGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	ls, err := p.core.GetLogScope(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(logScopeToWire(logScopeParent(name), ls)), nil
}

func (p *Provider) LogScopeList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	parent := strParam(nr, "parent")
	pageSize, err := intParam(nr, "pageSize")
	if err != nil {
		return nil, err
	}
	scopes, next, err := p.core.ListLogScopes(ctx, parent, pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if len(scopes) > 0 {
		list := make([]any, 0, len(scopes))
		for _, ls := range scopes {
			list = append(list, logScopeToWire(parent, ls))
		}
		out["logScopes"] = list
	}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) LogScopeUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	ls, err := p.core.UpdateLogScope(ctx, name, logScopeFromWire(resourceBody(bodyOf(nr), "logScope")), updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(logScopeToWire(logScopeParent(name), ls)), nil
}

func (p *Provider) LogScopeDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteLogScope(ctx, strParam(nr, "name")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) SettingsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	st, err := p.core.GetSettings(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(settingsToWire(name, st)), nil
}

func (p *Provider) SettingsUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	st, err := p.core.UpdateSettings(ctx, name, settingsFromWire(bodyOf(nr)), updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(settingsToWire(name, st)), nil
}

func (p *Provider) CmekGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	st, err := p.core.GetCmekSettings(ctx, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(cmekToWire(name, st)), nil
}

func (p *Provider) CmekUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	st, err := p.core.UpdateCmekSettings(ctx, name, cmekFromWire(bodyOf(nr)), updateMaskFromQuery(nr.Params["updateMask"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(cmekToWire(name, st)), nil
}

// ─── wire transcoding ─────────────────────────────────────────────────────────

func bucketParent(name string) string {
	if lp, _, err := core.ParseBucketName(name); err == nil {
		return lp
	}
	return ""
}

func viewParent(name string) string {
	if b, _, err := core.ParseViewName(name); err == nil {
		return b
	}
	return ""
}

func linkParent(name string) string {
	if b, _, err := core.ParseLinkName(name); err == nil {
		return b
	}
	return ""
}

// linkOperationLocation maps a bucket name to the location parent real GCP
// scopes that bucket's operations under.
func linkOperationLocation(bucket string) string {
	if lp, _, err := core.ParseBucketName(bucket); err == nil {
		return lp
	}
	return bucket
}

func logScopeParent(name string) string {
	if lp, _, err := core.ParseLogScopeName(name); err == nil {
		return lp
	}
	return ""
}

func bucketFromWire(v any) loggingstore.LogBucket {
	m, _ := v.(map[string]any)
	if m == nil {
		return loggingstore.LogBucket{}
	}
	b := loggingstore.LogBucket{
		Name:             strFrom(m["name"]),
		Description:      strFrom(m["description"]),
		RetentionDays:    int32(intFrom(m["retentionDays"])),
		Locked:           boolFrom(m["locked"]),
		LifecycleState:   strFrom(m["lifecycleState"]),
		AnalyticsEnabled: boolFrom(m["analyticsEnabled"]),
	}
	if arr, ok := m["restrictedFields"].([]any); ok {
		for _, e := range arr {
			if s, ok := e.(string); ok {
				b.RestrictedFields = append(b.RestrictedFields, s)
			}
		}
	}
	if arr, ok := m["indexConfigs"].([]any); ok {
		for _, e := range arr {
			em, _ := e.(map[string]any)
			if em == nil {
				continue
			}
			b.IndexConfigs = append(b.IndexConfigs, loggingstore.LogIndexConfig{
				FieldPath: strFrom(em["fieldPath"]),
				Type:      strFrom(em["type"]),
			})
		}
	}
	if cm, ok := m["cmekSettings"].(map[string]any); ok {
		c := cmekFromWire(cm)
		b.Cmek = &c
	}
	return b
}

func bucketToWire(locationParent string, b loggingstore.LogBucket) map[string]any {
	out := map[string]any{"name": core.BucketResourceName(locationParent, b.Name)}
	if b.Description != "" {
		out["description"] = b.Description
	}
	if b.RetentionDays != 0 {
		out["retentionDays"] = b.RetentionDays
	}
	if b.Locked {
		out["locked"] = true
	}
	if b.LifecycleState != "" {
		out["lifecycleState"] = b.LifecycleState
	}
	if b.AnalyticsEnabled {
		out["analyticsEnabled"] = true
	}
	if len(b.RestrictedFields) > 0 {
		fields := make([]any, 0, len(b.RestrictedFields))
		for _, f := range b.RestrictedFields {
			fields = append(fields, f)
		}
		out["restrictedFields"] = fields
	}
	if len(b.IndexConfigs) > 0 {
		list := make([]any, 0, len(b.IndexConfigs))
		for _, ic := range b.IndexConfigs {
			e := map[string]any{"fieldPath": ic.FieldPath, "type": ic.Type}
			if ic.CreateTime != 0 {
				e["createTime"] = unixNanosRFC3339(ic.CreateTime)
			}
			list = append(list, e)
		}
		out["indexConfigs"] = list
	}
	if b.Cmek != nil {
		out["cmekSettings"] = cmekToWire(core.BucketResourceName(locationParent, b.Name), *b.Cmek)
	}
	if b.CreateTime != 0 {
		out["createTime"] = unixNanosRFC3339(b.CreateTime)
	}
	if b.UpdateTime != 0 {
		out["updateTime"] = unixNanosRFC3339(b.UpdateTime)
	}
	return out
}

func viewFromWire(v any) loggingstore.LogView {
	m, _ := v.(map[string]any)
	if m == nil {
		return loggingstore.LogView{}
	}
	return loggingstore.LogView{
		Name:        strFrom(m["name"]),
		Description: strFrom(m["description"]),
		Filter:      strFrom(m["filter"]),
	}
}

func viewToWire(bucketName string, v loggingstore.LogView) map[string]any {
	out := map[string]any{"name": core.ViewResourceName(bucketName, v.Name)}
	if v.Description != "" {
		out["description"] = v.Description
	}
	if v.Filter != "" {
		out["filter"] = v.Filter
	}
	if v.CreateTime != 0 {
		out["createTime"] = unixNanosRFC3339(v.CreateTime)
	}
	if v.UpdateTime != 0 {
		out["updateTime"] = unixNanosRFC3339(v.UpdateTime)
	}
	return out
}

func linkFromWire(v any) loggingstore.LogLink {
	m, _ := v.(map[string]any)
	if m == nil {
		return loggingstore.LogLink{}
	}
	l := loggingstore.LogLink{
		Name:        strFrom(m["name"]),
		Description: strFrom(m["description"]),
	}
	if ds, ok := m["bigqueryDataset"].(map[string]any); ok {
		l.BigQueryDatasetID = strFrom(ds["datasetId"])
	}
	return l
}

func linkToWire(bucketName string, l loggingstore.LogLink) map[string]any {
	out := map[string]any{"name": core.LinkResourceName(bucketName, l.Name)}
	if l.Description != "" {
		out["description"] = l.Description
	}
	if l.BigQueryDatasetID != "" {
		out["bigqueryDataset"] = map[string]any{"datasetId": l.BigQueryDatasetID}
	}
	if l.LifecycleState != "" {
		out["lifecycleState"] = l.LifecycleState
	}
	if l.CreateTime != 0 {
		out["createTime"] = unixNanosRFC3339(l.CreateTime)
	}
	return out
}

func logScopeFromWire(v any) loggingstore.LogScope {
	m, _ := v.(map[string]any)
	if m == nil {
		return loggingstore.LogScope{}
	}
	return loggingstore.LogScope{
		Name:          strFrom(m["name"]),
		Description:   strFrom(m["description"]),
		ResourceNames: strListFrom(m["resourceNames"]),
	}
}

func logScopeToWire(locationParent string, ls loggingstore.LogScope) map[string]any {
	out := map[string]any{"name": core.LogScopeResourceName(locationParent, ls.Name)}
	if ls.Description != "" {
		out["description"] = ls.Description
	}
	if len(ls.ResourceNames) > 0 {
		out["resourceNames"] = ls.ResourceNames
	}
	if ls.CreateTime != 0 {
		out["createTime"] = unixNanosRFC3339(ls.CreateTime)
	}
	if ls.UpdateTime != 0 {
		out["updateTime"] = unixNanosRFC3339(ls.UpdateTime)
	}
	return out
}

func settingsFromWire(v any) loggingstore.LogSettings {
	m, _ := v.(map[string]any)
	if m == nil {
		return loggingstore.LogSettings{}
	}
	s := loggingstore.LogSettings{
		KmsKeyName:          strFrom(m["kmsKeyName"]),
		KmsServiceAccountID: strFrom(m["kmsServiceAccountId"]),
		StorageLocation:     strFrom(m["storageLocation"]),
		DisableDefaultSink:  boolFrom(m["disableDefaultSink"]),
	}
	if _, ok := m["defaultSinkConfig"]; ok {
		if raw, err := json.Marshal(m["defaultSinkConfig"]); err == nil {
			s.DefaultSinkConfig = raw
		}
	}
	return s
}

func settingsToWire(name string, s loggingstore.LogSettings) map[string]any {
	out := map[string]any{"name": name}
	if s.KmsKeyName != "" {
		out["kmsKeyName"] = s.KmsKeyName
	}
	if s.KmsServiceAccountID != "" {
		out["kmsServiceAccountId"] = s.KmsServiceAccountID
	}
	if s.StorageLocation != "" {
		out["storageLocation"] = s.StorageLocation
	}
	if s.DisableDefaultSink {
		out["disableDefaultSink"] = true
	}
	if len(s.DefaultSinkConfig) > 0 {
		var v any
		if err := json.Unmarshal(s.DefaultSinkConfig, &v); err == nil {
			out["defaultSinkConfig"] = v
		}
	}
	return out
}

func cmekFromWire(m map[string]any) loggingstore.LogCmekSettings {
	return loggingstore.LogCmekSettings{
		KmsKeyName:        strFrom(m["kmsKeyName"]),
		KmsKeyVersionName: strFrom(m["kmsKeyVersionName"]),
		ServiceAccountID:  strFrom(m["serviceAccountId"]),
	}
}

func cmekToWire(name string, c loggingstore.LogCmekSettings) map[string]any {
	out := map[string]any{"name": name}
	if c.KmsKeyName != "" {
		out["kmsKeyName"] = c.KmsKeyName
	}
	if c.KmsKeyVersionName != "" {
		out["kmsKeyVersionName"] = c.KmsKeyVersionName
	}
	if c.ServiceAccountID != "" {
		out["serviceAccountId"] = c.ServiceAccountID
	}
	return out
}

// operationToWire renders a terminal google.longrunning.Operation as the REST
// JSON the grpc-gateway transcode produces: a done operation with a typed
// response.
func operationToWire(name, typeURL string, response map[string]any) map[string]any {
	resp := map[string]any{"@type": "type.googleapis.com/" + typeURL}
	for k, v := range response {
		resp[k] = v
	}
	return map[string]any{"name": name, "done": true, "response": resp}
}

// unixNanosRFC3339 renders a UnixNano timestamp as the protojson spelling of a
// google-datetime field.
func unixNanosRFC3339(nanos int64) string {
	return timestamp.Canonical(time.Unix(0, nanos))
}
