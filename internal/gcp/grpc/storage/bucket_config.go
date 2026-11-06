package storage

import (
	"strconv"
	"time"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	date "google.golang.org/genproto/googleapis/type/date"
)

// The gRPC bucket configuration blocks (lifecycle, CORS, encryption and soft
// delete) are stored under the same JSON-shaped keys and camelCase members the
// REST v1 insert body persists, so a bucket created on one transport reads back
// identically on the other. Only the members both surfaces define are mapped;
// REST-only lifecycle conditions and the encryption enforcement configs remain
// unmapped (they stay classified unsupported on gRPC).

// lifecycleToMap converts proto Bucket.Lifecycle into the REST storage#bucket
// lifecycle object ({rule: [{action, condition}]}).
func lifecycleToMap(lc *storagepb.Bucket_Lifecycle) map[string]any {
	if lc == nil {
		return nil
	}
	rules := make([]any, 0, len(lc.GetRule()))
	for _, r := range lc.GetRule() {
		rule := map[string]any{}
		if a := r.GetAction(); a != nil {
			action := map[string]any{}
			if v := a.GetType(); v != "" {
				action["type"] = v
			}
			if v := a.GetStorageClass(); v != "" {
				action["storageClass"] = v
			}
			rule["action"] = action
		}
		if c := r.GetCondition(); c != nil {
			rule["condition"] = lifecycleConditionToMap(c)
		}
		rules = append(rules, rule)
	}
	return map[string]any{"rule": rules}
}

func lifecycleConditionToMap(c *storagepb.Bucket_Lifecycle_Rule_Condition) map[string]any {
	out := map[string]any{}
	if c.AgeDays != nil {
		out["age"] = int(c.GetAgeDays())
	}
	if d := c.GetCreatedBefore(); d != nil {
		out["createdBefore"] = dateToREST(d)
	}
	if c.IsLive != nil {
		out["isLive"] = c.GetIsLive()
	}
	if c.NumNewerVersions != nil {
		out["numNewerVersions"] = int(c.GetNumNewerVersions())
	}
	if v := c.GetMatchesStorageClass(); len(v) > 0 {
		out["matchesStorageClass"] = stringsToAny(v)
	}
	if c.DaysSinceCustomTime != nil {
		out["daysSinceCustomTime"] = int(c.GetDaysSinceCustomTime())
	}
	if d := c.GetCustomTimeBefore(); d != nil {
		out["customTimeBefore"] = dateToREST(d)
	}
	if c.DaysSinceNoncurrentTime != nil {
		out["daysSinceNoncurrentTime"] = int(c.GetDaysSinceNoncurrentTime())
	}
	if d := c.GetNoncurrentTimeBefore(); d != nil {
		out["noncurrentTimeBefore"] = dateToREST(d)
	}
	if v := c.GetMatchesPrefix(); len(v) > 0 {
		out["matchesPrefix"] = stringsToAny(v)
	}
	if v := c.GetMatchesSuffix(); len(v) > 0 {
		out["matchesSuffix"] = stringsToAny(v)
	}
	return out
}

// lifecycleFromMap builds the proto Bucket.Lifecycle from a stored/REST-shaped
// lifecycle object.
func lifecycleFromMap(m map[string]any) *storagepb.Bucket_Lifecycle {
	if m == nil {
		return nil
	}
	lc := &storagepb.Bucket_Lifecycle{}
	for _, raw := range anySlice(m["rule"]) {
		rm, _ := raw.(map[string]any)
		if rm == nil {
			continue
		}
		rule := &storagepb.Bucket_Lifecycle_Rule{}
		if am, _ := rm["action"].(map[string]any); am != nil {
			rule.Action = &storagepb.Bucket_Lifecycle_Rule_Action{
				Type:         mapString(am, "type"),
				StorageClass: mapString(am, "storageClass"),
			}
		}
		if cm, _ := rm["condition"].(map[string]any); cm != nil {
			rule.Condition = lifecycleConditionFromMap(cm)
		}
		lc.Rule = append(lc.Rule, rule)
	}
	return lc
}

func lifecycleConditionFromMap(m map[string]any) *storagepb.Bucket_Lifecycle_Rule_Condition {
	c := &storagepb.Bucket_Lifecycle_Rule_Condition{
		MatchesStorageClass: mapStrings(m["matchesStorageClass"]),
		MatchesPrefix:       mapStrings(m["matchesPrefix"]),
		MatchesSuffix:       mapStrings(m["matchesSuffix"]),
	}
	if v, ok := mapInt32(m, "age"); ok {
		c.AgeDays = &v
	}
	if v := mapString(m, "createdBefore"); v != "" {
		if d, err := dateFromREST(v); err == nil {
			c.CreatedBefore = d
		}
	}
	if v, ok := mapBool(m, "isLive"); ok {
		c.IsLive = &v
	}
	if v, ok := mapInt32(m, "numNewerVersions"); ok {
		c.NumNewerVersions = &v
	}
	if v, ok := mapInt32(m, "daysSinceCustomTime"); ok {
		c.DaysSinceCustomTime = &v
	}
	if v := mapString(m, "customTimeBefore"); v != "" {
		if d, err := dateFromREST(v); err == nil {
			c.CustomTimeBefore = d
		}
	}
	if v, ok := mapInt32(m, "daysSinceNoncurrentTime"); ok {
		c.DaysSinceNoncurrentTime = &v
	}
	if v := mapString(m, "noncurrentTimeBefore"); v != "" {
		if d, err := dateFromREST(v); err == nil {
			c.NoncurrentTimeBefore = d
		}
	}
	return c
}

// corsToMap converts proto Bucket.Cors rules into the REST cors array.
func corsToMap(rules []*storagepb.Bucket_Cors) []any {
	if rules == nil {
		return nil
	}
	out := make([]any, 0, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		m := map[string]any{}
		if v := r.GetOrigin(); len(v) > 0 {
			m["origin"] = stringsToAny(v)
		}
		if v := r.GetMethod(); len(v) > 0 {
			m["method"] = stringsToAny(v)
		}
		if v := r.GetResponseHeader(); len(v) > 0 {
			m["responseHeader"] = stringsToAny(v)
		}
		if v := r.GetMaxAgeSeconds(); v != 0 {
			m["maxAgeSeconds"] = int(v)
		}
		out = append(out, m)
	}
	return out
}

// corsFromMap builds the proto CORS rules from a stored/REST-shaped cors array.
func corsFromMap(v any) []*storagepb.Bucket_Cors {
	list := anySlice(v)
	if list == nil {
		return nil
	}
	out := make([]*storagepb.Bucket_Cors, 0, len(list))
	for _, raw := range list {
		m, _ := raw.(map[string]any)
		if m == nil {
			continue
		}
		out = append(out, &storagepb.Bucket_Cors{
			Origin:         mapStrings(m["origin"]),
			Method:         mapStrings(m["method"]),
			ResponseHeader: mapStrings(m["responseHeader"]),
			MaxAgeSeconds:  int32(mapInt(m, "maxAgeSeconds")),
		})
	}
	return out
}

// encryptionToMap converts proto Bucket.Encryption into the REST encryption
// object. Only the CMEK default key is shared by both surfaces; the
// enforcement configs have no REST-equivalent mapping here yet.
func encryptionToMap(e *storagepb.Bucket_Encryption) map[string]any {
	if e == nil {
		return nil
	}
	m := map[string]any{}
	if v := e.GetDefaultKmsKey(); v != "" {
		m["defaultKmsKeyName"] = v
	}
	return m
}

func encryptionFromMap(m map[string]any) *storagepb.Bucket_Encryption {
	if m == nil {
		return nil
	}
	return &storagepb.Bucket_Encryption{DefaultKmsKey: mapString(m, "defaultKmsKeyName")}
}

// softDeletePolicyToMap converts proto Bucket.SoftDeletePolicy into the REST
// softDeletePolicy object. effective_time is OUTPUT_ONLY and server-owned: the
// caller's value is ignored and the server stamps the apply time.
func softDeletePolicyToMap(sd *storagepb.Bucket_SoftDeletePolicy, now string) map[string]any {
	if sd == nil || sd.GetRetentionDuration() == nil {
		return nil
	}
	return map[string]any{
		"retentionDurationSeconds": strconv.FormatInt(int64(sd.GetRetentionDuration().AsDuration().Seconds()), 10),
		"effectiveTime":            now,
	}
}

// dateToREST renders a google.type.Date as the RFC 3339 date-only string the
// JSON API uses.
func dateToREST(d *date.Date) string {
	if d == nil {
		return ""
	}
	return time.Date(int(d.GetYear()), time.Month(d.GetMonth()), int(d.GetDay()), 0, 0, 0, 0, time.UTC).Format("2006-01-02")
}

// dateFromREST parses an RFC 3339 date-only string into google.type.Date.
func dateFromREST(s string) (*date.Date, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return nil, err
	}
	return &date.Date{Year: int32(t.Year()), Month: int32(t.Month()), Day: int32(t.Day())}, nil
}

// anySlice normalizes a stored list — []any after a JSON round-trip, or
// []string/[]map from an in-process writer.
func anySlice(v any) []any {
	switch list := v.(type) {
	case []any:
		return list
	case []map[string]any:
		out := make([]any, 0, len(list))
		for _, m := range list {
			out = append(out, m)
		}
		return out
	default:
		return nil
	}
}

func mapInt(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	}
	return 0
}

func mapInt32(m map[string]any, key string) (int32, bool) {
	if _, ok := m[key]; !ok {
		return 0, false
	}
	switch v := m[key].(type) {
	case float64:
		return int32(v), true
	case int:
		return int32(v), true
	case int32:
		return v, true
	case int64:
		return int32(v), true
	}
	return 0, false
}

func mapBool(m map[string]any, key string) (bool, bool) {
	v, ok := m[key].(bool)
	return v, ok
}
