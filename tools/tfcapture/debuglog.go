package main

import (
	"encoding/json"
	"regexp"
	"strings"
)

// Request is one provider HTTP request recovered from a TF_LOG=DEBUG log.
type Request struct {
	Method string
	Path   string
	Body   map[string]any
	Raw    string
}

// requestPrefix matches the per-line prefix the plugin SDK writes in front of
// each provider log line, e.g.
//
//	2026-11-05T17:47:42.957-0800 [DEBUG] provider.terraform-provider-google_v7.46.2_x5: ---[ REQUEST ]---
//
// The timestamp and provider version vary, so only the marker through the
// colon-space is anchored.
var requestPrefix = regexp.MustCompile(`\[DEBUG\] provider\.terraform-provider-google[^:]*: ?`)

// requestLine matches "GET /path?query HTTP/1.1".
var requestLine = regexp.MustCompile(`^([A-Z]+) (\S+) HTTP/\d(?:\.\d)?$`)

// ParseDebugLog extracts the `---[ REQUEST ]---` blocks from a TF_LOG=DEBUG
// capture. Blocks without a JSON body (GET/DELETE, multipart uploads) are
// returned with a nil Body so the caller can still count them.
func ParseDebugLog(log string) []Request {
	var (
		out     []Request
		inBlock bool
		lines   []string
	)
	flush := func() {
		if len(lines) == 0 {
			inBlock = false
			return
		}
		if req, ok := parseBlock(lines); ok {
			out = append(out, req)
		}
		lines = nil
		inBlock = false
	}
	for _, raw := range strings.Split(log, "\n") {
		line := raw
		if loc := requestPrefix.FindStringIndex(line); loc != nil {
			line = line[loc[1]:]
		}
		switch {
		case strings.HasPrefix(line, "---[ REQUEST ]"):
			flush()
			inBlock = true
			lines = nil
		case inBlock && strings.HasPrefix(line, "-----"):
			flush()
		case inBlock:
			lines = append(lines, line)
		}
	}
	flush()
	return out
}

func parseBlock(lines []string) (Request, bool) {
	var req Request
	idx := -1
	for i, line := range lines {
		if m := requestLine.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			req.Method = m[1]
			req.Path = m[2]
			idx = i
			break
		}
	}
	if idx < 0 {
		return Request{}, false
	}
	// The JSON body starts at the first line that opens an object/array: the
	// google provider logs headers without a separating blank line, and the
	// plugin can interleave debug lines after the body.
	bodyStart := -1
	for i := idx + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			bodyStart = i
			break
		}
	}
	if bodyStart < 0 {
		return req, true
	}
	raw := strings.Join(lines[bodyStart:], "\n")
	var body map[string]any
	// A decoder stops after the first JSON value, so trailing plugin debug
	// lines cannot break the parse.
	if err := json.NewDecoder(strings.NewReader(raw)).Decode(&body); err != nil {
		// A non-JSON body (multipart upload) is not part of the attribute join.
		return req, true
	}
	req.Body = body
	req.Raw = raw
	return req, true
}

// pathRules maps a REST path shape to the Terraform provider resource type.
// Only resources in the provider-attributes mapping are listed; an unlisted
// request is counted but not joined.
var pathRules = []struct {
	method string
	re     *regexp.Regexp
	res    string
}{
	{method: "POST", re: regexp.MustCompile(`^/storage/v1/b/?$`), res: "google_storage_bucket"},
	// Bucket update only: /storage/v1/b/{bucket}/o/{object} is objects.update
	// and /storage/v1/b/{bucket}/iam is setIamPolicy, which must not be
	// attributed to the bucket resource.
	{method: "PATCH", re: regexp.MustCompile(`^/storage/v1/b/[^/?]+/?$`), res: "google_storage_bucket"},
	{method: "PUT", re: regexp.MustCompile(`^/storage/v1/b/[^/?]+/?$`), res: "google_storage_bucket"},
	{method: "PUT", re: regexp.MustCompile(`^/v1/projects/[^/]+/topics/[^/?]+`), res: "google_pubsub_topic"},
	{method: "PATCH", re: regexp.MustCompile(`^/v1/projects/[^/]+/topics/[^/?]+`), res: "google_pubsub_topic"},
	{method: "PUT", re: regexp.MustCompile(`^/v1/projects/[^/]+/subscriptions/[^/?]+`), res: "google_pubsub_subscription"},
	{method: "PATCH", re: regexp.MustCompile(`^/v1/projects/[^/]+/subscriptions/[^/?]+`), res: "google_pubsub_subscription"},
	{method: "POST", re: regexp.MustCompile(`^/v1/projects/[^/]+/databases/?$`), res: "google_firestore_database"},
	{method: "PATCH", re: regexp.MustCompile(`^/v1/projects/[^/]+/databases/[^/?]+`), res: "google_firestore_database"},
	{method: "POST", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/keyRings/?$`), res: "google_kms_key_ring"},
	{method: "POST", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/?$`), res: "google_kms_crypto_key"},
	{method: "PATCH", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/?]+`), res: "google_kms_crypto_key"},
	{method: "POST", re: regexp.MustCompile(`^/dns/v1/projects/[^/]+/managedZones/?$`), res: "google_dns_managed_zone"},
	{method: "PATCH", re: regexp.MustCompile(`^/dns/v1/projects/[^/]+/managedZones/[^/?]+`), res: "google_dns_managed_zone"},
	{method: "PUT", re: regexp.MustCompile(`^/dns/v1/projects/[^/]+/managedZones/[^/?]+`), res: "google_dns_managed_zone"},
	// Cloud Functions v1 (google_cloudfunctions_function) and v2
	// (google_cloudfunctions2_function) create with the resource under the
	// location parent; both update in place with PATCH + updateMask.
	{method: "POST", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/functions/?$`), res: "google_cloudfunctions_function"},
	{method: "PATCH", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/functions/[^/?]+`), res: "google_cloudfunctions_function"},
	{method: "PUT", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/functions/[^/?]+`), res: "google_cloudfunctions_function"},
	{method: "POST", re: regexp.MustCompile(`^/v2/projects/[^/]+/locations/[^/]+/functions/?$`), res: "google_cloudfunctions2_function"},
	{method: "PATCH", re: regexp.MustCompile(`^/v2/projects/[^/]+/locations/[^/]+/functions/[^/?]+`), res: "google_cloudfunctions2_function"},
	{method: "PUT", re: regexp.MustCompile(`^/v2/projects/[^/]+/locations/[^/]+/functions/[^/?]+`), res: "google_cloudfunctions2_function"},
	// Memorystore (google_redis_instance).
	{method: "POST", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/instances/?$`), res: "google_redis_instance"},
	{method: "PATCH", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/instances/[^/?]+`), res: "google_redis_instance"},
	{method: "PUT", re: regexp.MustCompile(`^/v1/projects/[^/]+/locations/[^/]+/instances/[^/?]+`), res: "google_redis_instance"},
	// Cloud Monitoring (google_monitoring_uptime_check_config and
	// google_monitoring_alert_policy) share the /v3 project surface.
	{method: "POST", re: regexp.MustCompile(`^/v3/projects/[^/]+/uptimeCheckConfigs/?$`), res: "google_monitoring_uptime_check_config"},
	{method: "PATCH", re: regexp.MustCompile(`^/v3/projects/[^/]+/uptimeCheckConfigs/[^/?]+`), res: "google_monitoring_uptime_check_config"},
	{method: "PUT", re: regexp.MustCompile(`^/v3/projects/[^/]+/uptimeCheckConfigs/[^/?]+`), res: "google_monitoring_uptime_check_config"},
	{method: "POST", re: regexp.MustCompile(`^/v3/projects/[^/]+/alertPolicies/?$`), res: "google_monitoring_alert_policy"},
	{method: "PATCH", re: regexp.MustCompile(`^/v3/projects/[^/]+/alertPolicies/[^/?]+`), res: "google_monitoring_alert_policy"},
	{method: "PUT", re: regexp.MustCompile(`^/v3/projects/[^/]+/alertPolicies/[^/?]+`), res: "google_monitoring_alert_policy"},
	// Cloud Run v2 (google_cloud_run_v2_service). The collection POST creates a
	// service; PATCH updates one. The service IAM custom methods are POSTs on
	// .../services/{name}:getIamPolicy and :setIamPolicy, so the POST rule must
	// anchor on the collection and the PATCH rule must exclude ':' and '/'.
	{method: "POST", re: regexp.MustCompile(`^/v2/projects/[^/]+/locations/[^/]+/services/?$`), res: "google_cloud_run_v2_service"},
	{method: "PATCH", re: regexp.MustCompile(`^/v2/projects/[^/]+/locations/[^/]+/services/[^/:?]+/?$`), res: "google_cloud_run_v2_service"},
	// BigQuery (google_bigquery_dataset, google_bigquery_table). The provider
	// creates with a collection POST and updates with a full-body PUT (both
	// datasets.update and tables.update use PUT; the merge-equivalent PATCH is
	// accepted by the emulator adapter but the provider does not send it).
	// tables.insert/tabledata paths are excluded by anchoring on the exact
	// collection/resource shape.
	{method: "POST", re: regexp.MustCompile(`^/bigquery/v2/projects/[^/]+/datasets/?$`), res: "google_bigquery_dataset"},
	{method: "PUT", re: regexp.MustCompile(`^/bigquery/v2/projects/[^/]+/datasets/[^/?]+/?$`), res: "google_bigquery_dataset"},
	{method: "PATCH", re: regexp.MustCompile(`^/bigquery/v2/projects/[^/]+/datasets/[^/?]+/?$`), res: "google_bigquery_dataset"},
	{method: "POST", re: regexp.MustCompile(`^/bigquery/v2/projects/[^/]+/datasets/[^/]+/tables/?$`), res: "google_bigquery_table"},
	{method: "PUT", re: regexp.MustCompile(`^/bigquery/v2/projects/[^/]+/datasets/[^/]+/tables/[^/:?]+/?$`), res: "google_bigquery_table"},
	{method: "PATCH", re: regexp.MustCompile(`^/bigquery/v2/projects/[^/]+/datasets/[^/]+/tables/[^/:?]+/?$`), res: "google_bigquery_table"},
}

// ResourceForPath returns the provider resource type a captured request writes,
// or "" when the path is not part of the mapped surface.
func ResourceForPath(method, path string) string {
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	for _, rule := range pathRules {
		if rule.method != method {
			continue
		}
		if rule.re.MatchString(path) {
			return rule.res
		}
	}
	return ""
}
