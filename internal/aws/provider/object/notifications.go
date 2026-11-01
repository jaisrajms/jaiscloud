package object

import (
	"context"
	"encoding/json"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

type s3KeyFilter struct {
	Prefix string
	Suffix string
}

type s3QueueNotification struct {
	Id       string
	QueueArn string
	Events   []string
	Filter   s3KeyFilter
}

type s3TopicNotification struct {
	Id       string
	TopicArn string
	Events   []string
	Filter   s3KeyFilter
}

type s3LambdaNotification struct {
	Id            string
	LambdaFuncArn string
	Events        []string
	Filter        s3KeyFilter
}

type bucketNotificationConfig struct {
	QueueConfigurations  []s3QueueNotification
	TopicConfigurations  []s3TopicNotification
	LambdaConfigurations []s3LambdaNotification
	// EventBridge is true when the bucket was configured with
	// <EventBridgeConfiguration/>; S3 sends events to EventBridge only then.
	EventBridge bool
}

// extractNotificationConfig builds a bucketNotificationConfig from parsed params.
// The S3 codec stores the raw notification config in nr.Params["_notification_config"]
// as a map[string]any representing the parsed XML.
func extractNotificationConfig(params map[string]any) bucketNotificationConfig {
	raw, ok := params["_notification_config"]
	if !ok {
		return bucketNotificationConfig{}
	}
	cfg, ok := raw.(map[string]any)
	if !ok {
		return bucketNotificationConfig{}
	}

	var result bucketNotificationConfig

	if qcs, ok := cfg["QueueConfigurations"].([]any); ok {
		for _, qc := range qcs {
			m, ok := qc.(map[string]any)
			if !ok {
				continue
			}
			n := s3QueueNotification{
				Id:       stringField(m, "Id"),
				QueueArn: stringField(m, "QueueArn"),
				Events:   stringList(m, "Events"),
				Filter:   extractFilter(m),
			}
			result.QueueConfigurations = append(result.QueueConfigurations, n)
		}
	}
	if tcs, ok := cfg["TopicConfigurations"].([]any); ok {
		for _, tc := range tcs {
			m, ok := tc.(map[string]any)
			if !ok {
				continue
			}
			n := s3TopicNotification{
				Id:       stringField(m, "Id"),
				TopicArn: stringField(m, "TopicArn"),
				Events:   stringList(m, "Events"),
				Filter:   extractFilter(m),
			}
			result.TopicConfigurations = append(result.TopicConfigurations, n)
		}
	}
	if lcs, ok := cfg["LambdaConfigurations"].([]any); ok {
		for _, lc := range lcs {
			m, ok := lc.(map[string]any)
			if !ok {
				continue
			}
			n := s3LambdaNotification{
				Id:            stringField(m, "Id"),
				LambdaFuncArn: stringField(m, "LambdaFunctionArn"),
				Events:        stringList(m, "Events"),
				Filter:        extractFilter(m),
			}
			result.LambdaConfigurations = append(result.LambdaConfigurations, n)
		}
	}
	if _, ok := cfg["EventBridgeConfiguration"]; ok {
		result.EventBridge = true
	}
	return result
}

func stringField(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func stringList(m map[string]any, key string) []string {
	raw, ok := m[key]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, s := range v {
			if str, ok := s.(string); ok {
				out = append(out, str)
			}
		}
		return out
	}
	return nil
}

func extractFilter(m map[string]any) s3KeyFilter {
	f, ok := m["Filter"].(map[string]any)
	if !ok {
		return s3KeyFilter{}
	}
	s3key, ok := f["S3Key"].(map[string]any)
	if !ok {
		return s3KeyFilter{}
	}
	var filter s3KeyFilter
	rules, _ := s3key["FilterRules"].([]any)
	for _, r := range rules {
		rm, ok := r.(map[string]any)
		if !ok {
			continue
		}
		name, _ := rm["Name"].(string)
		value, _ := rm["Value"].(string)
		switch strings.ToLower(name) {
		case "prefix":
			filter.Prefix = value
		case "suffix":
			filter.Suffix = value
		}
	}
	return filter
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

func (p *ObjectProvider) PutBucketNotificationConfiguration(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["Bucket"].(string)
	if bucket == "" {
		bucket = strParam(nr.Params, "_bucket")
	}
	if bucket == "" {
		return nil, model.NewProviderError("InvalidBucketName", "Bucket name is required", 400)
	}
	cfg := extractNotificationConfig(nr.Params)
	data, _ := json.Marshal(cfg)
	p.notifStore.Store(bucket, string(data))
	return provider.OK(map[string]any{}), nil
}

func (p *ObjectProvider) GetBucketNotificationConfiguration(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["Bucket"].(string)
	if bucket == "" {
		bucket = strParam(nr.Params, "_bucket")
	}
	raw, ok := p.notifStore.Load(bucket)
	if !ok {
		return provider.OK(map[string]any{
			"QueueConfigurations":  []any{},
			"TopicConfigurations":  []any{},
			"LambdaConfigurations": []any{},
		}), nil
	}
	var cfg bucketNotificationConfig
	json.Unmarshal([]byte(raw.(string)), &cfg)
	out := map[string]any{
		"QueueConfigurations":  cfg.QueueConfigurations,
		"TopicConfigurations":  cfg.TopicConfigurations,
		"LambdaConfigurations": cfg.LambdaConfigurations,
	}
	if cfg.EventBridge {
		out["EventBridgeConfiguration"] = map[string]any{}
	}
	return provider.OK(out), nil
}

// ─── Dispatch ─────────────────────────────────────────────────────────────────

// dispatchNotification is the legacy entry point called from PutObject/etc. with only
// event name. It delegates to dispatchS3Notification when the fanout is wired.
func (p *ObjectProvider) dispatchNotification(ctx context.Context, bucket, key, eventName string) {
	// Use new fanout path if wired.
	if p.fanout.SQS != nil || p.fanout.SNSPublisher != nil || p.fanout.Lambda != nil || p.fanout.EventBridge != nil {
		p.dispatchS3Notification(ctx, bucket, key, eventName, "", 0, "", "")
		return
	}
	// Legacy bus path (no-op if bus is nil).
	if p.bus == nil {
		return
	}
}

func matchesEvent(configured []string, eventName string) bool {
	for _, e := range configured {
		if e == eventName {
			return true
		}
		if strings.HasSuffix(e, "*") && strings.HasPrefix(eventName, e[:len(e)-1]) {
			return true
		}
	}
	return false
}

func matchesFilter(f s3KeyFilter, key string) bool {
	if f.Prefix != "" && !strings.HasPrefix(key, f.Prefix) {
		return false
	}
	if f.Suffix != "" && !strings.HasSuffix(key, f.Suffix) {
		return false
	}
	return true
}
