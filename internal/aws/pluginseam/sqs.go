// Package pluginseam routes AWS providers' internal cross-service deliveries
// through the plugin chain, so a plugin that overrides a service sees every
// flow into it, not only calls that arrive over the wire.
//
// Each seam is installed only when a plugin host exists; otherwise the
// providers behave exactly as before.
package pluginseam

import (
	"context"
	"strings"

	awsarn "jaiscloud/internal/aws/arn"
	"jaiscloud/internal/aws/provider/queue"
	"jaiscloud/internal/model"
	"jaiscloud/internal/pluginhost"
	"jaiscloud/internal/provider"
)

// SQSInternalSend returns the hook for queue.QueueProvider.SetInternalSendHook.
// It makes SNS→SQS, S3 notification→SQS and Lambda DLQ→SQS deliveries appear
// to plugins as an SQS SendMessage with SourceArn/ServicePrincipal set, and
// lets before/after/replace links act on them. The built-in delivery stays the
// innermost step, so unrelated behaviour (queue resolution, waiters) is kept.
//
// key is the registry key of SQS SendMessage ("Queue.SendMessage" on AWS).
func SQSInternalSend(h *pluginhost.Host, key, defaultAccount, defaultRegion string) queue.InternalSendHook {
	return func(ctx context.Context, target, body string, attrs map[string]queue.MessageAttribute, src queue.SourceContext, deliver queue.InternalSendDeliver) error {
		account, region := scopeOf(target, defaultAccount, defaultRegion)
		params := map[string]any{"QueueUrl": target, "MessageBody": body}
		if len(attrs) > 0 {
			wire := make(map[string]any, len(attrs))
			for k, v := range attrs {
				wire[k] = map[string]any{"DataType": v.DataType, "StringValue": v.StringValue}
			}
			params["MessageAttributes"] = wire
		}
		nr := &model.NormalizedRequest{
			Service: "sqs", Action: "SendMessage", Cloud: model.CloudAWS,
			AccountID: account, Region: region, Params: params,
			SourceArn: src.SourceArn, ServicePrincipal: src.ServicePrincipal,
		}
		core := func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			// Honour edits a before-hook made to the delivery.
			t, _ := nr.Params["QueueUrl"].(string)
			b, _ := nr.Params["MessageBody"].(string)
			if err := deliver(ctx, t, b, attrsFromParams(nr.Params["MessageAttributes"]), src); err != nil {
				return nil, err
			}
			return provider.OK(map[string]any{}), nil
		}
		_, err := h.InternalCall(ctx, key, nr, core)
		return err
	}
}

// scopeOf derives account and region from an ARN; a URL carries only the account.
func scopeOf(target, defaultAccount, defaultRegion string) (account, region string) {
	account, region = defaultAccount, defaultRegion
	if strings.HasPrefix(target, "arn:") {
		if a, err := awsarn.Parse(target); err == nil {
			if a.AccountID != "" {
				account = a.AccountID
			}
			if a.Region != "" {
				region = a.Region
			}
		}
	}
	return account, region
}

func attrsFromParams(v any) map[string]queue.MessageAttribute {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]queue.MessageAttribute, len(m))
	for k, raw := range m {
		if a, ok := raw.(map[string]any); ok {
			dt, _ := a["DataType"].(string)
			sv, _ := a["StringValue"].(string)
			out[k] = queue.MessageAttribute{DataType: dt, StringValue: sv}
		}
	}
	return out
}
