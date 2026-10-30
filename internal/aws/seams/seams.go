// Package seams turns one AWS service's internal calls into another service's
// registry requests.
//
// Rule: a provider never calls another provider's methods. It calls a seam, and
// the seam calls Registry.Dispatch with Origin=internal. That is what makes a
// plugin that overrides a key the implementation for every caller — wire,
// internal and UI — exactly as if the built-in did not exist. The built-in
// handler for the key serves the internal origin itself (see
// queue.QueueProvider.SendMessage).
//
// Seams are thin on purpose: build the wire-shaped request, dispatch, translate
// the answer. Nothing here knows about plugins.
package seams

import (
	"context"
	"strings"

	awsarn "jaiscloud/internal/aws/arn"
	"jaiscloud/internal/aws/provider/queue"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Scope is the account and region used when a target does not carry its own.
type Scope struct {
	Account, Region string
	// Clock and Port are what the gateway injects into every wire request;
	// internal callers get the same so a handler cannot tell them apart.
	Clock clock.Clock
	Port  int
}

// SQS delivers messages to queues on behalf of SNS, S3 notifications, Lambda
// DLQs and other producers. It implements queue.InternalSendAPI.
type SQS struct {
	reg   *provider.Registry
	key   string // registry key of SQS SendMessage ("Queue.SendMessage")
	scope Scope
}

var _ queue.InternalSendAPI = (*SQS)(nil)

// NewSQS returns the SQS seam. key is the registry key of SQS SendMessage.
func NewSQS(reg *provider.Registry, key string, scope Scope) *SQS {
	return &SQS{reg: reg, key: key, scope: scope}
}

// InternalSend sends body to the queue named by an ARN or URL.
func (s *SQS) InternalSend(ctx context.Context, target, body string, attrs map[string]queue.MessageAttribute, src queue.SourceContext) error {
	account, region := scopeOf(target, s.scope)
	params := map[string]any{"QueueUrl": target, "MessageBody": body}
	if len(attrs) > 0 {
		wire := make(map[string]any, len(attrs))
		for k, v := range attrs {
			wire[k] = map[string]any{"DataType": v.DataType, "StringValue": v.StringValue}
		}
		params["MessageAttributes"] = wire
	}
	_, err := s.reg.Dispatch(ctx, s.key, &model.NormalizedRequest{
		Service: "sqs", Action: "SendMessage", Cloud: model.CloudAWS, Origin: model.OriginInternal,
		AccountID: account, Region: region, Params: params,
		SourceArn: src.SourceArn, ServicePrincipal: src.ServicePrincipal,
		Clock: s.scope.Clock, Port: s.scope.Port, ResourceID: awsarn.ResourceID(region, account),
	})
	return err
}

// scopeOf derives account and region from an ARN. Anything else (a queue URL, a
// bucket or log-group name, a function name) falls back to the default scope.
func scopeOf(target string, def Scope) (account, region string) {
	account, region = def.Account, def.Region
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
