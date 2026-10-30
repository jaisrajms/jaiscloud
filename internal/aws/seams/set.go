package seams

import (
	"context"
	"errors"
	"fmt"

	lambdaprovider "jaiscloud/internal/aws/provider/lambda"
	lambdaesm "jaiscloud/internal/aws/provider/lambda/esm"
	"jaiscloud/internal/aws/provider/notification"
	"jaiscloud/internal/aws/provider/queue"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Set builds the seams that let one AWS service call another through the
// registry. serviceToProvider maps a wire service ("sqs") to its registry
// prefix ("Queue"), so no key is hard-coded here.
type Set struct {
	reg   *provider.Registry
	sp    func(service string) string
	scope Scope
}

func NewSet(reg *provider.Registry, serviceToProvider func(string) string, scope Scope) *Set {
	return &Set{reg: reg, sp: serviceToProvider, scope: scope}
}

func (s *Set) key(service, action string) string { return s.sp(service) + "." + action }

// call dispatches one internal request: the same key a client request would use.
func (s *Set) call(ctx context.Context, service, action, target string, params map[string]any, src queue.SourceContext) (*model.ProviderResponse, error) {
	account, region := scopeOf(target, s.scope)
	return s.reg.Dispatch(ctx, s.key(service, action), &model.NormalizedRequest{
		Service: service, Action: action, Cloud: model.CloudAWS, Origin: model.OriginInternal,
		AccountID: account, Region: region, Params: params,
		SourceArn: src.SourceArn, ServicePrincipal: src.ServicePrincipal,
	})
}

// SQS returns the producer seam (SNS, S3, EventBridge, Lambda DLQs → SQS).
func (s *Set) SQS() *SQS { return NewSQS(s.reg, s.key("sqs", "SendMessage"), s.scope) }

// ---- SQS consumer: the Lambda event-source poller ------------------------------

// SQSConsumer lets the event-source poller receive from and delete from a queue
// through the registry. It implements lambdaesm.QueueInternalAPI.
type SQSConsumer struct{ s *Set }

var _ lambdaesm.QueueInternalAPI = (*SQSConsumer)(nil)

func (s *Set) SQSConsumer() *SQSConsumer { return &SQSConsumer{s} }

func (c *SQSConsumer) InternalReceive(ctx context.Context, queueName string, maxMessages, waitTimeSec int) ([]lambdaesm.InternalMessage, error) {
	resp, err := c.s.call(ctx, "sqs", "ReceiveMessage", queueName, map[string]any{
		"QueueName": queueName, "MaxNumberOfMessages": maxMessages, "WaitTimeSeconds": waitTimeSec,
	}, queue.SourceContext{})
	if err != nil {
		return nil, err
	}
	list, _ := resp.Data["Messages"].([]any)
	out := make([]lambdaesm.InternalMessage, 0, len(list))
	for _, raw := range list {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		im := lambdaesm.InternalMessage{
			MessageID: str(m["MessageId"]), ReceiptHandle: str(m["ReceiptHandle"]),
			Body: str(m["Body"]), MD5OfBody: str(m["MD5OfBody"]),
			Attributes: map[string]string{},
		}
		if a, ok := m["Attributes"].(map[string]any); ok {
			for k, v := range a {
				im.Attributes[k] = str(v)
			}
		}
		if a, ok := m["MessageAttributes"].(map[string]any); ok {
			im.MessageAttributes = a
		}
		out = append(out, im)
	}
	return out, nil
}

func (c *SQSConsumer) InternalDeleteBatch(ctx context.Context, queueName string, receiptHandles []string) error {
	entries := make([]any, len(receiptHandles))
	for i, rh := range receiptHandles {
		entries[i] = map[string]any{"Id": fmt.Sprint(i), "ReceiptHandle": rh}
	}
	_, err := c.s.call(ctx, "sqs", "DeleteMessageBatch", queueName,
		map[string]any{"QueueName": queueName, "Entries": entries}, queue.SourceContext{})
	return err
}

// ---- Lambda ----------------------------------------------------------------------

// Lambda invokes functions on behalf of SNS, the event-source poller,
// CloudWatch, EventBridge targets, S3 notifications and secret rotation.
// One registry request (Function.InvokeFunction, origin internal) serves every
// variant of the typed API below.
type Lambda struct{ s *Set }

func (s *Set) Lambda() *Lambda { return &Lambda{s} }

var (
	_ lambdaprovider.InternalInvoker = (*Lambda)(nil)
	_ lambdaprovider.RawInvoker      = (*Lambda)(nil)
	_ lambdaesm.FunctionInvoker      = (*Lambda)(nil)
	_ notification.FunctionInvoker   = (*Lambda)(nil)
)

func (l *Lambda) invoke(ctx context.Context, name string, payload []byte, invType string) (*lambdaprovider.InvokeResult, error) {
	resp, err := l.s.call(ctx, "lambda", "InvokeFunction", name, map[string]any{
		"_function_name": name, "_payload": payload, "_invocation_type": invType,
	}, queue.SourceContext{})
	if err != nil {
		return nil, err
	}
	res := &lambdaprovider.InvokeResult{StatusCode: resp.HTTPStatus}
	res.Payload, _ = resp.Data["_payload"].([]byte)
	res.FunctionError = str(resp.Data["_function_error"])
	return res, nil
}

// InternalInvoke implements lambdaprovider.InternalInvoker.
func (l *Lambda) InternalInvoke(ctx context.Context, nameOrARN string, payload []byte, invocationType string) (*lambdaprovider.InvokeResult, error) {
	return l.invoke(ctx, nameOrARN, payload, invocationType)
}

// InternalInvokeRaw implements lambdaprovider.RawInvoker (fire and forget).
func (l *Lambda) InternalInvokeRaw(ctx context.Context, nameOrARN string, payload []byte) error {
	_, err := l.invoke(ctx, nameOrARN, payload, "Event")
	return err
}

// InvokeInternal implements lambdaesm.FunctionInvoker and
// notification.FunctionInvoker: synchronous, a function error is an error.
func (l *Lambda) InvokeInternal(ctx context.Context, name string, payload []byte) ([]byte, error) {
	res, err := l.invoke(ctx, name, payload, "RequestResponse")
	if err != nil {
		return nil, err
	}
	if res.FunctionError != "" {
		return res.Payload, errors.New(res.FunctionError)
	}
	return res.Payload, nil
}

// ---- SNS --------------------------------------------------------------------------

// SNS publishes to topics on behalf of S3 notifications, CloudWatch alarms and
// EventBridge targets.
type SNS struct{ s *Set }

func (s *Set) SNS() *SNS { return &SNS{s} }

var _ notification.InternalPublisher = (*SNS)(nil)

// InternalPublish implements notification.InternalPublisher.
func (n *SNS) InternalPublish(ctx context.Context, topicARN, message string, attrs map[string]notification.MessageAttribute) error {
	params := map[string]any{"TopicArn": topicARN, "Message": message}
	if len(attrs) > 0 {
		wire := make(map[string]any, len(attrs))
		for k, v := range attrs {
			wire[k] = map[string]any{"DataType": v.DataType, "StringValue": v.StringValue}
		}
		params["MessageAttributes"] = wire
	}
	_, err := n.s.call(ctx, "sns", "Publish", topicARN, params, queue.SourceContext{})
	return err
}

// InternalPublishRaw publishes without attributes (S3 and CloudWatch use this).
func (n *SNS) InternalPublishRaw(ctx context.Context, topicARN, message string) error {
	return n.InternalPublish(ctx, topicARN, message, nil)
}

// ---- EventBridge --------------------------------------------------------------------

// EventBridge puts events on behalf of S3 notifications, Parameter Store,
// Step Functions and the executors.
type EventBridge struct{ s *Set }

func (s *Set) EventBridge() *EventBridge { return &EventBridge{s} }

// InternalPutEvents delivers events into the rule-matching pipeline.
func (e *EventBridge) InternalPutEvents(ctx context.Context, entries []map[string]any) error {
	list := make([]any, len(entries))
	for i, en := range entries {
		list[i] = en
	}
	_, err := e.s.call(ctx, "events", "PutEvents", "", map[string]any{"Entries": list}, queue.SourceContext{})
	return err
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
