// Package seams turns one AWS service's internal calls into another service's
// registry requests.
//
// Rule: a provider never calls another provider's methods. It calls a seam, and
// the seam calls Registry.Dispatch with Origin=internal. That is what makes a
// plugin that overrides a key the implementation for every caller — wire,
// internal and UI — exactly as if the built-in did not exist. A seam acts like
// a client of the key it calls, so one key means one thing whoever calls it. The
// few built-in handlers that still branch on Origin only skip client-side
// validation for the emulator's own producers (see queue.QueueProvider.SendMessage).
//
// Seams are thin on purpose: build the wire-shaped request, dispatch, translate
// the answer. Nothing here knows about plugins.
package seams

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"jaiscloud/internal/aws/arn"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	lambdaprovider "jaiscloud/internal/aws/provider/lambda"
	lambdaesm "jaiscloud/internal/aws/provider/lambda/esm"
	"jaiscloud/internal/aws/provider/notification"
	"jaiscloud/internal/aws/provider/queue"
	streamstore "jaiscloud/internal/aws/store/stream"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/logstream"
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
	return s.reg.Dispatch(ctx, s.key(service, action), s.request(service, action, account, region, params, src))
}

// request builds an internal request with the fields the gateway would inject.
func (s *Set) request(service, action, account, region string, params map[string]any, src queue.SourceContext) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service: service, Action: action, Cloud: model.CloudAWS, Origin: model.OriginInternal,
		AccountID: account, Region: region, Params: params,
		SourceArn: src.SourceArn, ServicePrincipal: src.ServicePrincipal,
		Clock: s.scope.Clock, Port: s.scope.Port, ResourceID: arn.ResourceID(region, account),
	}
}

// SQS returns the producer seam (SNS, S3, EventBridge, Lambda DLQs → SQS).
func (s *Set) SQS() *SQS { return &SQS{s} }

// SQS delivers messages to queues on behalf of SNS, S3 notifications, Lambda
// DLQs and other producers. It implements queue.InternalSendAPI.
type SQS struct{ s *Set }

var _ queue.InternalSendAPI = (*SQS)(nil)

// InternalSend sends body to the queue named by an ARN or URL.
func (q *SQS) InternalSend(ctx context.Context, target, body string, attrs map[string]queue.MessageAttribute, src queue.SourceContext) error {
	params := map[string]any{"QueueUrl": target, "MessageBody": body}
	if len(attrs) > 0 {
		wire := make(map[string]any, len(attrs))
		for k, v := range attrs {
			wire[k] = map[string]any{"DataType": v.DataType, "StringValue": v.StringValue}
		}
		params["MessageAttributes"] = wire
	}
	_, err := q.s.call(ctx, "sqs", "SendMessage", target, params, src)
	return err
}

// ---- SQS consumer: the Lambda event-source poller ------------------------------

// SQSConsumer lets the event-source poller receive from and delete from a queue
// through the registry. It implements lambdaesm.QueueInternalAPI.
type SQSConsumer struct{ s *Set }

var _ lambdaesm.QueueInternalAPI = (*SQSConsumer)(nil)

func (s *Set) SQSConsumer() *SQSConsumer { return &SQSConsumer{s} }

// queueOf resolves a queue ARN to the account, region and URL a client would use.
func (c *SQSConsumer) queueOf(queueArn string) (url string) {
	account, _ := scopeOf(queueArn, c.s.scope)
	name := queueArn
	if a, err := arn.Parse(queueArn); err == nil {
		name = a.Resource
	}
	return queue.QueueURL(c.s.scope.Port, account, name)
}

// InternalReceive is an ordinary ReceiveMessage by QueueUrl, asking for every
// attribute because the Lambda event carries them.
func (c *SQSConsumer) InternalReceive(ctx context.Context, queueArn string, maxMessages, waitTimeSec int) ([]lambdaesm.InternalMessage, error) {
	resp, err := c.s.call(ctx, "sqs", "ReceiveMessage", queueArn, map[string]any{
		"QueueUrl": c.queueOf(queueArn), "MaxNumberOfMessages": maxMessages, "WaitTimeSeconds": waitTimeSec,
		"AttributeNames": []any{"All"}, "MessageAttributeNames": []any{"All"},
	}, queue.SourceContext{})
	if err != nil {
		return nil, err
	}
	// A built-in answers with typed values, a plugin with JSON; read both the same way.
	list := asList(plain(resp.Data["Messages"]))
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

// InternalDeleteBatch is an ordinary DeleteMessageBatch by QueueUrl.
func (c *SQSConsumer) InternalDeleteBatch(ctx context.Context, queueArn string, receiptHandles []string) error {
	url := c.queueOf(queueArn)
	for start := 0; start < len(receiptHandles); start += maxDeleteBatch {
		end := min(start+maxDeleteBatch, len(receiptHandles))
		entries := make([]any, 0, end-start)
		for i, rh := range receiptHandles[start:end] {
			entries = append(entries, map[string]any{"Id": fmt.Sprint(i), "ReceiptHandle": rh})
		}
		if _, err := c.s.call(ctx, "sqs", "DeleteMessageBatch", queueArn,
			map[string]any{"QueueUrl": url, "Entries": entries}, queue.SourceContext{}); err != nil {
			return err
		}
	}
	return nil
}

// maxDeleteBatch is the most entries one DeleteMessageBatch takes.
const maxDeleteBatch = 10

// plain turns typed values (map[string]string, structs) into the generic JSON
// shape a plugin's answer already has. A value that cannot be encoded is returned as is.
func plain(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if json.Unmarshal(b, &out) != nil {
		return v
	}
	return out
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

// ---- CloudWatch Logs ---------------------------------------------------------------

// Logs lets workloads (Lambda, ECS, EMR, Step Functions) and EventBridge log
// targets write to CloudWatch Logs through the registry. It implements
// logstream.Ingestor.
type Logs struct{ s *Set }

var _ logstream.Ingestor = (*Logs)(nil)

func (s *Set) Logs() *Logs { return &Logs{s} }

// InternalCreateLogGroup creates the group if it is not there yet: CreateLogGroup,
// treating "already exists" as success (the log streamers call it for every batch).
func (l *Logs) InternalCreateLogGroup(ctx context.Context, group string) error {
	_, err := l.s.call(ctx, "logs", "CreateLogGroup", group, map[string]any{"logGroupName": group}, queue.SourceContext{})
	if errorCode(err) == "ResourceAlreadyExistsException" {
		return nil
	}
	return err
}

// Limits of one PutLogEvents call.
const (
	maxLogEventsPerCall = 10000
	maxLogMessageBytes  = 262144
)

// InternalPutEvents writes a batch the way an agent would: PutLogEvents, and if
// the stream is missing, CreateLogStream and PutLogEvents again. A batch for a
// group that does not exist is dropped (a workload can outlive its group), and
// batches are split and long lines cut to the limits a real call has.
func (l *Logs) InternalPutEvents(ctx context.Context, group, stream string, events []logstream.Event) error {
	for len(events) > 0 {
		n := min(len(events), maxLogEventsPerCall)
		if err := l.putBatch(ctx, group, stream, events[:n]); err != nil {
			return err
		}
		events = events[n:]
	}
	return nil
}

func (l *Logs) putBatch(ctx context.Context, group, stream string, events []logstream.Event) error {
	list := make([]any, len(events))
	for i, e := range events {
		msg := e.Message
		if len(msg) > maxLogMessageBytes {
			msg = cutUTF8(msg, maxLogMessageBytes)
		}
		list[i] = map[string]any{"timestamp": float64(e.Timestamp), "message": msg}
	}
	put := func() error {
		_, err := l.s.call(ctx, "logs", "PutLogEvents", group, map[string]any{
			"logGroupName": group, "logStreamName": stream, "logEvents": list,
		}, queue.SourceContext{})
		return err
	}
	err := put()
	if errorCode(err) != "ResourceNotFoundException" {
		return err
	}
	if notFoundResource(err) == cwlogs.ResourceLogGroup {
		return nil // dropped: the group is gone
	}
	if _, cerr := l.s.call(ctx, "logs", "CreateLogStream", group, map[string]any{
		"logGroupName": group, "logStreamName": stream,
	}, queue.SourceContext{}); cerr != nil && errorCode(cerr) != "ResourceAlreadyExistsException" {
		return cerr
	}
	return put()
}

// cutUTF8 cuts s to at most n bytes without splitting a multi-byte character.
func cutUTF8(s string, n int) string {
	for n > 0 && n < len(s) && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// notFoundResource returns what a ResourceNotFoundException says is missing
// (Data["resource"]: "log-group" or "log-stream"). An answer without it, as a
// plugin may give, is read as a missing stream, which creating the stream fixes.
func notFoundResource(err error) string {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		r, _ := pe.Data["resource"].(string)
		return r
	}
	return ""
}

// errorCode returns the code of a registry error, or "" for nil or any other error.
func errorCode(err error) string {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// ---- Service-to-service calls with wire-shaped params -------------------------------

// Invoke calls registry key (e.g. "Queue.CreateQueue") on behalf of an emulator
// service that behaves like a client of another one: CloudFormation resource
// handlers. Params are wire-shaped, so the origin is OriginService and built-in
// handlers serve it like a wire request.
func (s *Set) Invoke(ctx context.Context, key string, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	nr.Origin = model.OriginService
	if nr.Cloud == "" {
		nr.Cloud = model.CloudAWS
	}
	// CloudFormation Delete handlers have no request to inherit a scope from.
	if nr.AccountID == "" {
		nr.AccountID = s.scope.Account
	}
	if nr.Region == "" {
		nr.Region = s.scope.Region
	}
	if nr.Clock == nil {
		nr.Clock = s.scope.Clock
	}
	if nr.Port == 0 {
		nr.Port = s.scope.Port
	}
	if nr.ResourceID == nil {
		nr.ResourceID = arn.ResourceID(nr.Region, nr.AccountID)
	}
	if i := strings.IndexByte(key, '.'); i >= 0 {
		nr.Action = key[i+1:]
	}
	return s.reg.Dispatch(ctx, key, nr)
}

// ---- S3 -----------------------------------------------------------------------------

// S3 lets EMR and EMR on EKS (log upload), Firehose (delivery) and Glue crawlers
// (schema inference) use buckets through the registry. Bodies cross whole as
// "_body" ([]byte); an out-of-process plugin sees them base64-encoded.
type S3 struct{ s *Set }

func (s *Set) S3() *S3 { return &S3{s} }

// InternalPutObject writes the object the way a client would: PutObject, and if
// the bucket does not exist yet, CreateBucket and PutObject again. Callers such
// as the Firehose delivery and the EMR log upload rely on the bucket appearing.
func (o *S3) InternalPutObject(ctx context.Context, bucket, key, contentType string, body []byte) error {
	put := func() error {
		_, err := o.s.call(ctx, "s3", "PutObject", "", map[string]any{
			"_bucket": bucket, "_key": key, "_content_type": contentType, "_body": body,
		}, queue.SourceContext{})
		return err
	}
	err := put()
	if errorCode(err) != "NoSuchBucket" {
		return err
	}
	if _, cerr := o.s.call(ctx, "s3", "CreateBucket", "", map[string]any{"_bucket": bucket}, queue.SourceContext{}); cerr != nil &&
		errorCode(cerr) != "BucketAlreadyOwnedByYou" {
		return cerr
	}
	return put()
}

// InternalGetObject returns the whole object body. The built-in answers with a
// stream ("_stream"); a plugin answers with "_body" bytes ({"$base64": "..."}
// over JSON). Anything else is an error rather than an empty file.
func (o *S3) InternalGetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	resp, err := o.s.call(ctx, "s3", "GetObject", "", map[string]any{"_bucket": bucket, "_key": key}, queue.SourceContext{})
	if err != nil {
		return nil, err
	}
	if rc, ok := resp.Data["_stream"].(io.ReadCloser); ok {
		defer rc.Close()
		return io.ReadAll(rc)
	}
	body, ok := resp.Data["_body"].([]byte)
	if !ok {
		return nil, fmt.Errorf("s3 GetObject %s/%s: response has neither _stream nor _body bytes (got %T)", bucket, key, resp.Data["_body"])
	}
	return body, nil
}

// InternalListObjects returns the keys under prefix (one page of up to 10000).
func (o *S3) InternalListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	resp, err := o.s.call(ctx, "s3", "ListObjectsV2", "", map[string]any{
		"_bucket": bucket, "prefix": prefix, "max-keys": 10000,
	}, queue.SourceContext{})
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, c := range asList(resp.Data["Contents"]) {
		if m, ok := c.(map[string]any); ok {
			keys = append(keys, str(m["Key"]))
		}
	}
	return keys, nil
}

// BucketCORSRules returns a bucket's CORS rules for the gateway's preflight
// handling. A bucket with no rules (or no bucket) yields nil, which the gateway
// treats as "CORS not configured". If the registry cannot answer (for example a
// plugin that owns the key is down) the rules are also nil, so the browser sees
// no CORS headers rather than a 5xx; the failure is logged.
func (o *S3) BucketCORSRules(ctx context.Context, bucket string) []map[string]any {
	if bucket == "" {
		return nil
	}
	resp, err := o.s.call(ctx, "s3", "GetBucketCors", "", map[string]any{"_bucket": bucket}, queue.SourceContext{})
	if err != nil {
		// No rules for the bucket is the normal case; a client that went away is not news.
		if code := errorCode(err); code != "NoSuchCORSConfiguration" && code != "NoSuchBucket" && ctx.Err() == nil {
			slog.Warn("cors: could not read bucket CORS rules", "bucket", bucket, "err", err)
		}
		return nil
	}
	var rules []map[string]any
	for _, r := range asList(resp.Data["CORSRules"]) {
		if m := asMap(r); m != nil {
			rules = append(rules, m)
		}
	}
	return rules
}

// ---- Lambda code ----------------------------------------------------------------------

// LambdaCode fetches function and layer packages through the registry for the
// container executors (container.CodeLoader) and the admin code endpoint
// (admin.LambdaCodeFetcher). Function.GetFunctionCodeZip and
// Function.GetLayerCodeZip are internal-only actions: a plugin that owns the
// function store answers them with "_body" bytes ({"$base64": "..."} over JSON).
type LambdaCode struct{ s *Set }

func (s *Set) LambdaCode() *LambdaCode { return &LambdaCode{s} }

func (c *LambdaCode) fetch(ctx context.Context, account, action string, params map[string]any) ([]byte, error) {
	resp, err := c.s.reg.Dispatch(ctx, c.s.key("lambda", action),
		c.s.request("lambda", action, account, c.s.scope.Region, params, queue.SourceContext{}))
	if err != nil {
		return nil, err
	}
	body, ok := resp.Data["_body"].([]byte)
	if !ok {
		return nil, fmt.Errorf("lambda %s: response has no _body bytes (got %T)", action, resp.Data["_body"])
	}
	return body, nil
}

func (c *LambdaCode) GetFunctionCodeZip(ctx context.Context, account, functionName, qualifier string) ([]byte, error) {
	return c.fetch(ctx, account, "GetFunctionCodeZip", map[string]any{"functionName": functionName, "qualifier": qualifier})
}

func (c *LambdaCode) GetLayerCodeZip(ctx context.Context, account, layerName string, version int64) ([]byte, error) {
	return c.fetch(ctx, account, "GetLayerCodeZip", map[string]any{"layerName": layerName, "version": version})
}

// LoadCode implements container.CodeLoader.
func (c *LambdaCode) LoadCode(ctx context.Context, account, funcName, version string) ([]byte, error) {
	return c.GetFunctionCodeZip(ctx, account, funcName, version)
}

// ---- Secrets Manager ----------------------------------------------------------------

// Secrets resolves /aws/reference/secretsmanager/ references for SSM. It
// implements parameter's secret getter.
type Secrets struct{ s *Set }

func (s *Set) Secrets() *Secrets { return &Secrets{s} }

func (g *Secrets) InternalGetSecretValue(ctx context.Context, accountID, secretID string) (string, error) {
	nr := g.s.request("secretsmanager", "GetSecretValue", accountID, g.s.scope.Region,
		map[string]any{"SecretId": secretID}, queue.SourceContext{})
	resp, err := g.s.reg.Dispatch(ctx, g.s.key("secretsmanager", "GetSecretValue"), nr)
	if err != nil {
		return "", err
	}
	if v, ok := resp.Data["SecretString"].(string); ok {
		return v, nil
	}
	switch b := resp.Data["SecretBinary"].(type) {
	case []byte:
		return string(b), nil
	case string: // a plugin that did not use the {"$base64": ...} marker
		return b, nil
	}
	return "", fmt.Errorf("secret %s has no value", secretID)
}

// ---- CloudWatch metrics -------------------------------------------------------------

// Metrics lets CloudWatch Logs metric filters publish to CloudWatch. It
// implements the logs provider's MetricDataPutter. The wire action carries the
// Query protocol's flattened "MetricData.member.N.*" keys.
type Metrics struct{ s *Set }

func (s *Set) Metrics() *Metrics { return &Metrics{s} }

func (m *Metrics) InternalPutMetricData(ctx context.Context, namespace string, data []cwlogs.MetricDatum) error {
	params := map[string]any{"Namespace": namespace}
	for i, d := range data {
		p := fmt.Sprintf("MetricData.member.%d.", i+1)
		params[p+"MetricName"] = d.Name
		params[p+"Value"] = d.Value
		if d.Unit != "" {
			params[p+"Unit"] = d.Unit
		}
	}
	_, err := m.s.call(ctx, "monitoring", "PutMetricData", "", params, queue.SourceContext{})
	return err
}

// ---- DynamoDB Streams → Lambda event source mapping ------------------------------------

// Streams lets the event-source poller read a table's stream through the
// registry (Streams.ListStreams / DescribeStream / GetShardIterator /
// GetRecords). It implements lambdaesm.StreamStoreAPI.
type Streams struct {
	s    *Set
	mu   sync.Mutex
	arns map[string]string // table -> stream ARN
}

var _ lambdaesm.StreamStoreAPI = (*Streams)(nil)

func (s *Set) Streams() *Streams { return &Streams{s: s, arns: map[string]string{}} }

func (st *Streams) call(ctx context.Context, action string, params map[string]any) (*model.ProviderResponse, error) {
	return st.s.call(ctx, "dynamodbstreams", action, "", params, queue.SourceContext{})
}

func (st *Streams) streamArn(ctx context.Context, table string) (string, bool, error) {
	st.mu.Lock()
	arn := st.arns[table]
	st.mu.Unlock()
	if arn != "" {
		return arn, true, nil
	}
	resp, err := st.call(ctx, "ListStreams", map[string]any{"TableName": table})
	if err != nil {
		return "", false, err
	}
	for _, e := range asList(resp.Data["Streams"]) {
		if m, ok := e.(map[string]any); ok && str(m["TableName"]) == table {
			arn = str(m["StreamArn"])
			st.mu.Lock()
			st.arns[table] = arn
			st.mu.Unlock()
			return arn, arn != "", nil
		}
	}
	return "", false, nil
}

// GetStreamInfo reports whether table has a stream. The error is non-nil when the
// answer could not be had (for example the service is unreachable).
func (st *Streams) GetStreamInfo(ctx context.Context, table string) (streamstore.StreamInfo, bool, error) {
	arn, ok, err := st.streamArn(ctx, table)
	if err != nil || !ok {
		return streamstore.StreamInfo{}, false, err
	}
	resp, err := st.call(ctx, "DescribeStream", map[string]any{"StreamArn": arn})
	if err != nil {
		st.mu.Lock()
		delete(st.arns, table) // the stream may have been re-created
		st.mu.Unlock()
		return streamstore.StreamInfo{}, false, err
	}
	d, _ := resp.Data["StreamDescription"].(map[string]any)
	return streamstore.StreamInfo{
		StreamArn: arn, TableName: table, StreamLabel: str(d["StreamLabel"]),
		Enabled: str(d["StreamStatus"]) == "ENABLED",
	}, true, nil
}

// GetRecords returns the records after sequence number afterSeq (-1: from the start).
func (st *Streams) GetRecords(ctx context.Context, table string, afterSeq int) ([]streamstore.Record, int, error) {
	arn, ok, err := st.streamArn(ctx, table)
	if err != nil || !ok {
		return nil, afterSeq, err
	}
	it := map[string]any{"StreamArn": arn, "ShardIteratorType": "TRIM_HORIZON"}
	if afterSeq >= 0 {
		it["ShardIteratorType"], it["SequenceNumber"] = "AFTER_SEQUENCE_NUMBER", strconv.Itoa(afterSeq)
	}
	resp, err := st.call(ctx, "GetShardIterator", it)
	if err != nil {
		return nil, afterSeq, err
	}
	resp, err = st.call(ctx, "GetRecords", map[string]any{"ShardIterator": str(resp.Data["ShardIterator"])})
	if err != nil {
		return nil, afterSeq, err
	}
	var out []streamstore.Record
	next := afterSeq + 1
	for _, raw := range asList(resp.Data["Records"]) {
		m, _ := raw.(map[string]any)
		d, _ := m["dynamodb"].(map[string]any)
		seq, _ := strconv.Atoi(str(d["SequenceNumber"]))
		r := streamstore.Record{
			SequenceNumber: seq, EventID: str(m["eventID"]), EventName: str(m["eventName"]),
			ApproximateCreationDateTime: time.Unix(toInt64(d["ApproximateCreationDateTime"]), 0).UTC(),
			Keys:                        asMap(d["Keys"]), NewImage: asMap(d["NewImage"]), OldImage: asMap(d["OldImage"]),
		}
		out = append(out, r)
		next = seq + 1
	}
	return out, next, nil
}

// asList returns v as a []any, accepting the typed slices built-in handlers
// return as well as the []any a plugin's JSON decodes to.
func asList(v any) []any {
	switch l := v.(type) {
	case []any:
		return l
	case []map[string]any:
		out := make([]any, len(l))
		for i := range l {
			out[i] = l[i]
		}
		return out
	}
	return nil
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

// ---- KMS --------------------------------------------------------------------------------

// KMS lets Secrets Manager and SSM encrypt what they store through the registry.
// It implements model.KeyEncryptor. Plaintext and ciphertext cross as base64
// strings, like the wire actions.
type KMS struct{ s *Set }

var _ model.KeyEncryptor = (*KMS)(nil)

func (s *Set) KMS() *KMS { return &KMS{s} }

func (k *KMS) call(ctx context.Context, action, keyID string, params map[string]any) (*model.ProviderResponse, error) {
	return k.s.call(ctx, "kms", action, keyID, params, queue.SourceContext{})
}

func encCtxParam(c map[string]string) map[string]any {
	if len(c) == 0 {
		return nil
	}
	out := make(map[string]any, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}

func (k *KMS) Encrypt(ctx context.Context, keyID string, pt []byte, encCtx map[string]string) ([]byte, error) {
	p := map[string]any{"KeyId": keyID, "Plaintext": base64.StdEncoding.EncodeToString(pt)}
	if c := encCtxParam(encCtx); c != nil {
		p["EncryptionContext"] = c
	}
	resp, err := k.call(ctx, "Encrypt", keyID, p)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(str(resp.Data["CiphertextBlob"]))
}

func (k *KMS) Decrypt(ctx context.Context, keyID string, ct []byte, encCtx map[string]string) ([]byte, error) {
	p := map[string]any{"CiphertextBlob": base64.StdEncoding.EncodeToString(ct)}
	if c := encCtxParam(encCtx); c != nil {
		p["EncryptionContext"] = c
	}
	resp, err := k.call(ctx, "Decrypt", keyID, p)
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(str(resp.Data["Plaintext"]))
}

func (k *KMS) GenerateDataKey(ctx context.Context, keyID string, bits int) (ptDEK, ctDEK []byte, err error) {
	spec := "AES_256"
	if bits == 128 {
		spec = "AES_128"
	}
	resp, err := k.call(ctx, "GenerateDataKey", keyID, map[string]any{"KeyId": keyID, "KeySpec": spec})
	if err != nil {
		return nil, nil, err
	}
	if ptDEK, err = base64.StdEncoding.DecodeString(str(resp.Data["Plaintext"])); err != nil {
		return nil, nil, err
	}
	ctDEK, err = base64.StdEncoding.DecodeString(str(resp.Data["CiphertextBlob"]))
	return ptDEK, ctDEK, err
}

// Fetch reads an s3:// or s3a:// object, for EMR bootstrap scripts. It implements
// blobfs.BlobFetcher.
func (o *S3) Fetch(ctx context.Context, uri string) ([]byte, error) {
	bucket, key, err := blobfs.ParseS3URI(uri)
	if err != nil {
		return nil, err
	}
	body, err := o.InternalGetObject(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("blobfetch %s: %w", uri, err)
	}
	return body, nil
}

// GetBucket reports whether a bucket exists (HeadBucket), for Firehose's
// destination check. It implements firehose.BucketChecker.
func (o *S3) GetBucket(ctx context.Context, bucket string) (map[string]any, error) {
	resp, err := o.s.call(ctx, "s3", "HeadBucket", "", map[string]any{"_bucket": bucket}, queue.SourceContext{})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// scopeOf derives account and region from an ARN. Anything else (a queue URL, a
// bucket or log-group name, a function name) falls back to the default scope.
func scopeOf(target string, def Scope) (account, region string) {
	account, region = def.Account, def.Region
	if strings.HasPrefix(target, "arn:") {
		if a, err := arn.Parse(target); err == nil {
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
