package seams

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/aws/arn"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	lambdaprovider "jaiscloud/internal/aws/provider/lambda"
	lambdaesm "jaiscloud/internal/aws/provider/lambda/esm"
	"jaiscloud/internal/aws/provider/notification"
	"jaiscloud/internal/aws/provider/queue"
	streamstore "jaiscloud/internal/aws/store/stream"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/logstream"
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

// ---- CloudWatch Logs ---------------------------------------------------------------

// Logs lets workloads (Lambda, ECS, EMR, Step Functions) and EventBridge log
// targets write to CloudWatch Logs through the registry. It implements
// logstream.Ingestor.
type Logs struct{ s *Set }

var _ logstream.Ingestor = (*Logs)(nil)

func (s *Set) Logs() *Logs { return &Logs{s} }

func (l *Logs) InternalCreateLogGroup(ctx context.Context, group string) error {
	_, err := l.s.call(ctx, "logs", "CreateLogGroup", group, map[string]any{"logGroupName": group}, queue.SourceContext{})
	return err
}

func (l *Logs) InternalPutEvents(ctx context.Context, group, stream string, events []logstream.Event) error {
	list := make([]any, len(events))
	for i, e := range events {
		list[i] = map[string]any{"timestamp": float64(e.Timestamp), "message": e.Message}
	}
	_, err := l.s.call(ctx, "logs", "PutLogEvents", group, map[string]any{
		"logGroupName": group, "logStreamName": stream, "logEvents": list,
	}, queue.SourceContext{})
	return err
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

// InternalPutObject creates the bucket if needed and writes the object.
func (o *S3) InternalPutObject(ctx context.Context, bucket, key, contentType string, body []byte) error {
	_, err := o.s.call(ctx, "s3", "PutObject", "", map[string]any{
		"_bucket": bucket, "_key": key, "_content_type": contentType, "_body": body,
	}, queue.SourceContext{})
	return err
}

// InternalGetObject returns the whole object body.
func (o *S3) InternalGetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	resp, err := o.s.call(ctx, "s3", "GetObject", "", map[string]any{"_bucket": bucket, "_key": key}, queue.SourceContext{})
	if err != nil {
		return nil, err
	}
	body, ok := resp.Data["_body"].([]byte)
	if !ok {
		// A plugin that replaces GetObject must answer internal calls with "_body"
		// bytes ({"$base64": "..."} over JSON); an empty file would hide the mistake.
		return nil, fmt.Errorf("s3 GetObject %s/%s: response has no _body bytes (got %T)", bucket, key, resp.Data["_body"])
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

func (st *Streams) streamArn(ctx context.Context, table string) (string, bool) {
	st.mu.Lock()
	arn := st.arns[table]
	st.mu.Unlock()
	if arn != "" {
		return arn, true
	}
	resp, err := st.call(ctx, "ListStreams", map[string]any{"TableName": table})
	if err != nil {
		return "", false
	}
	for _, e := range asList(resp.Data["Streams"]) {
		if m, ok := e.(map[string]any); ok && str(m["TableName"]) == table {
			arn = str(m["StreamArn"])
			st.mu.Lock()
			st.arns[table] = arn
			st.mu.Unlock()
			return arn, arn != ""
		}
	}
	return "", false
}

func (st *Streams) GetStreamInfo(table string) (streamstore.StreamInfo, bool) {
	ctx := context.Background()
	arn, ok := st.streamArn(ctx, table)
	if !ok {
		return streamstore.StreamInfo{}, false
	}
	resp, err := st.call(ctx, "DescribeStream", map[string]any{"StreamArn": arn})
	if err != nil {
		st.mu.Lock()
		delete(st.arns, table) // the stream may have been re-created
		st.mu.Unlock()
		return streamstore.StreamInfo{}, false
	}
	d, _ := resp.Data["StreamDescription"].(map[string]any)
	return streamstore.StreamInfo{
		StreamArn: arn, TableName: table, StreamLabel: str(d["StreamLabel"]),
		Enabled: str(d["StreamStatus"]) == "ENABLED",
	}, true
}

// GetRecords returns the records after sequence number afterSeq (-1: from the start).
func (st *Streams) GetRecords(table string, afterSeq int) ([]streamstore.Record, int) {
	ctx := context.Background()
	arn, ok := st.streamArn(ctx, table)
	if !ok {
		return nil, afterSeq
	}
	it := map[string]any{"StreamArn": arn, "ShardIteratorType": "TRIM_HORIZON"}
	if afterSeq >= 0 {
		it["ShardIteratorType"], it["SequenceNumber"] = "AFTER_SEQUENCE_NUMBER", strconv.Itoa(afterSeq)
	}
	resp, err := st.call(ctx, "GetShardIterator", it)
	if err != nil {
		return nil, afterSeq
	}
	resp, err = st.call(ctx, "GetRecords", map[string]any{"ShardIterator": str(resp.Data["ShardIterator"])})
	if err != nil {
		return nil, afterSeq
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
	return out, next
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
