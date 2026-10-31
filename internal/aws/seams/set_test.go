package seams_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	keyprovider "jaiscloud/internal/aws/key"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	objectprovider "jaiscloud/internal/aws/provider/object"
	"jaiscloud/internal/aws/seams"
	s3impl "jaiscloud/internal/aws/store/s3"
	streamstore "jaiscloud/internal/aws/store/stream"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/logstream"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

var wireToPrefix = map[string]string{
	"s3": "Object", "monitoring": "CloudWatch", "dynamodbstreams": "Streams",
	"kms": "Key", "secretsmanager": "Secret", "logs": "CloudWatchLogs", "lambda": "Function", "sqs": "Queue",
}

func newSet(reg *provider.Registry) *seams.Set {
	return seams.NewSet(reg, func(s string) string { return wireToPrefix[s] },
		seams.Scope{Account: "111111111111", Region: "eu-west-1", Clock: clock.RealClock{}, Port: 4566})
}

// recorder registers a handler that records the request and answers with resp.
func recorder(reg *provider.Registry, key string, resp map[string]any, into *[]*model.NormalizedRequest) {
	reg.RegisterAll(map[string]provider.HandlerFunc{key: func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		*into = append(*into, nr)
		return provider.OK(resp), nil
	}})
}

func TestInternalRequestsCarryWhatTheGatewayWouldInject(t *testing.T) {
	reg := provider.NewRegistry()
	var seen []*model.NormalizedRequest
	recorder(reg, "Object.PutObject", map[string]any{}, &seen)
	if err := newSet(reg).S3().InternalPutObject(context.Background(), "b", "k", "text/plain", []byte("x")); err != nil {
		t.Fatal(err)
	}
	nr := seen[0]
	if nr.Origin != model.OriginInternal || nr.AccountID != "111111111111" || nr.Region != "eu-west-1" ||
		nr.Clock == nil || nr.Port != 4566 || nr.ResourceID == nil {
		t.Fatalf("request = %+v; an in-process handler or plugin would see a half-built request", nr)
	}
	if nr.Params["_bucket"] != "b" || nr.Params["_key"] != "k" || nr.Params["_content_type"] != "text/plain" ||
		!bytes.Equal(nr.Params["_body"].([]byte), []byte("x")) {
		t.Fatalf("params = %v", nr.Params)
	}
}

func TestInvokeFillsScopeForHandlersWithoutARequest(t *testing.T) {
	reg := provider.NewRegistry()
	var seen []*model.NormalizedRequest
	recorder(reg, "Object.DeleteBucket", map[string]any{}, &seen)
	// CloudFormation Delete handlers have no parent request to copy a scope from.
	if _, err := newSet(reg).Invoke(context.Background(), "Object.DeleteBucket", &model.NormalizedRequest{Params: map[string]any{"_bucket": "b"}}); err != nil {
		t.Fatal(err)
	}
	nr := seen[0]
	if nr.Origin != model.OriginService || nr.Action != "DeleteBucket" || nr.AccountID != "111111111111" ||
		nr.Region != "eu-west-1" || nr.Clock == nil || nr.ResourceID == nil {
		t.Fatalf("request = %+v", nr)
	}
	if got := nr.ResourceID("s3-bucket", "b"); got == "" {
		t.Fatal("ResourceID must produce an ARN with the filled scope")
	}
}

func TestS3GetObjectNeedsBodyBytesAndListParsesKeys(t *testing.T) {
	reg := provider.NewRegistry()
	var seen []*model.NormalizedRequest
	recorder(reg, "Object.ListObjectsV2", map[string]any{"Contents": []map[string]any{{"Key": "a"}, {"Key": "b"}}}, &seen)
	reg.RegisterAll(map[string]provider.HandlerFunc{"Object.GetObject": func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		if nr.Params["_key"] == "good" {
			return provider.OK(map[string]any{"_body": []byte("data")}), nil
		}
		return provider.OK(map[string]any{"_body": "not-bytes"}), nil // a plugin that skipped the $base64 marker
	}})
	s3 := newSet(reg).S3()
	keys, err := s3.InternalListObjects(context.Background(), "b", "p/")
	if err != nil || len(keys) != 2 || keys[1] != "b" {
		t.Fatalf("keys = %v, %v", keys, err)
	}
	if seen[0].Params["prefix"] != "p/" || seen[0].Params["max-keys"] != 10000 {
		t.Fatalf("list params = %v", seen[0].Params)
	}
	if body, err := s3.InternalGetObject(context.Background(), "b", "good"); err != nil || string(body) != "data" {
		t.Fatalf("good = %q, %v", body, err)
	}
	if body, err := s3.InternalGetObject(context.Background(), "b", "bad"); err == nil {
		t.Fatalf("a response without _body bytes must be an error, not an empty file (got %q)", body)
	}
}

func TestMetricsFlattenToTheQueryProtocolKeys(t *testing.T) {
	reg := provider.NewRegistry()
	var seen []*model.NormalizedRequest
	recorder(reg, "CloudWatch.PutMetricData", map[string]any{}, &seen)
	err := newSet(reg).Metrics().InternalPutMetricData(context.Background(), "Ns", []cwlogs.MetricDatum{
		{Name: "A", Value: 2, Unit: "Count"}, {Name: "B", Value: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := seen[0].Params
	if p["Namespace"] != "Ns" || p["MetricData.member.1.MetricName"] != "A" || p["MetricData.member.1.Value"] != 2.0 ||
		p["MetricData.member.1.Unit"] != "Count" || p["MetricData.member.2.MetricName"] != "B" {
		t.Fatalf("params = %v", p)
	}
	if _, has := p["MetricData.member.2.Unit"]; has {
		t.Fatal("an empty unit must be omitted")
	}
}

// The Streams seam reads a table's stream through the wire actions and converts
// the records back, preserving the poller's cursor semantics.
func TestStreamsSeamReadsThroughTheWireActions(t *testing.T) {
	reg := provider.NewRegistry()
	var iterators []map[string]any
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"Streams.ListStreams": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return provider.OK(map[string]any{"Streams": []map[string]any{{"StreamArn": "arn:s", "TableName": "t", "StreamLabel": "l"}}}), nil
		},
		"Streams.DescribeStream": func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return provider.OK(map[string]any{"StreamDescription": map[string]any{"StreamStatus": "ENABLED", "StreamLabel": "l"}}), nil
		},
		"Streams.GetShardIterator": func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			iterators = append(iterators, nr.Params)
			return provider.OK(map[string]any{"ShardIterator": "it"}), nil
		},
		"Streams.GetRecords": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return provider.OK(map[string]any{"Records": []any{ // []any: what a plugin's JSON decodes to
				map[string]any{"eventID": "e1", "eventName": "INSERT", "dynamodb": map[string]any{
					"SequenceNumber": "000000000000000000007", "ApproximateCreationDateTime": float64(1700000000),
					"Keys": map[string]any{"id": map[string]any{"S": "1"}}, "NewImage": map[string]any{"id": map[string]any{"S": "1"}}}},
			}}), nil
		},
	})
	st := newSet(reg).Streams()

	info, ok, err := st.GetStreamInfo(context.Background(), "t")
	if err != nil || !ok || !info.Enabled || info.StreamArn != "arn:s" {
		t.Fatalf("info = %+v, %v", info, ok)
	}
	recs, next, err := st.GetRecords(context.Background(), "t", -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].SequenceNumber != 7 || recs[0].EventName != "INSERT" || recs[0].EventID != "e1" ||
		recs[0].ApproximateCreationDateTime.Unix() != 1700000000 || recs[0].Keys["id"] == nil || next != 8 {
		t.Fatalf("records = %+v next=%d", recs, next)
	}
	_, _, _ = st.GetRecords(context.Background(), "t", 7)
	if iterators[0]["ShardIteratorType"] != "TRIM_HORIZON" {
		t.Fatalf("cursor -1 must start at the beginning, got %v", iterators[0])
	}
	if iterators[1]["ShardIteratorType"] != "AFTER_SEQUENCE_NUMBER" || iterators[1]["SequenceNumber"] != strconv.Itoa(7) {
		t.Fatalf("cursor 7 must resume after 7, got %v", iterators[1])
	}
	_ = streamstore.Record{} // the seam returns the poller's own record type
}

// Secrets and SSM keep working on ciphertexts written before they went through
// the registry: the seam and the direct KMS entry points are interchangeable.
func TestKMSSeamIsInterchangeableWithTheDirectPath(t *testing.T) {
	ctx := context.Background()
	kp := keyprovider.New(keyprovider.NewMemoryKeyStore(), nil, bytes.Repeat([]byte{7}, 32))
	reg := provider.NewRegistry().Register(kp)
	resp, err := reg.Dispatch(ctx, "Key.CreateKey", &model.NormalizedRequest{Cloud: model.CloudAWS, AccountID: "111111111111",
		Region: "eu-west-1", Clock: clock.RealClock{}, Params: map[string]any{},
		ResourceID: func(_, name string) string { return name }})
	if err != nil {
		t.Fatal(err)
	}
	md, _ := resp.Data["KeyMetadata"].(map[string]any)
	keyID, _ := md["KeyId"].(string)
	if keyID == "" {
		t.Fatalf("CreateKey = %v", resp.Data)
	}

	kms := newSet(reg).KMS()
	encCtx := map[string]string{"purpose": "test"}

	viaSeam, err := kms.Encrypt(ctx, keyID, []byte("secret"), encCtx)
	if err != nil {
		t.Fatalf("seam Encrypt: %v", err)
	}
	if pt, err := kp.Decrypt2(ctx, keyID, viaSeam, encCtx); err != nil || string(pt) != "secret" {
		t.Fatalf("direct Decrypt2 of a seam ciphertext = %q, %v", pt, err)
	}
	direct, err := kp.Encrypt2(ctx, keyID, []byte("older"), encCtx)
	if err != nil {
		t.Fatal(err)
	}
	if pt, err := kms.Decrypt(ctx, keyID, direct, encCtx); err != nil || string(pt) != "older" {
		t.Fatalf("seam Decrypt of an older ciphertext = %q, %v", pt, err)
	}
	pt, ct, err := kms.GenerateDataKey(ctx, keyID, 256)
	if err != nil || len(pt) != 32 {
		t.Fatalf("GenerateDataKey = %d bytes, %v", len(pt), err)
	}
	if got, err := kp.Decrypt2(ctx, keyID, ct, nil); err != nil || !bytes.Equal(got, pt) {
		t.Fatalf("data key must decrypt to its plaintext: %v", err)
	}
}

func TestBucketCORSRulesReadThroughTheRegistry(t *testing.T) {
	reg := provider.NewRegistry()
	var seen []*model.NormalizedRequest
	// A plugin answers with JSON-shaped data: []any, not []map[string]any.
	recorder(reg, "Object.GetBucketCors", map[string]any{"CORSRules": []any{
		map[string]any{"AllowedOrigins": []any{"https://app"}, "AllowedMethods": []any{"GET"}, "MaxAgeSeconds": float64(60)},
	}}, &seen)
	rules := newSet(reg).S3().BucketCORSRules(context.Background(), "b")
	if len(rules) != 1 || len(seen) != 1 || seen[0].Params["_bucket"] != "b" {
		t.Fatalf("rules=%v seen=%v", rules, seen)
	}
	if got := rules[0]["AllowedOrigins"].([]any); got[0] != "https://app" {
		t.Fatalf("rule = %v", rules[0])
	}
}

func TestBucketCORSRulesFailSoft(t *testing.T) {
	reg := provider.NewRegistry()
	reg.RegisterAll(map[string]provider.HandlerFunc{"Object.GetBucketCors": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return nil, model.NewProviderError("PluginUnavailable", "down", 502)
	}})
	set := newSet(reg)
	if rules := set.S3().BucketCORSRules(context.Background(), "b"); rules != nil {
		t.Fatalf("an unreachable owner must yield no rules, got %v", rules)
	}
	// No bucket name means the request is not an S3 bucket request: do not dispatch at all.
	var seen []*model.NormalizedRequest
	reg2 := provider.NewRegistry()
	recorder(reg2, "Object.GetBucketCors", map[string]any{}, &seen)
	if rules := newSet(reg2).S3().BucketCORSRules(context.Background(), ""); rules != nil || len(seen) != 0 {
		t.Fatalf("empty bucket dispatched: rules=%v calls=%d", rules, len(seen))
	}
}

func TestLambdaCodeReadsPackagesThroughTheRegistry(t *testing.T) {
	reg := provider.NewRegistry()
	var fn, layer []*model.NormalizedRequest
	recorder(reg, "Function.GetFunctionCodeZip", map[string]any{"_body": []byte("fn-zip")}, &fn)
	recorder(reg, "Function.GetLayerCodeZip", map[string]any{"_body": []byte("layer-zip")}, &layer)
	code := newSet(reg).LambdaCode()

	got, err := code.LoadCode(context.Background(), "222222222222", "f", "$LATEST")
	if err != nil || string(got) != "fn-zip" {
		t.Fatalf("LoadCode = %q, %v", got, err)
	}
	if nr := fn[0]; nr.AccountID != "222222222222" || nr.Origin != model.OriginInternal ||
		nr.Params["functionName"] != "f" || nr.Params["qualifier"] != "$LATEST" {
		t.Fatalf("function request = %+v", nr)
	}
	got, err = code.GetLayerCodeZip(context.Background(), "222222222222", "l", 3)
	if err != nil || string(got) != "layer-zip" || layer[0].Params["layerName"] != "l" || layer[0].Params["version"] != int64(3) {
		t.Fatalf("GetLayerCodeZip = %q, %v, %v", got, err, layer)
	}
}

func TestLambdaCodeRejectsAnswerWithoutBytes(t *testing.T) {
	reg := provider.NewRegistry()
	var seen []*model.NormalizedRequest
	recorder(reg, "Function.GetFunctionCodeZip", map[string]any{"_body": "not-bytes"}, &seen)
	if _, err := newSet(reg).LambdaCode().GetFunctionCodeZip(context.Background(), "1", "f", "$LATEST"); err == nil {
		t.Fatal("a plugin answer without _body bytes must be an error, not an empty package")
	}
}

// ---- the seams against the real providers ----------------------------------------

func realObject(t *testing.T) (*seams.S3, *provider.Registry) {
	t.Helper()
	reg := provider.NewRegistry()
	reg.Register(objectprovider.New(s3impl.NewMemoryS3ObjectMetaStore(), blobfs.NewMemoryBlobStore()))
	return newSet(reg).S3(), reg
}

// An internal write behaves like a client: it creates the bucket when it is
// missing, then reads it back through the same key a client uses.
func TestS3SeamCreatesTheBucketOnDemandAndReadsBack(t *testing.T) {
	s3, _ := realObject(t)
	ctx := context.Background()
	if _, err := s3.InternalGetObject(ctx, "fresh", "k"); err == nil {
		t.Fatal("reading from a bucket that does not exist must fail")
	}
	if err := s3.InternalPutObject(ctx, "fresh", "dir/k", "text/plain", []byte("hello")); err != nil {
		t.Fatalf("write to a new bucket: %v", err)
	}
	if err := s3.InternalPutObject(ctx, "fresh", "dir/k2", "text/plain", []byte("again")); err != nil {
		t.Fatalf("write to an existing bucket: %v", err)
	}
	got, err := s3.InternalGetObject(ctx, "fresh", "dir/k")
	if err != nil || string(got) != "hello" {
		t.Fatalf("GetObject = %q, %v", got, err)
	}
	if _, err := s3.InternalGetObject(ctx, "fresh", "missing"); err == nil {
		t.Fatal("a missing key must be an error")
	}
	keys, err := s3.InternalListObjects(ctx, "fresh", "dir/")
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys = %v, %v", keys, err)
	}
}

func TestS3SeamCreatesBucketOnlyOnFirstWrite(t *testing.T) {
	reg := provider.NewRegistry()
	calls := map[string]int{}
	exists := false
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"Object.PutObject": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			calls["put"]++
			if !exists {
				return nil, model.NewProviderError("NoSuchBucket", "no", 404)
			}
			return provider.OK(map[string]any{}), nil
		},
		"Object.CreateBucket": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			calls["create"]++
			exists = true
			return provider.OK(map[string]any{}), nil
		},
	})
	s3 := newSet(reg).S3()
	for i := 0; i < 3; i++ {
		if err := s3.InternalPutObject(context.Background(), "b", "k", "", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if calls["create"] != 1 || calls["put"] != 4 {
		t.Fatalf("calls = %v; want 1 CreateBucket and 4 PutObject (the first write is tried twice, the next two once)", calls)
	}
}

func TestS3SeamStopsOnOtherErrors(t *testing.T) {
	reg := provider.NewRegistry()
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"Object.PutObject": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return nil, model.NewProviderError("AccessDenied", "no", 403)
		},
	})
	if err := newSet(reg).S3().InternalPutObject(context.Background(), "b", "k", "", nil); errorCode(err) != "AccessDenied" {
		t.Fatalf("err = %v", err)
	}
}

func errorCode(err error) string {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.Code
	}
	return ""
}

func realLogs(t *testing.T) (*seams.Logs, *cwlogs.Provider) {
	t.Helper()
	reg := provider.NewRegistry()
	p := cwlogs.New()
	reg.Register(p)
	return newSet(reg).Logs(), p
}

// The streamers' contract, now met by acting like a client: groups are created
// if absent (repeatedly), a stream appears on first write, and a batch for a
// group that is gone is dropped.
func TestLogsSeamGivesStreamersTheirCreateOnDemandSemantics(t *testing.T) {
	logs, p := realLogs(t)
	ctx := context.Background()
	ev := []logstream.Event{{Timestamp: 1700000000000, Message: "hello"}}

	if err := logs.InternalPutEvents(ctx, "/g", "s", ev); err != nil {
		t.Fatalf("a batch for a missing group must be dropped, got %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := logs.InternalCreateLogGroup(ctx, "/g"); err != nil {
			t.Fatalf("create-if-absent #%d: %v", i, err)
		}
	}
	if err := logs.InternalPutEvents(ctx, "/g", "s", ev); err != nil {
		t.Fatalf("first write must create the stream: %v", err)
	}
	if err := logs.InternalPutEvents(ctx, "/g", "s", ev); err != nil {
		t.Fatal(err)
	}
	resp, err := p.GetLogEvents(ctx, &model.NormalizedRequest{Service: "logs", Action: "GetLogEvents",
		Params: map[string]any{"logGroupName": "/g", "logStreamName": "s"}})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := resp.Data["events"].([]any); len(got) != 2 {
		t.Fatalf("events = %v, want the 2 ingested lines (the first, dropped batch must not appear)", resp.Data["events"])
	}
}

func TestLogsSeamSplitsAndTrimsToTheCallLimits(t *testing.T) {
	reg := provider.NewRegistry()
	var sizes []int
	var longest int
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"CloudWatchLogs.PutLogEvents": func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			list := nr.Params["logEvents"].([]any)
			sizes = append(sizes, len(list))
			for _, e := range list {
				if n := len(e.(map[string]any)["message"].(string)); n > longest {
					longest = n
				}
			}
			return provider.OK(map[string]any{}), nil
		},
	})
	events := make([]logstream.Event, 10001)
	events[0].Message = string(bytes.Repeat([]byte("x"), 300000))
	if err := newSet(reg).Logs().InternalPutEvents(context.Background(), "/g", "s", events); err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 || sizes[0] != 10000 || sizes[1] != 1 || longest != 262144 {
		t.Fatalf("calls = %v, longest = %d; want [10000 1] and a 262144-byte cap", sizes, longest)
	}
}

func TestLogsSeamPropagatesOtherErrors(t *testing.T) {
	reg := provider.NewRegistry()
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"CloudWatchLogs.PutLogEvents": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return nil, model.NewProviderError(model.CodePluginUnavailable, "down", 503)
		},
	})
	err := newSet(reg).Logs().InternalPutEvents(context.Background(), "/g", "s", []logstream.Event{{Message: "x"}})
	if !model.IsOutage(err) {
		t.Fatalf("an outage must reach the caller, got %v", err)
	}
}

// The poller speaks the same contract as a client: QueueUrl (built from the ARN),
// every attribute requested, deletes in batches of at most ten. A built-in answers
// with typed values and a plugin with JSON; both are read the same way.
func TestSQSConsumerIsAnOrdinaryClient(t *testing.T) {
	const arn = "arn:aws:sqs:us-west-2:222222222222:orders"
	reg := provider.NewRegistry()
	var recv, del []*model.NormalizedRequest
	typed := true
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"Queue.ReceiveMessage": func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			recv = append(recv, nr)
			if typed {
				return provider.OK(map[string]any{"Messages": []map[string]any{{
					"MessageId": "m1", "ReceiptHandle": "rh1", "Body": "b", "MD5OfBody": "x",
					"Attributes":        map[string]string{"ApproximateReceiveCount": "2"},
					"MessageAttributes": map[string]struct{ DataType, StringValue string }{"k": {"String", "v"}},
				}}}), nil
			}
			return provider.OK(map[string]any{"Messages": []any{map[string]any{
				"MessageId": "m1", "ReceiptHandle": "rh1", "Body": "b", "MD5OfBody": "x",
				"Attributes":        map[string]any{"ApproximateReceiveCount": "2"},
				"MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "v"}},
			}}}), nil
		},
		"Queue.DeleteMessageBatch": func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			del = append(del, nr)
			return provider.OK(map[string]any{}), nil
		},
	})
	c := newSet(reg).SQSConsumer()
	for _, isTyped := range []bool{true, false} {
		typed = isTyped
		msgs, err := c.InternalReceive(context.Background(), arn, 10, 20)
		if err != nil || len(msgs) != 1 {
			t.Fatalf("typed=%v: %v, %v", isTyped, msgs, err)
		}
		m := msgs[0]
		if m.MessageID != "m1" || m.Attributes["ApproximateReceiveCount"] != "2" || m.MessageAttributes["k"] == nil {
			t.Fatalf("typed=%v: message = %+v; attributes were lost", isTyped, m)
		}
	}
	nr := recv[0]
	if nr.Params["QueueUrl"] != "http://localhost:4566/222222222222/orders" || nr.Params["QueueName"] != nil ||
		nr.AccountID != "222222222222" || nr.Region != "us-west-2" || nr.Origin != model.OriginInternal {
		t.Fatalf("ReceiveMessage request = %+v", nr)
	}
	if got := nr.Params["AttributeNames"].([]any); got[0] != "All" {
		t.Fatalf("AttributeNames = %v", got)
	}

	handles := make([]string, 23)
	for i := range handles {
		handles[i] = "rh" + strconv.Itoa(i)
	}
	if err := c.InternalDeleteBatch(context.Background(), arn, handles); err != nil {
		t.Fatal(err)
	}
	if len(del) != 3 {
		t.Fatalf("23 handles must be 3 batches, got %d calls", len(del))
	}
	for _, d := range del {
		if n := len(d.Params["Entries"].([]any)); n > 10 || d.Params["QueueUrl"] != "http://localhost:4566/222222222222/orders" || d.Params["QueueName"] != nil {
			t.Fatalf("DeleteMessageBatch = %d entries, params %v", n, d.Params)
		}
	}
}

// What is missing is read from the error's resource field, not from its wording.
func TestLogsSeamReadsTheMissingResourceFromTheError(t *testing.T) {
	for name, tc := range map[string]struct {
		data       map[string]any
		wantStream bool // true: the seam creates the stream and retries
	}{
		"group":                {map[string]any{"resource": cwlogs.ResourceLogGroup}, false},
		"stream":               {map[string]any{"resource": cwlogs.ResourceLogStream}, true},
		"plugin gave no field": {nil, true},
	} {
		reg := provider.NewRegistry()
		var puts, creates int
		reg.RegisterAll(map[string]provider.HandlerFunc{
			"CloudWatchLogs.PutLogEvents": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
				puts++
				// Deliberately unhelpful wording: the discriminator is the field.
				return nil, model.NewProviderError("ResourceNotFoundException", "not found", 400).WithData(tc.data)
			},
			"CloudWatchLogs.CreateLogStream": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
				creates++
				return provider.OK(map[string]any{}), nil
			},
		})
		err := newSet(reg).Logs().InternalPutEvents(context.Background(), "/g", "s", []logstream.Event{{Message: "x"}})
		if tc.wantStream {
			if puts != 2 || creates != 1 {
				t.Errorf("%s: puts=%d creates=%d, want the stream created and the put retried", name, puts, creates)
			}
		} else if err != nil || puts != 1 || creates != 0 {
			t.Errorf("%s: err=%v puts=%d creates=%d, want the batch dropped", name, err, puts, creates)
		}
	}
}

func TestLogsSeamCutsLongLinesOnACharacterBoundary(t *testing.T) {
	reg := provider.NewRegistry()
	var got string
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"CloudWatchLogs.PutLogEvents": func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			got = nr.Params["logEvents"].([]any)[0].(map[string]any)["message"].(string)
			return provider.OK(map[string]any{}), nil
		},
	})
	// "é" is two bytes, so a 262144-byte cut falls inside a character when the line starts with one byte.
	line := "a" + strings.Repeat("é", 200000)
	if err := newSet(reg).Logs().InternalPutEvents(context.Background(), "/g", "s", []logstream.Event{{Message: line}}); err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(got) || len(got) > 262144 || len(got) < 262142 {
		t.Fatalf("cut line: valid=%v len=%d", utf8.ValidString(got), len(got))
	}
}

// A stream the poller cannot read is an error it can act on, and a cancelled
// poller does not keep retrying.
func TestStreamsSeamReportsErrorsAndHonoursCancellation(t *testing.T) {
	reg := provider.NewRegistry()
	down := model.NewProviderError(model.CodePluginUnavailable, "plugin down", 503)
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"Streams.ListStreams": func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, down
		},
	})
	st := newSet(reg).Streams()
	if _, _, err := st.GetStreamInfo(context.Background(), "t"); !model.IsOutage(err) {
		t.Fatalf("GetStreamInfo err = %v, want the outage", err)
	}
	if _, _, err := st.GetRecords(context.Background(), "t", -1); !model.IsOutage(err) {
		t.Fatalf("GetRecords err = %v, want the outage", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := st.GetRecords(ctx, "t", -1); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled poller's call must carry its context, got %v", err)
	}
}
