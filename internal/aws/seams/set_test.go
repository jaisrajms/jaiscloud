package seams_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	keyprovider "jaiscloud/internal/aws/key"
	cwlogs "jaiscloud/internal/aws/provider/cloudwatch/logs"
	"jaiscloud/internal/aws/seams"
	streamstore "jaiscloud/internal/aws/store/stream"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

var wireToPrefix = map[string]string{
	"s3": "Object", "monitoring": "CloudWatch", "dynamodbstreams": "Streams",
	"kms": "Key", "secretsmanager": "Secret", "logs": "CloudWatchLogs",
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

	info, ok := st.GetStreamInfo("t")
	if !ok || !info.Enabled || info.StreamArn != "arn:s" {
		t.Fatalf("info = %+v, %v", info, ok)
	}
	recs, next := st.GetRecords("t", -1)
	if len(recs) != 1 || recs[0].SequenceNumber != 7 || recs[0].EventName != "INSERT" || recs[0].EventID != "e1" ||
		recs[0].ApproximateCreationDateTime.Unix() != 1700000000 || recs[0].Keys["id"] == nil || next != 8 {
		t.Fatalf("records = %+v next=%d", recs, next)
	}
	st.GetRecords("t", 7)
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
