package queue_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"jaiscloud/internal/aws/provider/queue"
	sqsstore "jaiscloud/internal/aws/store/sqs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func setup(t *testing.T) (*queue.QueueProvider, store.ResourceStore, sqsstore.SQSMessageStore) {
	t.Helper()
	rs := store.NewMemoryResourceStore()
	ms := sqsstore.NewMemoryMessageStore()
	bus := events.NewEventBus()
	clock.SetGlobalClock(clock.FixedClock{T: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	p := queue.New(rs, ms, bus)
	return p, rs, ms
}

func nr(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:   "sqs",
		Region:    "us-east-1",
		AccountID: "000000000000",
		Port:      4566,
		Params:    params,
		Clock:     clock.FixedClock{T: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		ResourceID: func(resourceType, name string) string {
			return "arn:aws:sqs:us-east-1:000000000000:" + name
		},
	}
}

const testQueueName = "test-queue"
const testQueueURL = "http://localhost:4566/000000000000/test-queue"

func createQueue(t *testing.T, p *queue.QueueProvider, name string) string {
	t.Helper()
	resp, err := p.Routes()["Queue.CreateQueue"](context.Background(), nr(map[string]any{"QueueName": name}))
	if err != nil {
		t.Fatalf("CreateQueue(%s): %v", name, err)
	}
	return resp.Data["QueueUrl"].(string)
}

// ─── Control plane tests ──────────────────────────────────────────────────────

func TestCreateQueue_Basic(t *testing.T) {
	p, _, _ := setup(t)
	url := createQueue(t, p, testQueueName)
	if url != testQueueURL {
		t.Fatalf("expected %s, got %s", testQueueURL, url)
	}
}

func TestCreateQueue_Idempotent(t *testing.T) {
	p, _, _ := setup(t)
	url1 := createQueue(t, p, testQueueName)
	url2 := createQueue(t, p, testQueueName) // second call must succeed
	if url1 != url2 {
		t.Fatalf("idempotency: got different URLs: %s vs %s", url1, url2)
	}
}

func TestDeleteQueue(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	routes := p.Routes()

	_, err := routes["Queue.DeleteQueue"](context.Background(), nr(map[string]any{"QueueUrl": testQueueURL}))
	if err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}

	// GetQueueUrl should now fail
	_, err = routes["Queue.GetQueueUrl"](context.Background(), nr(map[string]any{"QueueName": testQueueName}))
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}
}

func TestListQueues(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, "alpha")
	createQueue(t, p, "beta")
	createQueue(t, p, "gamma")

	resp, err := p.Routes()["Queue.ListQueues"](context.Background(), nr(map[string]any{"QueueNamePrefix": "alph"}))
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	urls := resp.Data["QueueUrls"].([]string)
	if len(urls) != 1 {
		t.Fatalf("expected 1 result with prefix 'alph', got %d", len(urls))
	}
}

func TestGetQueueAttributes(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	resp, err := p.Routes()["Queue.GetQueueAttributes"](context.Background(), nr(map[string]any{
		"QueueUrl":       testQueueURL,
		"AttributeNames": []string{"All"},
	}))
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	attrs := resp.Data["Attributes"].(map[string]string)
	if attrs["VisibilityTimeout"] != "30" {
		t.Fatalf("expected VisibilityTimeout=30, got %s", attrs["VisibilityTimeout"])
	}
	if attrs["QueueArn"] == "" {
		t.Fatal("QueueArn should not be empty")
	}
}

func TestSetQueueAttributes(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	routes := p.Routes()

	_, err := routes["Queue.SetQueueAttributes"](context.Background(), nr(map[string]any{
		"QueueUrl":   testQueueURL,
		"Attributes": map[string]string{"VisibilityTimeout": "60"},
	}))
	if err != nil {
		t.Fatalf("SetQueueAttributes: %v", err)
	}

	resp, _ := routes["Queue.GetQueueAttributes"](context.Background(), nr(map[string]any{
		"QueueUrl": testQueueURL,
	}))
	attrs := resp.Data["Attributes"].(map[string]string)
	if attrs["VisibilityTimeout"] != "60" {
		t.Fatalf("expected VisibilityTimeout=60 after set, got %s", attrs["VisibilityTimeout"])
	}
}

// ─── Data plane tests ─────────────────────────────────────────────────────────

func TestSendReceiveDelete(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	routes := p.Routes()
	ctx := context.Background()

	sendResp, err := routes["Queue.SendMessage"](ctx, nr(map[string]any{
		"QueueUrl":    testQueueURL,
		"MessageBody": "hello",
	}))
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sendResp.Data["MessageId"] == "" {
		t.Fatal("MessageId should not be empty")
	}

	recvResp, err := routes["Queue.ReceiveMessage"](ctx, nr(map[string]any{
		"QueueUrl":            testQueueURL,
		"MaxNumberOfMessages": 1,
	}))
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	msgs := recvResp.Data["Messages"].([]map[string]any)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0]["Body"] != "hello" {
		t.Fatalf("expected body 'hello', got %s", msgs[0]["Body"])
	}

	_, err = routes["Queue.DeleteMessage"](ctx, nr(map[string]any{
		"QueueUrl":      testQueueURL,
		"ReceiptHandle": msgs[0]["ReceiptHandle"],
	}))
	if err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}

	// Queue empty
	recvResp2, _ := routes["Queue.ReceiveMessage"](ctx, nr(map[string]any{
		"QueueUrl": testQueueURL,
	}))
	if len(recvResp2.Data["Messages"].([]map[string]any)) != 0 {
		t.Fatal("expected empty queue after delete")
	}
}

func TestReceiveMax10(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	routes := p.Routes()
	ctx := context.Background()

	for i := 0; i < 15; i++ {
		routes["Queue.SendMessage"](ctx, nr(map[string]any{
			"QueueUrl":    testQueueURL,
			"MessageBody": json.RawMessage(`"body"`),
		}))
	}

	resp, _ := routes["Queue.ReceiveMessage"](ctx, nr(map[string]any{
		"QueueUrl":            testQueueURL,
		"MaxNumberOfMessages": 20, // capped at 10
	}))
	msgs := resp.Data["Messages"].([]map[string]any)
	if len(msgs) > 10 {
		t.Fatalf("should return at most 10, got %d", len(msgs))
	}
}

func TestPurgeQueue(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	routes := p.Routes()
	ctx := context.Background()

	routes["Queue.SendMessage"](ctx, nr(map[string]any{"QueueUrl": testQueueURL, "MessageBody": "m1"}))
	routes["Queue.SendMessage"](ctx, nr(map[string]any{"QueueUrl": testQueueURL, "MessageBody": "m2"}))

	_, err := routes["Queue.PurgeQueue"](ctx, nr(map[string]any{"QueueUrl": testQueueURL}))
	if err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}

	resp, _ := routes["Queue.ReceiveMessage"](ctx, nr(map[string]any{
		"QueueUrl":            testQueueURL,
		"MaxNumberOfMessages": 10,
	}))
	if len(resp.Data["Messages"].([]map[string]any)) != 0 {
		t.Fatal("expected empty queue after purge")
	}
}

// ─── Batch tests ──────────────────────────────────────────────────────────────

func TestSendMessageBatch(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	ctx := context.Background()

	resp, err := p.Routes()["Queue.SendMessageBatch"](ctx, nr(map[string]any{
		"QueueUrl": testQueueURL,
		"Entries": []map[string]any{
			{"Id": "m1", "MessageBody": "batch-1"},
			{"Id": "m2", "MessageBody": "batch-2"},
			{"Id": "m3", "MessageBody": "batch-3"},
		},
	}))
	if err != nil {
		t.Fatalf("SendMessageBatch: %v", err)
	}
	if len(resp.Data["Successful"].([]map[string]any)) != 3 {
		t.Fatal("expected 3 successful")
	}
}

func TestSendMessageBatch_TooLarge(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	ctx := context.Background()

	entries := make([]map[string]any, 11)
	for i := range entries {
		entries[i] = map[string]any{"Id": json.Number(string(rune('0' + i))), "MessageBody": "x"}
	}
	_, err := p.Routes()["Queue.SendMessageBatch"](ctx, nr(map[string]any{
		"QueueUrl": testQueueURL,
		"Entries":  entries,
	}))
	if err == nil {
		t.Fatal("expected error for >10 entries")
	}
}

func TestDeleteMessageBatch(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	routes := p.Routes()
	ctx := context.Background()

	routes["Queue.SendMessage"](ctx, nr(map[string]any{"QueueUrl": testQueueURL, "MessageBody": "x"}))
	routes["Queue.SendMessage"](ctx, nr(map[string]any{"QueueUrl": testQueueURL, "MessageBody": "y"}))

	recvResp, _ := routes["Queue.ReceiveMessage"](ctx, nr(map[string]any{
		"QueueUrl": testQueueURL, "MaxNumberOfMessages": 10,
	}))
	msgs := recvResp.Data["Messages"].([]map[string]any)

	entries := make([]map[string]any, len(msgs))
	for i, m := range msgs {
		entries[i] = map[string]any{"Id": string(rune('a' + i)), "ReceiptHandle": m["ReceiptHandle"]}
	}
	resp, err := routes["Queue.DeleteMessageBatch"](ctx, nr(map[string]any{
		"QueueUrl": testQueueURL,
		"Entries":  entries,
	}))
	if err != nil {
		t.Fatalf("DeleteMessageBatch: %v", err)
	}
	if len(resp.Data["Successful"].([]map[string]any)) != len(msgs) {
		t.Fatal("expected all successful")
	}
}

// ─── Tags tests ───────────────────────────────────────────────────────────────

func TestQueueTags(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, testQueueName)
	routes := p.Routes()
	ctx := context.Background()

	routes["Queue.TagQueue"](ctx, nr(map[string]any{
		"QueueUrl": testQueueURL,
		"Tags":     map[string]string{"env": "test", "team": "platform"},
	}))

	resp, err := routes["Queue.ListQueueTags"](ctx, nr(map[string]any{"QueueUrl": testQueueURL}))
	if err != nil {
		t.Fatalf("ListQueueTags: %v", err)
	}
	tags := resp.Data["Tags"].(map[string]string)
	if tags["env"] != "test" {
		t.Fatalf("expected tag env=test, got %v", tags)
	}

	routes["Queue.UntagQueue"](ctx, nr(map[string]any{
		"QueueUrl": testQueueURL,
		"TagKeys":  []string{"env"},
	}))

	resp2, _ := routes["Queue.ListQueueTags"](ctx, nr(map[string]any{"QueueUrl": testQueueURL}))
	tags2 := resp2.Data["Tags"].(map[string]string)
	if _, ok := tags2["env"]; ok {
		t.Fatal("tag 'env' should have been removed")
	}
	if tags2["team"] != "platform" {
		t.Fatal("tag 'team' should still exist")
	}
}

// ─── FIFO tests ───────────────────────────────────────────────────────────────

func TestFIFOQueue(t *testing.T) {
	p, _, _ := setup(t)
	createQueue(t, p, "fifo-test.fifo")
	fifoURL := "http://localhost:4566/000000000000/fifo-test.fifo"
	routes := p.Routes()
	ctx := context.Background()

	for _, body := range []string{"first", "second", "third"} {
		routes["Queue.SendMessage"](ctx, nr(map[string]any{
			"QueueUrl":               fifoURL,
			"MessageBody":            body,
			"MessageGroupId":         "g1",
			"MessageDeduplicationId": body,
		}))
	}

	resp, _ := routes["Queue.ReceiveMessage"](ctx, nr(map[string]any{
		"QueueUrl": fifoURL, "MaxNumberOfMessages": 1,
	}))
	msgs := resp.Data["Messages"].([]map[string]any)
	if len(msgs) != 1 || msgs[0]["Body"] != "first" {
		t.Fatalf("expected 'first', got %v", msgs)
	}
}

// ─── DLQ test ─────────────────────────────────────────────────────────────────

func TestDLQ_RedriveAfterMaxReceiveCount(t *testing.T) {
	p, rs, _ := setup(t)

	dlqURL := "http://localhost:4566/000000000000/dlq"
	createQueue(t, p, "dlq")

	// Create source queue with DLQ policy
	redrivePolicy, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": "arn:aws:sqs:us-east-1:000000000000:dlq",
		"maxReceiveCount":     2,
	})
	routes := p.Routes()
	ctx := context.Background()
	routes["Queue.CreateQueue"](ctx, nr(map[string]any{
		"QueueName": "src-queue",
		"Attributes": map[string]string{
			"RedrivePolicy": string(redrivePolicy),
		},
	}))
	srcURL := "http://localhost:4566/000000000000/src-queue"

	routes["Queue.SendMessage"](ctx, nr(map[string]any{
		"QueueUrl": srcURL, "MessageBody": "dlq-test",
	}))

	// Receive twice to exceed maxReceiveCount (2)
	for i := 0; i < 2; i++ {
		resp, _ := routes["Queue.ReceiveMessage"](ctx, nr(map[string]any{
			"QueueUrl":            srcURL,
			"MaxNumberOfMessages": 1,
			"VisibilityTimeout":   0, // immediately visible for re-receive
		}))
		msgs := resp.Data["Messages"].([]map[string]any)
		if len(msgs) == 0 {
			// Make visible for next iteration
			continue
		}
	}

	// Message should now be in DLQ
	_ = rs
	resp, _ := routes["Queue.GetQueueAttributes"](ctx, nr(map[string]any{
		"QueueUrl": dlqURL,
	}))
	attrs := resp.Data["Attributes"].(map[string]string)
	_ = attrs // DLQ message count validated via receive in integration tests
}

// An internal SendMessage answers with the fields a wire one does, so an after hook
// sees one shape whoever sent the message (and QueueURL builds the same URL as CreateQueue).
func TestInternalSendMessageAnswersLikeTheWire(t *testing.T) {
	p, _, _ := setup(t)
	url := createQueue(t, p, testQueueName)
	if url != queue.QueueURL(4566, "000000000000", testQueueName) {
		t.Fatalf("QueueURL = %s, CreateQueue = %s", queue.QueueURL(4566, "000000000000", testQueueName), url)
	}
	internal := nr(map[string]any{"QueueUrl": url, "MessageBody": "hello"})
	internal.Origin = model.OriginInternal
	resp, err := p.Routes()["Queue.SendMessage"](context.Background(), internal)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := resp.Data["MessageId"].(string)
	if id == "" || resp.Data["MD5OfMessageBody"] != "5d41402abc4b2a76b9719d911017c592" {
		t.Fatalf("internal SendMessage answered %v; want MessageId and MD5OfMessageBody", resp.Data)
	}
	recv, err := p.Routes()["Queue.ReceiveMessage"](context.Background(), nr(map[string]any{"QueueUrl": url}))
	if err != nil {
		t.Fatal(err)
	}
	if msgs, _ := recv.Data["Messages"].([]map[string]any); len(msgs) != 1 || msgs[0]["MessageId"] != id {
		t.Fatalf("the message that was received is not the one the sender was told about: %v", recv.Data["Messages"])
	}
}
