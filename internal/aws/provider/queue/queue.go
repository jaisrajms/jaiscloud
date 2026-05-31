package queue

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	sqsstore "jaiscloud/internal/aws/store/sqs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// QueueProvider implements all SQS operations.
// It is cloud-agnostic — the SQS codec translates wire format, this
// provider works with NormalizedRequest.Params maps.
type QueueProvider struct {
	resources     store.ResourceStore
	messages      sqsstore.SQSMessageStore
	bus           *events.EventBus
	waiters       *Waiters
	recentDeletes map[string]time.Time // queueName → deletion time
	rdMu          sync.Mutex
}

func New(resources store.ResourceStore, messages sqsstore.SQSMessageStore, bus *events.EventBus) *QueueProvider {
	return &QueueProvider{
		resources:     resources,
		messages:      messages,
		bus:           bus,
		waiters:       NewWaiters(),
		recentDeletes: make(map[string]time.Time),
	}
}

// Reset clears in-memory waiter state; satisfies admin.Resetter.
func (p *QueueProvider) Reset(ctx context.Context) {
	p.waiters.Reset(ctx)
	resetMoveTasks()
	p.rdMu.Lock()
	p.recentDeletes = make(map[string]time.Time)
	p.rdMu.Unlock()
}

// Routes returns the provider's route map for registration in the Registry.
func (p *QueueProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Control plane
		"Queue.CreateQueue":        p.CreateQueue,
		"Queue.DeleteQueue":        p.DeleteQueue,
		"Queue.ListQueues":         p.ListQueues,
		"Queue.GetQueueUrl":        p.GetQueueUrl,
		"Queue.GetQueueAttributes": p.GetQueueAttributes,
		"Queue.SetQueueAttributes": p.SetQueueAttributes,

		// Data plane
		"Queue.SendMessage":             p.SendMessage,
		"Queue.ReceiveMessage":          p.ReceiveMessage,
		"Queue.DeleteMessage":           p.DeleteMessage,
		"Queue.ChangeMessageVisibility": p.ChangeMessageVisibility,
		"Queue.PurgeQueue":              p.PurgeQueue,

		// Batch operations
		"Queue.SendMessageBatch":             p.SendMessageBatch,
		"Queue.DeleteMessageBatch":           p.DeleteMessageBatch,
		"Queue.ChangeMessageVisibilityBatch": p.ChangeMessageVisibilityBatch,

		// Tags
		"Queue.TagQueue":      p.TagQueue,
		"Queue.UntagQueue":    p.UntagQueue,
		"Queue.ListQueueTags": p.ListQueueTags,

		// MessageMoveTask (P4.10)
		"Queue.StartMessageMoveTask":  p.StartMessageMoveTask,
		"Queue.CancelMessageMoveTask": p.CancelMessageMoveTask,
		"Queue.ListMessageMoveTasks":  p.ListMessageMoveTasks,
		// DLQ
		"Queue.ListDeadLetterSourceQueues": p.ListDeadLetterSourceQueues,
		// Permissions
		"Queue.AddPermission":    p.AddPermission,
		"Queue.RemovePermission": p.RemovePermission,
	}
}

// ─── Control Plane ────────────────────────────────────────────────────────────

var validQueueNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func validateQueueAttrs(attrs map[string]string) error {
	type rangeCheck struct {
		key      string
		min, max int
	}
	checks := []rangeCheck{
		{"VisibilityTimeout", 0, 43200},
		{"DelaySeconds", 0, 900},
		{"MessageRetentionPeriod", 60, 1209600},
		{"MaximumMessageSize", 1024, 262144},
		{"ReceiveMessageWaitTimeSeconds", 0, 20},
	}
	for _, c := range checks {
		if v, ok := attrs[c.key]; ok && v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < c.min || n > c.max {
				return model.NewProviderError("InvalidAttributeValue",
					fmt.Sprintf("Value %s for parameter %s is invalid. Reason: Must be between %d and %d, inclusive.", v, c.key, c.min, c.max), 400)
			}
		}
	}
	return nil
}

func (p *QueueProvider) CreateQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, ok := stringParam(nr.Params, "QueueName")
	if !ok || name == "" {
		return nil, model.NewProviderError("InvalidParameter", "QueueName is required", 400)
	}

	// Queue name validation (Task 1.12).
	baseName := strings.TrimSuffix(name, ".fifo")
	if len(name) > 80 || !validQueueNameRe.MatchString(baseName) {
		return nil, model.NewProviderError("InvalidParameterValue",
			"The specified queue name is not valid.", 400)
	}

	// §1.5.7: QueueDeletedRecently gate — AWS blocks re-creation for 60s.
	p.rdMu.Lock()
	if deletedAt, ok := p.recentDeletes[name]; ok {
		if clock.Now().Sub(deletedAt) < 60*time.Second {
			p.rdMu.Unlock()
			return nil, model.NewProviderError("QueueDeletedRecently",
				"You must wait 60 seconds after deleting a queue before you can create another with the same name.", 400)
		}
		delete(p.recentDeletes, name)
	}
	p.rdMu.Unlock()

	attrs := attrsParam(nr.Params, "Attributes")

	// Attribute range validation (Task 1.12).
	if err := validateQueueAttrs(attrs); err != nil {
		return nil, err
	}

	isFIFO := strings.HasSuffix(name, ".fifo")
	if attrs["FifoQueue"] == "true" && !isFIFO {
		return nil, model.NewProviderError("InvalidParameterValue",
			"The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length.", 400)
	}
	if isFIFO {
		if attrs["FifoQueue"] != "true" {
			attrs["FifoQueue"] = "true"
		}
	}

	queueURL := fmt.Sprintf("http://localhost:%d/%s/%s", nr.Port, nr.AccountID, name)

	// Idempotency: if queue exists, check for attribute mismatch (Task 1.10).
	if existing, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL); err == nil {
		if len(attrs) > 0 {
			var existingState map[string]any
			json.Unmarshal(existing.Data, &existingState)
			existingAttrs := attrsFromState(existingState)
			mismatch := []string{"VisibilityTimeout", "MaximumMessageSize", "MessageRetentionPeriod", "DelaySeconds", "ReceiveMessageWaitTimeSeconds"}
			for _, k := range mismatch {
				req := attrs[k]
				if req != "" && existingAttrs[k] != "" && existingAttrs[k] != req {
					return nil, model.NewProviderError("QueueAlreadyExists",
						"A queue already exists with the same name and a different value for attribute "+k, 400)
				}
			}
		}
		return provider.OK(map[string]any{"QueueUrl": queueURL}), nil
	}

	now := clock.Now()
	state := map[string]any{
		"QueueName":                     name,
		"QueueUrl":                      queueURL,
		"QueueArn":                      nr.ResourceID("sqs-queue", name),
		"IsFifo":                        isFIFO,
		"Attributes":                    attrs,
		"Tags":                          map[string]string{},
		"CreatedTimestamp":              strconv.FormatInt(now.Unix(), 10),
		"LastModifiedTimestamp":         strconv.FormatInt(now.Unix(), 10),
		"VisibilityTimeout":             attrOrDefault(attrs, "VisibilityTimeout", "30"),
		"DelaySeconds":                  attrOrDefault(attrs, "DelaySeconds", "0"),
		"MaximumMessageSize":            attrOrDefault(attrs, "MaximumMessageSize", "262144"),
		"MessageRetentionPeriod":        attrOrDefault(attrs, "MessageRetentionPeriod", "345600"),
		"ReceiveMessageWaitTimeSeconds": attrOrDefault(attrs, "ReceiveMessageWaitTimeSeconds", "0"),
	}

	// Persist new attributes (Task 1.11).
	for _, k := range []string{
		"RedrivePolicy", "MaxReceiveCount",
		"SqsManagedSseEnabled", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds",
		"FifoThroughputLimit", "DeduplicationScope", "RedriveAllowPolicy",
		"ContentBasedDeduplication",
	} {
		if v, ok := attrs[k]; ok {
			state[k] = v
		}
	}

	data, _ := json.Marshal(state)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{
		Type: "sqs_queues", ID: queueURL, Data: data, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return nil, err
	}

	if retSecs, err := strconv.Atoi(attrOrDefault(attrs, "MessageRetentionPeriod", "345600")); err == nil {
		p.messages.SetQueueRetention(ctx, nr.AccountID, nr.Region, queueURL, retSecs)
	}

	return provider.OK(map[string]any{"QueueUrl": queueURL}), nil
}

func (p *QueueProvider) DeleteQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "queue does not exist", 400)
		}
		return nil, err
	}
	p.messages.Purge(ctx, nr.AccountID, nr.Region, queueURL)
	// Record deletion for QueueDeletedRecently gate.
	queueName := queueURL[strings.LastIndex(queueURL, "/")+1:]
	p.rdMu.Lock()
	p.recentDeletes[queueName] = clock.Now()
	p.rdMu.Unlock()
	return provider.OK(map[string]any{}), nil
}

func (p *QueueProvider) ListQueues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	prefix, _ := stringParam(nr.Params, "QueueNamePrefix")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, "sqs_queues", prefix)
	if err != nil {
		return nil, err
	}

	// Collect all matching URLs.
	allURLs := make([]string, 0, len(entries))
	for _, e := range entries {
		allURLs = append(allURLs, e.ID)
	}

	// Apply NextToken cursor (Task 1.13).
	nextToken, _ := stringParam(nr.Params, "NextToken")
	start := 0
	if nextToken != "" {
		for i, u := range allURLs {
			if u == nextToken {
				start = i + 1
				break
			}
		}
	}
	allURLs = allURLs[start:]

	// Apply MaxResults limit.
	maxResults := 1000
	if v, ok := nr.Params["MaxResults"]; ok {
		if n := toInt(v); n > 0 && n <= 1000 {
			maxResults = n
		}
	}

	var outNextToken string
	if len(allURLs) > maxResults {
		outNextToken = allURLs[maxResults-1]
		allURLs = allURLs[:maxResults]
	}

	resp := map[string]any{"QueueUrls": allURLs}
	if outNextToken != "" {
		resp["NextToken"] = outNextToken
	}
	return provider.OK(resp), nil
}

func (p *QueueProvider) GetQueueUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, ok := stringParam(nr.Params, "QueueName")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueName is required", 400)
	}
	queueURL := fmt.Sprintf("http://localhost:%d/%s/%s", nr.Port, nr.AccountID, name)
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	return provider.OK(map[string]any{"QueueUrl": queueURL}), nil
}

func (p *QueueProvider) GetQueueAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	entry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}

	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	now := clock.Now()
	vis, notVis, delayed, _ := p.messages.GetApproximateCounts(ctx, nr.AccountID, nr.Region, queueURL, now)

	attrs := buildAttributes(state, vis, notVis, delayed)
	return provider.OK(map[string]any{"Attributes": attrs}), nil
}

func (p *QueueProvider) SetQueueAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	entry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}

	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	newAttrs := attrsParam(nr.Params, "Attributes")
	existing := attrsFromState(state)
	for k, v := range newAttrs {
		existing[k] = v
		// Also update top-level fields
		state[k] = v
	}
	state["Attributes"] = existing
	state["LastModifiedTimestamp"] = strconv.FormatInt(clock.Now().Unix(), 10)

	data, _ := json.Marshal(state)
	entry.Data = data
	if err := p.resources.Update(ctx, nr.AccountID, nr.Region, entry); err != nil {
		return nil, err
	}

	if rp, ok := newAttrs["MessageRetentionPeriod"]; ok {
		if retSecs, err := strconv.Atoi(rp); err == nil {
			p.messages.SetQueueRetention(ctx, nr.AccountID, nr.Region, queueURL, retSecs)
		}
	}

	return provider.OK(map[string]any{}), nil
}

// ─── Data Plane ───────────────────────────────────────────────────────────────

func (p *QueueProvider) SendMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	body, _ := stringParam(nr.Params, "MessageBody")

	entry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	// Per-message size validation (Task 1.9).
	maxMsgSize := intFromState(state, "MaximumMessageSize", 262144)
	msgAttrs := parseMessageAttributes(nr.Params["MessageAttributes"])
	if len(body)+messageAttributesWireSize(msgAttrs) > maxMsgSize {
		return nil, model.NewProviderError("InvalidParameterValue",
			fmt.Sprintf("One or more parameters are invalid. Reason: Message must be shorter than %d bytes.", maxMsgSize), 400)
	}

	isFIFO, _ := state["IsFifo"].(bool)
	groupID, hasGroupID := stringParam(nr.Params, "MessageGroupId")
	dedupID, hasDedupID := stringParam(nr.Params, "MessageDeduplicationId")

	if isFIFO {
		if !hasGroupID || groupID == "" {
			return nil, model.NewProviderError("MissingParameter",
				"The request must contain the parameter MessageGroupId", 400)
		}
		// FIFO queues do not support per-message DelaySeconds.
		if d, hasDelay := nr.Params["DelaySeconds"]; hasDelay && toInt(d) > 0 {
			return nil, model.NewProviderError("InvalidParameterValue",
				"Value 0 for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.", 400)
		}
		// ContentBasedDeduplication removes the requirement for MessageDeduplicationId.
		// The attribute is stored as a string in state["Attributes"].
		queueAttrs := attrsFromState(state)
		contentBasedDedup := queueAttrs["ContentBasedDeduplication"] == "true"
		if !contentBasedDedup && !hasDedupID {
			return nil, model.NewProviderError("InvalidParameterValue",
				"The queue requires MessageDeduplicationId when ContentBasedDeduplication is disabled", 400)
		}
	}

	now := clock.Now()
	msgID := newMessageID()
	delaySec := intFromState(state, "DelaySeconds", 0)
	if d, ok := nr.Params["DelaySeconds"]; ok {
		delaySec = toInt(d)
		if delaySec > 900 {
			return nil, model.NewProviderError("InvalidParameterValue",
				"Value "+fmt.Sprint(delaySec)+" for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900, inclusive.", 400)
		}
	}

	msg := sqsstore.SQSMessage{
		MessageID:  msgID,
		QueueURL:   queueURL,
		Body:       body,
		SentAt:     now,
		Attributes: map[string]string{},
	}
	if delaySec > 0 {
		msg.DelayUntil = now.Add(time.Duration(delaySec) * time.Second)
	}
	if hasGroupID {
		msg.GroupID = groupID
	}
	if hasDedupID {
		msg.DeduplicationID = dedupID
	} else if isFIFO {
		// ContentBasedDeduplication: auto-assign SHA-256 of body as dedup ID.
		h := sha256.Sum256([]byte(body))
		msg.DeduplicationID = fmt.Sprintf("%x", h)
	}
	if isFIFO {
		queueAttrs := attrsFromState(state)
		if queueAttrs["MessageDeduplicationScope"] == "messageGroup" {
			msg.DedupScope = "messageGroup"
		}
	}
	if ma, ok := nr.Params["MessageAttributes"]; ok {
		msg.MessageAttributes = parseMessageAttributes(ma)
	}

	origID, seqNum, err := p.messages.Send(ctx, nr.AccountID, nr.Region, msg)
	if err != nil {
		return nil, err
	}
	if origID != "" {
		msgID = origID // FIFO dedup: return the original message's ID
	}
	p.waiters.Notify(queueURL)

	md5Body := fmt.Sprintf("%x", md5.Sum([]byte(body)))
	resp := map[string]any{
		"MessageId":        msgID,
		"MD5OfMessageBody": md5Body,
	}
	if seqNum != "" {
		resp["SequenceNumber"] = seqNum
	}
	if len(msg.MessageAttributes) > 0 {
		resp["MD5OfMessageAttributes"] = md5MessageAttributes(msg.MessageAttributes)
	}
	return provider.OK(resp), nil
}

func (p *QueueProvider) ReceiveMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}

	entry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	maxMessages := 1
	if m, ok := nr.Params["MaxNumberOfMessages"]; ok {
		maxMessages = toInt(m)
	}
	if maxMessages > 10 {
		maxMessages = 10
	}
	if maxMessages < 1 {
		maxMessages = 1
	}

	defaultVisTimeout := intFromState(state, "VisibilityTimeout", 30)
	visTimeout := defaultVisTimeout
	if v, ok := nr.Params["VisibilityTimeout"]; ok {
		visTimeout = toInt(v)
	}

	// Which message attributes to return
	attrNames := stringSliceParam(nr.Params, "MessageAttributeNames")
	sysAttrNames := stringSliceParam(nr.Params, "AttributeNames")

	// Long polling: WaitTimeSeconds > 0 blocks until a message arrives or deadline.
	waitSec := 0
	if w, ok := nr.Params["WaitTimeSeconds"]; ok {
		waitSec = toInt(w)
	} else {
		waitSec = intFromState(state, "ReceiveMessageWaitTimeSeconds", 0)
	}
	if waitSec < 0 {
		waitSec = 0
	}
	if waitSec > 20 {
		return nil, model.NewProviderError("InvalidParameterValue", "Value for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20", 400)
	}

	now := clock.Now()
	var msgs []sqsstore.SQSMessage
	if waitSec > 0 {
		msgs, err = WaitForMessages(ctx, p.messages, p.waiters, nr.AccountID, nr.Region, queueURL, maxMessages, time.Duration(waitSec)*time.Second)
	} else {
		msgs, err = p.messages.Receive(ctx, nr.AccountID, nr.Region, queueURL, maxMessages, now)
	}
	if err != nil {
		return nil, err
	}

	// Evaluate DLQ redrive BEFORE building the response — messages that exceed
	// maxReceiveCount must not be delivered to the caller.
	var deliverable []sqsstore.SQSMessage
	for i := range msgs {
		if p.checkDLQ(ctx, state, nr.AccountID, nr.Region, queueURL, &msgs[i]) {
			continue // moved to DLQ; exclude from this response
		}
		p.messages.ChangeVisibility(ctx, nr.AccountID, nr.Region, queueURL, msgs[i].ReceiptHandle, visTimeout, now)
		msgs[i].VisibleAt = now.Add(time.Duration(visTimeout) * time.Second)
		deliverable = append(deliverable, msgs[i])
	}
	msgs = deliverable

	// Build wire-format messages
	var wireMessages []map[string]any
	for _, m := range msgs {
		wm := map[string]any{
			"MessageId":     m.MessageID,
			"ReceiptHandle": m.ReceiptHandle,
			"Body":          m.Body,
			"MD5OfBody":     m.MD5OfBody,
		}
		// System attributes
		sysAttrs := buildSysAttributes(m)
		if len(sysAttrNames) > 0 {
			filtered := map[string]string{}
			for _, n := range sysAttrNames {
				if n == "All" {
					filtered = sysAttrs
					break
				}
				if v, ok := sysAttrs[n]; ok {
					filtered[n] = v
				}
			}
			if len(filtered) > 0 {
				wm["Attributes"] = filtered
			}
		}
		// Message attributes
		if len(attrNames) > 0 && len(m.MessageAttributes) > 0 {
			filtered := map[string]sqsstore.MessageAttribute{}
			for _, n := range attrNames {
				if n == "All" {
					filtered = m.MessageAttributes
					break
				}
				if v, ok := m.MessageAttributes[n]; ok {
					filtered[n] = v
				}
			}
			if len(filtered) > 0 {
				wm["MessageAttributes"] = filtered
				wm["MD5OfMessageAttributes"] = md5MessageAttributes(filtered)
			}
		}
		wireMessages = append(wireMessages, wm)
	}

	if wireMessages == nil {
		wireMessages = []map[string]any{}
	}
	return provider.OK(map[string]any{"Messages": wireMessages}), nil
}

func (p *QueueProvider) DeleteMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	receiptHandle, _ := stringParam(nr.Params, "ReceiptHandle")
	if err := p.messages.Delete(ctx, nr.AccountID, nr.Region, queueURL, receiptHandle); err != nil {
		// SQS returns success even for invalid handles (fire-and-forget)
		return provider.OK(map[string]any{}), nil
	}
	return provider.OK(map[string]any{}), nil
}

func (p *QueueProvider) ChangeMessageVisibility(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	receiptHandle, _ := stringParam(nr.Params, "ReceiptHandle")
	timeout := 0
	if v, ok := nr.Params["VisibilityTimeout"]; ok {
		timeout = toInt(v)
	}
	now := clock.Now()
	// AWS silently succeeds for invalid/expired receipt handles — do not return error.
	p.messages.ChangeVisibility(ctx, nr.AccountID, nr.Region, queueURL, receiptHandle, timeout, now)
	return provider.OK(map[string]any{}), nil
}

func (p *QueueProvider) PurgeQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	if _, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	p.messages.Purge(ctx, nr.AccountID, nr.Region, queueURL)
	return provider.OK(map[string]any{}), nil
}

// ─── Batch Operations ─────────────────────────────────────────────────────────

func (p *QueueProvider) SendMessageBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}

	entries := batchEntries(nr.Params, "Entries")
	if len(entries) == 0 {
		return nil, model.NewProviderError("AWS.SimpleQueueService.EmptyBatchRequest",
			"There is nothing to send. At least one SQS message must be sent with each batch.", 400)
	}
	if len(entries) > 10 {
		return nil, model.NewProviderError("AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
			"Maximum number of entries per request are 10.", 400)
	}

	// Validate entry IDs: must be unique; duplicate IDs fail the whole batch.
	seenIDs := map[string]bool{}
	for _, e := range entries {
		id, _ := e["Id"].(string)
		if seenIDs[id] {
			return nil, model.NewProviderError("AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
				"Two or more batch entries in the request have the same Id.", 400)
		}
		seenIDs[id] = true
	}

	storeEntry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(storeEntry.Data, &state)
	isFIFO, _ := state["IsFifo"].(bool)
	queueAttrs := attrsFromState(state)
	contentBasedDedup := queueAttrs["ContentBasedDeduplication"] == "true"

	// Check total batch size against queue's MaximumMessageSize (default 256 KB).
	maxMsgSize := intFromState(state, "MaximumMessageSize", 262144)
	totalSize := 0
	for _, e := range entries {
		if body, ok := e["MessageBody"].(string); ok {
			totalSize += len(body)
		}
		if ma, ok := e["MessageAttributes"]; ok {
			totalSize += messageAttributesWireSize(parseMessageAttributes(ma))
		}
	}
	if totalSize > maxMsgSize {
		return nil, model.NewProviderError("AWS.SimpleQueueService.BatchRequestTooLong",
			"The combined size of all messages in the batch exceeds the maximum you can send with a batch.", 400)
	}

	var successful []map[string]any
	var failed []map[string]any

	for _, e := range entries {
		id, _ := e["Id"].(string)
		if !validBatchEntryID(id) {
			failed = append(failed, map[string]any{
				"Id": id, "Code": "InvalidParameterValue",
				"Message":     "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
				"SenderFault": true,
			})
			continue
		}
		body, _ := e["MessageBody"].(string)
		msgID := newMessageID()
		now := clock.Now()

		// Per-entry FIFO validation
		if isFIFO {
			groupID, hasGroup := e["MessageGroupId"].(string)
			if !hasGroup || groupID == "" {
				failed = append(failed, map[string]any{
					"Id": id, "Code": "MissingParameter",
					"Message":     "The request must contain the parameter MessageGroupId",
					"SenderFault": true,
				})
				continue
			}
			_, hasDedupID := e["MessageDeduplicationId"].(string)
			if !contentBasedDedup && !hasDedupID {
				failed = append(failed, map[string]any{
					"Id": id, "Code": "InvalidParameterValue",
					"Message":     "The queue requires MessageDeduplicationId when ContentBasedDeduplication is disabled",
					"SenderFault": true,
				})
				continue
			}
		}

		msg := sqsstore.SQSMessage{
			MessageID: msgID,
			QueueURL:  queueURL,
			Body:      body,
			SentAt:    now,
		}
		if g, ok := e["MessageGroupId"].(string); ok {
			msg.GroupID = g
		}
		if d, ok := e["MessageDeduplicationId"].(string); ok {
			msg.DeduplicationID = d
		} else if isFIFO {
			h := sha256.Sum256([]byte(body))
			msg.DeduplicationID = fmt.Sprintf("%x", h)
		}
		if isFIFO && queueAttrs["MessageDeduplicationScope"] == "messageGroup" {
			msg.DedupScope = "messageGroup"
		}
		if ds, ok := e["DelaySeconds"]; ok {
			if sec := toInt(ds); sec > 0 {
				msg.DelayUntil = now.Add(time.Duration(sec) * time.Second)
			}
		}
		if ma, ok := e["MessageAttributes"]; ok {
			msg.MessageAttributes = parseMessageAttributes(ma)
		}

		origID, seqNum, sendErr := p.messages.Send(ctx, nr.AccountID, nr.Region, msg)
		if sendErr != nil {
			failed = append(failed, map[string]any{"Id": id, "Code": "InternalError", "Message": sendErr.Error(), "SenderFault": false})
			continue
		}
		if origID != "" {
			msgID = origID
		}
		md5Body := fmt.Sprintf("%x", md5.Sum([]byte(body)))
		entry := map[string]any{
			"Id":               id,
			"MessageId":        msgID,
			"MD5OfMessageBody": md5Body,
		}
		if seqNum != "" {
			entry["SequenceNumber"] = seqNum
		}
		if len(msg.MessageAttributes) > 0 {
			entry["MD5OfMessageAttributes"] = md5MessageAttributes(msg.MessageAttributes)
		}
		successful = append(successful, entry)
	}

	if len(successful) > 0 {
		p.waiters.Notify(queueURL)
	}

	return provider.OK(map[string]any{
		"Successful": successful,
		"Failed":     failed,
	}), nil
}

var batchEntryIDRegexp = regexp.MustCompile(`^[\w-]{1,80}$`)

func validBatchEntryID(id string) bool {
	return id != "" && batchEntryIDRegexp.MatchString(id)
}

func (p *QueueProvider) DeleteMessageBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entries := batchEntries(nr.Params, "Entries")
	if len(entries) == 0 {
		return nil, model.NewProviderError("AWS.SimpleQueueService.EmptyBatchRequest", "There is nothing to delete.", 400)
	}
	if len(entries) > 10 {
		return nil, model.NewProviderError("AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
			"Maximum number of entries per request are 10.", 400)
	}
	seenIDs := map[string]bool{}
	for _, e := range entries {
		id, _ := e["Id"].(string)
		if seenIDs[id] {
			return nil, model.NewProviderError("AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
				"Two or more batch entries in the request have the same Id.", 400)
		}
		seenIDs[id] = true
	}

	var successful []map[string]any
	var failed []map[string]any

	for _, e := range entries {
		id, _ := e["Id"].(string)
		if !validBatchEntryID(id) {
			failed = append(failed, map[string]any{
				"Id": id, "Code": "InvalidParameterValue",
				"Message":     "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
				"SenderFault": true,
			})
			continue
		}
		rh, _ := e["ReceiptHandle"].(string)
		if err := p.messages.Delete(ctx, nr.AccountID, nr.Region, queueURL, rh); err != nil {
			failed = append(failed, map[string]any{
				"Id": id, "Code": "ReceiptHandleIsInvalid",
				"Message": "The input receipt handle is invalid.", "SenderFault": true,
			})
		} else {
			successful = append(successful, map[string]any{"Id": id})
		}
	}

	return provider.OK(map[string]any{"Successful": successful, "Failed": failed}), nil
}

func (p *QueueProvider) ChangeMessageVisibilityBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entries := batchEntries(nr.Params, "Entries")

	if len(entries) > 10 {
		return nil, model.NewProviderError("AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
			"Maximum number of entries per request are 10.", 400)
	}
	seenIDs := map[string]bool{}
	for _, e := range entries {
		id, _ := e["Id"].(string)
		if seenIDs[id] {
			return nil, model.NewProviderError("AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
				"Two or more batch entries in the request have the same Id.", 400)
		}
		seenIDs[id] = true
	}

	now := clock.Now()
	var successful []map[string]any
	var failed []map[string]any

	for _, e := range entries {
		id, _ := e["Id"].(string)
		if !validBatchEntryID(id) {
			failed = append(failed, map[string]any{
				"Id": id, "Code": "InvalidParameterValue",
				"Message":     "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
				"SenderFault": true,
			})
			continue
		}
		rh, _ := e["ReceiptHandle"].(string)
		timeout := toInt(e["VisibilityTimeout"])
		if err := p.messages.ChangeVisibility(ctx, nr.AccountID, nr.Region, queueURL, rh, timeout, now); err != nil {
			failed = append(failed, map[string]any{"Id": id, "Code": "InvalidParameterValue", "Message": err.Error(), "SenderFault": true})
		} else {
			successful = append(successful, map[string]any{"Id": id})
		}
	}

	return provider.OK(map[string]any{"Successful": successful, "Failed": failed}), nil
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *QueueProvider) TagQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	tags := tagsFromState(state)
	for k, v := range attrsParam(nr.Params, "Tags") {
		tags[k] = v
	}
	state["Tags"] = tags
	data, _ := json.Marshal(state)
	entry.Data = data
	return provider.OK(map[string]any{}), p.resources.Update(ctx, nr.AccountID, nr.Region, entry)
}

func (p *QueueProvider) UntagQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)

	tags := tagsFromState(state)
	keys := stringSliceParam(nr.Params, "TagKeys")
	for _, k := range keys {
		delete(tags, k)
	}
	state["Tags"] = tags
	data, _ := json.Marshal(state)
	entry.Data = data
	return provider.OK(map[string]any{}), p.resources.Update(ctx, nr.AccountID, nr.Region, entry)
}

func (p *QueueProvider) ListQueueTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	entry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var state map[string]any
	json.Unmarshal(entry.Data, &state)
	return provider.OK(map[string]any{"Tags": tagsFromState(state)}), nil
}

// PeekMessages returns a page of messages from the queue without changing their
// state. This is a non-standard, UI-only operation.
func (p *QueueProvider) PeekMessages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, _ := stringParam(nr.Params, "QueueUrl")
	if queueURL == "" {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	limit := 50
	if l, ok := nr.Params["Limit"].(int); ok && l > 0 {
		limit = l
	}
	offset := 0
	if o, ok := nr.Params["Offset"].(int); ok && o > 0 {
		offset = o
	}
	msgs, total, err := p.messages.Peek(ctx, nr.AccountID, nr.Region, queueURL, offset, limit)
	if err != nil {
		return nil, model.NewProviderError("InternalError", err.Error(), 500)
	}
	now := nr.Clock.Now()
	items := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		status := "visible"
		if !m.VisibleAt.IsZero() && now.Before(m.VisibleAt) {
			status = "in-flight"
		} else if !m.DelayUntil.IsZero() && now.Before(m.DelayUntil) {
			status = "delayed"
		}
		item := map[string]any{
			"messageId":    m.MessageID,
			"body":         m.Body,
			"sentAt":       m.SentAt,
			"receiveCount": m.ReceiveCount,
			"status":       status,
		}
		if m.GroupID != "" {
			item["groupId"] = m.GroupID
		}
		items = append(items, item)
	}
	return provider.OK(map[string]any{"messages": items, "total": total, "offset": offset, "limit": limit}), nil
}

// ─── DLQ ──────────────────────────────────────────────────────────────────────

// checkDLQ evaluates whether msg has exceeded its maxReceiveCount and, if so,
// moves it to the dead-letter queue. Returns true if the message was moved (and
// must therefore NOT be delivered to the caller).
func (p *QueueProvider) checkDLQ(ctx context.Context, state map[string]any, account, region, queueURL string, msg *sqsstore.SQSMessage) bool {
	rp, ok := state["RedrivePolicy"].(string)
	if !ok || rp == "" {
		return false
	}
	var policy map[string]any
	if err := json.Unmarshal([]byte(rp), &policy); err != nil {
		return false
	}
	maxCount := toInt(policy["maxReceiveCount"])
	if maxCount <= 0 {
		maxCount = 1
	}
	dlqArn, _ := policy["deadLetterTargetArn"].(string)
	// AWS triggers DLQ when ReceiveCount > maxReceiveCount (strictly greater).
	if dlqArn == "" || msg.ReceiveCount <= maxCount {
		return false
	}

	// Move message to DLQ — reset delivery metadata so it is immediately visible.
	dlqURL := arnToURL(dlqArn, nr_port(state))
	dlqMsg := *msg
	dlqMsg.QueueURL = dlqURL
	dlqMsg.ReceiptHandle = ""
	dlqMsg.VisibleAt = time.Time{}  // clear in-flight timeout
	dlqMsg.DelayUntil = time.Time{} // no delay in DLQ
	dlqMsg.ReceiveCount = 0
	dlqParts := strings.Split(dlqArn, ":")
	dlqAccount, dlqRegion := "", ""
	if len(dlqParts) >= 5 {
		dlqRegion = dlqParts[3]
		dlqAccount = dlqParts[4]
	}
	p.messages.Send(ctx, dlqAccount, dlqRegion, dlqMsg) //nolint:errcheck
	p.messages.Delete(ctx, account, region, queueURL, msg.ReceiptHandle)

	p.bus.Publish(events.Event{
		Type: events.EventMessageDLQ,
		Payload: events.DLQEvent{
			SourceQueueURL: queueURL,
			DLQQueueURL:    dlqURL,
			MessageID:      msg.MessageID,
		},
	})
	return true
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func stringParam(params map[string]any, key string) (string, bool) {
	v, ok := params[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func attrsParam(params map[string]any, key string) map[string]string {
	v, ok := params[key]
	if !ok {
		return map[string]string{}
	}
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		result := make(map[string]string, len(m))
		for k, val := range m {
			result[k] = fmt.Sprintf("%v", val)
		}
		return result
	}
	return map[string]string{}
}

func stringSliceParam(params map[string]any, key string) []string {
	v, ok := params[key]
	if !ok {
		return nil
	}
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		result := make([]string, 0, len(s))
		for _, item := range s {
			result = append(result, fmt.Sprintf("%v", item))
		}
		return result
	}
	return nil
}

func batchEntries(params map[string]any, key string) []map[string]any {
	v, ok := params[key]
	if !ok {
		return nil
	}
	switch entries := v.(type) {
	case []map[string]any:
		return entries
	case []any:
		result := make([]map[string]any, 0, len(entries))
		for _, e := range entries {
			if m, ok := e.(map[string]any); ok {
				result = append(result, m)
			}
		}
		return result
	}
	return nil
}

func attrOrDefault(attrs map[string]string, key, def string) string {
	if v, ok := attrs[key]; ok && v != "" {
		return v
	}
	return def
}

func attrsFromState(state map[string]any) map[string]string {
	if v, ok := state["Attributes"]; ok {
		return attrsParam(map[string]any{"a": v}, "a")
	}
	return map[string]string{}
}

func tagsFromState(state map[string]any) map[string]string {
	if v, ok := state["Tags"]; ok {
		switch t := v.(type) {
		case map[string]string:
			return t
		case map[string]any:
			result := map[string]string{}
			for k, val := range t {
				result[k] = fmt.Sprintf("%v", val)
			}
			return result
		}
	}
	return map[string]string{}
}

func intFromState(state map[string]any, key string, def int) int {
	v, ok := state[key]
	if !ok {
		return def
	}
	return toInt(v)
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

func buildAttributes(state map[string]any, visible, notVisible, delayed int) map[string]string {
	a := map[string]string{
		"QueueArn":                              str(state["QueueArn"]),
		"CreatedTimestamp":                      str(state["CreatedTimestamp"]),
		"LastModifiedTimestamp":                 str(state["LastModifiedTimestamp"]),
		"VisibilityTimeout":                     str(state["VisibilityTimeout"]),
		"DelaySeconds":                          str(state["DelaySeconds"]),
		"MaximumMessageSize":                    str(state["MaximumMessageSize"]),
		"MessageRetentionPeriod":                str(state["MessageRetentionPeriod"]),
		"ReceiveMessageWaitTimeSeconds":         str(state["ReceiveMessageWaitTimeSeconds"]),
		"ApproximateNumberOfMessages":           strconv.Itoa(visible),
		"ApproximateNumberOfMessagesNotVisible": strconv.Itoa(notVisible),
		"ApproximateNumberOfMessagesDelayed":    strconv.Itoa(delayed),
	}
	if rp, ok := state["RedrivePolicy"]; ok {
		a["RedrivePolicy"] = str(rp)
	}
	if isFIFO, _ := state["IsFifo"].(bool); isFIFO {
		a["FifoQueue"] = "true"
	}
	// Extended attributes.
	for _, k := range []string{
		"SqsManagedSseEnabled", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds",
		"FifoThroughputLimit", "DeduplicationScope", "RedriveAllowPolicy",
		"ContentBasedDeduplication",
	} {
		if v, ok := state[k]; ok {
			a[k] = str(v)
		}
	}
	// FIFO-only attrs from Attributes sub-map
	if isFIFO, _ := state["IsFifo"].(bool); isFIFO {
		queueAttrs := attrsFromState(state)
		for _, k := range []string{"FifoThroughputLimit", "DeduplicationScope", "ContentBasedDeduplication"} {
			if v, ok := queueAttrs[k]; ok && v != "" && a[k] == "" {
				a[k] = v
			}
		}
	}
	return a
}

func buildSysAttributes(m sqsstore.SQSMessage) map[string]string {
	a := map[string]string{
		"SenderId":                         "000000000000",
		"SentTimestamp":                    strconv.FormatInt(m.SentAt.UnixMilli(), 10),
		"ApproximateReceiveCount":          strconv.Itoa(m.ReceiveCount),
		"ApproximateFirstReceiveTimestamp": "0",
	}
	if m.FirstReceivedAt != nil {
		a["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(m.FirstReceivedAt.UnixMilli(), 10)
	}
	if m.GroupID != "" {
		a["MessageGroupId"] = m.GroupID
	}
	if m.DeduplicationID != "" {
		a["MessageDeduplicationId"] = m.DeduplicationID
	}
	if m.SequenceNumber != "" {
		a["SequenceNumber"] = m.SequenceNumber
	}
	return a
}

func parseMessageAttributes(v any) map[string]sqsstore.MessageAttribute {
	result := map[string]sqsstore.MessageAttribute{}
	switch m := v.(type) {
	case map[string]any:
		for k, val := range m {
			if attr, ok := val.(map[string]any); ok {
				ma := sqsstore.MessageAttribute{
					DataType: str(attr["DataType"]),
				}
				if sv, ok := attr["StringValue"]; ok {
					ma.StringValue = str(sv)
				}
				if bv, ok := attr["BinaryValue"]; ok {
					switch v := bv.(type) {
					case []byte:
						ma.BinaryValue = v
					case string:
						// JSON protocol sends binary as base64; decode it.
						if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
							ma.BinaryValue = decoded
						} else {
							ma.BinaryValue = []byte(v)
						}
					}
				}
				result[k] = ma
			}
		}
	case map[string]sqsstore.MessageAttribute:
		return m
	}
	return result
}

// messageAttributesWireSize returns the total wire size (name + dataType + value bytes) for batch size checks.
func messageAttributesWireSize(attrs map[string]sqsstore.MessageAttribute) int {
	n := 0
	for name, attr := range attrs {
		n += len(name) + len(attr.DataType)
		if strings.HasPrefix(attr.DataType, "Binary") {
			n += len(attr.BinaryValue)
		} else {
			n += len(attr.StringValue)
		}
	}
	return n
}

// md5MessageAttributes computes the AWS-compatible MD5 over message attributes.
// The algorithm: for each attribute sorted by name, write:
//
//	4-byte big-endian len(name) + name bytes
//	4-byte big-endian len(dataType) + dataType bytes
//	1-byte transport type: 1 = String/Number, 2 = Binary
//	4-byte big-endian len(value) + value bytes
func md5MessageAttributes(attrs map[string]sqsstore.MessageAttribute) string {
	names := make([]string, 0, len(attrs))
	for k := range attrs {
		names = append(names, k)
	}
	sort.Strings(names)

	h := md5.New()
	buf4 := make([]byte, 4)
	writeBytes := func(b []byte) {
		binary.BigEndian.PutUint32(buf4, uint32(len(b)))
		h.Write(buf4)
		h.Write(b)
	}
	for _, name := range names {
		attr := attrs[name]
		writeBytes([]byte(name))
		writeBytes([]byte(attr.DataType))
		if strings.HasPrefix(attr.DataType, "Binary") {
			h.Write([]byte{2})
			writeBytes(attr.BinaryValue)
		} else {
			// String or Number
			h.Write([]byte{1})
			writeBytes([]byte(attr.StringValue))
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func arnToURL(arn string, port int) string {
	// arn:aws:sqs:us-east-1:000000000000:queue-name → http://localhost:port/000000000000/queue-name
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return arn
	}
	return fmt.Sprintf("http://localhost:%d/%s/%s", port, parts[4], parts[5])
}

func nr_port(state map[string]any) int {
	url, _ := state["QueueUrl"].(string)
	if url == "" {
		return 4566
	}
	// http://localhost:4566/...
	parts := strings.Split(url, ":")
	if len(parts) >= 3 {
		portStr := strings.Split(parts[2], "/")[0]
		p, _ := strconv.Atoi(portStr)
		if p > 0 {
			return p
		}
	}
	return 4566
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func newMessageID() string {
	return fmt.Sprintf("%x-%x-%x", rand.Int31(), rand.Int31(), rand.Int31())
}

// ─── ListDeadLetterSourceQueues (Task 1.8) ────────────────────────────────────

func (p *QueueProvider) ListDeadLetterSourceQueues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queueURL, ok := stringParam(nr.Params, "QueueUrl")
	if !ok {
		return nil, model.NewProviderError("InvalidParameter", "QueueUrl is required", 400)
	}
	dlqEntry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, "sqs_queues", queueURL)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "queue does not exist")
	}
	var dlqState map[string]any
	json.Unmarshal(dlqEntry.Data, &dlqState)
	dlqARN := str(dlqState["QueueArn"])

	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, "sqs_queues", "")
	if err != nil {
		return nil, err
	}
	var urls []string
	for _, e := range entries {
		var state map[string]any
		json.Unmarshal(e.Data, &state)
		rp := str(state["RedrivePolicy"])
		if rp == "" {
			continue
		}
		var policy map[string]any
		if json.Unmarshal([]byte(rp), &policy) == nil {
			if str(policy["deadLetterTargetArn"]) == dlqARN {
				urls = append(urls, e.ID)
			}
		}
	}
	if urls == nil {
		urls = []string{}
	}
	return provider.OK(map[string]any{"QueueUrls": urls}), nil
}
