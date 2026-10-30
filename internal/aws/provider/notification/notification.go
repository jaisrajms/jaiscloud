// Package notification implements the SNS provider.
package notification

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/aws/provider/events/pattern"
	sqsstore "jaiscloud/internal/aws/store/sqs"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// FunctionInvoker is the narrow interface SNS uses to invoke Lambda functions.
type FunctionInvoker interface {
	InvokeInternal(ctx context.Context, functionName string, payload []byte) ([]byte, error)
}

// SQSSender is the narrow interface SNS uses to deliver messages to SQS queues.
// Implementations must resolve ARNs to queue URLs before writing.
type SQSSender interface {
	InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]SQSMessageAttribute, src SQSSourceContext) error
}

// SQSMessageAttribute carries a single SQS message attribute for cross-service delivery.
type SQSMessageAttribute struct {
	DataType    string
	StringValue string
}

// SQSSourceContext carries caller identity for internal SQS deliveries.
type SQSSourceContext struct {
	SourceArn        string
	ServicePrincipal string
}

// SNSProvider handles SNS operations.
type SNSProvider struct {
	resources  store.ResourceStore
	messages   sqsstore.SQSMessageStore
	bus        *events.EventBus
	invoker    FunctionInvoker
	sqsSender  SQSSender
	httpClient *http.Client
}

func New(resources store.ResourceStore, messages sqsstore.SQSMessageStore, bus *events.EventBus) *SNSProvider {
	return &SNSProvider{
		resources:  resources,
		messages:   messages,
		bus:        bus,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// SetLambdaInvoker wires the Lambda invoker for SNS→Lambda protocol delivery.
func (p *SNSProvider) SetLambdaInvoker(inv FunctionInvoker) { p.invoker = inv }

// SetSQSSender wires the SQS sender for SNS→SQS protocol delivery with ARN resolution.
func (p *SNSProvider) SetSQSSender(s SQSSender) { p.sqsSender = s }

func (p *SNSProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Notification.CreateTopic":               p.CreateTopic,
		"Notification.DeleteTopic":               p.DeleteTopic,
		"Notification.GetTopicAttributes":        p.GetTopicAttributes,
		"Notification.SetTopicAttributes":        p.SetTopicAttributes,
		"Notification.ListTopics":                p.ListTopics,
		"Notification.Subscribe":                 p.Subscribe,
		"Notification.Unsubscribe":               p.Unsubscribe,
		"Notification.ConfirmSubscription":       p.ConfirmSubscription,
		"Notification.ListSubscriptions":         p.ListSubscriptions,
		"Notification.ListSubscriptionsByTopic":  p.ListSubscriptionsByTopic,
		"Notification.GetSubscriptionAttributes": p.GetSubscriptionAttributes,
		"Notification.SetSubscriptionAttributes": p.SetSubscriptionAttributes,
		"Notification.Publish":                   p.Publish,
		"Notification.PublishBatch":              p.PublishBatch,
		"Notification.TagResource":               p.TagResource,
		"Notification.UntagResource":             p.UntagResource,
		"Notification.ListTagsForResource":       p.ListTagsForResource,
		// Permissions
		"Notification.AddPermission":    p.AddPermission,
		"Notification.RemovePermission": p.RemovePermission,
		// Platform applications and endpoints
		"Notification.CreatePlatformApplication":          p.CreatePlatformApplication,
		"Notification.GetPlatformApplicationAttributes":   p.GetPlatformApplicationAttributes,
		"Notification.SetPlatformApplicationAttributes":   p.SetPlatformApplicationAttributes,
		"Notification.DeletePlatformApplication":          p.DeletePlatformApplication,
		"Notification.ListPlatformApplications":           p.ListPlatformApplications,
		"Notification.CreatePlatformEndpoint":             p.CreatePlatformEndpoint,
		"Notification.GetEndpointAttributes":              p.GetEndpointAttributes,
		"Notification.SetEndpointAttributes":              p.SetEndpointAttributes,
		"Notification.DeleteEndpoint":                     p.DeleteEndpoint,
		"Notification.ListEndpointsByPlatformApplication": p.ListEndpointsByPlatformApplication,
		// SMS
		"Notification.SetSMSAttributes":             p.SetSMSAttributes,
		"Notification.GetSMSAttributes":             p.GetSMSAttributes,
		"Notification.OptInPhoneNumber":             p.OptInPhoneNumber,
		"Notification.CheckIfPhoneNumberIsOptedOut": p.CheckIfPhoneNumberIsOptedOut,
		"Notification.ListPhoneNumbersOptedOut":     p.ListPhoneNumbersOptedOut,
		"Notification.ListOriginationNumbers":       p.ListOriginationNumbers,
		// Data protection
		"Notification.PutDataProtectionPolicy": p.PutDataProtectionPolicy,
		"Notification.GetDataProtectionPolicy": p.GetDataProtectionPolicy,
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func topicArn(nr *model.NormalizedRequest, name string) string {
	return nr.ResourceID("sns-topic", name)
}

func subArn(nr *model.NormalizedRequest, topicName string) string {
	suffix := fmt.Sprintf("%x", md5.Sum([]byte(topicName+clock.Now().String())))
	return nr.ResourceID("sns-subscription", topicName+":"+suffix)
}

func saveEntry(ctx context.Context, rs store.ResourceStore, account, region, resType, id string, data any) error {
	raw, _ := json.Marshal(data)
	entry := store.ResourceEntry{Type: resType, ID: id, Data: json.RawMessage(raw)}
	return rs.Upsert(ctx, account, region, entry)
}

func loadEntry(ctx context.Context, rs store.ResourceStore, account, region, resType, id string, out any) error {
	e, err := rs.Get(ctx, account, region, resType, id)
	if err != nil {
		return err
	}
	return json.Unmarshal(e.Data, out)
}

// dlqARNFromPolicy parses a RedrivePolicy JSON string and returns the
// deadLetterTargetArn, or an empty string if the policy is absent or malformed.
func dlqARNFromPolicy(rdp string) string {
	if rdp == "" {
		return ""
	}
	var v struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}
	if err := json.Unmarshal([]byte(rdp), &v); err != nil {
		return ""
	}
	return v.DeadLetterTargetArn
}

// ─── Topic data ───────────────────────────────────────────────────────────────

type topicData struct {
	TopicArn   string            `json:"TopicArn"`
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"Tags"`
	CreatedAt  time.Time         `json:"CreatedAt"`
	// DedupCache maps deduplication ID → unix expiry (FIFO topics only).
	DedupCache map[string]int64 `json:"DedupCache,omitempty"`
}

func (p *SNSProvider) CreateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "Name")
	if name == "" {
		return nil, model.NewProviderError("InvalidParameter", "Name is required", 400)
	}
	arn := topicArn(nr, name)

	isFIFO := strings.HasSuffix(name, ".fifo")
	attrs := map[string]string{
		"TopicArn":                arn,
		"DisplayName":             name,
		"SubscriptionsConfirmed":  "0",
		"SubscriptionsPending":    "0",
		"SubscriptionsDeleted":    "0",
		"EffectiveDeliveryPolicy": `{"defaultHealthyRetryPolicy":{"numRetries":3}}`,
	}
	if isFIFO {
		attrs["FifoTopic"] = "true"
		attrs["ContentBasedDeduplication"] = "false"
		// Allow caller to override ContentBasedDeduplication via Attributes
	}
	// Apply caller-provided attributes (e.g., ContentBasedDeduplication)
	if attrsMap, ok := nr.Params["Attributes"].(map[string]any); ok {
		for k, v := range attrsMap {
			attrs[k] = fmt.Sprint(v)
		}
	}

	td := topicData{
		TopicArn:   arn,
		Attributes: attrs,
		Tags:       map[string]string{},
		CreatedAt:  clock.Now(),
	}
	if err := saveEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, td); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"TopicArn": arn}), nil
}

func (p *SNSProvider) DeleteTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, "sns_topics", arn); err != nil {
		if err == store.ErrNotFound {
			return nil, model.NewProviderError("NotFound", "Topic not found", 404)
		}
		return nil, err
	}
	// Delete all subscriptions for this topic.
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "sns_subscriptions", "")
	for _, e := range entries {
		var s subscriptionData
		if json.Unmarshal(e.Data, &s) == nil && s.TopicArn == arn {
			_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, "sns_subscriptions", s.SubscriptionArn)
		}
	}
	return provider.OK(nil), nil
}

func (p *SNSProvider) GetTopicAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Topic not found")
	}
	return provider.OK(map[string]any{"Attributes": td.Attributes}), nil
}

func (p *SNSProvider) SetTopicAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "TopicArn")
	attr := strParam(nr.Params, "AttributeName")
	val := strParam(nr.Params, "AttributeValue")
	var td topicData
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Topic not found")
	}
	td.Attributes[attr] = val
	return provider.OK(nil), saveEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, td)
}

func (p *SNSProvider) ListTopics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "sns_topics", "")
	var topics []map[string]any
	for _, e := range entries {
		var td topicData
		if json.Unmarshal(e.Data, &td) == nil {
			topics = append(topics, map[string]any{"TopicArn": td.TopicArn})
		}
	}
	if topics == nil {
		topics = []map[string]any{}
	}
	return provider.OK(map[string]any{"Topics": topics}), nil
}

// ─── Subscriptions ────────────────────────────────────────────────────────────

type subscriptionData struct {
	SubscriptionArn string            `json:"SubscriptionArn"`
	TopicArn        string            `json:"TopicArn"`
	Protocol        string            `json:"Protocol"`
	Endpoint        string            `json:"Endpoint"`
	Owner           string            `json:"Owner"`
	Attributes      map[string]string `json:"Attributes"`
	Token           string            `json:"Token,omitempty"` // confirmation token for http/https
	Confirmed       bool              `json:"Confirmed"`
	RedrivePolicy   string            `json:"RedrivePolicy,omitempty"` // JSON: {"deadLetterTargetArn":"arn:..."}
}

func (p *SNSProvider) Subscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	protocol := strParam(nr.Params, "Protocol")
	endpoint := strParam(nr.Params, "Endpoint")

	if tArn == "" || protocol == "" {
		return nil, model.NewProviderError("InvalidParameter", "TopicArn and Protocol are required", 400)
	}

	parts := strings.Split(tArn, ":")
	topicName := parts[len(parts)-1]
	sArn := subArn(nr, topicName)

	// Generate confirmation token for http/https; auto-confirm everything else.
	token := ""
	confirmed := true
	if protocol == "http" || protocol == "https" {
		token = fmt.Sprintf("%x", md5.Sum([]byte(sArn+clock.Now().String())))
		confirmed = true // auto-confirm for local dev
	}

	attrs := map[string]string{"SubscriptionArn": sArn}
	if reqAttrs, ok := nr.Params["Attributes"].(map[string]string); ok {
		for k, v := range reqAttrs {
			attrs[k] = v
		}
	}
	sd := subscriptionData{
		SubscriptionArn: sArn,
		TopicArn:        tArn,
		Protocol:        protocol,
		Endpoint:        endpoint,
		Owner:           nr.AccountID,
		Attributes:      attrs,
		Token:           token,
		Confirmed:       confirmed,
		RedrivePolicy:   attrs["RedrivePolicy"],
	}
	if err := saveEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_subscriptions", sArn, sd); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"SubscriptionArn": sArn}), nil
}

func (p *SNSProvider) Unsubscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	_ = p.resources.Delete(ctx, nr.AccountID, nr.Region, "sns_subscriptions", sArn)
	return provider.OK(nil), nil
}

// ConfirmSubscription validates the token generated at Subscribe time.
func (p *SNSProvider) ConfirmSubscription(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	token := strParam(nr.Params, "Token")

	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "sns_subscriptions", "")
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) != nil || sd.TopicArn != tArn {
			continue
		}
		if sd.Token == token || token == "" {
			sd.Confirmed = true
			_ = saveEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_subscriptions", sd.SubscriptionArn, sd)
			return provider.OK(map[string]any{"SubscriptionArn": sd.SubscriptionArn}), nil
		}
	}
	return nil, model.NewProviderError("AuthorizationError", "invalid confirmation token", 403)
}

func (p *SNSProvider) ListSubscriptions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "sns_subscriptions", "")
	return provider.OK(map[string]any{"Subscriptions": subscriptionList(entries)}), nil
}

func (p *SNSProvider) ListSubscriptionsByTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "sns_subscriptions", "")
	var filtered []store.ResourceEntry
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) == nil && sd.TopicArn == tArn {
			filtered = append(filtered, e)
		}
	}
	return provider.OK(map[string]any{"Subscriptions": subscriptionList(filtered)}), nil
}

func (p *SNSProvider) GetSubscriptionAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	var sd subscriptionData
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_subscriptions", sArn, &sd); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Subscription not found")
	}
	attrs := map[string]string{
		"SubscriptionArn": sd.SubscriptionArn,
		"TopicArn":        sd.TopicArn,
		"Protocol":        sd.Protocol,
		"Endpoint":        sd.Endpoint,
		"Owner":           sd.Owner,
	}
	for k, v := range sd.Attributes {
		attrs[k] = v
	}
	return provider.OK(map[string]any{"Attributes": attrs}), nil
}

func (p *SNSProvider) SetSubscriptionAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	sArn := strParam(nr.Params, "SubscriptionArn")
	attr := strParam(nr.Params, "AttributeName")
	val := strParam(nr.Params, "AttributeValue")
	if attr == "FilterPolicy" && val != "" {
		var check map[string]any
		if err := json.Unmarshal([]byte(val), &check); err != nil {
			return nil, model.NewProviderError("InvalidParameter", "Invalid JSON in FilterPolicy: "+err.Error(), http.StatusBadRequest)
		}
	}
	var sd subscriptionData
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_subscriptions", sArn, &sd); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Subscription not found")
	}
	sd.Attributes[attr] = val
	if attr == "RedrivePolicy" {
		sd.RedrivePolicy = val
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_subscriptions", sArn, sd)
}

func subscriptionList(entries []store.ResourceEntry) []map[string]any {
	var subs []map[string]any
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) == nil {
			subs = append(subs, map[string]any{
				"SubscriptionArn": sd.SubscriptionArn,
				"TopicArn":        sd.TopicArn,
				"Protocol":        sd.Protocol,
				"Endpoint":        sd.Endpoint,
				"Owner":           sd.Owner,
			})
		}
	}
	if subs == nil {
		subs = []map[string]any{}
	}
	return subs
}

// ─── Publish ──────────────────────────────────────────────────────────────────

func (p *SNSProvider) Publish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if nr.Origin == model.OriginInternal {
		return p.publishInternal(ctx, nr)
	}
	tArn := strParam(nr.Params, "TopicArn")
	message := strParam(nr.Params, "Message")
	subject := strParam(nr.Params, "Subject")
	messageID := fmt.Sprintf("%x", md5.Sum([]byte(message+clock.Now().String())))

	if tArn == "" {
		return nil, model.NewProviderError("InvalidParameter", "TopicArn is required", 400)
	}

	var msgAttrs map[string]any
	if ma, ok := nr.Params["MessageAttributes"].(map[string]any); ok {
		msgAttrs = ma
	}

	// ── FIFO dedup ────────────────────────────────────────────────────────────
	var td topicData
	if loadErr := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", tArn, &td); loadErr == nil {
		if td.Attributes["FifoTopic"] == "true" {
			dedupID := strParam(nr.Params, "MessageDeduplicationId")
			if dedupID == "" && td.Attributes["ContentBasedDeduplication"] == "true" {
				dedupID = fmt.Sprintf("%x", md5.Sum([]byte(message)))
			}
			if dedupID != "" {
				now := clock.Now().Unix()
				if td.DedupCache == nil {
					td.DedupCache = make(map[string]int64)
				}
				// Prune expired entries.
				for k, exp := range td.DedupCache {
					if exp < now {
						delete(td.DedupCache, k)
					}
				}
				if _, seen := td.DedupCache[dedupID]; seen {
					return provider.OK(map[string]any{"MessageId": messageID}), nil
				}
				td.DedupCache[dedupID] = now + 300 // 5-min window
				_ = saveEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", tArn, td)
			}
		}
	}

	// ── Deliver to each matching subscription ─────────────────────────────────
	entries, _ := p.resources.List(ctx, nr.AccountID, nr.Region, "sns_subscriptions", "")
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) != nil || sd.TopicArn != tArn {
			continue
		}
		// Evaluate filter policy.
		if fp := sd.Attributes["FilterPolicy"]; fp != "" {
			if !matchesFilterPolicy(fp, msgAttrs) {
				continue
			}
		}
		rawDelivery := sd.Attributes["RawMessageDelivery"] == "true"

		// buildBody returns the message body that would be sent to the subscriber,
		// used both for delivery and for DLQ forwarding on failure.
		buildBody := func() string {
			if rawDelivery {
				return message
			}
			envelope := buildSNSEnvelope(tArn, message, subject, messageID, sd.SubscriptionArn, msgAttrs)
			b, _ := json.Marshal(envelope)
			return string(b)
		}

		var deliveryErr error
		switch sd.Protocol {
		case "sqs":
			deliveryErr = p.deliverToSQS(ctx, sd.Endpoint, tArn, messageID, message, subject,
				nr.Region, nr.AccountID, sd.SubscriptionArn, msgAttrs, rawDelivery)
		case "lambda":
			sdCopy := sd
			bgCtx := context.WithoutCancel(ctx)
			go func() {
				if err := p.deliverToLambda(bgCtx, sdCopy.SubscriptionArn, tArn, messageID, message, subject, sdCopy.Endpoint, msgAttrs); err != nil {
					p.sendToDLQ(bgCtx, sdCopy.RedrivePolicy, tArn, buildBody())
				}
			}()
			continue // DLQ is handled inside the goroutine
		case "http", "https":
			sdCopy := sd
			bgCtx := context.WithoutCancel(ctx)
			go func() {
				if err := p.deliverToHTTP(tArn, messageID, message, subject, sdCopy.Endpoint, msgAttrs); err != nil {
					p.sendToDLQ(bgCtx, sdCopy.RedrivePolicy, tArn, buildBody())
				}
			}()
			continue // DLQ is handled inside the goroutine
		case "email", "email-json":
			slog.Info("SNS email delivery (stub)", "endpoint", sd.Endpoint, "messageID", messageID)
			continue
		case "sms":
			slog.Info("SNS SMS delivery (stub)", "endpoint", sd.Endpoint, "messageID", messageID)
			continue
		case "application":
			slog.Info("SNS application delivery (stub)", "endpoint", sd.Endpoint, "messageID", messageID)
			continue
		}

		// For synchronous protocols (sqs), route to DLQ on failure.
		if deliveryErr != nil {
			p.sendToDLQ(ctx, sd.RedrivePolicy, tArn, buildBody())
		}
	}

	return provider.OK(map[string]any{"MessageId": messageID}), nil
}

func (p *SNSProvider) deliverToSQS(ctx context.Context, queueURL, tArn, messageID, message, subject, region, accountID, subARN string, msgAttrs map[string]any, rawDelivery bool) error {
	var bodyStr string
	if rawDelivery {
		bodyStr = message
	} else {
		envelope := buildSNSEnvelope(tArn, message, subject, messageID, subARN, msgAttrs)
		b, _ := json.Marshal(envelope)
		bodyStr = string(b)
	}

	// Prefer sqsSender (resolves ARN → URL correctly) over raw message store.
	if p.sqsSender != nil {
		sqsAttrs := make(map[string]SQSMessageAttribute, len(msgAttrs))
		for k, v := range msgAttrs {
			if m, ok := v.(map[string]any); ok {
				dt, _ := m["DataType"].(string)
				sv, _ := m["StringValue"].(string)
				sqsAttrs[k] = SQSMessageAttribute{DataType: dt, StringValue: sv}
			}
		}
		return p.sqsSender.InternalSend(ctx, queueURL, bodyStr, sqsAttrs, SQSSourceContext{
			SourceArn:        tArn,
			ServicePrincipal: "sns.amazonaws.com",
		})
	}

	if p.messages == nil {
		return nil
	}
	sqsMsgID := fmt.Sprintf("%x-%x-%x", rand.Int31(), rand.Int31(), rand.Int31())
	msg := sqsstore.SQSMessage{
		MessageID: sqsMsgID,
		QueueURL:  queueURL,
		Body:      bodyStr,
		SentAt:    clock.Now(),
	}
	_, _, err := p.messages.Send(ctx, "", "", msg)
	return err
}

func (p *SNSProvider) deliverToLambda(ctx context.Context, subArn, tArn, messageID, message, subject, functionName string, msgAttrs map[string]any) error {
	if p.invoker == nil {
		return nil
	}
	// Extract bare function name from ARN or use as-is.
	fnName := functionName
	if parts := strings.Split(functionName, ":"); len(parts) >= 7 {
		fnName = parts[6]
	}
	record := map[string]any{
		"EventSource":          "aws:sns",
		"EventVersion":         "1.0",
		"EventSubscriptionArn": subArn,
		"Sns": map[string]any{
			"Type":              "Notification",
			"MessageId":         messageID,
			"TopicArn":          tArn,
			"Subject":           subject,
			"Message":           message,
			"Timestamp":         clock.Now().Format(time.RFC3339),
			"SignatureVersion":  "1",
			"Signature":         "EXAMPLE",
			"SigningCertUrl":    "EXAMPLE",
			"UnsubscribeUrl":    "EXAMPLE",
			"MessageAttributes": msgAttrs,
		},
	}
	payload, _ := json.Marshal(map[string]any{"Records": []any{record}})
	_, err := p.invoker.InvokeInternal(ctx, fnName, payload)
	return err
}

func (p *SNSProvider) deliverToHTTP(tArn, messageID, message, subject, endpoint string, msgAttrs map[string]any) error {
	envelope := map[string]any{
		"Type":             "Notification",
		"MessageId":        messageID,
		"TopicArn":         tArn,
		"Subject":          subject,
		"Message":          message,
		"Timestamp":        clock.Now().Format(time.RFC3339),
		"SignatureVersion": "1",
		"Signature":        "EXAMPLE",
		"SigningCertURL":   "EXAMPLE",
	}
	if len(msgAttrs) > 0 {
		envelope["MessageAttributes"] = msgAttrs
	}
	body, _ := json.Marshal(envelope)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("x-amz-sns-message-type", "Notification")
	req.Header.Set("x-amz-sns-topic-arn", tArn)
	req.Header.Set("x-amz-sns-message-id", messageID)
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// sendToDLQ delivers a failed message to the dead-letter queue specified in the
// subscription's RedrivePolicy. It is a best-effort operation: errors are logged
// but not propagated to the caller.
func (p *SNSProvider) sendToDLQ(ctx context.Context, redrivePolicy, topicARN, body string) {
	dlqARN := dlqARNFromPolicy(redrivePolicy)
	if dlqARN == "" || p.sqsSender == nil {
		return
	}
	if err := p.sqsSender.InternalSend(ctx, dlqARN, body, nil, SQSSourceContext{
		SourceArn:        topicARN,
		ServicePrincipal: "sns.amazonaws.com",
	}); err != nil {
		slog.Warn("sns: failed to send message to DLQ", "dlq", dlqARN, "err", err)
	}
}

func (p *SNSProvider) PublishBatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tArn := strParam(nr.Params, "TopicArn")
	entries, _ := nr.Params["PublishBatchRequestEntries"].([]any)
	var successful []map[string]any
	for _, e := range entries {
		m, _ := e.(map[string]any)
		id, _ := m["Id"].(string)
		message, _ := m["Message"].(string)
		msgID := fmt.Sprintf("%x", md5.Sum([]byte(message)))
		fakeNR := &model.NormalizedRequest{
			Params:    map[string]any{"TopicArn": tArn, "Message": message},
			Region:    nr.Region,
			AccountID: nr.AccountID,
		}
		if _, err := p.Publish(ctx, fakeNR); err == nil {
			successful = append(successful, map[string]any{"Id": id, "MessageId": msgID})
		}
	}
	return provider.OK(map[string]any{"Successful": successful, "Failed": []map[string]any{}}), nil
}

// ─── Filter policy ────────────────────────────────────────────────────────────

// matchesFilterPolicy returns true if the message attributes satisfy the filter policy.
// It delegates to the EventBridge pattern engine in SNS mode.
func matchesFilterPolicy(filterPolicyJSON string, msgAttrs map[string]any) bool {
	if filterPolicyJSON == "" {
		return true
	}
	pat, err := pattern.Compile(filterPolicyJSON, pattern.ModeSNS)
	if err != nil {
		return false
	}
	return pat.Match(snsAttrsToEventDoc(msgAttrs))
}

// snsAttrsToEventDoc unwraps the SNS attribute envelope
// {"key":{"DataType":"String","StringValue":"x"}} → {"key":"x"}
// so the pattern engine receives a flat document it can match against.
func snsAttrsToEventDoc(msgAttrs map[string]any) map[string]any {
	doc := make(map[string]any, len(msgAttrs))
	for k, v := range msgAttrs {
		switch m := v.(type) {
		case map[string]any:
			if sv, ok := m["StringValue"].(string); ok {
				doc[k] = sv
			} else if sv, ok := m["Value"].(string); ok {
				doc[k] = sv
			} else {
				doc[k] = v
			}
		default:
			doc[k] = v
		}
	}
	return doc
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (p *SNSProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	if tags, ok := nr.Params["Tags"].([]any); ok {
		for _, t := range tags {
			if tm, ok := t.(map[string]any); ok {
				k, _ := tm["Key"].(string)
				v, _ := tm["Value"].(string)
				td.Tags[k] = v
			}
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, td)
}

func (p *SNSProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	if keys, ok := nr.Params["TagKeys"].([]any); ok {
		for _, k := range keys {
			delete(td.Tags, fmt.Sprintf("%v", k))
		}
	}
	return provider.OK(nil), saveEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, td)
}

func (p *SNSProvider) ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "ResourceArn")
	var td topicData
	if err := loadEntry(ctx, p.resources, nr.AccountID, nr.Region, "sns_topics", arn, &td); err != nil {
		return nil, provider.StoreNotFoundError(err, "NotFound", "Resource not found")
	}
	var tags []map[string]any
	for k, v := range td.Tags {
		tags = append(tags, map[string]any{"Key": k, "Value": v})
	}
	if tags == nil {
		tags = []map[string]any{}
	}
	return provider.OK(map[string]any{"Tags": tags}), nil
}
