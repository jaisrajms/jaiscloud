// Package pubsubui serves the Pub/Sub UI API. Handlers call the Pub/Sub
// provider directly (in-process) rather than over the wire.
package pubsubui

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Pub/Sub UI API requests by calling the Pub/Sub provider.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// topicParam returns the decoded single-segment topic id, or writes 400 and
// reports false. A decoded '/' is rejected: topic ids are one path segment and
// handlers build the resource name as "topics/"+id.
func topicParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, ok := uihelper.Segment(r, "topic")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid topic name", http.StatusBadRequest)
		return "", false
	}
	return id, true
}

// subscriptionParam returns the decoded single-segment subscription id, or
// writes 400 and reports false (see topicParam).
func subscriptionParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, ok := uihelper.Segment(r, "subscription")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid subscription name", http.StatusBadRequest)
		return "", false
	}
	return id, true
}

// ─── mapping helpers ─────────────────────────────────────────────────────────

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func boolAt(m map[string]any, key string) bool {
	b, _ := m[key].(bool)
	return b
}

func intAt(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

func mapAt(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	return v
}

// lastSegment returns the final "/"-separated segment of a full resource name,
// or the input unchanged when it has no slash (e.g. "_deleted-topic_").
func lastSegment(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// stringMapAt extracts a string-valued map, dropping any non-string entries.
func stringMapAt(m map[string]any, key string) map[string]string {
	raw := mapAt(m, key)
	if raw == nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func topicFromMap(m map[string]any) Topic {
	full := str(m, "name")
	return Topic{
		Name:                     lastSegment(full),
		FullName:                 full,
		MessageRetentionDuration: str(m, "messageRetentionDuration"),
		KmsKeyName:               str(m, "kmsKeyName"),
		Labels:                   stringMapAt(m, "labels"),
	}
}

func subscriptionFromMap(m map[string]any) Subscription {
	full := str(m, "name")
	topicFull := str(m, "topic")
	sub := Subscription{
		Name:                      lastSegment(full),
		FullName:                  full,
		Topic:                     lastSegment(topicFull),
		TopicFull:                 topicFull,
		AckDeadlineSeconds:        intAt(m, "ackDeadlineSeconds"),
		MessageRetentionDuration:  str(m, "messageRetentionDuration"),
		EnableExactlyOnceDelivery: boolAt(m, "enableExactlyOnceDelivery"),
		EnableMessageOrdering:     boolAt(m, "enableMessageOrdering"),
		Filter:                    str(m, "filter"),
		State:                     str(m, "state"),
		Detached:                  boolAt(m, "detached"),
		RetryPolicy:               mapAt(m, "retryPolicy"),
		Labels:                    stringMapAt(m, "labels"),
	}
	if pc := mapAt(m, "pushConfig"); pc != nil {
		sub.PushEndpoint = str(pc, "pushEndpoint")
	}
	if ep := mapAt(m, "expirationPolicy"); ep != nil {
		sub.ExpirationTtl = str(ep, "ttl")
	}
	if dp := mapAt(m, "deadLetterPolicy"); dp != nil {
		sub.DeadLetterTopic = lastSegment(str(dp, "deadLetterTopic"))
		sub.MaxDeliveryAttempts = intAt(dp, "maxDeliveryAttempts")
	}
	return sub
}

// pageParams copies pageSize/pageToken query parameters onto the request.
func pageParams(r *http.Request, nr map[string]any) {
	if v := r.URL.Query().Get("pageSize"); v != "" {
		nr["pageSize"] = v
	}
	if v := r.URL.Query().Get("pageToken"); v != "" {
		nr["pageToken"] = v
	}
}

// ─── Topics ──────────────────────────────────────────────────────────────────

// GET /topics
func (h *Handler) ListTopics(w http.ResponseWriter, r *http.Request) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.TopicList", "global", account)
	pageParams(r, nr.Params)

	resp, err := h.provider.TopicList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["topics"])
	topics := make([]Topic, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			topics = append(topics, topicFromMap(m))
		}
	}
	next, _ := resp.Data["nextPageToken"].(string)
	uihelper.WriteJSON(w, ListTopicsResponse{Topics: topics, Total: len(topics), NextPageToken: next})
}

// POST /topics
func (h *Handler) CreateTopic(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.TopicCreate", "global", account)
	nr.Params["name"] = "topics/" + req.Name
	body := map[string]any{}
	if req.MessageRetentionDuration != "" {
		body["messageRetentionDuration"] = req.MessageRetentionDuration
	}
	if req.KmsKeyName != "" {
		body["kmsKeyName"] = req.KmsKeyName
	}
	if len(req.Labels) > 0 {
		body["labels"] = req.Labels
	}
	nr.Params["body"] = body

	resp, err := h.provider.TopicCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, topicFromMap(resp.Data))
}

// GET /topics/{topic}
func (h *Handler) GetTopic(w http.ResponseWriter, r *http.Request) {
	topic, ok := topicParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.TopicGet", "global", account)
	nr.Params["name"] = "topics/" + topic

	resp, err := h.provider.TopicGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, topicFromMap(resp.Data))
}

// DELETE /topics/{topic}
func (h *Handler) DeleteTopic(w http.ResponseWriter, r *http.Request) {
	topic, ok := topicParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.TopicDelete", "global", account)
	nr.Params["name"] = "topics/" + topic

	if _, err := h.provider.TopicDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /topics/{topic}/publish  body: { "messages": [{data, attributes, orderingKey}] }
func (h *Handler) PublishTopic(w http.ResponseWriter, r *http.Request) {
	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if len(req.Messages) == 0 {
		uihelper.UIError(w, "BadRequest", "at least one message is required", http.StatusBadRequest)
		return
	}

	topic, ok := topicParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.TopicPublish", "global", account)
	nr.Params["name"] = "topics/" + topic

	messages := make([]any, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Data == "" {
			uihelper.UIError(w, "BadRequest", "message data is required", http.StatusBadRequest)
			return
		}
		msg := map[string]any{"data": m.Data}
		if len(m.Attributes) > 0 {
			msg["attributes"] = m.Attributes
		}
		if m.OrderingKey != "" {
			msg["orderingKey"] = m.OrderingKey
		}
		messages = append(messages, msg)
	}
	nr.Params["body"] = map[string]any{"messages": messages}

	resp, err := h.provider.TopicPublish(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /topics/{topic}/iam
func (h *Handler) GetTopicIam(w http.ResponseWriter, r *http.Request) {
	topic, ok := topicParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.TopicGetIamPolicy", "global", account)
	nr.Params["name"] = "topics/" + topic

	resp, err := h.provider.TopicGetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PUT /topics/{topic}/iam
func (h *Handler) PutTopicIam(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeBody(w, r)
	if !ok {
		return
	}
	topic, ok := topicParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.TopicSetIamPolicy", "global", account)
	nr.Params["name"] = "topics/" + topic
	nr.Params["body"] = body

	resp, err := h.provider.TopicSetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── Subscriptions ───────────────────────────────────────────────────────────

// GET /subscriptions
func (h *Handler) ListSubscriptions(w http.ResponseWriter, r *http.Request) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.SubscriptionList", "global", account)
	pageParams(r, nr.Params)

	resp, err := h.provider.SubscriptionList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["subscriptions"])
	subs := make([]Subscription, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			subs = append(subs, subscriptionFromMap(m))
		}
	}
	next, _ := resp.Data["nextPageToken"].(string)
	uihelper.WriteJSON(w, ListSubscriptionsResponse{Subscriptions: subs, Total: len(subs), NextPageToken: next})
}

// POST /subscriptions
func (h *Handler) CreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req CreateSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	if req.Topic == "" {
		uihelper.UIError(w, "BadRequest", "topic is required", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.SubscriptionCreate", "global", account)
	nr.Params["name"] = "subscriptions/" + req.Name

	body := map[string]any{"topic": nr.ResourceID("pubsub-topic", req.Topic)}
	if req.AckDeadlineSeconds > 0 {
		// float64 matches a JSON-decoded number; the provider type-asserts it.
		body["ackDeadlineSeconds"] = float64(req.AckDeadlineSeconds)
	}
	if req.MessageRetentionDuration != "" {
		body["messageRetentionDuration"] = req.MessageRetentionDuration
	}
	if req.EnableExactlyOnceDelivery {
		body["enableExactlyOnceDelivery"] = true
	}
	if req.EnableMessageOrdering {
		body["enableMessageOrdering"] = true
	}
	if req.Filter != "" {
		body["filter"] = req.Filter
	}
	if req.PushEndpoint != "" {
		body["pushConfig"] = map[string]any{"pushEndpoint": req.PushEndpoint}
	}
	if req.DeadLetterTopic != "" || req.MaxDeliveryAttempts != 0 {
		dp := map[string]any{}
		if req.DeadLetterTopic != "" {
			dp["deadLetterTopic"] = nr.ResourceID("pubsub-topic", req.DeadLetterTopic)
		}
		if req.MaxDeliveryAttempts != 0 {
			dp["maxDeliveryAttempts"] = req.MaxDeliveryAttempts
		}
		body["deadLetterPolicy"] = dp
	}
	nr.Params["body"] = body

	resp, err := h.provider.SubscriptionCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, subscriptionFromMap(resp.Data))
}

// GET /subscriptions/{subscription}
func (h *Handler) GetSubscription(w http.ResponseWriter, r *http.Request) {
	sub, ok := subscriptionParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.SubscriptionGet", "global", account)
	nr.Params["name"] = "subscriptions/" + sub

	resp, err := h.provider.SubscriptionGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, subscriptionFromMap(resp.Data))
}

// PATCH /subscriptions/{subscription}  body: { subscription: {...}, updateMask? }
func (h *Handler) UpdateSubscription(w http.ResponseWriter, r *http.Request) {
	var req UpdateSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if len(req.Subscription) == 0 {
		uihelper.UIError(w, "BadRequest", "subscription is required", http.StatusBadRequest)
		return
	}
	mask := req.UpdateMask
	if mask == "" {
		mask = deriveUpdateMask(req.Subscription)
	}

	sub, ok := subscriptionParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.SubscriptionUpdate", "global", account)
	nr.Params["name"] = "subscriptions/" + sub
	nr.Params["body"] = map[string]any{"subscription": req.Subscription, "updateMask": mask}

	resp, err := h.provider.SubscriptionUpdate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, subscriptionFromMap(resp.Data))
}

// DELETE /subscriptions/{subscription}
func (h *Handler) DeleteSubscription(w http.ResponseWriter, r *http.Request) {
	sub, ok := subscriptionParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.SubscriptionDelete", "global", account)
	nr.Params["name"] = "subscriptions/" + sub

	if _, err := h.provider.SubscriptionDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /subscriptions/{subscription}/iam
func (h *Handler) GetSubscriptionIam(w http.ResponseWriter, r *http.Request) {
	sub, ok := subscriptionParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.SubscriptionGetIamPolicy", "global", account)
	nr.Params["name"] = "subscriptions/" + sub

	resp, err := h.provider.SubscriptionGetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PUT /subscriptions/{subscription}/iam
func (h *Handler) PutSubscriptionIam(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeBody(w, r)
	if !ok {
		return
	}
	sub, ok := subscriptionParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "pubsub", "PubSub.SubscriptionSetIamPolicy", "global", account)
	nr.Params["name"] = "subscriptions/" + sub
	nr.Params["body"] = body

	resp, err := h.provider.SubscriptionSetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// decodeBody decodes a generic JSON object for a setIamPolicy request.
func decodeBody(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return nil, false
	}
	return body, true
}

// deriveUpdateMask builds a deterministic comma-separated field mask from the
// subscription keys the client sent (camelCase matches the provider's folded
// mask spelling). Retry-policy sub-objects update as a single root path.
func deriveUpdateMask(subscription map[string]any) string {
	keys := make([]string, 0, len(subscription))
	for k := range subscription {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}
