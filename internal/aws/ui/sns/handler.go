package snsui

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves SNS UI API requests by calling the SNS provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /topics
func (h *Handler) ListTopics(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	pageToken := r.URL.Query().Get("nextToken")

	nr := uihelper.NR(r.Context(), h.cfg, "sns", "ListTopics", region, account)
	if pageToken != "" {
		nr.Params["NextToken"] = pageToken
	}

	resp, err := h.provider.ListTopics(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawTopics, _ := resp.Data["Topics"].([]map[string]any)
	nextTok, _ := resp.Data["NextToken"].(string)

	topics := make([]Topic, 0, len(rawTopics))
	for _, t := range rawTopics {
		arn, _ := t["TopicArn"].(string)
		topic := h.fetchTopicDetail(r, arn, region, account)
		topics = append(topics, topic)
	}

	uihelper.WriteJSON(w, ListTopicsResponse{
		Items:     topics,
		NextToken: nextTok,
		Total:     len(topics),
	})
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

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	topicName := req.Name
	if req.FIFO && !strings.HasSuffix(topicName, ".fifo") {
		topicName += ".fifo"
	}

	nr := uihelper.NR(r.Context(), h.cfg, "sns", "CreateTopic", region, account)
	nr.Params["Name"] = topicName
	if req.Attributes != nil {
		nr.Params["Attributes"] = req.Attributes
	}
	if req.Tags != nil {
		nr.Params["Tags"] = req.Tags
	}
	if req.FIFO {
		attrs, _ := nr.Params["Attributes"].(map[string]string)
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs["FifoTopic"] = "true"
		nr.Params["Attributes"] = attrs
	}

	resp, err := h.provider.CreateTopic(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	topicARN, _ := resp.Data["TopicArn"].(string)
	topic := h.fetchTopicDetail(r, topicARN, region, account)
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, topic)
}

// DELETE /topics?arn=<urlEncoded>
func (h *Handler) DeleteTopic(w http.ResponseWriter, r *http.Request) {
	topicARN, err := url.QueryUnescape(r.URL.Query().Get("arn"))
	if err != nil || topicARN == "" {
		uihelper.UIError(w, "BadRequest", "arn query param is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "sns", "DeleteTopic", region, account)
	nr.Params["TopicArn"] = topicARN

	if _, err := h.provider.DeleteTopic(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /topics/detail?arn=<urlEncoded>
func (h *Handler) GetTopic(w http.ResponseWriter, r *http.Request) {
	topicARN, err := url.QueryUnescape(r.URL.Query().Get("arn"))
	if err != nil || topicARN == "" {
		uihelper.UIError(w, "BadRequest", "arn query param is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	topic := h.fetchTopicDetail(r, topicARN, region, account)
	uihelper.WriteJSON(w, topic)
}

// GET /topics/subscriptions?arn=<urlEncoded>
func (h *Handler) ListSubscriptionsByTopic(w http.ResponseWriter, r *http.Request) {
	topicARN, err := url.QueryUnescape(r.URL.Query().Get("arn"))
	if err != nil || topicARN == "" {
		uihelper.UIError(w, "BadRequest", "arn query param is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "sns", "ListSubscriptionsByTopic", region, account)
	nr.Params["TopicArn"] = topicARN

	resp, err := h.provider.ListSubscriptionsByTopic(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawSubs, _ := resp.Data["Subscriptions"].([]map[string]any)
	nextTok, _ := resp.Data["NextToken"].(string)

	subs := parseSubscriptions(rawSubs)
	uihelper.WriteJSON(w, ListSubscriptionsResponse{Items: subs, NextToken: nextTok})
}

// POST /topics/subscribe?arn=<urlEncoded>
func (h *Handler) Subscribe(w http.ResponseWriter, r *http.Request) {
	topicARN, err := url.QueryUnescape(r.URL.Query().Get("arn"))
	if err != nil || topicARN == "" {
		uihelper.UIError(w, "BadRequest", "arn query param is required", http.StatusBadRequest)
		return
	}

	var req SubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Protocol == "" {
		uihelper.UIError(w, "BadRequest", "protocol is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "sns", "Subscribe", region, account)
	nr.Params["TopicArn"] = topicARN
	nr.Params["Protocol"] = req.Protocol
	nr.Params["Endpoint"] = req.Endpoint

	resp, err := h.provider.Subscribe(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /topics/subscriptions/{subArn}
func (h *Handler) Unsubscribe(w http.ResponseWriter, r *http.Request) {
	subARN, _ := url.QueryUnescape(chi.URLParam(r, "subArn"))
	if subARN == "" {
		uihelper.UIError(w, "BadRequest", "subscription ARN is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "sns", "Unsubscribe", region, account)
	nr.Params["SubscriptionArn"] = subARN

	if _, err := h.provider.Unsubscribe(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /topics/publish?arn=<urlEncoded>
func (h *Handler) Publish(w http.ResponseWriter, r *http.Request) {
	topicARN, err := url.QueryUnescape(r.URL.Query().Get("arn"))
	if err != nil || topicARN == "" {
		uihelper.UIError(w, "BadRequest", "arn query param is required", http.StatusBadRequest)
		return
	}

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Message == "" {
		uihelper.UIError(w, "BadRequest", "message is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "sns", "Publish", region, account)
	nr.Params["TopicArn"] = topicARN
	nr.Params["Message"] = req.Message
	if req.Subject != "" {
		nr.Params["Subject"] = req.Subject
	}

	resp, err := h.provider.Publish(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// fetchTopicDetail calls GetTopicAttributes and assembles a Topic struct.
func (h *Handler) fetchTopicDetail(r *http.Request, topicARN, region, account string) Topic {
	nr := uihelper.NR(r.Context(), h.cfg, "sns", "GetTopicAttributes", region, account)
	nr.Params["TopicArn"] = topicARN

	resp, err := h.provider.GetTopicAttributes(r.Context(), nr)
	if err != nil {
		return Topic{ARN: topicARN, Name: topicNameFromARN(topicARN)}
	}

	attrs, _ := resp.Data["Attributes"].(map[string]string)

	name := topicNameFromARN(topicARN)
	topicType := "Standard"
	if attrs["FifoTopic"] == "true" {
		topicType = "FIFO"
	}

	subCount := 0
	if sc, ok := attrs["SubscriptionsConfirmed"]; ok {
		for _, c := range sc {
			if c >= '0' && c <= '9' {
				subCount = subCount*10 + int(c-'0')
			}
		}
	}

	return Topic{
		ARN:               topicARN,
		Name:              name,
		DisplayName:       attrs["DisplayName"],
		SubscriptionCount: subCount,
		Type:              topicType,
		Attributes:        attrs,
	}
}

func topicNameFromARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return arn
}

func parseSubscriptions(raw []map[string]any) []Subscription {
	subs := make([]Subscription, 0, len(raw))
	for _, s := range raw {
		sub := Subscription{
			SubscriptionARN: strAny(s, "SubscriptionArn"),
			TopicARN:        strAny(s, "TopicArn"),
			Protocol:        strAny(s, "Protocol"),
			Endpoint:        strAny(s, "Endpoint"),
			Owner:           strAny(s, "Owner"),
		}
		subs = append(subs, sub)
	}
	return subs
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
