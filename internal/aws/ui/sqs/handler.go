package sqsui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves SQS UI API requests by calling the SQS provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /queues?pageSize=50&nextToken=...
func (h *Handler) ListQueues(w http.ResponseWriter, r *http.Request) {
	pageSize := uihelper.PageSizeFrom(r, 50, 1000)
	pageToken := uihelper.PageTokenFrom(r)
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "ListQueues", region, account)
	nr.Params["MaxResults"] = pageSize
	if pageToken != "" {
		nr.Params["NextToken"] = pageToken
	}

	resp, err := h.provider.ListQueues(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	urls, _ := resp.Data["QueueUrls"].([]string)
	nextTok, _ := resp.Data["NextToken"].(string)

	queues := make([]Queue, 0, len(urls))
	for _, qURL := range urls {
		q := h.fetchQueueDetail(r, qURL, region, account)
		queues = append(queues, q)
	}

	total := int64(len(queues))
	uihelper.WriteJSON(w, ListQueuesResponse{
		Items:     queues,
		NextToken: nextTok,
		Total:     &total,
	})
}

// POST /queues
func (h *Handler) CreateQueue(w http.ResponseWriter, r *http.Request) {
	var req CreateQueueRequest
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
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "CreateQueue", region, account)
	queueName := req.Name
	if req.Type == "FIFO" && !strings.HasSuffix(queueName, ".fifo") {
		queueName += ".fifo"
	}
	nr.Params["QueueName"] = queueName
	if req.VisibilityTimeout != nil {
		nr.Params["Attributes"] = map[string]string{"VisibilityTimeout": strconv.Itoa(*req.VisibilityTimeout)}
	}
	if req.RetentionPeriod != nil {
		attrs, _ := nr.Params["Attributes"].(map[string]string)
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs["MessageRetentionPeriod"] = strconv.Itoa(*req.RetentionPeriod)
		nr.Params["Attributes"] = attrs
	}
	if req.Type == "FIFO" {
		attrs, _ := nr.Params["Attributes"].(map[string]string)
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs["FifoQueue"] = "true"
		nr.Params["Attributes"] = attrs
	}
	if req.Tags != nil {
		nr.Params["Tags"] = req.Tags
	}

	resp, err := h.provider.CreateQueue(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	queueURL, _ := resp.Data["QueueUrl"].(string)
	q := h.fetchQueueDetail(r, queueURL, region, account)
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, q)
}

// DELETE /queues?url=<urlEncoded>
func (h *Handler) DeleteQueue(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "DeleteQueue", region, account)
	nr.Params["QueueUrl"] = queueURL

	if _, err := h.provider.DeleteQueue(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /queues/detail?url=<urlEncoded>
func (h *Handler) GetQueue(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	q := h.fetchQueueDetail(r, queueURL, region, account)
	uihelper.WriteJSON(w, q)
}

// POST /queues/purge?url=<urlEncoded>
func (h *Handler) PurgeQueue(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "PurgeQueue", region, account)
	nr.Params["QueueUrl"] = queueURL

	if _, err := h.provider.PurgeQueue(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /queues/messages?url=<urlEncoded>  body: SendMessageRequest
func (h *Handler) SendMessage(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}

	var req SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Body == "" {
		uihelper.UIError(w, "BadRequest", "body is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "SendMessage", region, account)
	nr.Params["QueueUrl"] = queueURL
	nr.Params["MessageBody"] = req.Body
	if req.DelaySeconds != nil {
		nr.Params["DelaySeconds"] = *req.DelaySeconds
	}
	if req.MessageAttributes != nil {
		nr.Params["MessageAttributes"] = req.MessageAttributes
	}
	if req.MessageGroupId != "" {
		nr.Params["MessageGroupId"] = req.MessageGroupId
	}
	if req.MessageDeduplicationId != "" {
		nr.Params["MessageDeduplicationId"] = req.MessageDeduplicationId
	}

	resp, err := h.provider.SendMessage(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /queues/messages?url=<urlEncoded>&maxMessages=10&waitTimeSeconds=0
func (h *Handler) ReceiveMessages(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}

	maxMsgs := 10
	if s := r.URL.Query().Get("maxMessages"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 1 && n <= 10 {
			maxMsgs = n
		}
	}
	waitSecs := 0
	if s := r.URL.Query().Get("waitTimeSeconds"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 0 {
			waitSecs = n
		}
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "ReceiveMessage", region, account)
	nr.Params["QueueUrl"] = queueURL
	nr.Params["MaxNumberOfMessages"] = maxMsgs
	nr.Params["WaitTimeSeconds"] = waitSecs
	nr.Params["AttributeNames"] = []string{"All"}
	nr.Params["MessageAttributeNames"] = []string{"All"}

	resp, err := h.provider.ReceiveMessage(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawMsgs, _ := resp.Data["Messages"].([]map[string]any)
	messages := make([]Message, 0, len(rawMsgs))
	for _, m := range rawMsgs {
		msg := Message{
			MessageId:     strVal(m, "MessageId"),
			Body:          strVal(m, "Body"),
			ReceiptHandle: strVal(m, "ReceiptHandle"),
		}
		if attrs, ok := m["Attributes"].(map[string]any); ok {
			msg.Attributes = attrs
			msg.SentAt = strAny(attrs, "SentTimestamp")
		}
		messages = append(messages, msg)
	}
	uihelper.WriteJSON(w, ReceiveMessagesResponse{Messages: messages})
}

// DELETE /queues/messages/receipt?url=<urlEncoded>&receiptHandle=<encoded>
func (h *Handler) DeleteMessage(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}
	receiptHandle, err := url.QueryUnescape(r.URL.Query().Get("receiptHandle"))
	if err != nil || receiptHandle == "" {
		uihelper.UIError(w, "BadRequest", "receiptHandle query param is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "DeleteMessage", region, account)
	nr.Params["QueueUrl"] = queueURL
	nr.Params["ReceiptHandle"] = receiptHandle

	if _, err := h.provider.DeleteMessage(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /queues/dlq-sources?url=<urlEncoded>
func (h *Handler) ListDLQSources(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "ListDeadLetterSourceQueues", region, account)
	nr.Params["QueueUrl"] = queueURL

	resp, err := h.provider.ListDeadLetterSourceQueues(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	urls, _ := resp.Data["queueUrls"].([]string)
	queues := make([]Queue, 0, len(urls))
	for _, u := range urls {
		q := h.fetchQueueDetail(r, u, region, account)
		queues = append(queues, q)
	}
	uihelper.WriteJSON(w, DLQSourceQueuesResponse{Items: queues})
}

// GET /queues/messages/peek?url=<urlEncoded>&limit=N&offset=N
func (h *Handler) PeekMessages(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}
	limit := 50
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err2 := strconv.Atoi(s); err2 == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	offset := 0
	if s := r.URL.Query().Get("offset"); s != "" {
		if n, err2 := strconv.Atoi(s); err2 == nil && n >= 0 {
			offset = n
		}
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "PeekMessages", region, account)
	nr.Params["QueueUrl"] = queueURL
	nr.Params["Limit"] = limit
	nr.Params["Offset"] = offset

	resp, err := h.provider.PeekMessages(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawMsgs, _ := resp.Data["messages"].([]map[string]any)
	msgs := make([]PeekedMessage, 0, len(rawMsgs))
	for _, m := range rawMsgs {
		pm := PeekedMessage{
			MessageId: strVal(m, "messageId"),
			Body:      strVal(m, "body"),
			Status:    strVal(m, "status"),
			GroupId:   strVal(m, "groupId"),
		}
		if rc, ok := m["receiveCount"].(int); ok {
			pm.ReceiveCount = rc
		}
		pm.SentAt = fmt.Sprintf("%v", m["sentAt"])
		msgs = append(msgs, pm)
	}
	total, _ := resp.Data["total"].(int)
	uihelper.WriteJSON(w, PeekMessagesResponse{Messages: msgs, Total: total, Offset: offset, Limit: limit})
}

// GET /queues/tags?url=<urlEncoded>
func (h *Handler) GetTags(w http.ResponseWriter, r *http.Request) {
	queueURL, err := url.QueryUnescape(r.URL.Query().Get("url"))
	if err != nil || queueURL == "" {
		uihelper.UIError(w, "BadRequest", "url query param is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "ListQueueTags", region, account)
	nr.Params["QueueUrl"] = queueURL

	resp, err := h.provider.ListQueueTags(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	// Provider returns {"Tags": map[string]any{...}} — unwrap to just the tags map.
	raw, _ := resp.Data["Tags"].(map[string]any)
	result := make(map[string]string, len(raw))
	for k, v := range raw {
		result[k] = fmt.Sprintf("%v", v)
	}
	uihelper.WriteJSON(w, result)
}

// fetchQueueDetail calls GetQueueAttributes and assembles a Queue struct.
// This is the N+1 pattern: acceptable at developer-tool scale (<200 queues).
func (h *Handler) fetchQueueDetail(r *http.Request, queueURL, region, account string) Queue {
	nr := uihelper.NR(r.Context(), h.cfg, "sqs", "GetQueueAttributes", region, account)
	nr.Params["QueueUrl"] = queueURL
	nr.Params["AttributeNames"] = []string{"All"}

	resp, err := h.provider.GetQueueAttributes(r.Context(), nr)
	if err != nil {
		return Queue{URL: queueURL}
	}

	attrs, _ := resp.Data["Attributes"].(map[string]string)

	queueType := "Standard"
	if attrs["FifoQueue"] == "true" {
		queueType = "FIFO"
	}

	name := queueNameFromURL(queueURL)

	q := Queue{
		Name:              name,
		URL:               queueURL,
		ARN:               attrs["QueueArn"],
		Type:              queueType,
		MessagesAvailable: parseInt64(attrs["ApproximateNumberOfMessages"]),
		MessagesInFlight:  parseInt64(attrs["ApproximateNumberOfMessagesNotVisible"]),
		VisibilityTimeout: parseInt64(attrs["VisibilityTimeout"]),
		RetentionPeriod:   parseInt64(attrs["MessageRetentionPeriod"]),
		MaxMessageSize:    parseInt64(attrs["MaximumMessageSize"]),
		CreatedAt:         attrs["CreatedTimestamp"],
	}

	if rp := attrs["RedrivePolicy"]; rp != "" {
		var rdp map[string]any
		if json.Unmarshal([]byte(rp), &rdp) == nil {
			if arn, ok := rdp["deadLetterTargetArn"].(string); ok {
				q.DLQArn = arn
			}
			if maxRec, ok := rdp["maxReceiveCount"]; ok {
				switch v := maxRec.(type) {
				case float64:
					q.DLQMaxReceive = int64(v)
				case string:
					q.DLQMaxReceive = parseInt64(v)
				}
			}
		}
	}

	return q
}

// helpers

func queueNameFromURL(u string) string {
	parsed, err := url.Parse(u)
	if err != nil {
		return u
	}
	parts := parsed.Path
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] == '/' {
			return parts[i+1:]
		}
	}
	return parts
}

func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func strVal(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func strAny(m map[string]any, key string) string {
	switch v := m[key].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return ""
}
