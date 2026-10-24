package tasksui

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/policy"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/model"
)

// Handler serves Cloud Tasks UI API requests by calling the Cloud Tasks core.
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

// ─── mapping helpers ─────────────────────────────────────────────────────────

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return d.String()
}

func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// renderQueue flattens a stored queue into the console row.
func renderQueue(q tasksstore.Queue) Queue {
	out := Queue{
		Name:      q.Name,
		Location:  q.Location,
		State:     string(q.State),
		PurgeTime: formatTime(q.PurgeTime),
	}
	if q.RateLimits != nil {
		out.MaxDispatchesPerSecond = q.RateLimits.MaxDispatchesPerSecond
		out.MaxBurstSize = q.RateLimits.MaxBurstSize
		out.MaxConcurrentDispatches = q.RateLimits.MaxConcurrentDispatches
	}
	if q.RetryConfig != nil {
		out.MaxAttempts = q.RetryConfig.MaxAttempts
		out.MaxDoublings = q.RetryConfig.MaxDoublings
		out.MaxRetryDuration = formatDuration(q.RetryConfig.MaxRetryDuration)
		out.MinBackoff = formatDuration(q.RetryConfig.MinBackoff)
		out.MaxBackoff = formatDuration(q.RetryConfig.MaxBackoff)
	}
	return out
}

// queueFromInput builds a store queue from the form body.
func queueFromInput(in QueueInput) (tasksstore.Queue, error) {
	q := tasksstore.Queue{
		Name: in.Name,
		RateLimits: &tasksstore.RateLimits{
			MaxDispatchesPerSecond:  in.MaxDispatchesPerSecond,
			MaxBurstSize:            in.MaxBurstSize,
			MaxConcurrentDispatches: in.MaxConcurrentDispatches,
		},
		RetryConfig: &tasksstore.RetryConfig{
			MaxAttempts:  in.MaxAttempts,
			MaxDoublings: in.MaxDoublings,
		},
	}
	var err error
	if q.RetryConfig.MinBackoff, err = parseDuration(in.MinBackoff); err != nil {
		return tasksstore.Queue{}, invalidArgument("invalid minBackoff")
	}
	if q.RetryConfig.MaxBackoff, err = parseDuration(in.MaxBackoff); err != nil {
		return tasksstore.Queue{}, invalidArgument("invalid maxBackoff")
	}
	if q.RetryConfig.MaxRetryDuration, err = parseDuration(in.MaxRetryDuration); err != nil {
		return tasksstore.Queue{}, invalidArgument("invalid maxRetryDuration")
	}
	return q, nil
}

// renderTask flattens a stored task into the console row.
func renderTask(t tasksstore.Task) Task {
	out := Task{
		Name:             t.Name,
		Location:         t.Location,
		Queue:            t.Queue,
		Target:           string(t.Target),
		ScheduleTime:     formatTime(t.ScheduleTime),
		CreateTime:       formatTime(t.CreateTime),
		DispatchDeadline: formatDuration(t.DispatchDeadline),
		DispatchCount:    t.DispatchCount,
		ResponseCount:    t.ResponseCount,
	}
	switch {
	case t.HTTP != nil:
		out.HTTPURL = t.HTTP.URL
		out.HTTPMethod = t.HTTP.HTTPMethod
		out.HTTPHeaders = t.HTTP.Headers
		out.HTTPBody = string(t.HTTP.Body)
	case t.AppEngine != nil:
		out.AppEngineURI = t.AppEngine.RelativeURI
		out.AppEngineMethod = t.AppEngine.HTTPMethod
	}
	if t.LastAttempt != nil && t.LastAttempt.ResponseStatus != nil {
		out.LastAttemptStatus = &TaskStatus{
			Code:    t.LastAttempt.ResponseStatus.Code,
			Message: t.LastAttempt.ResponseStatus.Message,
		}
	}
	return out
}

// taskFromInput builds a store task from the form body.
func taskFromInput(in TaskInput) (tasksstore.Task, error) {
	t := tasksstore.Task{Name: in.Name}
	switch in.Target {
	case "", string(tasksstore.TargetHTTP):
		t.Target = tasksstore.TargetHTTP
		t.HTTP = &tasksstore.HttpRequest{
			URL:        in.HTTPURL,
			HTTPMethod: in.HTTPMethod,
			Headers:    in.HTTPHeaders,
			Body:       []byte(in.HTTPBody),
		}
	case string(tasksstore.TargetAppEngine):
		t.Target = tasksstore.TargetAppEngine
		t.AppEngine = &tasksstore.AppEngineHttpRequest{
			RelativeURI: in.AppEngineURI,
			HTTPMethod:  in.AppEngineMethod,
		}
	default:
		return tasksstore.Task{}, invalidArgument("unknown target " + in.Target)
	}
	if in.ScheduleTime != "" {
		ts, err := time.Parse(time.RFC3339, in.ScheduleTime)
		if err != nil {
			return tasksstore.Task{}, invalidArgument("invalid scheduleTime")
		}
		t.ScheduleTime = ts
	}
	if in.DispatchDeadline != "" {
		d, err := time.ParseDuration(in.DispatchDeadline)
		if err != nil {
			return tasksstore.Task{}, invalidArgument("invalid dispatchDeadline")
		}
		t.DispatchDeadline = d
	}
	return t, nil
}

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, http.StatusBadRequest)
}

func decodeInput[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var in T
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		uihelper.UIError(w, "InvalidArgument", "invalid JSON body", http.StatusBadRequest)
		return in, false
	}
	return in, true
}

// ─── Queues ──────────────────────────────────────────────────────────────────

// GET /queues
func (h *Handler) ListQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := h.provider.ListQueuesByProject(r.Context(), h.account(r))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Queue, 0, len(queues))
	for _, q := range queues {
		out = append(out, renderQueue(q))
	}
	uihelper.WriteJSON(w, ListQueuesResponse{Queues: out, Total: len(out)})
}

// POST /queues
func (h *Handler) CreateQueue(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput[QueueInput](w, r)
	if !ok {
		return
	}
	if in.Location == "" {
		uihelper.UIError(w, "InvalidArgument", "location is required", http.StatusBadRequest)
		return
	}
	q, err := queueFromInput(in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	created, err := h.provider.CreateQueue(r.Context(), h.account(r), in.Location, q)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderQueue(created))
}

// GET /queues/{location}/{queue}
func (h *Handler) GetQueue(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetQueue(w, r)
	if !ok {
		return
	}
	q, err := h.provider.GetQueue(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderQueue(q))
}

// PUT /queues/{location}/{queue}
func (h *Handler) UpdateQueue(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetQueue(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[QueueInput](w, r)
	if !ok {
		return
	}
	// The path carries the identity; the form's name/location are ignored. Send
	// a selective mask so fields the form does not manage
	// (appEngineRoutingOverride, stackdriverLoggingConfig) are preserved rather
	// than cleared by a full replace.
	in.Name = name
	q, err := queueFromInput(in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	updated, err := h.provider.UpdateQueue(r.Context(), h.account(r), location, name, q, []string{"rateLimits", "retryConfig"})
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderQueue(updated))
}

// DELETE /queues/{location}/{queue}
func (h *Handler) DeleteQueue(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetQueue(w, r)
	if !ok {
		return
	}
	if err := h.provider.DeleteQueue(r.Context(), h.account(r), location, name); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /queues/{location}/{queue}/pause
func (h *Handler) PauseQueue(w http.ResponseWriter, r *http.Request) {
	h.mutateQueue(w, r, h.provider.PauseQueue)
}

// POST /queues/{location}/{queue}/resume
func (h *Handler) ResumeQueue(w http.ResponseWriter, r *http.Request) {
	h.mutateQueue(w, r, h.provider.ResumeQueue)
}

// POST /queues/{location}/{queue}/purge
func (h *Handler) PurgeQueue(w http.ResponseWriter, r *http.Request) {
	h.mutateQueue(w, r, h.provider.PurgeQueue)
}

// mutateQueue runs a state-changing queue action and renders the resulting queue.
func (h *Handler) mutateQueue(w http.ResponseWriter, r *http.Request, fn func(context.Context, string, string, string) (tasksstore.Queue, error)) {
	location, name, ok := h.targetQueue(w, r)
	if !ok {
		return
	}
	q, err := fn(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderQueue(q))
}

// GET /queues/{location}/{queue}/iam
func (h *Handler) GetQueueIam(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetQueue(w, r)
	if !ok {
		return
	}
	pol, err := h.provider.QueueGetIamPolicy(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, policy.ToMap(pol))
}

// PUT /queues/{location}/{queue}/iam
func (h *Handler) SetQueueIam(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetQueue(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[uihelper.IamPolicy](w, r)
	if !ok {
		return
	}
	pol, err := h.provider.QueueSetIamPolicy(r.Context(), h.account(r), location, name, uihelper.IamPolicyBody(in))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, policy.ToMap(pol))
}

// ─── Tasks ───────────────────────────────────────────────────────────────────

// GET /queues/{location}/{queue}/tasks
func (h *Handler) ListTasks(w http.ResponseWriter, r *http.Request) {
	location, queue, ok := h.targetQueue(w, r)
	if !ok {
		return
	}
	tasks, err := h.provider.ListTasks(r.Context(), h.account(r), location, queue)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Task, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, renderTask(t))
	}
	uihelper.WriteJSON(w, ListTasksResponse{Tasks: out, Total: len(out)})
}

// POST /queues/{location}/{queue}/tasks
func (h *Handler) CreateTask(w http.ResponseWriter, r *http.Request) {
	location, queue, ok := h.targetQueue(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[TaskInput](w, r)
	if !ok {
		return
	}
	t, err := taskFromInput(in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	created, err := h.provider.CreateTask(r.Context(), h.account(r), location, queue, t)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderTask(created))
}

// GET /queues/{location}/{queue}/tasks/{task}
func (h *Handler) GetTask(w http.ResponseWriter, r *http.Request) {
	location, queue, name, ok := h.targetTask(w, r)
	if !ok {
		return
	}
	t, err := h.provider.GetTask(r.Context(), h.account(r), location, queue, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderTask(t))
}

// DELETE /queues/{location}/{queue}/tasks/{task}
func (h *Handler) DeleteTask(w http.ResponseWriter, r *http.Request) {
	location, queue, name, ok := h.targetTask(w, r)
	if !ok {
		return
	}
	if err := h.provider.DeleteTask(r.Context(), h.account(r), location, queue, name); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /queues/{location}/{queue}/tasks/{task}/run
func (h *Handler) RunTask(w http.ResponseWriter, r *http.Request) {
	location, queue, name, ok := h.targetTask(w, r)
	if !ok {
		return
	}
	t, err := h.provider.RunTask(r.Context(), h.account(r), location, queue, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderTask(t))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// targetQueue reads and validates the {location}/{queue} path parameters,
// writing a 400 and returning false when either is missing or malformed.
func (h *Handler) targetQueue(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	location, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return "", "", false
	}
	queue, ok := uihelper.Segment(r, "queue")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid queue", http.StatusBadRequest)
		return "", "", false
	}
	return location, queue, true
}

// targetTask reads and validates the {location}/{queue}/{task} path parameters.
func (h *Handler) targetTask(w http.ResponseWriter, r *http.Request) (string, string, string, bool) {
	location, queue, ok := h.targetQueue(w, r)
	if !ok {
		return "", "", "", false
	}
	name, ok := uihelper.Segment(r, "task")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid task", http.StatusBadRequest)
		return "", "", "", false
	}
	return location, queue, name, true
}
