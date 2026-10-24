package schedulerui

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"jaiscloud/internal/config"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/model"
)

// Handler serves Cloud Scheduler UI API requests by calling the Cloud Scheduler
// core.
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

// render flattens a stored job into the console row.
func render(j schedstore.Job) Job {
	out := Job{
		Name:            j.Name,
		Location:        j.Location,
		State:           string(j.State),
		Description:     j.Description,
		Schedule:        j.Schedule,
		TimeZone:        j.TimeZone,
		ScheduleTime:    formatTime(j.ScheduleTime),
		LastAttemptTime: formatTime(j.LastAttemptTime),
		UserUpdateTime:  formatTime(j.UserUpdateTime),
	}
	if j.RetryConfig != nil {
		out.RetryCount = j.RetryConfig.RetryCount
	}
	if j.AttemptDeadline > 0 {
		out.AttemptDeadline = j.AttemptDeadline.String()
	}
	if j.Status != nil {
		out.LastStatus = &JobStatus{Code: j.Status.Code, Message: j.Status.Message}
	}
	switch {
	case j.HTTP != nil:
		out.Target = string(schedstore.TargetHTTP)
		out.HTTPURI = j.HTTP.URI
		out.HTTPMethod = j.HTTP.HTTPMethod
		out.HTTPBody = string(j.HTTP.Body)
		out.HTTPHeaders = j.HTTP.Headers
	case j.PubSub != nil:
		out.Target = string(schedstore.TargetPubSub)
		out.PubSubTopic = j.PubSub.TopicName
		out.PubSubData = string(j.PubSub.Data)
	case j.AppEngine != nil:
		out.Target = string(schedstore.TargetAppEngine)
		out.AppEngineURI = j.AppEngine.RelativeURI
		out.AppEngineMethod = j.AppEngine.HTTPMethod
	default:
		out.Target = string(j.Target)
	}
	return out
}

// jobFromInput builds a store job from the form body.
func jobFromInput(in JobInput) (schedstore.Job, error) {
	j := schedstore.Job{
		Name:        in.Name,
		Schedule:    in.Schedule,
		TimeZone:    in.TimeZone,
		Description: in.Description,
	}
	switch in.Target {
	case "", string(schedstore.TargetHTTP):
		j.Target = schedstore.TargetHTTP
		j.HTTP = &schedstore.HttpTarget{
			URI:        in.HTTPURI,
			HTTPMethod: in.HTTPMethod,
			Headers:    in.HTTPHeaders,
			Body:       []byte(in.HTTPBody),
		}
	case string(schedstore.TargetPubSub):
		j.Target = schedstore.TargetPubSub
		j.PubSub = &schedstore.PubsubTarget{TopicName: in.PubSubTopic, Data: []byte(in.PubSubData)}
	case string(schedstore.TargetAppEngine):
		j.Target = schedstore.TargetAppEngine
		j.AppEngine = &schedstore.AppEngineTarget{RelativeURI: in.AppEngineURI, HTTPMethod: in.AppEngineMethod}
	default:
		return schedstore.Job{}, model.NewProviderError("InvalidArgument", "unknown target "+in.Target, http.StatusBadRequest)
	}
	if in.RetryCount > 0 {
		j.RetryConfig = &schedstore.RetryConfig{RetryCount: in.RetryCount}
	}
	if in.AttemptDeadline != "" {
		d, err := time.ParseDuration(in.AttemptDeadline)
		if err != nil {
			return schedstore.Job{}, model.NewProviderError("InvalidArgument", "invalid attemptDeadline", http.StatusBadRequest)
		}
		j.AttemptDeadline = d
	}
	return j, nil
}

func decodeInput(w http.ResponseWriter, r *http.Request) (JobInput, bool) {
	var in JobInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		uihelper.UIError(w, "InvalidArgument", "invalid JSON body", http.StatusBadRequest)
		return JobInput{}, false
	}
	return in, true
}

// ─── Jobs ────────────────────────────────────────────────────────────────────

// GET /jobs
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.provider.ListJobsByProject(r.Context(), h.account(r))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, render(j))
	}
	uihelper.WriteJSON(w, ListJobsResponse{Jobs: out, Total: len(out)})
}

// POST /jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput(w, r)
	if !ok {
		return
	}
	if in.Location == "" {
		uihelper.UIError(w, "InvalidArgument", "location is required", http.StatusBadRequest)
		return
	}
	job, err := jobFromInput(in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	created, err := h.provider.CreateJob(r.Context(), h.account(r), in.Location, job)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, render(created))
}

// GET /jobs/{location}/{job}
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	job, err := h.provider.GetJob(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, render(job))
}

// PUT /jobs/{location}/{job}
func (h *Handler) UpdateJob(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput(w, r)
	if !ok {
		return
	}
	// The path carries the identity; the form's name/location are ignored.
	in.Name = name
	job, err := jobFromInput(in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	updated, err := h.provider.UpdateJob(r.Context(), h.account(r), location, name, job, nil)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, render(updated))
}

// DELETE /jobs/{location}/{job}
func (h *Handler) DeleteJob(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	if err := h.provider.DeleteJob(r.Context(), h.account(r), location, name); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /jobs/{location}/{job}/pause
func (h *Handler) PauseJob(w http.ResponseWriter, r *http.Request) {
	h.mutate(w, r, h.provider.PauseJob)
}

// POST /jobs/{location}/{job}/resume
func (h *Handler) ResumeJob(w http.ResponseWriter, r *http.Request) {
	h.mutate(w, r, h.provider.ResumeJob)
}

// POST /jobs/{location}/{job}/run
func (h *Handler) RunJob(w http.ResponseWriter, r *http.Request) {
	h.mutate(w, r, h.provider.RunJob)
}

// mutate runs a state-changing job action and renders the resulting job.
func (h *Handler) mutate(w http.ResponseWriter, r *http.Request, fn func(context.Context, string, string, string) (schedstore.Job, error)) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	job, err := fn(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, render(job))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// target reads and validates the {location}/{job} path parameters, writing a
// 400 and returning false when either is missing or malformed.
func (h *Handler) target(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	location, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return "", "", false
	}
	name, ok := uihelper.Segment(r, "job")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid job", http.StatusBadRequest)
		return "", "", false
	}
	return location, name, true
}
