package functionsui

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/policy"
	functionscore "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Cloud Functions UI API requests by calling the Functions core.
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

func decodeInput[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var in T
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		uihelper.UIError(w, "InvalidArgument", "invalid JSON body", http.StatusBadRequest)
		return in, false
	}
	return in, true
}

// renderFunction flattens a stored function into the console row. Output-only
// fields (resource name, url, timestamps) come from the core's canonical v2
// rendering so the console stays in sync with REST/gRPC.
func renderFunction(project string, f functionsstore.Function) Function {
	m := functionscore.FunctionJSON(functionscore.V2, project, f)
	out := Function{
		ID:                            f.ID,
		Location:                      f.Location,
		Name:                          uihelper.Str(m, "name"),
		Status:                        f.Status,
		URL:                           uihelper.Str(m, "url"),
		Runtime:                       f.Runtime,
		EntryPoint:                    f.EntryPoint,
		AvailableMemoryMB:             f.AvailableMemoryMB,
		Timeout:                       f.Timeout,
		MinInstanceCount:              f.MinInstanceCount,
		MaxInstanceCount:              f.MaxInstanceCount,
		MaxInstanceRequestConcurrency: f.MaxInstanceRequestConcurrency,
		AvailableCPU:                  f.AvailableCPU,
		Revision:                      f.Revision,
		SourceArchiveURL:              f.SourceArchiveURL,
		SourceUploadURL:               f.SourceUploadURL,
		SourceSize:                    f.SourceSize,
		SourceSHA256:                  f.SourceSHA256,
		Description:                   f.Description,
		Labels:                        f.Labels,
		EnvironmentVariables:          f.EnvironmentVariables,
		CreateTime:                    uihelper.Str(m, "createTime"),
		UpdateTime:                    uihelper.Str(m, "updateTime"),
	}
	if svc := uihelper.MapAt(m, "serviceConfig"); svc != nil {
		out.TimeoutSeconds = intOf(svc["timeoutSeconds"])
	}
	if f.EventTrigger != nil {
		out.TriggerType = "event"
		out.EventTrigger = &EventTrigger{
			EventType:   f.EventTrigger.EventType,
			Resource:    f.EventTrigger.Resource,
			Retry:       f.EventTrigger.Retries(),
			RetryPolicy: f.EventTrigger.RetryPolicy,
			Trigger:     f.EventTrigger.Trigger,
		}
	} else {
		out.TriggerType = "http"
	}
	return out
}

// renderDelivery flattens a persisted delivery record.
func renderDelivery(d functionsstore.Delivery) Delivery {
	return Delivery{
		ID:              d.ID,
		FunctionID:      d.FunctionID,
		Source:          d.Source,
		EventType:       d.EventType,
		Resource:        d.Resource,
		EventID:         d.EventID,
		Status:          d.Status,
		Attempts:        d.Attempts,
		Error:           d.Error,
		Result:          d.Result,
		DeadLetterTopic: d.DeadLetterTopic,
		Attributes:      d.Attributes,
		CreateTime:      formatTime(d.CreateTime),
		UpdateTime:      formatTime(d.UpdateTime),
	}
}

// formatTime renders a stored timestamp as RFC3339, or "" when zero.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// intOf reads a JSON number surfaced as int, float64, or json.Number.
func intOf(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
	}
	return 0
}

// ─── Functions ───────────────────────────────────────────────────────────────

// GET /functions
func (h *Handler) ListFunctions(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	fns, err := h.provider.ListFunctions(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Function, 0, len(fns))
	for _, f := range fns {
		out = append(out, renderFunction(project, f))
	}
	uihelper.WriteJSON(w, ListFunctionsResponse{Functions: out, Total: len(out)})
}

// POST /functions/upload
//
// Provisions the GCS upload target for a browser source upload (the real v2
// generateUploadUrl flow). The console PUTs the archive to the same route, then
// sends the returned bucket/object as storageSource on create.
func (h *Handler) GenerateUploadURL(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput[UploadInput](w, r)
	if !ok {
		return
	}
	if in.Location == "" {
		uihelper.UIError(w, "InvalidArgument", "location is required", http.StatusBadRequest)
		return
	}
	up, err := h.provider.GenerateUploadURL(r.Context(), h.account(r), in.Location)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, SourceUploadTarget{Bucket: up.Bucket, Object: up.Object})
}

// maxSourceUploadBytes caps the browser source archive at Cloud Functions'
// documented zipped-source limit (100 MB). It is a var so tests can exercise
// the cap without allocating the full archive.
var maxSourceUploadBytes int64 = 100 << 20

// PUT /functions/upload?location=..&object=..  (raw body = source zip)
//
// Stores the archive the browser uploads for an issued storageSource target (the
// PUT half of the generateUploadUrl flow). The bucket is derived server-side.
func (h *Handler) UploadSource(w http.ResponseWriter, r *http.Request) {
	location := r.URL.Query().Get("location")
	object := r.URL.Query().Get("object")
	if location == "" || object == "" {
		uihelper.UIError(w, "BadRequest", "location and object are required", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSourceUploadBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			uihelper.UIError(w, "OutOfRange", "source archive exceeds the 100 MB upload limit", http.StatusRequestEntityTooLarge)
			return
		}
		uihelper.UIError(w, "InvalidArgument", "failed to read source archive", http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		uihelper.UIError(w, "InvalidArgument", "source archive is empty", http.StatusBadRequest)
		return
	}
	if err := h.provider.PutSourceArchive(r.Context(), h.account(r), location, object, body); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /functions
func (h *Handler) CreateFunction(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput[CreateInput](w, r)
	if !ok {
		return
	}
	if in.Location == "" {
		uihelper.UIError(w, "InvalidArgument", "location is required", http.StatusBadRequest)
		return
	}
	if in.ID == "" {
		uihelper.UIError(w, "InvalidArgument", "function id is required", http.StatusBadRequest)
		return
	}
	if in.Runtime == "" {
		uihelper.UIError(w, "InvalidArgument", "runtime is required", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	created, err := h.provider.CreateFunction(r.Context(), project, in.Location, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderFunction(project, created))
}

// GET /functions/{location}/{function}
func (h *Handler) GetFunction(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	project := h.account(r)
	f, err := h.provider.GetFunction(r.Context(), project, location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderFunction(project, f))
}

// DELETE /functions/{location}/{function}
func (h *Handler) DeleteFunction(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	if err := h.provider.DeleteFunction(r.Context(), h.account(r), location, name); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /functions/{location}/{function}/call
func (h *Handler) CallFunction(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[CallInput](w, r)
	if !ok {
		return
	}
	executionID, result, invokeErr, err := h.provider.CallFunction(r.Context(), h.account(r), location, name, in.Data)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, CallResponse{ExecutionID: executionID, Result: result, Error: invokeErr})
}

// ─── IAM ─────────────────────────────────────────────────────────────────────

// GET /functions/{location}/{function}/iam
func (h *Handler) GetIam(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	pol, err := h.provider.GetIamPolicy(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, policy.ToMap(pol))
}

// PUT /functions/{location}/{function}/iam
func (h *Handler) SetIam(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[uihelper.IamPolicy](w, r)
	if !ok {
		return
	}
	pol, err := h.provider.SetIamPolicy(r.Context(), h.account(r), location, name, uihelper.IamPolicyBody(in))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, policy.ToMap(pol))
}

// ─── Deliveries (emulator-only Executions tab) ───────────────────────────────

// GET /functions/{location}/{function}/deliveries
func (h *Handler) ListDeliveries(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.target(w, r)
	if !ok {
		return
	}
	records, err := h.provider.ListDeliveries(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Delivery, 0, len(records))
	for _, d := range records {
		out = append(out, renderDelivery(d))
	}
	uihelper.WriteJSON(w, ListDeliveriesResponse{Deliveries: out, Total: len(out)})
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// target reads and validates the {location}/{function} path parameters, writing
// a 400 and returning false when either is missing or malformed.
func (h *Handler) target(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	location, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return "", "", false
	}
	id, ok := uihelper.Segment(r, "function")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid function", http.StatusBadRequest)
		return "", "", false
	}
	return location, id, true
}
