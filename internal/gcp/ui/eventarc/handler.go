package eventarcui

import (
	"encoding/json"
	"net/http"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/policy"
	eventarccore "jaiscloud/internal/gcp/service/eventarc"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/model"
)

// Handler serves Eventarc UI API requests by calling the Eventarc core.
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

// ─── mapping helpers ─────────────────────────────────────────────────────────

// destinationTypeKeys is the resolution order for a trigger destination. Real
// Eventarc validates exactly one, so the first present key wins.
var destinationTypeKeys = []string{"cloudFunction", "cloudRun", "workflow", "gke", "httpEndpoint"}

// renderTrigger flattens a stored trigger into the console row using the core's
// canonical wire rendering, so output-only fields (uid/etag/times, and a
// platform-provisioned transport subscription) stay in sync with REST/gRPC.
func renderTrigger(project string, t eventarcstore.Trigger) Trigger {
	m := eventarccore.TriggerJSON(project, t)
	out := Trigger{
		Name:                 t.Name,
		Location:             t.Location,
		UID:                  uihelper.Str(m, "uid"),
		Etag:                 uihelper.Str(m, "etag"),
		CreateTime:           uihelper.Str(m, "createTime"),
		UpdateTime:           uihelper.Str(m, "updateTime"),
		Labels:               t.Labels,
		ServiceAccount:       uihelper.Str(m, "serviceAccount"),
		Channel:              uihelper.Str(m, "channel"),
		EventDataContentType: uihelper.Str(m, "eventDataContentType"),
		Config:               t.Config,
	}
	if dest := uihelper.MapAt(m, "destination"); dest != nil {
		for _, typ := range destinationTypeKeys {
			v, ok := dest[typ]
			if !ok {
				continue
			}
			out.DestinationType = typ
			switch typ {
			case "cloudFunction", "workflow":
				out.Destination = uihelper.Str(dest, typ)
			case "cloudRun":
				if cm := uihelper.MapAt(dest, "cloudRun"); cm != nil {
					out.Destination = uihelper.Str(cm, "service")
					out.DestinationRegion = uihelper.Str(cm, "region")
				}
			default:
				if b, err := json.Marshal(v); err == nil {
					out.Destination = string(b)
				}
			}
			break
		}
	}
	for _, f := range uihelper.AsSlice(m["eventFilters"]) {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}
		out.EventFilters = append(out.EventFilters, EventFilter{
			Attribute: uihelper.Str(fm, "attribute"),
			Operator:  uihelper.Str(fm, "operator"),
			Value:     uihelper.Str(fm, "value"),
		})
	}
	if tr := uihelper.MapAt(m, "transport"); tr != nil {
		if ps := uihelper.MapAt(tr, "pubsub"); ps != nil {
			out.TransportPubsubTopic = uihelper.Str(ps, "topic")
			out.TransportPubsubSubscription = uihelper.Str(ps, "subscription")
		}
	}
	return out
}

// renderChannel flattens a stored channel into the console row using the core's
// canonical wire rendering (which synthesizes pubsubTopic/activationToken/state).
func renderChannel(project string, c eventarcstore.Channel) Channel {
	m := eventarccore.ChannelJSON(project, c)
	return Channel{
		Name:            c.Name,
		Location:        c.Location,
		UID:             uihelper.Str(m, "uid"),
		Etag:            uihelper.Str(m, "etag"),
		ActivationToken: uihelper.Str(m, "activationToken"),
		PubsubTopic:     uihelper.Str(m, "pubsubTopic"),
		State:           uihelper.Str(m, "state"),
		CreateTime:      uihelper.Str(m, "createTime"),
		UpdateTime:      uihelper.Str(m, "updateTime"),
		Labels:          c.Labels,
		Provider:        uihelper.Str(m, "provider"),
		CryptoKeyName:   uihelper.Str(m, "cryptoKeyName"),
		Config:          c.Config,
	}
}

// ─── Triggers ────────────────────────────────────────────────────────────────

// GET /triggers
func (h *Handler) ListTriggers(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	triggers, err := h.provider.ListTriggersByProject(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Trigger, 0, len(triggers))
	for _, t := range triggers {
		out = append(out, renderTrigger(project, t))
	}
	uihelper.WriteJSON(w, ListTriggersResponse{Triggers: out, Total: len(out)})
}

// POST /triggers
func (h *Handler) CreateTrigger(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput[TriggerInput](w, r)
	if !ok {
		return
	}
	if in.Location == "" {
		uihelper.UIError(w, "InvalidArgument", "location is required", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	created, err := h.provider.CreateTrigger(r.Context(), project, in.Location, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderTrigger(project, created))
}

// GET /triggers/{location}/{trigger}
func (h *Handler) GetTrigger(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetTrigger(w, r)
	if !ok {
		return
	}
	project := h.account(r)
	t, err := h.provider.GetTrigger(r.Context(), project, location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderTrigger(project, t))
}

// PUT /triggers/{location}/{trigger}
func (h *Handler) UpdateTrigger(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetTrigger(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[TriggerInput](w, r)
	if !ok {
		return
	}
	// The path carries the identity; the form's name/location are ignored.
	in.Name, in.Location = name, location
	project := h.account(r)
	updated, err := h.provider.UpdateTrigger(r.Context(), project, location, name, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderTrigger(project, updated))
}

// DELETE /triggers/{location}/{trigger}
func (h *Handler) DeleteTrigger(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetTrigger(w, r)
	if !ok {
		return
	}
	if err := h.provider.DeleteTrigger(r.Context(), h.account(r), location, name); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /triggers/{location}/{trigger}/iam
func (h *Handler) GetTriggerIam(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetTrigger(w, r)
	if !ok {
		return
	}
	pol, err := h.provider.TriggerGetIamPolicy(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, policy.ToMap(pol))
}

// PUT /triggers/{location}/{trigger}/iam
func (h *Handler) SetTriggerIam(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetTrigger(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[uihelper.IamPolicy](w, r)
	if !ok {
		return
	}
	pol, err := h.provider.TriggerSetIamPolicy(r.Context(), h.account(r), location, name, uihelper.IamPolicyBody(in))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, policy.ToMap(pol))
}

// ─── Channels ────────────────────────────────────────────────────────────────

// GET /channels
func (h *Handler) ListChannels(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	channels, err := h.provider.ListChannelsByProject(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Channel, 0, len(channels))
	for _, c := range channels {
		out = append(out, renderChannel(project, c))
	}
	uihelper.WriteJSON(w, ListChannelsResponse{Channels: out, Total: len(out)})
}

// POST /channels
func (h *Handler) CreateChannel(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput[ChannelInput](w, r)
	if !ok {
		return
	}
	if in.Location == "" {
		uihelper.UIError(w, "InvalidArgument", "location is required", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	created, err := h.provider.CreateChannel(r.Context(), project, in.Location, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderChannel(project, created))
}

// GET /channels/{location}/{channel}
func (h *Handler) GetChannel(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetChannel(w, r)
	if !ok {
		return
	}
	project := h.account(r)
	c, err := h.provider.GetChannel(r.Context(), project, location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderChannel(project, c))
}

// PUT /channels/{location}/{channel}
func (h *Handler) UpdateChannel(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetChannel(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[ChannelInput](w, r)
	if !ok {
		return
	}
	in.Name, in.Location = name, location
	project := h.account(r)
	updated, err := h.provider.UpdateChannel(r.Context(), project, location, name, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderChannel(project, updated))
}

// DELETE /channels/{location}/{channel}
func (h *Handler) DeleteChannel(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetChannel(w, r)
	if !ok {
		return
	}
	if err := h.provider.DeleteChannel(r.Context(), h.account(r), location, name); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /channels/{location}/{channel}/iam
func (h *Handler) GetChannelIam(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetChannel(w, r)
	if !ok {
		return
	}
	pol, err := h.provider.ChannelGetIamPolicy(r.Context(), h.account(r), location, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, policy.ToMap(pol))
}

// PUT /channels/{location}/{channel}/iam
func (h *Handler) SetChannelIam(w http.ResponseWriter, r *http.Request) {
	location, name, ok := h.targetChannel(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[uihelper.IamPolicy](w, r)
	if !ok {
		return
	}
	pol, err := h.provider.ChannelSetIamPolicy(r.Context(), h.account(r), location, name, uihelper.IamPolicyBody(in))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, policy.ToMap(pol))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// targetTrigger reads and validates the {location}/{trigger} path parameters,
// writing a 400 and returning false when either is missing or malformed.
func (h *Handler) targetTrigger(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	return h.segmentPair(w, r, "trigger")
}

// targetChannel reads and validates the {location}/{channel} path parameters.
func (h *Handler) targetChannel(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	return h.segmentPair(w, r, "channel")
}

// segmentPair reads {location}/{<id>} for an Eventarc resource.
func (h *Handler) segmentPair(w http.ResponseWriter, r *http.Request, idKey string) (string, string, bool) {
	location, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return "", "", false
	}
	id, ok := uihelper.Segment(r, idKey)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid "+idKey, http.StatusBadRequest)
		return "", "", false
	}
	return location, id, true
}
