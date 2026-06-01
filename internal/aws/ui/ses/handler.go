package sesui

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves SES UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /identities
func (h *Handler) ListIdentities(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "email", "ListIdentities", region, account)

	resp, err := h.provider.ListIdentities(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawIdentities, _ := resp.Data["Identities"].([]string)
	items := make([]Identity, 0, len(rawIdentities))
	for _, id := range rawIdentities {
		idType := "EmailAddress"
		// Domains typically contain a dot but no @
		if !strings.Contains(id, "@") && strings.Contains(id, ".") {
			idType = "Domain"
		}
		items = append(items, Identity{Identity: id, Type: idType})
	}
	uihelper.WriteJSON(w, ListIdentitiesResponse{Items: items, Total: len(items)})
}

// POST /identities  body: { "identity": "user@example.com" }
func (h *Handler) VerifyEmailIdentity(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Identity string `json:"identity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Identity == "" {
		uihelper.UIError(w, "BadRequest", "identity is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "email", "VerifyEmailIdentity", region, account)
	nr.Params["EmailAddress"] = req.Identity

	resp, err := h.provider.VerifyEmailIdentity(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /identities/{identity}
func (h *Handler) DeleteIdentity(w http.ResponseWriter, r *http.Request) {
	identity := chi.URLParam(r, "identity")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "email", "DeleteIdentity", region, account)
	nr.Params["Identity"] = identity

	if _, err := h.provider.DeleteIdentity(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
