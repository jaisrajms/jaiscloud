package secretsmanagerui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves SecretsManager UI API requests by calling the SecretsManager provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /secrets
func (h *Handler) ListSecrets(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	pageToken := r.URL.Query().Get("nextToken")

	nr := uihelper.NR(r.Context(), h.cfg, "secretsmanager", "ListSecrets", region, account)
	if pageToken != "" {
		nr.Params["NextToken"] = pageToken
	}

	resp, err := h.provider.ListSecrets(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawSecrets, _ := resp.Data["SecretList"].([]map[string]any)
	nextToken, _ := resp.Data["NextToken"].(string)

	secrets := make([]Secret, 0, len(rawSecrets))
	for _, s := range rawSecrets {
		secrets = append(secrets, parseSecret(s))
	}

	uihelper.WriteJSON(w, ListSecretsResponse{Items: secrets, NextToken: nextToken, Total: len(secrets)})
}

// POST /secrets
func (h *Handler) CreateSecret(w http.ResponseWriter, r *http.Request) {
	var req CreateSecretRequest
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

	nr := uihelper.NR(r.Context(), h.cfg, "secretsmanager", "CreateSecret", region, account)
	nr.Params["Name"] = req.Name
	if req.Description != "" {
		nr.Params["Description"] = req.Description
	}
	if req.SecretString != "" {
		nr.Params["SecretString"] = req.SecretString
	}
	if req.KMSKeyID != "" {
		nr.Params["KmsKeyId"] = req.KMSKeyID
	}
	if req.Tags != nil {
		tagList := make([]any, 0, len(req.Tags))
		for k, v := range req.Tags {
			tagList = append(tagList, map[string]any{"Key": k, "Value": v})
		}
		nr.Params["Tags"] = tagList
	}

	resp, err := h.provider.CreateSecret(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /secrets/{name}
func (h *Handler) GetSecret(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "secretsmanager", "DescribeSecret", region, account)
	nr.Params["SecretId"] = name

	resp, err := h.provider.DescribeSecret(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /secrets/{name}
func (h *Handler) DeleteSecret(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "secretsmanager", "DeleteSecret", region, account)
	nr.Params["SecretId"] = name
	nr.Params["ForceDeleteWithoutRecovery"] = true

	if _, err := h.provider.DeleteSecret(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /secrets/{name}/value
func (h *Handler) GetSecretValue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "secretsmanager", "GetSecretValue", region, account)
	nr.Params["SecretId"] = name

	resp, err := h.provider.GetSecretValue(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /secrets/{name}/value  body: { "secretString": "..." }
func (h *Handler) PutSecretValue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var req PutSecretValueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.SecretString == "" {
		uihelper.UIError(w, "BadRequest", "secretString is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "secretsmanager", "PutSecretValue", region, account)
	nr.Params["SecretId"] = name
	nr.Params["SecretString"] = req.SecretString

	resp, err := h.provider.PutSecretValue(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /secrets/{name}/versions
func (h *Handler) ListSecretVersions(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	pageToken := r.URL.Query().Get("nextToken")

	nr := uihelper.NR(r.Context(), h.cfg, "secretsmanager", "ListSecretVersionIds", region, account)
	nr.Params["SecretId"] = name
	if pageToken != "" {
		nr.Params["NextToken"] = pageToken
	}

	resp, err := h.provider.ListSecretVersionIds(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawVersions, _ := resp.Data["Versions"].([]map[string]any)
	nextToken, _ := resp.Data["NextToken"].(string)

	versions := make([]SecretVersion, 0, len(rawVersions))
	for _, v := range rawVersions {
		sv := SecretVersion{
			VersionID:    strAny(v, "VersionId"),
			CreatedDate:  strAny(v, "CreatedDate"),
			LastAccessedDate: strAny(v, "LastAccessedDate"),
		}
		if stages, ok := v["VersionStages"].([]string); ok {
			sv.VersionStages = stages
		} else if stages, ok := v["VersionStages"].([]any); ok {
			for _, s := range stages {
				if str, ok := s.(string); ok {
					sv.VersionStages = append(sv.VersionStages, str)
				}
			}
		}
		versions = append(versions, sv)
	}
	uihelper.WriteJSON(w, ListSecretVersionsResponse{Items: versions, NextToken: nextToken})
}

// parseSecret converts raw provider data to a Secret.
func parseSecret(s map[string]any) Secret {
	sec := Secret{
		ARN:             strAny(s, "ARN"),
		Name:            strAny(s, "Name"),
		Description:     strAny(s, "Description"),
		KMSKeyID:        strAny(s, "KmsKeyId"),
		LastChangedDate: strAny(s, "LastChangedDate"),
		CreatedDate:     strAny(s, "CreatedDate"),
		DeletedDate:     strAny(s, "DeletedDate"),
	}
	if rawTags, ok := s["Tags"].([]map[string]any); ok {
		sec.Tags = make(map[string]string, len(rawTags))
		for _, t := range rawTags {
			k, _ := t["Key"].(string)
			v, _ := t["Value"].(string)
			if k != "" {
				sec.Tags[k] = v
			}
		}
	}
	return sec
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
