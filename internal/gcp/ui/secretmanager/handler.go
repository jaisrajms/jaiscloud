package secretmanagerui

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Secret Manager UI API requests by calling the provider.
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

// afterSegment returns the path segment immediately following marker in a full
// resource name, or "" when the marker is absent.
func afterSegment(name, marker string) string {
	i := strings.Index(name, marker)
	if i < 0 {
		return ""
	}
	rest := name[i+len(marker):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		return rest[:j]
	}
	return rest
}

func secretFromMap(m map[string]any) Secret {
	name := uihelper.Str(m, "name")
	rotation := uihelper.MapAt(m, "rotation")
	return Secret{
		Name:             name,
		SecretID:         afterSegment(name, "/secrets/"),
		CreateTime:       uihelper.Str(m, "createTime"),
		Etag:             uihelper.Str(m, "etag"),
		Labels:           uihelper.StringMapAt(m, "labels"),
		Annotations:      uihelper.StringMapAt(m, "annotations"),
		Topics:           topicsFromMap(m),
		RotationPeriod:   uihelper.Str(rotation, "rotationPeriod"),
		NextRotationTime: uihelper.Str(rotation, "nextRotationTime"),
		KmsKeyName:       kmsKeyNameFromMap(m),
	}
}

// topicsFromMap flattens the Secret's `topics` array to the topic resource
// names the console edits.
func topicsFromMap(m map[string]any) []string {
	items := uihelper.AsSlice(m["topics"])
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		t, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if name := uihelper.Str(t, "name"); name != "" {
			out = append(out, name)
		}
	}
	return out
}

// kmsKeyNameFromMap pulls the CMEK name out of the nested replication object:
// the automatic arm's key, or the first user-managed replica that names one.
func kmsKeyNameFromMap(m map[string]any) string {
	repl := uihelper.MapAt(m, "replication")
	if auto := uihelper.MapAt(repl, "automatic"); auto != nil {
		if cmek := uihelper.MapAt(auto, "customerManagedEncryption"); cmek != nil {
			return uihelper.Str(cmek, "kmsKeyName")
		}
	}
	if um := uihelper.MapAt(repl, "userManaged"); um != nil {
		for _, item := range uihelper.AsSlice(um["replicas"]) {
			r, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if cmek := uihelper.MapAt(r, "customerManagedEncryption"); cmek != nil {
				if key := uihelper.Str(cmek, "kmsKeyName"); key != "" {
					return key
				}
			}
		}
	}
	return ""
}

func versionFromMap(m map[string]any) SecretVersion {
	name := uihelper.Str(m, "name")
	return SecretVersion{
		Name:        name,
		VersionID:   afterSegment(name, "/versions/"),
		State:       uihelper.Str(m, "state"),
		CreateTime:  uihelper.Str(m, "createTime"),
		DestroyTime: uihelper.Str(m, "destroyTime"),
		Etag:        uihelper.Str(m, "etag"),
	}
}

func secretBody(labels, annotations map[string]string, topics []string, rotationPeriod, nextRotationTime string, removeRotation bool, kmsKeyName string) map[string]any {
	body := map[string]any{}
	if len(labels) > 0 {
		body["labels"] = uihelper.StringMapToAny(labels)
	}
	if len(annotations) > 0 {
		body["annotations"] = uihelper.StringMapToAny(annotations)
	}
	// topics is always sent, so the console's (pre-filled) list is the source of
	// truth: an empty list removes every event notification.
	topicList := make([]any, 0, len(topics))
	for _, t := range topics {
		topicList = append(topicList, map[string]any{"name": t})
	}
	body["topics"] = topicList
	switch {
	case removeRotation:
		// An empty rotation object on the (maskless) patch clears the schedule;
		// the provider keeps the output-only managedRotationStatus.
		body["rotation"] = map[string]any{}
	case rotationPeriod != "" || nextRotationTime != "":
		rotation := map[string]any{}
		if rotationPeriod != "" {
			rotation["rotationPeriod"] = rotationPeriod
		}
		if nextRotationTime != "" {
			rotation["nextRotationTime"] = nextRotationTime
		}
		body["rotation"] = rotation
	}
	if kmsKeyName != "" {
		body["replication"] = map[string]any{
			"automatic": map[string]any{
				"customerManagedEncryption": map[string]any{"kmsKeyName": kmsKeyName},
			},
		}
	}
	return body
}

// ─── target helpers ──────────────────────────────────────────────────────────

func (h *Handler) secretName(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, ok := uihelper.Segment(r, "secret")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid secret", http.StatusBadRequest)
		return "", false
	}
	return "secrets/" + id, true
}

func (h *Handler) versionName(w http.ResponseWriter, r *http.Request) (string, bool) {
	base, ok := h.secretName(w, r)
	if !ok {
		return "", false
	}
	version, ok := uihelper.Segment(r, "version")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid version", http.StatusBadRequest)
		return "", false
	}
	return base + "/versions/" + version, true
}

// ─── Secrets ─────────────────────────────────────────────────────────────────

// GET /secrets
func (h *Handler) ListSecrets(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.ListSecrets", "global", h.account(r))
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.List(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["secrets"])
	secrets := make([]Secret, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			secrets = append(secrets, secretFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListSecretsResponse{
		Secrets:       secrets,
		Total:         len(secrets),
		NextPageToken: uihelper.Str(resp.Data, "nextPageToken"),
	})
}

// POST /secrets
func (h *Handler) CreateSecret(w http.ResponseWriter, r *http.Request) {
	var req CreateSecretRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.SecretID == "" {
		uihelper.UIError(w, "BadRequest", "secretId is required", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.CreateSecret", "global", h.account(r))
	nr.Params["secretId"] = req.SecretID
	nr.Params["body"] = secretBody(req.Labels, req.Annotations, req.Topics, req.RotationPeriod, req.NextRotationTime, false, req.KmsKeyName)

	resp, err := h.provider.Create(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, secretFromMap(resp.Data))
}

// GET /secrets/{secret}
func (h *Handler) GetSecret(w http.ResponseWriter, r *http.Request) {
	name, ok := h.secretName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.GetSecret", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.Get(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, secretFromMap(resp.Data))
}

// PATCH /secrets/{secret}
func (h *Handler) UpdateSecret(w http.ResponseWriter, r *http.Request) {
	name, ok := h.secretName(w, r)
	if !ok {
		return
	}
	var req UpdateSecretRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.UpdateSecret", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = secretBody(req.Labels, req.Annotations, req.Topics, req.RotationPeriod, req.NextRotationTime, req.RemoveRotation, req.KmsKeyName)

	resp, err := h.provider.Update(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, secretFromMap(resp.Data))
}

// DELETE /secrets/{secret}
func (h *Handler) DeleteSecret(w http.ResponseWriter, r *http.Request) {
	name, ok := h.secretName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.DeleteSecret", "global", h.account(r))
	nr.Params["name"] = name

	if _, err := h.provider.Delete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── IAM policy ──────────────────────────────────────────────────────────────

// GET /secrets/{secret}/iam
func (h *Handler) GetIamPolicy(w http.ResponseWriter, r *http.Request) {
	name, ok := h.secretName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.GetIamPolicy", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.GetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PUT /secrets/{secret}/iam
func (h *Handler) SetIamPolicy(w http.ResponseWriter, r *http.Request) {
	name, ok := h.secretName(w, r)
	if !ok {
		return
	}
	var req SetIamPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.SetIamPolicy", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"policy": uihelper.IamPolicyBody(req.Policy)}

	resp, err := h.provider.SetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /secrets/{secret}/iam/testIamPermissions
func (h *Handler) TestIamPermissions(w http.ResponseWriter, r *http.Request) {
	name, ok := h.secretName(w, r)
	if !ok {
		return
	}
	var req TestIamPermissionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.TestIamPermissions", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"permissions": uihelper.StringsToAny(req.Permissions)}

	resp, err := h.provider.TestIamPermissions(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── Versions ────────────────────────────────────────────────────────────────

// GET /secrets/{secret}/versions
func (h *Handler) ListVersions(w http.ResponseWriter, r *http.Request) {
	name, ok := h.secretName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.ListSecretVersions", "global", h.account(r))
	nr.Params["name"] = name
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.ListVersions(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["versions"])
	versions := make([]SecretVersion, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			versions = append(versions, versionFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListSecretVersionsResponse{
		Versions:      versions,
		Total:         len(versions),
		NextPageToken: uihelper.Str(resp.Data, "nextPageToken"),
	})
}

// POST /secrets/{secret}/versions
func (h *Handler) AddVersion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.secretName(w, r)
	if !ok {
		return
	}
	var req AddVersionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.AddSecretVersion", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"payload": map[string]any{"data": req.Payload}}

	resp, err := h.provider.AddVersion(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, versionFromMap(resp.Data))
}

// GET /secrets/{secret}/versions/{version}
func (h *Handler) GetVersion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.GetSecretVersion", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.GetVersion(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, versionFromMap(resp.Data))
}

// GET /secrets/{secret}/versions/{version}/access
func (h *Handler) AccessVersion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.AccessSecretVersion", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.Access(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	payload := uihelper.MapAt(resp.Data, "payload")
	uihelper.WriteJSON(w, AccessSecretVersionResponse{
		Name: uihelper.Str(resp.Data, "name"),
		Data: uihelper.Str(payload, "data"),
	})
}

// POST /secrets/{secret}/versions/{version}/destroy
func (h *Handler) DestroyVersion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", "SecretManager.DestroySecretVersion", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.DestroyVersion(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, versionFromMap(resp.Data))
}

// POST /secrets/{secret}/versions/{version}/disable
func (h *Handler) DisableVersion(w http.ResponseWriter, r *http.Request) {
	h.setVersionState(w, r, true)
}

// POST /secrets/{secret}/versions/{version}/enable
func (h *Handler) EnableVersion(w http.ResponseWriter, r *http.Request) {
	h.setVersionState(w, r, false)
}

func (h *Handler) setVersionState(w http.ResponseWriter, r *http.Request, disabled bool) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	action := "SecretManager.EnableSecretVersion"
	if disabled {
		action = "SecretManager.DisableSecretVersion"
	}
	nr := uihelper.NR(r.Context(), h.cfg, "secretmanager", action, "global", h.account(r))
	nr.Params["name"] = name

	call := h.provider.EnableVersion
	if disabled {
		call = h.provider.DisableVersion
	}
	resp, err := call(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, versionFromMap(resp.Data))
}
