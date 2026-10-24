package kmsui

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/model"
)

// Handler serves Cloud KMS UI API requests by calling the KMS provider.
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

func keyRingFromMap(m map[string]any) KeyRing {
	name := uihelper.Str(m, "name")
	return KeyRing{
		Name:       name,
		Location:   afterSegment(name, "/locations/"),
		KeyRingID:  afterSegment(name, "/keyRings/"),
		CreateTime: uihelper.Str(m, "createTime"),
	}
}

func cryptoKeyFromMap(m map[string]any) CryptoKey {
	name := uihelper.Str(m, "name")
	primary := uihelper.MapAt(m, "primary")
	template := uihelper.MapAt(m, "versionTemplate")
	return CryptoKey{
		Name:             name,
		CryptoKeyID:      afterSegment(name, "/cryptoKeys/"),
		Purpose:          uihelper.Str(m, "purpose"),
		Algorithm:        uihelper.Str(template, "algorithm"),
		PrimaryState:     uihelper.Str(primary, "state"),
		PrimaryVersion:   afterSegment(uihelper.Str(primary, "name"), "/cryptoKeyVersions/"),
		CreateTime:       uihelper.Str(m, "createTime"),
		RotationPeriod:   uihelper.Str(m, "rotationPeriod"),
		NextRotationTime: uihelper.Str(m, "nextRotationTime"),
		Labels:           uihelper.StringMapAt(m, "labels"),
	}
}

func versionFromMap(m map[string]any) CryptoKeyVersion {
	name := uihelper.Str(m, "name")
	return CryptoKeyVersion{
		Name:             name,
		VersionID:        afterSegment(name, "/cryptoKeyVersions/"),
		State:            uihelper.Str(m, "state"),
		Algorithm:        uihelper.Str(m, "algorithm"),
		CreateTime:       uihelper.Str(m, "createTime"),
		DestroyTime:      uihelper.Str(m, "destroyTime"),
		DestroyEventTime: uihelper.Str(m, "destroyEventTime"),
	}
}

// ─── target helpers ──────────────────────────────────────────────────────────

func (h *Handler) keyRingName(w http.ResponseWriter, r *http.Request) (string, bool) {
	loc, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return "", false
	}
	kr, ok := uihelper.Segment(r, "keyRing")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid key ring", http.StatusBadRequest)
		return "", false
	}
	return "locations/" + loc + "/keyRings/" + kr, true
}

func (h *Handler) cryptoKeyName(w http.ResponseWriter, r *http.Request) (string, bool) {
	base, ok := h.keyRingName(w, r)
	if !ok {
		return "", false
	}
	key, ok := uihelper.Segment(r, "key")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid crypto key", http.StatusBadRequest)
		return "", false
	}
	return base + "/cryptoKeys/" + key, true
}

func (h *Handler) versionName(w http.ResponseWriter, r *http.Request) (string, bool) {
	base, ok := h.cryptoKeyName(w, r)
	if !ok {
		return "", false
	}
	version, ok := uihelper.Segment(r, "version")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid version", http.StatusBadRequest)
		return "", false
	}
	return base + "/cryptoKeyVersions/" + version, true
}

// ─── Key rings ───────────────────────────────────────────────────────────────

// GET /locations/{location}/keyRings
func (h *Handler) ListKeyRings(w http.ResponseWriter, r *http.Request) {
	loc, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.KeyRingList", loc, h.account(r))
	nr.Params["location"] = loc
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.KeyRingList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["keyRings"])
	keyRings := make([]KeyRing, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			keyRings = append(keyRings, keyRingFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListKeyRingsResponse{
		KeyRings:      keyRings,
		Total:         len(keyRings),
		NextPageToken: uihelper.Str(resp.Data, "nextPageToken"),
	})
}

// POST /locations/{location}/keyRings
func (h *Handler) CreateKeyRing(w http.ResponseWriter, r *http.Request) {
	loc, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return
	}
	var req CreateKeyRingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.KeyRingID == "" {
		uihelper.UIError(w, "BadRequest", "keyRingId is required", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.KeyRingCreate", loc, h.account(r))
	nr.Params["location"] = loc
	nr.Params["keyRingId"] = req.KeyRingID

	resp, err := h.provider.KeyRingCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, keyRingFromMap(resp.Data))
}

// GET /locations/{location}/keyRings/{keyRing}
func (h *Handler) GetKeyRing(w http.ResponseWriter, r *http.Request) {
	name, ok := h.keyRingName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.KeyRingGet", "", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.KeyRingGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, keyRingFromMap(resp.Data))
}

// ─── Crypto keys ─────────────────────────────────────────────────────────────

// GET /locations/{location}/keyRings/{keyRing}/cryptoKeys
func (h *Handler) ListCryptoKeys(w http.ResponseWriter, r *http.Request) {
	name, ok := h.keyRingName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyList", "", h.account(r))
	nr.Params["name"] = name
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.CryptoKeyList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["cryptoKeys"])
	keys := make([]CryptoKey, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			keys = append(keys, cryptoKeyFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListCryptoKeysResponse{
		CryptoKeys:    keys,
		Total:         len(keys),
		NextPageToken: uihelper.Str(resp.Data, "nextPageToken"),
	})
}

// POST /locations/{location}/keyRings/{keyRing}/cryptoKeys
func (h *Handler) CreateCryptoKey(w http.ResponseWriter, r *http.Request) {
	name, ok := h.keyRingName(w, r)
	if !ok {
		return
	}
	var req CreateCryptoKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.CryptoKeyID == "" {
		uihelper.UIError(w, "BadRequest", "cryptoKeyId is required", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyCreate", "", h.account(r))
	nr.Params["name"] = name
	nr.Params["cryptoKeyId"] = req.CryptoKeyID
	body := map[string]any{}
	if req.Purpose != "" {
		body["purpose"] = req.Purpose
	}
	if req.Algorithm != "" {
		body["versionTemplate"] = map[string]any{"algorithm": req.Algorithm}
	}
	if req.RotationPeriod != "" {
		body["rotationPeriod"] = req.RotationPeriod
		// A rotation_schedule requires next_rotation_time. The console (like
		// gcloud) starts the first rotation one period out unless the caller
		// supplied its own; only derive when the period parses.
		if req.NextRotationTime != "" {
			body["nextRotationTime"] = req.NextRotationTime
		} else if period, err := time.ParseDuration(req.RotationPeriod); err == nil {
			body["nextRotationTime"] = clock.Now().Add(period).UTC().Format(time.RFC3339Nano)
		}
	}
	if len(req.Labels) > 0 {
		body["labels"] = uihelper.StringMapToAny(req.Labels)
	}
	nr.Params["body"] = body

	resp, err := h.provider.CryptoKeyCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, cryptoKeyFromMap(resp.Data))
}

// GET /locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}
func (h *Handler) GetCryptoKey(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyGet", "", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.CryptoKeyGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, cryptoKeyFromMap(resp.Data))
}

// POST /locations/{location}/keyRings/{keyRing}/cryptoKeys/{key}/setPrimary
func (h *Handler) SetPrimaryVersion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	var req SetPrimaryVersionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.CryptoKeyVersionID == "" {
		uihelper.UIError(w, "BadRequest", "cryptoKeyVersionId is required", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyUpdatePrimaryVersion", "", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"cryptoKeyVersionId": req.CryptoKeyVersionID}

	resp, err := h.provider.CryptoKeyUpdatePrimaryVersion(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, cryptoKeyFromMap(resp.Data))
}

// ─── Crypto key versions ─────────────────────────────────────────────────────

// GET .../cryptoKeys/{key}/versions
func (h *Handler) ListVersions(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyVersionList", "", h.account(r))
	nr.Params["name"] = name
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.CryptoKeyVersionList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["cryptoKeyVersions"])
	versions := make([]CryptoKeyVersion, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			versions = append(versions, versionFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListCryptoKeyVersionsResponse{
		CryptoKeyVersions: versions,
		Total:             len(versions),
		NextPageToken:     uihelper.Str(resp.Data, "nextPageToken"),
	})
}

// POST .../cryptoKeys/{key}/versions
func (h *Handler) CreateVersion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyVersionCreate", "", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.CryptoKeyVersionCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, versionFromMap(resp.Data))
}

// GET .../cryptoKeys/{key}/versions/{version}
func (h *Handler) GetVersion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyVersionGet", "", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.CryptoKeyVersionGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, versionFromMap(resp.Data))
}

// POST .../cryptoKeys/{key}/versions/{version}/destroy
func (h *Handler) DestroyVersion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyVersionDestroy", "", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.CryptoKeyVersionDestroy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, versionFromMap(resp.Data))
}

// POST .../cryptoKeys/{key}/versions/{version}/disable
func (h *Handler) DisableVersion(w http.ResponseWriter, r *http.Request) {
	h.setVersionState(w, r, "DISABLED")
}

// POST .../cryptoKeys/{key}/versions/{version}/enable
func (h *Handler) EnableVersion(w http.ResponseWriter, r *http.Request) {
	h.setVersionState(w, r, "ENABLED")
}

func (h *Handler) setVersionState(w http.ResponseWriter, r *http.Request, state string) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.CryptoKeyVersionUpdate", "", h.account(r))
	nr.Params["name"] = name
	nr.Params["updateMask"] = "state"
	nr.Params["body"] = map[string]any{"state": state}

	resp, err := h.provider.CryptoKeyVersionUpdate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, versionFromMap(resp.Data))
}

// ─── Crypto operations ───────────────────────────────────────────────────────

// POST .../cryptoKeys/{key}/encrypt
func (h *Handler) Encrypt(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	var req EncryptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Plaintext == "" {
		uihelper.UIError(w, "BadRequest", "plaintext is required", http.StatusBadRequest)
		return
	}
	body := map[string]any{"plaintext": req.Plaintext}
	if req.AdditionalAuthenticatedData != "" {
		body["additionalAuthenticatedData"] = req.AdditionalAuthenticatedData
	}
	h.cryptoOp(w, r, "KMS.CryptoKeyEncrypt", name, body, func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return h.provider.CryptoKeyEncrypt(r.Context(), nr)
	})
}

// POST .../cryptoKeys/{key}/decrypt
func (h *Handler) Decrypt(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	var req DecryptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Ciphertext == "" {
		uihelper.UIError(w, "BadRequest", "ciphertext is required", http.StatusBadRequest)
		return
	}
	body := map[string]any{"ciphertext": req.Ciphertext}
	if req.AdditionalAuthenticatedData != "" {
		body["additionalAuthenticatedData"] = req.AdditionalAuthenticatedData
	}
	h.cryptoOp(w, r, "KMS.CryptoKeyDecrypt", name, body, func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return h.provider.CryptoKeyDecrypt(r.Context(), nr)
	})
}

// POST .../versions/{version}/asymmetricSign
func (h *Handler) AsymmetricSign(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	var req AsymmetricSignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if !validDigest(req.Digest) {
		uihelper.UIError(w, "BadRequest", "digest is required", http.StatusBadRequest)
		return
	}
	h.cryptoOp(w, r, "KMS.CryptoKeyVersionAsymmetricSign", name, map[string]any{"digest": req.Digest},
		func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return h.provider.CryptoKeyVersionAsymmetricSign(r.Context(), nr)
		})
}

// POST .../versions/{version}/asymmetricDecrypt
func (h *Handler) AsymmetricDecrypt(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	var req AsymmetricDecryptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Ciphertext == "" {
		uihelper.UIError(w, "BadRequest", "ciphertext is required", http.StatusBadRequest)
		return
	}
	h.cryptoOp(w, r, "KMS.CryptoKeyVersionAsymmetricDecrypt", name, map[string]any{"ciphertext": req.Ciphertext},
		func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return h.provider.CryptoKeyVersionAsymmetricDecrypt(r.Context(), nr)
		})
}

// POST .../versions/{version}/macSign
func (h *Handler) MacSign(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	var req MacSignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Data == "" {
		uihelper.UIError(w, "BadRequest", "data is required", http.StatusBadRequest)
		return
	}
	h.cryptoOp(w, r, "KMS.CryptoKeyVersionMacSign", name, map[string]any{"data": req.Data},
		func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return h.provider.CryptoKeyVersionMacSign(r.Context(), nr)
		})
}

// POST .../versions/{version}/macVerify
func (h *Handler) MacVerify(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	var req MacVerifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Data == "" || req.Mac == "" {
		uihelper.UIError(w, "BadRequest", "data and mac are required", http.StatusBadRequest)
		return
	}
	h.cryptoOp(w, r, "KMS.CryptoKeyVersionMacVerify", name, map[string]any{"data": req.Data, "mac": req.Mac},
		func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return h.provider.CryptoKeyVersionMacVerify(r.Context(), nr)
		})
}

// GET .../versions/{version}/publicKey
func (h *Handler) GetPublicKey(w http.ResponseWriter, r *http.Request) {
	name, ok := h.versionName(w, r)
	if !ok {
		return
	}
	h.cryptoOp(w, r, "KMS.CryptoKeyVersionGetPublicKey", name, nil,
		func(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return h.provider.CryptoKeyVersionGetPublicKey(r.Context(), nr)
		})
}

// cryptoOp runs one direct provider call, forwarding name/body on the
// NormalizedRequest and echoing the provider's Discovery-shaped response. The
// provider's crypto methods accept the same body field names as the wire API
// (plaintext/ciphertext/digest/data/mac), so the console needs no reshaping.
func (h *Handler) cryptoOp(
	w http.ResponseWriter,
	r *http.Request,
	action, name string,
	body map[string]any,
	call func(*model.NormalizedRequest) (*model.ProviderResponse, error),
) {
	nr := uihelper.NR(r.Context(), h.cfg, "kms", action, "", h.account(r))
	nr.Params["name"] = name
	if body != nil {
		nr.Params["body"] = body
	}
	resp, err := call(nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// validDigest reports whether an asymmetric-sign digest names exactly one of
// the base64 digest fields Cloud KMS accepts (its Digest is a oneof).
func validDigest(digest map[string]any) bool {
	n := 0
	for _, k := range []string{"sha256", "sha384", "sha512"} {
		if s, _ := digest[k].(string); s != "" {
			n++
		}
	}
	return n == 1
}

// ─── IAM policy ──────────────────────────────────────────────────────────────

// GET /locations/{location}/keyRings/{keyRing}/iam
func (h *Handler) GetKeyRingIam(w http.ResponseWriter, r *http.Request) {
	name, ok := h.keyRingName(w, r)
	if !ok {
		return
	}
	h.getIam(w, r, name)
}

// PUT /locations/{location}/keyRings/{keyRing}/iam
func (h *Handler) SetKeyRingIam(w http.ResponseWriter, r *http.Request) {
	name, ok := h.keyRingName(w, r)
	if !ok {
		return
	}
	h.setIam(w, r, name)
}

// POST /locations/{location}/keyRings/{keyRing}/iam/testIamPermissions
func (h *Handler) TestKeyRingIam(w http.ResponseWriter, r *http.Request) {
	name, ok := h.keyRingName(w, r)
	if !ok {
		return
	}
	h.testIam(w, r, name)
}

// GET .../cryptoKeys/{key}/iam
func (h *Handler) GetCryptoKeyIam(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	h.getIam(w, r, name)
}

// PUT .../cryptoKeys/{key}/iam
func (h *Handler) SetCryptoKeyIam(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	h.setIam(w, r, name)
}

// POST .../cryptoKeys/{key}/iam/testIamPermissions
func (h *Handler) TestCryptoKeyIam(w http.ResponseWriter, r *http.Request) {
	name, ok := h.cryptoKeyName(w, r)
	if !ok {
		return
	}
	h.testIam(w, r, name)
}

func (h *Handler) getIam(w http.ResponseWriter, r *http.Request, name string) {
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.GetIamPolicy", "", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.GetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

func (h *Handler) setIam(w http.ResponseWriter, r *http.Request, name string) {
	var req SetIamPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.SetIamPolicy", "", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"policy": uihelper.IamPolicyBody(req.Policy)}

	resp, err := h.provider.SetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

func (h *Handler) testIam(w http.ResponseWriter, r *http.Request, name string) {
	var req TestIamPermissionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "KMS.TestIamPermissions", "", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"permissions": uihelper.StringsToAny(req.Permissions)}

	resp, err := h.provider.TestIamPermissions(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}
