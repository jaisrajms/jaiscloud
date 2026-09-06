// Package secret implements the Google Cloud Secret Manager provider.
package secretmanager

import (
	"context"
	"encoding/base64"
	"errors"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// rtSecretPolicy is the generic ResourceStore type for secret IAM policies.
const rtSecretPolicy = "gcp_secret_policy"

// Provider handles Secret Manager secrets and versions.
type Provider struct {
	secrets   secretmanagerstore.Store
	resources store.ResourceStore // IAM policies (control-plane)
	encryptor crypto.EnvelopeEncryptor
}

func New(secrets secretmanagerstore.Store, resources store.ResourceStore, encryptor crypto.EnvelopeEncryptor) *Provider {
	return &Provider{secrets: secrets, resources: resources, encryptor: encryptor}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Secret.Create":             p.Create,
		"Secret.List":               p.List,
		"Secret.Get":                p.Get,
		"Secret.Update":             p.Update,
		"Secret.Delete":             p.Delete,
		"Secret.AddVersion":         p.AddVersion,
		"Secret.Access":             p.Access,
		"Secret.GetVersion":         p.GetVersion,
		"Secret.DestroyVersion":     p.DestroyVersion,
		"Secret.DisableVersion":     p.DisableVersion,
		"Secret.EnableVersion":      p.EnableVersion,
		"Secret.GetIamPolicy":       p.GetIamPolicy,
		"Secret.SetIamPolicy":       p.SetIamPolicy,
		"Secret.TestIamPermissions": p.TestIamPermissions,
	}
}

type secretMeta struct {
	Name           string
	Labels         map[string]string
	CreateTime     string
	NextVer        int
	Rotation       *secretmanagerstore.Rotation
	VersionAliases map[string]int
	KmsKeyName     string
}

type versionMeta struct {
	Name       string `json:"name"`
	State      string `json:"state"`
	CreateTime string `json:"createTime"`
	Data       string `json:"data"` // base64 payload
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return n, nil
}

// parseSecretName splits a relative resource name or full GCP name into the secret ID and version.
func parseSecretName(name string) (secret, version string) {
	if i := strings.Index(name, "/secrets/"); i >= 0 {
		name = name[i+len("/secrets/"):]
	} else {
		name = strings.TrimPrefix(name, "secrets/")
	}
	if i := strings.Index(name, "/versions/"); i >= 0 {
		return name[:i], name[i+len("/versions/"):]
	}
	return name, ""
}

// secretID extracts the secret ID from a relative name or a full GCP name.
func secretID(name string) string {
	if i := strings.Index(name, "/secrets/"); i >= 0 {
		return name[i+len("/secrets/"):]
	}
	return strings.TrimPrefix(name, "secrets/")
}

func toStoreSecret(m secretMeta) secretmanagerstore.Secret {
	tc, _ := time.Parse(time.RFC3339Nano, m.CreateTime)
	return secretmanagerstore.Secret{
		ID: secretID(m.Name), Labels: m.Labels, CreateTime: tc, NextVer: m.NextVer,
		Rotation: m.Rotation, VersionAliases: m.VersionAliases,
		KmsKeyName: m.KmsKeyName,
	}
}

func fromStoreSecret(nr *model.NormalizedRequest, s secretmanagerstore.Secret) secretMeta {
	m := secretMeta{Name: nr.ResourceID("secret", s.ID), Labels: s.Labels, NextVer: s.NextVer, Rotation: s.Rotation, VersionAliases: s.VersionAliases, KmsKeyName: s.KmsKeyName}
	if !s.CreateTime.IsZero() {
		m.CreateTime = s.CreateTime.Format(time.RFC3339Nano)
	}
	return m
}

// rotationFromBody extracts the rotation schedule from a request body.
func rotationFromBody(body map[string]any) *secretmanagerstore.Rotation {
	r, ok := body["rotation"].(map[string]any)
	if !ok {
		return nil
	}
	out := &secretmanagerstore.Rotation{}
	if v, ok := r["nextRotationTime"].(string); ok {
		out.NextRotationTime = v
	}
	if v, ok := r["rotationPeriod"].(string); ok {
		out.RotationPeriod = v
	}
	return out
}

// versionAliasesFromBody extracts the versionAliases map (alias → version
// number) from a request body. Values arrive as float64 from parseJSON.
func versionAliasesFromBody(body map[string]any) map[string]int {
	m, ok := body["versionAliases"].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		if f, ok := v.(float64); ok {
			out[k] = int(f)
		}
	}
	return out
}

func kmsKeyNameFromBody(body map[string]any) string {
	if repl, ok := body["replication"].(map[string]any); ok {
		if auto, ok := repl["automatic"].(map[string]any); ok {
			if cmek, ok := auto["customerManagedEncryption"].(map[string]any); ok {
				if k, ok := cmek["kmsKeyName"].(string); ok {
					return k
				}
			}
		}
		if um, ok := repl["userManaged"].(map[string]any); ok {
			if reps, ok := um["replicas"].([]any); ok && len(reps) > 0 {
				if rMap, ok := reps[0].(map[string]any); ok {
					if cmek, ok := rMap["customerManagedEncryption"].(map[string]any); ok {
						if k, ok := cmek["kmsKeyName"].(string); ok {
							return k
						}
					}
				}
			}
		}
	}
	return ""
}

func fromStoreVersion(nr *model.NormalizedRequest, v secretmanagerstore.Version) versionMeta {
	m := versionMeta{
		Name:  nr.ResourceID("secret", v.SecretID) + "/versions/" + v.VersionID,
		State: v.State,
		Data:  v.Data,
	}
	if !v.CreateTime.IsZero() {
		m.CreateTime = v.CreateTime.Format(time.RFC3339Nano)
	}
	return m
}

func (p *Provider) Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	id, _ := nr.Params["secretId"].(string)
	if id == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			if n, _ := body["name"].(string); n != "" {
				id = secretID(n)
			}
		}
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing secretId", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	m := secretMeta{
		Name:       nr.ResourceID("secret", id),
		CreateTime: clock.Now().UTC().Format(time.RFC3339Nano),
		NextVer:    1,
	}
	if labels, ok := body["labels"].(map[string]any); ok {
		m.Labels = make(map[string]string, len(labels))
		for k, v := range labels {
			if s, ok := v.(string); ok {
				m.Labels[k] = s
			}
		}
	}
	if r := rotationFromBody(body); r != nil {
		m.Rotation = r
	}
	if va := versionAliasesFromBody(body); va != nil {
		m.VersionAliases = va
	}
	if kmsKeyName := kmsKeyNameFromBody(body); kmsKeyName != "" {
		m.KmsKeyName = kmsKeyName
	}
	if err := p.secrets.CreateSecret(ctx, nr.AccountID, id, toStoreSecret(m)); err != nil {
		if errors.Is(err, secretmanagerstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "secret already exists", 409)
		}
		return nil, err
	}
	return provider.OK(secretToMap(m)), nil
}

func (p *Provider) List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	secrets, err := p.secrets.ListSecrets(ctx, nr.AccountID)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(secrets, func(s secretmanagerstore.Secret) string { return s.ID }, nr.Params)

	items := make([]any, 0, len(page))
	for _, s := range page {
		items = append(items, secretToMap(fromStoreSecret(nr, s)))
	}
	resp := map[string]any{"secrets": items, "totalSize": len(secrets)}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	s, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	s = p.maybeRotate(ctx, nr.AccountID, id, s)
	return provider.OK(secretToMap(fromStoreSecret(nr, s))), nil
}

func (p *Provider) Update(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	s, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	m := fromStoreSecret(nr, s)
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if labels, ok := body["labels"].(map[string]any); ok {
			m.Labels = make(map[string]string, len(labels))
			for k, v := range labels {
				if sv, ok := v.(string); ok {
					m.Labels[k] = sv
				}
			}
		}
		if r := rotationFromBody(body); r != nil {
			m.Rotation = r
		}
		if va := versionAliasesFromBody(body); va != nil {
			m.VersionAliases = va
		}
		if kmsKeyName := kmsKeyNameFromBody(body); kmsKeyName != "" {
			m.KmsKeyName = kmsKeyName
		}
	}
	if err := p.secrets.UpdateSecret(ctx, nr.AccountID, id, toStoreSecret(m)); err != nil {
		return nil, mapSecretErr(err)
	}
	return provider.OK(secretToMap(m)), nil
}

// maybeRotate advances a secret's rotation schedule when it is due: it creates
// an empty version and advances nextRotationTime by rotationPeriod. This is
// GCP's automatic-rotation behavior, evaluated lazily on read (no background
// worker), mirroring how AWS SQS enforces visibility lazily on receive.
func (p *Provider) maybeRotate(ctx context.Context, account, id string, s secretmanagerstore.Secret) secretmanagerstore.Secret {
	if s.Rotation == nil || s.Rotation.NextRotationTime == "" {
		return s
	}
	next, err := time.Parse(time.RFC3339Nano, s.Rotation.NextRotationTime)
	if err != nil || clock.Now().Before(next) {
		return s
	}
	if s.Rotation.RotationPeriod != "" {
		if d, err := time.ParseDuration(s.Rotation.RotationPeriod); err == nil {
			s.Rotation.NextRotationTime = clock.Now().Add(d).UTC().Format(time.RFC3339Nano)
		}
	}
	ver, err := p.secrets.NextVersion(ctx, account, id)
	if err != nil {
		return s
	}
	s.NextVer = ver + 1
	_ = p.secrets.CreateVersion(ctx, account, secretmanagerstore.Version{
		SecretID: id, VersionID: strconv.Itoa(ver), State: "ENABLED", CreateTime: clock.Now(),
	})
	_ = p.secrets.UpdateSecret(ctx, account, id, s)
	return s
}

func (p *Provider) Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if err := p.secrets.DeleteSecret(ctx, nr.AccountID, secretID(name)); err != nil {
		return nil, mapSecretErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func (p *Provider) AddVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	body, _ := nr.Params["body"].(map[string]any)
	payload := ""
	if pl, ok := body["payload"].(map[string]any); ok {
		if d, _ := pl["data"].(string); d != "" {
			payload = d
		}
	}

	// Allocate the version number atomically (memory mutex / DB sequence-like
	// UPDATE ... RETURNING), avoiding a Get→increment→Update race.
	ver, err := p.secrets.NextVersion(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	version := strconv.Itoa(ver)

	sec, err := p.secrets.GetSecret(ctx, nr.AccountID, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}

	payloadBytes, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "payload is not base64", 400)
	}

	rawDEK, wrappedDEK, err := p.encryptor.Wrap(ctx, nr.AccountID, sec.KmsKeyName)
	if err != nil {
		return nil, err
	}

	encryptedPayloadBytes, err := kms.EncryptData(rawDEK, payloadBytes, nil)
	if err != nil {
		return nil, err
	}
	encryptedPayloadBase64 := base64.StdEncoding.EncodeToString(encryptedPayloadBytes)

	v := versionMeta{
		Name:       nr.ResourceID("secret", id) + "/versions/" + version,
		State:      "ENABLED",
		CreateTime: clock.Now().UTC().Format(time.RFC3339Nano),
		Data:       encryptedPayloadBase64,
	}
	stv := secretmanagerstore.Version{
		SecretID: id, VersionID: version, State: "ENABLED",
		CreateTime: mustParse(v.CreateTime), Data: encryptedPayloadBase64,
		KmsKeyName: sec.KmsKeyName, WrappedDEK: wrappedDEK,
	}
	if err := p.secrets.CreateVersion(ctx, nr.AccountID, stv); err != nil {
		return nil, err
	}
	return provider.OK(versionToMap(v)), nil
}

func (p *Provider) Access(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	secret, version := parseSecretName(name)
	version, err = p.resolveVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v, err := p.secrets.GetVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}

	encryptedPayloadBytes, err := base64.StdEncoding.DecodeString(v.Data)
	if err != nil {
		return nil, err
	}

	rawDEK, err := p.encryptor.Unwrap(ctx, nr.AccountID, v.KmsKeyName, v.WrappedDEK)
	if err != nil {
		return nil, err
	}

	decryptedPayloadBytes, err := kms.DecryptData(rawDEK, encryptedPayloadBytes, nil)
	if err != nil {
		return nil, err
	}
	decryptedPayloadBase64 := base64.StdEncoding.EncodeToString(decryptedPayloadBytes)

	m := fromStoreVersion(nr, v)
	m.Data = decryptedPayloadBase64

	return provider.OK(map[string]any{
		"name": m.Name,
		"payload": map[string]any{
			"data":       m.Data,
			"dataCrc32c": crc32c(m.Data),
		},
	}), nil
}

func (p *Provider) GetVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	secret, version := parseSecretName(name)
	version, err = p.resolveVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v, err := p.secrets.GetVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	return provider.OK(versionToMap(fromStoreVersion(nr, v))), nil
}

// resolveVersion resolves the "latest" version alias to the highest existing
// version number. Any other version id passes through unchanged.
func (p *Provider) resolveVersion(ctx context.Context, project, secret, version string) (string, error) {
	if version != "latest" {
		return version, nil
	}
	versions, err := p.secrets.ListVersions(ctx, project, secret)
	if err != nil {
		return "", err
	}
	latest := ""
	latestN := -1
	for _, v := range versions {
		if n, err := strconv.Atoi(v.VersionID); err == nil && n > latestN {
			latestN = n
			latest = v.VersionID
		}
	}
	if latest == "" {
		return "", secretmanagerstore.ErrNoSuchVersion
	}
	return latest, nil
}

func (p *Provider) DestroyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "DESTROYED")
}

func (p *Provider) DisableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "DISABLED")
}

func (p *Provider) EnableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setVersionState(ctx, nr, "ENABLED")
}

// setVersionState applies a lifecycle transition (DESTROYED/DISABLED/ENABLED)
// to a secret version.
func (p *Provider) setVersionState(ctx context.Context, nr *model.NormalizedRequest, state string) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	secret, version := parseSecretName(name)
	v, err := p.secrets.GetVersion(ctx, nr.AccountID, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v.State = state
	if err := p.secrets.UpdateVersion(ctx, nr.AccountID, v); err != nil {
		return nil, mapVersionErr(err)
	}
	return provider.OK(versionToMap(fromStoreVersion(nr, v))), nil
}

func (p *Provider) requireSecret(ctx context.Context, account, id string) error {
	if _, err := p.secrets.GetSecret(ctx, account, id); err != nil {
		return mapSecretErr(err)
	}
	return nil
}

func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	if err := p.requireSecret(ctx, nr.AccountID, id); err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(policy.Load(ctx, p.resources, nr.AccountID, rtSecretPolicy, id))), nil
}

func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	if err := p.requireSecret(ctx, nr.AccountID, id); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtSecretPolicy, id, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	id := secretID(name)
	if err := p.requireSecret(ctx, nr.AccountID, id); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}

func mapSecretErr(err error) error {
	if errors.Is(err, secretmanagerstore.ErrNoSuchSecret) {
		return model.NewProviderError("NotFound", "secret not found", 404)
	}
	return err
}

func mapVersionErr(err error) error {
	if errors.Is(err, secretmanagerstore.ErrNoSuchVersion) {
		return model.NewProviderError("NotFound", "secret version not found", 404)
	}
	return err
}

func mustParse(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// crc32c returns the CRC32C-Castagnoli checksum of the base64-decoded data as
// a decimal string (google.protobuf.Int64Value encoding).
func crc32c(b64 string) string {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "0"
	}
	return strconv.FormatUint(uint64(crc32.Checksum(raw, crc32.MakeTable(crc32.Castagnoli))), 10)
}

func secretToMap(m secretMeta) map[string]any {
	out := map[string]any{
		"name":       m.Name,
		"createTime": m.CreateTime,
		// etag is a stable hash of name+createTime (matches GCP's immutable-etag
		// semantics for secrets that never change their create time).
		"etag": policy.Etag(m.Name + m.CreateTime),
		"replication": map[string]any{
			"automatic": map[string]any{},
		},
	}
	if m.Labels != nil {
		out["labels"] = m.Labels
	}
	if m.Rotation != nil {
		out["rotation"] = map[string]any{
			"nextRotationTime": m.Rotation.NextRotationTime,
			"rotationPeriod":   m.Rotation.RotationPeriod,
		}
	}
	if m.VersionAliases != nil {
		out["versionAliases"] = m.VersionAliases
	}
	return out
}

func versionToMap(v versionMeta) map[string]any {
	return map[string]any{
		"name":       v.Name,
		"state":      v.State,
		"createTime": v.CreateTime,
	}
}
