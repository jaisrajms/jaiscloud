// Package secretmanager implements the Cloud Secret Manager gRPC service
// (SecretManagerService plus the google.iam.v1.IAMPolicy surface for secret
// IAM) over the same shared secretmanagerstore.Store + policy +
// crypto.EnvelopeEncryptor backing the REST provider, so REST and gRPC share
// state.
package secretmanager

import (
	"context"
	"encoding/base64"
	"errors"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// rtSecretPolicy is the generic ResourceStore type for secret IAM policies,
// matching the REST provider's persisted type so both transports share state.
const rtSecretPolicy = "gcp_secret_policy"

// Service implements secretmanagerpb.SecretManagerServiceServer and
// iampb.IAMPolicyServer over the shared stores.
type Service struct {
	secretmanagerpb.UnimplementedSecretManagerServiceServer
	iampb.UnimplementedIAMPolicyServer

	secrets     secretmanagerstore.Store
	resources   store.ResourceStore // IAM policies (control-plane)
	encryptor   crypto.EnvelopeEncryptor
	defaultProj string
}

// NewService returns a Secret Manager gRPC service backed by the shared stores.
// defaultProj is the config-default project used when a request carries none.
func NewService(secrets secretmanagerstore.Store, resources store.ResourceStore, encryptor crypto.EnvelopeEncryptor, defaultProj string) *Service {
	return &Service{secrets: secrets, resources: resources, encryptor: encryptor, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func mapSecretErr(err error) error {
	if errors.Is(err, secretmanagerstore.ErrNoSuchSecret) {
		return mapError(model.NewProviderError("NotFound", "secret not found", 404))
	}
	return mapError(err)
}

func mapVersionErr(err error) error {
	if errors.Is(err, secretmanagerstore.ErrNoSuchVersion) {
		return mapError(model.NewProviderError("NotFound", "secret version not found", 404))
	}
	return mapError(err)
}

// ─── resource-name parsing ────────────────────────────────────────────────────

func secretName(project, id string) string {
	return "projects/" + project + "/secrets/" + id
}

func versionName(project, id, ver string) string {
	return secretName(project, id) + "/versions/" + ver
}

// splitSecretResource parses a full resource name into project, secret id, and
// version. It accepts "projects/{p}/secrets/{s}" (4 parts) and
// "projects/{p}/secrets/{s}/versions/{v}" (6 parts).
func splitSecretResource(name string) (project, id, version string, ok bool) {
	parts := strings.Split(name, "/")
	switch len(parts) {
	case 4:
		if parts[0] == "projects" && parts[2] == "secrets" {
			return parts[1], parts[3], "", true
		}
	case 6:
		if parts[0] == "projects" && parts[2] == "secrets" && parts[4] == "versions" {
			return parts[1], parts[3], parts[5], true
		}
	}
	return "", "", "", false
}

// projectFromParent resolves the project from a "projects/{p}" parent (or a
// bare project id), falling back to metadata-derived project.
func (s *Service) projectFromParent(ctx context.Context, p string) string {
	if p != "" {
		return strings.TrimPrefix(p, "projects/")
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── Secret Manager service ───────────────────────────────────────────────────

func (s *Service) ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest) (*secretmanagerpb.ListSecretsResponse, error) {
	project := s.projectFromParent(ctx, req.GetParent())
	secrets, err := s.secrets.ListSecrets(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := paging.Page(secrets, func(sec secretmanagerstore.Secret) string { return sec.ID },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	out := make([]*secretmanagerpb.Secret, 0, len(page))
	for _, sec := range page {
		out = append(out, secretToProto(project, sec))
	}
	return &secretmanagerpb.ListSecretsResponse{
		Secrets:       out,
		NextPageToken: next,
		TotalSize:     int32(len(secrets)),
	}, nil
}

func (s *Service) CreateSecret(ctx context.Context, req *secretmanagerpb.CreateSecretRequest) (*secretmanagerpb.Secret, error) {
	project := s.projectFromParent(ctx, req.GetParent())
	id := req.GetSecretId()
	proto := req.GetSecret()
	if id == "" && proto != nil {
		_, id, _, _ = splitSecretResource(proto.GetName())
	}
	if id == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing secretId", 400))
	}

	sec := secretmanagerstore.Secret{
		ID:         id,
		CreateTime: clock.Now(),
		NextVer:    1,
	}
	if proto != nil {
		sec.Labels = proto.GetLabels()
		if r := proto.GetRotation(); r != nil {
			sec.Rotation = rotationFromProto(r)
		}
		if va := proto.GetVersionAliases(); va != nil {
			sec.VersionAliases = make(map[string]int, len(va))
			for k, v := range va {
				sec.VersionAliases[k] = int(v)
			}
		}
		sec.KmsKeyName = kmsKeyNameFromSecret(proto)
	}

	if err := s.secrets.CreateSecret(ctx, project, id, sec); err != nil {
		if errors.Is(err, secretmanagerstore.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "secret already exists", 409))
		}
		return nil, mapError(err)
	}
	return secretToProto(project, sec), nil
}

func (s *Service) GetSecret(ctx context.Context, req *secretmanagerpb.GetSecretRequest) (*secretmanagerpb.Secret, error) {
	project, id, _, ok := splitSecretResource(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	sec, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	sec = s.maybeRotate(ctx, project, id, sec)
	return secretToProto(project, sec), nil
}

func (s *Service) UpdateSecret(ctx context.Context, req *secretmanagerpb.UpdateSecretRequest) (*secretmanagerpb.Secret, error) {
	proto := req.GetSecret()
	if proto == nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "secret is required", 400))
	}
	project, id, _, ok := splitSecretResource(proto.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	cur, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	if labels := proto.GetLabels(); labels != nil {
		cur.Labels = labels
	}
	if r := proto.GetRotation(); r != nil {
		cur.Rotation = rotationFromProto(r)
	}
	if va := proto.GetVersionAliases(); va != nil {
		cur.VersionAliases = make(map[string]int, len(va))
		for k, v := range va {
			cur.VersionAliases[k] = int(v)
		}
	}
	if kmsKeyName := kmsKeyNameFromSecret(proto); kmsKeyName != "" {
		cur.KmsKeyName = kmsKeyName
	}
	if err := s.secrets.UpdateSecret(ctx, project, id, cur); err != nil {
		return nil, mapSecretErr(err)
	}
	return secretToProto(project, cur), nil
}

func (s *Service) DeleteSecret(ctx context.Context, req *secretmanagerpb.DeleteSecretRequest) (*emptypb.Empty, error) {
	project, id, _, ok := splitSecretResource(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.secrets.DeleteSecret(ctx, project, id); err != nil {
		return nil, mapSecretErr(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) AddSecretVersion(ctx context.Context, req *secretmanagerpb.AddSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	project, id, _, ok := splitSecretResource(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	var data []byte
	if payload := req.GetPayload(); payload != nil {
		data = payload.GetData()
	}

	// Allocate the version number atomically (memory mutex / DB UPDATE
	// ... RETURNING), avoiding a Get→increment→Update race.
	ver, err := s.secrets.NextVersion(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}
	version := strconv.Itoa(ver)

	sec, err := s.secrets.GetSecret(ctx, project, id)
	if err != nil {
		return nil, mapSecretErr(err)
	}

	rawDEK, wrappedDEK, err := s.encryptor.Wrap(ctx, project, sec.KmsKeyName)
	if err != nil {
		return nil, mapError(err)
	}
	encrypted, err := kmsstore.EncryptData(rawDEK, data, nil)
	if err != nil {
		return nil, mapError(err)
	}

	stv := secretmanagerstore.Version{
		SecretID: id, VersionID: version, State: "ENABLED",
		CreateTime: clock.Now(), Data: base64.StdEncoding.EncodeToString(encrypted),
		KmsKeyName: sec.KmsKeyName, WrappedDEK: wrappedDEK,
	}
	if err := s.secrets.CreateVersion(ctx, project, stv); err != nil {
		return nil, mapError(err)
	}
	return versionToProto(project, stv), nil
}

func (s *Service) AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	project, secret, version, ok := splitSecretResource(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	version, err := s.resolveVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v, err := s.secrets.GetVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}

	encrypted, err := base64.StdEncoding.DecodeString(v.Data)
	if err != nil {
		return nil, mapError(err)
	}
	rawDEK, err := s.encryptor.Unwrap(ctx, project, v.KmsKeyName, v.WrappedDEK)
	if err != nil {
		return nil, mapError(err)
	}
	plain, err := kmsstore.DecryptData(rawDEK, encrypted, nil)
	if err != nil {
		return nil, mapError(err)
	}

	crc := crc32cOf(plain)
	return &secretmanagerpb.AccessSecretVersionResponse{
		Name: versionName(project, secret, version),
		Payload: &secretmanagerpb.SecretPayload{
			Data:       plain,
			DataCrc32C: &crc,
		},
	}, nil
}

func (s *Service) GetSecretVersion(ctx context.Context, req *secretmanagerpb.GetSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	project, secret, version, ok := splitSecretResource(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	version, err := s.resolveVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v, err := s.secrets.GetVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	return versionToProto(project, v), nil
}

// resolveVersion resolves the "latest" version alias to the highest existing
// version number. Any other version id passes through unchanged.
func (s *Service) resolveVersion(ctx context.Context, project, secret, version string) (string, error) {
	if version != "latest" {
		return version, nil
	}
	versions, err := s.secrets.ListVersions(ctx, project, secret)
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

func (s *Service) ListSecretVersions(ctx context.Context, req *secretmanagerpb.ListSecretVersionsRequest) (*secretmanagerpb.ListSecretVersionsResponse, error) {
	project, id, _, ok := splitSecretResource(req.GetParent())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid parent resource name", 400))
	}
	versions, err := s.secrets.ListVersions(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := paging.Page(versions, func(v secretmanagerstore.Version) string { return v.VersionID },
		map[string]any{"pageSize": int(req.GetPageSize()), "pageToken": req.GetPageToken()})
	out := make([]*secretmanagerpb.SecretVersion, 0, len(page))
	for _, v := range page {
		out = append(out, versionToProto(project, v))
	}
	return &secretmanagerpb.ListSecretVersionsResponse{
		Versions:      out,
		NextPageToken: next,
		TotalSize:     int32(len(versions)),
	}, nil
}

func (s *Service) DisableSecretVersion(ctx context.Context, req *secretmanagerpb.DisableSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	return s.setVersionState(ctx, req.GetName(), "DISABLED")
}

func (s *Service) EnableSecretVersion(ctx context.Context, req *secretmanagerpb.EnableSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	return s.setVersionState(ctx, req.GetName(), "ENABLED")
}

func (s *Service) DestroySecretVersion(ctx context.Context, req *secretmanagerpb.DestroySecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	return s.setVersionState(ctx, req.GetName(), "DESTROYED")
}

// setVersionState applies a lifecycle transition (DISABLED/ENABLED/DESTROYED)
// to a secret version.
func (s *Service) setVersionState(ctx context.Context, name, state string) (*secretmanagerpb.SecretVersion, error) {
	project, secret, version, ok := splitSecretResource(name)
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	v, err := s.secrets.GetVersion(ctx, project, secret, version)
	if err != nil {
		return nil, mapVersionErr(err)
	}
	v.State = state
	if err := s.secrets.UpdateVersion(ctx, project, v); err != nil {
		return nil, mapVersionErr(err)
	}
	return versionToProto(project, v), nil
}

// maybeRotate advances a secret's rotation schedule when it is due: it creates
// an empty version and advances nextRotationTime by rotationPeriod. This is
// GCP's automatic-rotation behavior, evaluated lazily on read (mirrors the
// REST provider).
func (s *Service) maybeRotate(ctx context.Context, account, id string, sec secretmanagerstore.Secret) secretmanagerstore.Secret {
	if sec.Rotation == nil || sec.Rotation.NextRotationTime == "" {
		return sec
	}
	next, err := time.Parse(time.RFC3339Nano, sec.Rotation.NextRotationTime)
	if err != nil || clock.Now().Before(next) {
		return sec
	}
	if sec.Rotation.RotationPeriod != "" {
		if d, err := time.ParseDuration(sec.Rotation.RotationPeriod); err == nil {
			sec.Rotation.NextRotationTime = clock.Now().Add(d).UTC().Format(time.RFC3339Nano)
		}
	}
	ver, err := s.secrets.NextVersion(ctx, account, id)
	if err != nil {
		return sec
	}
	sec.NextVer = ver + 1
	_ = s.secrets.CreateVersion(ctx, account, secretmanagerstore.Version{
		SecretID: id, VersionID: strconv.Itoa(ver), State: "ENABLED", CreateTime: clock.Now(),
	})
	_ = s.secrets.UpdateSecret(ctx, account, id, sec)
	return sec
}

// ─── IAM (google.iam.v1.IAMPolicy over secrets) ───────────────────────────────

func (s *Service) requireSecret(ctx context.Context, project, id string) error {
	if _, err := s.secrets.GetSecret(ctx, project, id); err != nil {
		return mapSecretErr(err)
	}
	return nil
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	project, id, _, ok := splitSecretResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireSecret(ctx, project, id); err != nil {
		return nil, err
	}
	return policyToProto(policy.Load(ctx, s.resources, project, rtSecretPolicy, id)), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	project, id, _, ok := splitSecretResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireSecret(ctx, project, id); err != nil {
		return nil, err
	}
	pol, err := policy.Set(ctx, s.resources, project, rtSecretPolicy, id, protoPolicyToBody(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	project, id, _, ok := splitSecretResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireSecret(ctx, project, id); err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: policy.TestPermissions(req.GetPermissions())}, nil
}

// ─── proto ↔ internal transcoding ─────────────────────────────────────────────

func secretToProto(project string, s secretmanagerstore.Secret) *secretmanagerpb.Secret {
	out := &secretmanagerpb.Secret{
		Name: secretName(project, s.ID),
		// etag is a stable hash of name+createTime (matches GCP's
		// immutable-etag semantics and the REST provider's computation).
		Etag: policy.Etag(secretName(project, s.ID) + s.CreateTime.Format(time.RFC3339Nano)),
		Replication: &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_Automatic_{
				Automatic: &secretmanagerpb.Replication_Automatic{},
			},
		},
	}
	if !s.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(s.CreateTime)
	}
	if s.Labels != nil {
		out.Labels = s.Labels
	}
	if s.Rotation != nil {
		out.Rotation = rotationToProto(s.Rotation)
	}
	if s.VersionAliases != nil {
		out.VersionAliases = make(map[string]int64, len(s.VersionAliases))
		for k, v := range s.VersionAliases {
			out.VersionAliases[k] = int64(v)
		}
	}
	return out
}

func versionToProto(project string, v secretmanagerstore.Version) *secretmanagerpb.SecretVersion {
	out := &secretmanagerpb.SecretVersion{
		Name:  versionName(project, v.SecretID, v.VersionID),
		State: stateToProto(v.State),
	}
	if !v.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(v.CreateTime)
	}
	return out
}

func stateToProto(state string) secretmanagerpb.SecretVersion_State {
	switch state {
	case "ENABLED":
		return secretmanagerpb.SecretVersion_ENABLED
	case "DISABLED":
		return secretmanagerpb.SecretVersion_DISABLED
	case "DESTROYED":
		return secretmanagerpb.SecretVersion_DESTROYED
	}
	return secretmanagerpb.SecretVersion_STATE_UNSPECIFIED
}

func rotationFromProto(r *secretmanagerpb.Rotation) *secretmanagerstore.Rotation {
	out := &secretmanagerstore.Rotation{}
	if r.GetNextRotationTime() != nil {
		out.NextRotationTime = r.GetNextRotationTime().AsTime().UTC().Format(time.RFC3339Nano)
	}
	if r.GetRotationPeriod() != nil {
		out.RotationPeriod = r.GetRotationPeriod().AsDuration().String()
	}
	return out
}

func rotationToProto(r *secretmanagerstore.Rotation) *secretmanagerpb.Rotation {
	out := &secretmanagerpb.Rotation{}
	if r.NextRotationTime != "" {
		if t, err := time.Parse(time.RFC3339Nano, r.NextRotationTime); err == nil {
			out.NextRotationTime = timestamppb.New(t)
		}
	}
	if r.RotationPeriod != "" {
		if d, err := time.ParseDuration(r.RotationPeriod); err == nil {
			out.RotationPeriod = durationpb.New(d)
		}
	}
	return out
}

// kmsKeyNameFromSecret extracts the CMEK key name from the secret's
// replication policy (mirrors the REST provider's kmsKeyNameFromBody).
func kmsKeyNameFromSecret(sec *secretmanagerpb.Secret) string {
	repl := sec.GetReplication()
	if repl == nil {
		return ""
	}
	if auto := repl.GetAutomatic(); auto != nil {
		if cme := auto.GetCustomerManagedEncryption(); cme != nil {
			return cme.GetKmsKeyName()
		}
	}
	if um := repl.GetUserManaged(); um != nil {
		if reps := um.GetReplicas(); len(reps) > 0 {
			if cme := reps[0].GetCustomerManagedEncryption(); cme != nil {
				return cme.GetKmsKeyName()
			}
		}
	}
	return ""
}

// crc32cOf returns the CRC32C-Castagnoli checksum of the payload as an int64
// (google.protobuf.Int64Value encoding).
func crc32cOf(data []byte) int64 {
	return int64(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
}

func protoPolicyToBody(p *iampb.Policy) map[string]any {
	body := map[string]any{}
	if p == nil {
		return body
	}
	bindings := make([]any, 0, len(p.GetBindings()))
	for _, b := range p.GetBindings() {
		members := make([]any, 0, len(b.GetMembers()))
		for _, m := range b.GetMembers() {
			members = append(members, m)
		}
		bindings = append(bindings, map[string]any{"role": b.GetRole(), "members": members})
	}
	body["bindings"] = bindings
	if et := p.GetEtag(); len(et) > 0 {
		body["etag"] = string(et)
	}
	if p.GetVersion() != 0 {
		body["version"] = int(p.GetVersion())
	}
	return body
}

func policyToProto(p policy.Policy) *iampb.Policy {
	out := &iampb.Policy{Version: int32(p.Version)}
	if p.Etag != "" {
		out.Etag = []byte(p.Etag)
	}
	for _, b := range p.Bindings {
		m, ok := b.(map[string]any)
		if !ok {
			continue
		}
		binding := &iampb.Binding{}
		binding.Role, _ = m["role"].(string)
		for _, v := range toStrings(m["members"]) {
			binding.Members = append(binding.Members, v)
		}
		out.Bindings = append(out.Bindings, binding)
	}
	return out
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
