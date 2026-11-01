// Package iam is the gRPC transport for the IAM Admin API
// (google.iam.admin.v1.IAM). It is a thin proto adapter over the IAM provider
// in internal/gcp/provider/iam, which is the transport-neutral home of the
// service-account surface (the same code the REST codec dispatches to), so the
// two transports cannot drift.
//
// The admin proto declares the service-account lifecycle, its keys (including
// projects.serviceAccounts.keys.upload, DC27), the deprecated signBlob/signJwt,
// and service-account IAM policies; all of those are served here. The role
// RPCs (ListRoles/GetRole/CreateRole/.../QueryGrantableRoles/
// QueryTestablePermissions/QueryAuditableServices/LintPolicy) are not modelled
// by the emulator on any transport, so they stay Unimplemented — the fidelity
// matrix records them as explicit unsupported stubs.
//
// Request/response bodies cross the adapter as JSON objects: the provider
// consumes and produces the Discovery-shaped maps the REST codec uses, and
// protojson converts them at the wire boundary. That keeps all business logic
// in the provider — this file only addresses resources by proto field.
package iam

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	adminpb "cloud.google.com/go/iam/admin/apiv1/adminpb"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	iamprovider "jaiscloud/internal/gcp/provider/iam"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Service implements adminpb.IAMServer over the IAM provider.
type Service struct {
	adminpb.UnimplementedIAMServer

	provider *iamprovider.Provider
}

// NewService returns the IAM Admin gRPC service wrapping the provider.
func NewService(p *iamprovider.Provider) *Service {
	return &Service{provider: p}
}

// marshal/unmarshal are the protojson options of the map<->proto bridge. The
// provider's maps carry a couple of Discovery-only fields the proto does not
// model (a key's disableTime), so unknown fields are discarded rather than
// failing the response.
var (
	marshal   = protojson.MarshalOptions{}
	unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// splitName splits a fully qualified resource name
// ("projects/{project}/serviceAccounts/{id}[/keys/{key}]") into the project and
// the provider-relative name. Admin RPCs always carry the projects prefix, so
// there is no default-project fallback here.
func splitName(name string) (account, relative string, ok bool) {
	trimmed, found := strings.CutPrefix(name, "projects/")
	if !found {
		return "", "", false
	}
	if i := strings.IndexByte(trimmed, '/'); i >= 0 {
		return trimmed[:i], trimmed[i+1:], true
	}
	return trimmed, "", true
}

// request builds the provider request for a resource name, allowing an empty
// relative part (the parent of a list call).
func (s *Service) request(name string) (*model.NormalizedRequest, error) {
	account, relative, ok := splitName(name)
	if !ok || account == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource name", 400)
	}
	return &model.NormalizedRequest{
		AccountID:  account,
		Params:     map[string]any{"name": relative},
		ResourceID: resource.ResourceID(account),
	}, nil
}

// requestResource is request for a method that must address a resource (never a
// bare project).
func (s *Service) requestResource(name string) (*model.NormalizedRequest, error) {
	nr, err := s.request(name)
	if err != nil {
		return nil, err
	}
	if nr.Params["name"] == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return nr, nil
}

// call runs one provider handler and returns its Discovery-shaped response map.
func (s *Service) call(ctx context.Context, nr *model.NormalizedRequest, handler provider.HandlerFunc) (map[string]any, error) {
	resp, err := handler(ctx, nr)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return resp.Data, nil
}

// toProto decodes a provider response map into a protobuf response.
func toProto(src map[string]any, out proto.Message) error {
	raw, err := json.Marshal(src)
	if err != nil {
		return status.Error(codes.Internal, "encode response: "+err.Error())
	}
	if err := unmarshal.Unmarshal(raw, out); err != nil {
		return status.Error(codes.Internal, "decode response: "+err.Error())
	}
	return nil
}

// toMap renders a protobuf message as the Discovery-shaped request map the
// provider consumes.
func toMap(m proto.Message) (map[string]any, error) {
	raw, err := marshal.Marshal(m)
	if err != nil {
		return nil, status.Error(codes.Internal, "encode request: "+err.Error())
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, status.Error(codes.Internal, "decode request: "+err.Error())
	}
	return out, nil
}

// ─── Service accounts ────────────────────────────────────────────────────────

// ListServiceAccounts lists the project's service accounts.
func (s *Service) ListServiceAccounts(ctx context.Context, req *adminpb.ListServiceAccountsRequest) (*adminpb.ListServiceAccountsResponse, error) {
	nr, err := s.request(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	if req.GetPageSize() != 0 {
		nr.Params["pageSize"] = req.GetPageSize()
	}
	if req.GetPageToken() != "" {
		nr.Params["pageToken"] = req.GetPageToken()
	}
	data, err := s.call(ctx, nr, s.provider.List)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ListServiceAccountsResponse{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetServiceAccount returns one service account.
func (s *Service) GetServiceAccount(ctx context.Context, req *adminpb.GetServiceAccountRequest) (*adminpb.ServiceAccount, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	data, err := s.call(ctx, nr, s.provider.Get)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ServiceAccount{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// CreateServiceAccount creates a service account in the request's project.
func (s *Service) CreateServiceAccount(ctx context.Context, req *adminpb.CreateServiceAccountRequest) (*adminpb.ServiceAccount, error) {
	nr, err := s.request(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	nr.Params["accountId"] = req.GetAccountId()
	body := map[string]any{"accountId": req.GetAccountId()}
	if req.GetServiceAccount() != nil {
		sa, err := toMap(req.GetServiceAccount())
		if err != nil {
			return nil, err
		}
		body["serviceAccount"] = sa
	}
	nr.Params["body"] = body
	data, err := s.call(ctx, nr, s.provider.Create)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ServiceAccount{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// UpdateServiceAccount replaces the mutable fields (display name/description).
func (s *Service) UpdateServiceAccount(ctx context.Context, req *adminpb.ServiceAccount) (*adminpb.ServiceAccount, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	body, err := toMap(req)
	if err != nil {
		return nil, err
	}
	nr.Params["body"] = body
	data, err := s.call(ctx, nr, s.provider.Update)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ServiceAccount{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// PatchServiceAccount applies the request's field mask.
func (s *Service) PatchServiceAccount(ctx context.Context, req *adminpb.PatchServiceAccountRequest) (*adminpb.ServiceAccount, error) {
	nr, err := s.requestResource(req.GetServiceAccount().GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	body := map[string]any{}
	if req.GetServiceAccount() != nil {
		sa, err := toMap(req.GetServiceAccount())
		if err != nil {
			return nil, err
		}
		body["serviceAccount"] = sa
	}
	if mask := req.GetUpdateMask(); mask != nil {
		body["updateMask"] = strings.Join(mask.GetPaths(), ",")
	}
	nr.Params["body"] = body
	data, err := s.call(ctx, nr, s.provider.Update)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ServiceAccount{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteServiceAccount deletes a service account (keeping an undelete
// tombstone).
func (s *Service) DeleteServiceAccount(ctx context.Context, req *adminpb.DeleteServiceAccountRequest) (*emptypb.Empty, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	if _, err := s.call(ctx, nr, s.provider.Delete); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// UndeleteServiceAccount restores a deleted service account.
func (s *Service) UndeleteServiceAccount(ctx context.Context, req *adminpb.UndeleteServiceAccountRequest) (*adminpb.UndeleteServiceAccountResponse, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	data, err := s.call(ctx, nr, s.provider.Undelete)
	if err != nil {
		return nil, err
	}
	out := &adminpb.UndeleteServiceAccountResponse{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// EnableServiceAccount enables a disabled service account.
func (s *Service) EnableServiceAccount(ctx context.Context, req *adminpb.EnableServiceAccountRequest) (*emptypb.Empty, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	if _, err := s.call(ctx, nr, s.provider.Enable); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// DisableServiceAccount disables a service account.
func (s *Service) DisableServiceAccount(ctx context.Context, req *adminpb.DisableServiceAccountRequest) (*emptypb.Empty, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	if _, err := s.call(ctx, nr, s.provider.Disable); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ─── Service-account keys ────────────────────────────────────────────────────

// keysName returns the provider-relative keys collection for a request whose
// name is the service account.
func (s *Service) keysName(ctx context.Context, name string) (*model.NormalizedRequest, error) {
	nr, err := s.requestResource(name)
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	relative, _ := nr.Params["name"].(string)
	nr.Params["name"] = relative + "/keys"
	return nr, nil
}

// ListServiceAccountKeys lists a service account's keys.
func (s *Service) ListServiceAccountKeys(ctx context.Context, req *adminpb.ListServiceAccountKeysRequest) (*adminpb.ListServiceAccountKeysResponse, error) {
	nr, err := s.keysName(ctx, req.GetName())
	if err != nil {
		return nil, err
	}
	if types := req.GetKeyTypes(); len(types) > 0 {
		names := make([]string, 0, len(types))
		for _, t := range types {
			names = append(names, t.String())
		}
		nr.Params["keyTypes"] = strings.Join(names, ",")
	}
	data, err := s.call(ctx, nr, s.provider.ServiceAccountKeyList)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ListServiceAccountKeysResponse{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// GetServiceAccountKey returns one key (carrying its publicKeyData).
func (s *Service) GetServiceAccountKey(ctx context.Context, req *adminpb.GetServiceAccountKeyRequest) (*adminpb.ServiceAccountKey, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	data, err := s.call(ctx, nr, s.provider.ServiceAccountKeyGet)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ServiceAccountKey{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// CreateServiceAccountKey generates a user-managed key pair.
func (s *Service) CreateServiceAccountKey(ctx context.Context, req *adminpb.CreateServiceAccountKeyRequest) (*adminpb.ServiceAccountKey, error) {
	nr, err := s.keysName(ctx, req.GetName())
	if err != nil {
		return nil, err
	}
	body := map[string]any{}
	if alg := req.GetKeyAlgorithm(); alg != adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_UNSPECIFIED {
		body["keyAlgorithm"] = alg.String()
	}
	nr.Params["body"] = body
	data, err := s.call(ctx, nr, s.provider.ServiceAccountKeyCreate)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ServiceAccountKey{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// UploadServiceAccountKey stores a caller-provided public key (DC27).
func (s *Service) UploadServiceAccountKey(ctx context.Context, req *adminpb.UploadServiceAccountKeyRequest) (*adminpb.ServiceAccountKey, error) {
	nr, err := s.keysName(ctx, req.GetName())
	if err != nil {
		return nil, err
	}
	nr.Params["body"] = map[string]any{"publicKeyData": base64.StdEncoding.EncodeToString(req.GetPublicKeyData())}
	data, err := s.call(ctx, nr, s.provider.ServiceAccountKeyUpload)
	if err != nil {
		return nil, err
	}
	out := &adminpb.ServiceAccountKey{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// DeleteServiceAccountKey deletes a key.
func (s *Service) DeleteServiceAccountKey(ctx context.Context, req *adminpb.DeleteServiceAccountKeyRequest) (*emptypb.Empty, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	if _, err := s.call(ctx, nr, s.provider.ServiceAccountKeyDelete); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// DisableServiceAccountKey disables a key.
func (s *Service) DisableServiceAccountKey(ctx context.Context, req *adminpb.DisableServiceAccountKeyRequest) (*emptypb.Empty, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	if _, err := s.call(ctx, nr, s.provider.ServiceAccountKeyDisable); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// EnableServiceAccountKey re-enables a disabled key.
func (s *Service) EnableServiceAccountKey(ctx context.Context, req *adminpb.EnableServiceAccountKeyRequest) (*emptypb.Empty, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	if _, err := s.call(ctx, nr, s.provider.ServiceAccountKeyEnable); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ─── Signing (deprecated in favour of the IAM Credentials API) ───────────────

// SignBlob signs a blob with the account's system-managed key.
func (s *Service) SignBlob(ctx context.Context, req *adminpb.SignBlobRequest) (*adminpb.SignBlobResponse, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	nr.Params["body"] = map[string]any{"bytesToSign": base64.StdEncoding.EncodeToString(req.GetBytesToSign())}
	data, err := s.call(ctx, nr, s.provider.ServiceAccountSignBlob)
	if err != nil {
		return nil, err
	}
	out := &adminpb.SignBlobResponse{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// SignJwt signs a caller-supplied JWT claims set.
func (s *Service) SignJwt(ctx context.Context, req *adminpb.SignJwtRequest) (*adminpb.SignJwtResponse, error) {
	nr, err := s.requestResource(req.GetName())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	nr.Params["body"] = map[string]any{"payload": req.GetPayload()}
	data, err := s.call(ctx, nr, s.provider.ServiceAccountSignJwt)
	if err != nil {
		return nil, err
	}
	out := &adminpb.SignJwtResponse{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// ─── Service-account IAM policy ──────────────────────────────────────────────

// GetIamPolicy returns the policy attached to a service account.
func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	nr, err := s.requestResource(req.GetResource())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	if v := req.GetOptions().GetRequestedPolicyVersion(); v != 0 {
		nr.Params["options.requestedPolicyVersion"] = v
	}
	data, err := s.call(ctx, nr, s.provider.GetIamPolicy)
	if err != nil {
		return nil, err
	}
	out := &iampb.Policy{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// SetIamPolicy replaces the policy attached to a service account.
func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	nr, err := s.requestResource(req.GetResource())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	body, err := toMap(req.GetPolicy())
	if err != nil {
		return nil, err
	}
	nr.Params["body"] = body
	data, err := s.call(ctx, nr, s.provider.SetIamPolicy)
	if err != nil {
		return nil, err
	}
	out := &iampb.Policy{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// TestIamPermissions echoes the caller's permissions (authz is not enforced).
func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	nr, err := s.requestResource(req.GetResource())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	perms := make([]any, 0, len(req.GetPermissions()))
	for _, p := range req.GetPermissions() {
		perms = append(perms, p)
	}
	nr.Params["body"] = map[string]any{"permissions": perms}
	data, err := s.call(ctx, nr, s.provider.TestIamPermissions)
	if err != nil {
		return nil, err
	}
	out := &iampb.TestIamPermissionsResponse{}
	if err := toProto(data, out); err != nil {
		return nil, err
	}
	return out, nil
}

// compile-time assertion that Service implements the generated server.
var _ adminpb.IAMServer = (*Service)(nil)
