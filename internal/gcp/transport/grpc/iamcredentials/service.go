// Package iamcredentials is the gRPC transport for the IAM Service Account
// Credentials API (google.iam.credentials.v1.IAMCredentials). It is a thin
// proto adapter over the transport-neutral core in
// internal/gcp/service/iamcredentials: it transcodes between the generated
// protobuf messages and the core's typed API, and maps core errors to gRPC
// status codes. It owns no business logic.
//
// Unlike REST, gRPC addresses the service by a distinct protobuf service name,
// so all four methods (GenerateAccessToken, GenerateIdToken, SignBlob, SignJwt)
// are served here with faithful request/response shapes.
package iamcredentials

import (
	"context"
	"strings"

	credentialspb "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/iamcredentials"
	"jaiscloud/internal/gcp/serviceaccount"
)

// Service implements credentialspb.IAMCredentialsServer over the shared core.
type Service struct {
	credentialspb.UnimplementedIAMCredentialsServer

	core        *core.Service
	defaultProj string
}

// NewService returns an IAM Credentials gRPC service wrapping the core.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

// splitName parses "projects/{project}/serviceAccounts/{email}" into the
// project segment and email. The project may be the "-" wildcard.
func splitName(name string) (account, email string) {
	name = strings.TrimPrefix(name, "projects/")
	if i := strings.Index(name, "/serviceAccounts/"); i >= 0 {
		return name[:i], name[i+len("/serviceAccounts/"):]
	}
	return "", ""
}

func (s *Service) account(account string) string {
	if account != "" {
		return account
	}
	return s.defaultProj
}

// GenerateAccessToken mints an OAuth2 access token.
func (s *Service) GenerateAccessToken(ctx context.Context, req *credentialspb.GenerateAccessTokenRequest) (*credentialspb.GenerateAccessTokenResponse, error) {
	account, email := splitName(req.GetName())
	if req.GetLifetime() != nil && req.GetLifetime().AsDuration() <= 0 {
		return nil, grpcutil.GRPCStatus(serviceaccount.Invalid("lifetime must be positive"))
	}
	token, expires, err := s.core.GenerateAccessToken(ctx, s.account(account), email, req.GetScope(), req.GetLifetime().AsDuration())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return &credentialspb.GenerateAccessTokenResponse{
		AccessToken: token,
		ExpireTime:  timestamppb.New(expires),
	}, nil
}

// GenerateIdToken mints an OpenID Connect ID token.
func (s *Service) GenerateIdToken(ctx context.Context, req *credentialspb.GenerateIdTokenRequest) (*credentialspb.GenerateIdTokenResponse, error) {
	account, email := splitName(req.GetName())
	token, err := s.core.GenerateIDToken(ctx, s.account(account), email, req.GetAudience(), req.GetIncludeEmail())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return &credentialspb.GenerateIdTokenResponse{Token: token}, nil
}

// SignBlob signs a blob with the account's emulator key.
func (s *Service) SignBlob(ctx context.Context, req *credentialspb.SignBlobRequest) (*credentialspb.SignBlobResponse, error) {
	account, email := splitName(req.GetName())
	keyID, sig, err := s.core.SignBlob(ctx, s.account(account), email, req.GetPayload())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return &credentialspb.SignBlobResponse{KeyId: keyID, SignedBlob: sig}, nil
}

// SignJwt signs a caller-supplied JWT claims set.
func (s *Service) SignJwt(ctx context.Context, req *credentialspb.SignJwtRequest) (*credentialspb.SignJwtResponse, error) {
	account, email := splitName(req.GetName())
	keyID, signed, err := s.core.SignJWT(ctx, s.account(account), email, req.GetPayload())
	if err != nil {
		return nil, grpcutil.GRPCStatus(err)
	}
	return &credentialspb.SignJwtResponse{KeyId: keyID, SignedJwt: signed}, nil
}

// compile-time assertion that Service implements the generated server.
var _ credentialspb.IAMCredentialsServer = (*Service)(nil)
