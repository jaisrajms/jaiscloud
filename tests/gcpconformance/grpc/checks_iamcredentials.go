package grpcconformance

import (
	"context"
	"fmt"
	"strings"
	"time"

	credentials "cloud.google.com/go/iam/credentials/apiv1"
	credentialspb "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// iamCredentialsEmail is the run-addressed service account the checks
// impersonate. The account need not exist: GenerateAccessToken/GenerateIdToken
// lazily create an emulator signing key (a documented emulator divergence).
func iamCredentialsEmail(cfg Config) string {
	return fmt.Sprintf("conformance@%s.iam.gserviceaccount.com", cfg.Project)
}

// newIamCredentialsClient dials the IAM Credentials RPC service at the emulator
// with insecure transport and no authentication, mirroring the other official
// clients in this suite.
func newIamCredentialsClient(ctx context.Context, cfg Config) (*credentials.IamCredentialsClient, error) {
	return credentials.NewIamCredentialsClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// iamCredentialsChecks covers the four google.iam.credentials.v1.IAMCredentials
// RPCs. There is no path collision on gRPC, so all four — including signBlob
// and signJwt, which share a REST path with iam — are exercised directly.
func iamCredentialsChecks() []Check {
	return []Check{
		{Service: "iamcredentials", RPC: "GenerateAccessToken", KeyField: "accessToken is a non-empty opaque token; expireTime is in the future", Run: checkIamCredentialsGenerateAccessToken},
		{Service: "iamcredentials", RPC: "GenerateIdToken", KeyField: "token is a well-formed JWT", Run: checkIamCredentialsGenerateIdToken},
		{Service: "iamcredentials", RPC: "SignBlob", KeyField: "keyId + non-empty signedBlob", Run: checkIamCredentialsSignBlob},
		{Service: "iamcredentials", RPC: "SignJwt", KeyField: "keyId + well-formed signedJwt", Run: checkIamCredentialsSignJwt},
	}
}

func checkIamCredentialsGenerateAccessToken(ctx context.Context, cfg Config) error {
	client, err := newIamCredentialsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM Credentials client: %w", err)
	}
	defer client.Close()

	resp, err := client.GenerateAccessToken(ctx, &credentialspb.GenerateAccessTokenRequest{
		Name:     fmt.Sprintf("projects/-/serviceAccounts/%s", iamCredentialsEmail(cfg)),
		Scope:    []string{"https://www.googleapis.com/auth/cloud-platform"},
		Lifetime: durationpb.New(time.Hour),
	})
	if err != nil {
		return fmt.Errorf("GenerateAccessToken: %w", err)
	}
	if resp.GetAccessToken() == "" {
		return fmt.Errorf("GenerateAccessToken accessToken is empty")
	}
	if resp.GetExpireTime() == nil || !resp.GetExpireTime().AsTime().After(time.Now()) {
		return fmt.Errorf("GenerateAccessToken expireTime = %v, want a future timestamp", resp.GetExpireTime())
	}
	return nil
}

func checkIamCredentialsGenerateIdToken(ctx context.Context, cfg Config) error {
	client, err := newIamCredentialsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM Credentials client: %w", err)
	}
	defer client.Close()

	resp, err := client.GenerateIdToken(ctx, &credentialspb.GenerateIdTokenRequest{
		Name:         fmt.Sprintf("projects/-/serviceAccounts/%s", iamCredentialsEmail(cfg)),
		Audience:     "https://example.com",
		IncludeEmail: true,
	})
	if err != nil {
		return fmt.Errorf("GenerateIdToken: %w", err)
	}
	if strings.Count(resp.GetToken(), ".") != 2 {
		return fmt.Errorf("GenerateIdToken token is not a JWT: %q", resp.GetToken())
	}
	return nil
}

func checkIamCredentialsSignBlob(ctx context.Context, cfg Config) error {
	client, err := newIamCredentialsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM Credentials client: %w", err)
	}
	defer client.Close()

	resp, err := client.SignBlob(ctx, &credentialspb.SignBlobRequest{
		Name:    fmt.Sprintf("projects/-/serviceAccounts/%s", iamCredentialsEmail(cfg)),
		Payload: []byte("conformance"),
	})
	if err != nil {
		return fmt.Errorf("SignBlob: %w", err)
	}
	if resp.GetKeyId() == "" || len(resp.GetSignedBlob()) == 0 {
		return fmt.Errorf("SignBlob returned keyId=%q signedBlob len=%d", resp.GetKeyId(), len(resp.GetSignedBlob()))
	}
	return nil
}

func checkIamCredentialsSignJwt(ctx context.Context, cfg Config) error {
	client, err := newIamCredentialsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM Credentials client: %w", err)
	}
	defer client.Close()

	resp, err := client.SignJwt(ctx, &credentialspb.SignJwtRequest{
		Name:    fmt.Sprintf("projects/-/serviceAccounts/%s", iamCredentialsEmail(cfg)),
		Payload: `{"iss":"conformance"}`,
	})
	if err != nil {
		return fmt.Errorf("SignJwt: %w", err)
	}
	if resp.GetKeyId() == "" || strings.Count(resp.GetSignedJwt(), ".") != 2 {
		return fmt.Errorf("SignJwt returned keyId=%q signedJwt=%q", resp.GetKeyId(), resp.GetSignedJwt())
	}
	return nil
}
