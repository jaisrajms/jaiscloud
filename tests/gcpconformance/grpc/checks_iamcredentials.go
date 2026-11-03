package grpcconformance

import (
	"context"
	"fmt"
	"strings"
	"time"

	iamadminpb "cloud.google.com/go/iam/admin/apiv1/adminpb"
	credentials "cloud.google.com/go/iam/credentials/apiv1"
	credentialspb "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ensureIamCredentialsServiceAccount creates the run's target service account and
// returns its email plus a cleanup func. Real GCP requires the account to exist
// on every IAM Credentials method (J67), so each check provisions it through the
// IAM Admin API.
func ensureIamCredentialsServiceAccount(ctx context.Context, cfg Config) (string, func(), error) {
	client, err := newIAMAdminClient(ctx, cfg)
	if err != nil {
		return "", nil, fmt.Errorf("new IAM admin client: %w", err)
	}
	accountID := strings.ToLower(cfg.ResourceName("gcpc-ic"))
	sa, err := client.CreateServiceAccount(ctx, &iamadminpb.CreateServiceAccountRequest{
		Name:      "projects/" + cfg.Project,
		AccountId: accountID,
		ServiceAccount: &iamadminpb.ServiceAccount{
			DisplayName: "gRPC conformance IAM Credentials",
		},
	})
	if err != nil {
		client.Close()
		return "", nil, fmt.Errorf("CreateServiceAccount(%s/%s): %w", cfg.Project, accountID, err)
	}
	return sa.GetName()[strings.LastIndex(sa.GetName(), "/")+1:], func() {
		defer client.Close()
		_ = client.DeleteServiceAccount(ctx, &iamadminpb.DeleteServiceAccountRequest{Name: sa.GetName()})
	}, nil
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
		{Service: "iamcredentials", RPC: "GenerateAccessToken", KeyField: "accessToken is a non-empty opaque token; expireTime is in the future; a missing account is NOT_FOUND", Run: checkIamCredentialsGenerateAccessToken},
		{Service: "iamcredentials", RPC: "GenerateIdToken", KeyField: "token is a well-formed JWT for an existing account", Run: checkIamCredentialsGenerateIdToken},
		{Service: "iamcredentials", RPC: "SignBlob", KeyField: "keyId + non-empty signedBlob for an existing account", Run: checkIamCredentialsSignBlob},
		{Service: "iamcredentials", RPC: "SignJwt", KeyField: "keyId + well-formed signedJwt for an existing account", Run: checkIamCredentialsSignJwt},
	}
}

func checkIamCredentialsGenerateAccessToken(ctx context.Context, cfg Config) error {
	client, err := newIamCredentialsClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM Credentials client: %w", err)
	}
	defer client.Close()
	target, cleanup, err := ensureIamCredentialsServiceAccount(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	name := "projects/-/serviceAccounts/" + target
	// J67: the service account must exist. A name that resolves to no account is
	// NOT_FOUND, never a lazily minted token.
	_, err = client.GenerateAccessToken(ctx, &credentialspb.GenerateAccessTokenRequest{
		Name:     "projects/-/serviceAccounts/missing-" + target,
		Scope:    []string{"https://www.googleapis.com/auth/cloud-platform"},
		Lifetime: durationpb.New(time.Hour),
	})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("GenerateAccessToken for a missing account = %v, want NotFound", status.Code(err))
	}

	resp, err := client.GenerateAccessToken(ctx, &credentialspb.GenerateAccessTokenRequest{
		Name:     name,
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
	target, cleanup, err := ensureIamCredentialsServiceAccount(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.GenerateIdToken(ctx, &credentialspb.GenerateIdTokenRequest{
		Name:         "projects/-/serviceAccounts/" + target,
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
	target, cleanup, err := ensureIamCredentialsServiceAccount(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.SignBlob(ctx, &credentialspb.SignBlobRequest{
		Name:    "projects/-/serviceAccounts/" + target,
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
	target, cleanup, err := ensureIamCredentialsServiceAccount(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	resp, err := client.SignJwt(ctx, &credentialspb.SignJwtRequest{
		Name:    "projects/-/serviceAccounts/" + target,
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
