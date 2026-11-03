package iamcredentials

import (
	"context"
	"strings"
	"testing"
	"time"

	credentialspb "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	core "jaiscloud/internal/gcp/service/iamcredentials"
	"jaiscloud/internal/gcp/serviceaccount"
	"jaiscloud/internal/store"
)

const (
	testEmail = "compat@test-project.iam.gserviceaccount.com"
	testName  = "projects/-/serviceAccounts/" + testEmail
	cloudURL  = "https://www.googleapis.com/auth/cloud-platform"
)

// newServer returns a gRPC service whose testEmail exists in the IAM registry,
// as it would after projects.serviceAccounts.create.
func newServer(t *testing.T) *Service {
	t.Helper()
	res := store.NewMemoryResourceStore()
	ctx := context.Background()
	for _, email := range []string{testEmail} {
		if err := res.Create(ctx, "test-project", store.GlobalRegion, store.ResourceEntry{
			Type: serviceaccount.AccountResourceType,
			ID:   email,
			Data: []byte(`{"email":"` + email + `"}`),
		}); err != nil {
			t.Fatalf("seed service account %s: %v", email, err)
		}
	}
	return NewService(core.New(res), "test-project")
}

func TestGenerateAccessToken(t *testing.T) {
	s := newServer(t)
	resp, err := s.GenerateAccessToken(context.Background(), &credentialspb.GenerateAccessTokenRequest{
		Name:     testName,
		Scope:    []string{cloudURL},
		Lifetime: durationpb.New(time.Hour),
	})
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	if got := resp.GetAccessToken(); strings.Count(got, ".") != 2 {
		t.Errorf("accessToken = %q, want an opaque 3-part JWT", got)
	}
	if resp.GetExpireTime() == nil || !resp.GetExpireTime().AsTime().After(time.Now()) {
		t.Errorf("expireTime = %v", resp.GetExpireTime())
	}
}

func TestGenerateAccessTokenRejectsZeroLifetime(t *testing.T) {
	s := newServer(t)
	_, err := s.GenerateAccessToken(context.Background(), &credentialspb.GenerateAccessTokenRequest{
		Name:     testName,
		Scope:    []string{cloudURL},
		Lifetime: durationpb.New(0),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero-lifetime error = %v, want InvalidArgument", err)
	}
}

func TestGenerateIdTokenValidation(t *testing.T) {
	s := newServer(t)
	_, err := s.GenerateIdToken(context.Background(), &credentialspb.GenerateIdTokenRequest{Name: testName})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing audience error = %v, want InvalidArgument", err)
	}
	resp, err := s.GenerateIdToken(context.Background(), &credentialspb.GenerateIdTokenRequest{
		Name:         testName,
		Audience:     "https://example.com",
		IncludeEmail: true,
	})
	if err != nil {
		t.Fatalf("GenerateIdToken: %v", err)
	}
	if strings.Count(resp.GetToken(), ".") != 2 {
		t.Errorf("token is not a JWT: %q", resp.GetToken())
	}
}

func TestSignBlobAndJwt(t *testing.T) {
	s := newServer(t)
	blob, err := s.SignBlob(context.Background(), &credentialspb.SignBlobRequest{Name: testName, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("SignBlob: %v", err)
	}
	if blob.GetKeyId() == "" || len(blob.GetSignedBlob()) == 0 {
		t.Errorf("empty SignBlob response: %v", blob)
	}
	jwt, err := s.SignJwt(context.Background(), &credentialspb.SignJwtRequest{Name: testName, Payload: `{"iss":"test"}`})
	if err != nil {
		t.Fatalf("SignJwt: %v", err)
	}
	if jwt.GetKeyId() == "" || strings.Count(jwt.GetSignedJwt(), ".") != 2 {
		t.Errorf("bad SignJwt response: %v", jwt)
	}
}

// TestMissingServiceAccountIsNotFound covers J67 on the gRPC surface: all four
// RPCs answer NOT_FOUND for a service account that was never created, even
// through the documented "projects/-" name.
func TestMissingServiceAccountIsNotFound(t *testing.T) {
	s := newServer(t)
	ctx := context.Background()
	missing := "projects/-/serviceAccounts/ghost@test-project.iam.gserviceaccount.com"

	calls := map[string]func() error{
		"GenerateAccessToken": func() error {
			_, err := s.GenerateAccessToken(ctx, &credentialspb.GenerateAccessTokenRequest{
				Name: missing, Scope: []string{cloudURL}, Lifetime: durationpb.New(time.Hour),
			})
			return err
		},
		"GenerateIdToken": func() error {
			_, err := s.GenerateIdToken(ctx, &credentialspb.GenerateIdTokenRequest{Name: missing, Audience: "https://example.com"})
			return err
		},
		"SignBlob": func() error {
			_, err := s.SignBlob(ctx, &credentialspb.SignBlobRequest{Name: missing, Payload: []byte("hello")})
			return err
		},
		"SignJwt": func() error {
			_, err := s.SignJwt(ctx, &credentialspb.SignJwtRequest{Name: missing, Payload: `{"iss":"test"}`})
			return err
		},
	}
	for name, call := range calls {
		if code := status.Code(call()); code != codes.NotFound {
			t.Errorf("%s = %v, want NotFound", name, code)
		}
	}
}
