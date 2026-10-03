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
	"jaiscloud/internal/store"
)

const (
	testEmail = "compat@test-project.iam.gserviceaccount.com"
	testName  = "projects/-/serviceAccounts/" + testEmail
	cloudURL  = "https://www.googleapis.com/auth/cloud-platform"
)

func newServer() *Service {
	return NewService(core.New(store.NewMemoryResourceStore()), "test-project")
}

func TestGenerateAccessToken(t *testing.T) {
	s := newServer()
	resp, err := s.GenerateAccessToken(context.Background(), &credentialspb.GenerateAccessTokenRequest{
		Name:     testName,
		Scope:    []string{cloudURL},
		Lifetime: durationpb.New(time.Hour),
	})
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	if !strings.HasPrefix(resp.GetAccessToken(), core.AccessTokenPrefix) {
		t.Errorf("accessToken = %q", resp.GetAccessToken())
	}
	if resp.GetExpireTime() == nil || !resp.GetExpireTime().AsTime().After(time.Now()) {
		t.Errorf("expireTime = %v", resp.GetExpireTime())
	}
}

func TestGenerateAccessTokenRejectsZeroLifetime(t *testing.T) {
	s := newServer()
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
	s := newServer()
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
	s := newServer()
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
