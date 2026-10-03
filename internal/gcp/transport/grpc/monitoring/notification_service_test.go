package monitoring

import (
	"context"
	"net"
	"strings"
	"testing"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// channelServerForStore starts a NotificationChannelService backed by store and
// returns a client plus a cleanup func, so tests can pre-seed store state (e.g.
// an UNVERIFIED channel) that the public API cannot produce.
func channelServerForStore(t *testing.T, store monitoringstore.Store) (monitoringpb.NotificationChannelServiceClient, func()) {
	t.Helper()
	svc := NewService(core.NewService(store, "test"), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	monitoringpb.RegisterNotificationChannelServiceServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return monitoringpb.NewNotificationChannelServiceClient(conn), func() { conn.Close(); srv.Stop() }
}

// TestListNotificationChannelDescriptors asserts the static catalog is
// non-empty and every descriptor carries the wire fields the client relies on
// (name, type, display_name, description, and at least one label).
func TestListNotificationChannelDescriptors(t *testing.T) {
	nc, cleanup := testChannelServer(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := nc.ListNotificationChannelDescriptors(ctx, &monitoringpb.ListNotificationChannelDescriptorsRequest{Name: "projects/test"})
	if err != nil {
		t.Fatalf("list descriptors: %v", err)
	}
	if len(resp.GetChannelDescriptors()) == 0 {
		t.Fatal("list descriptors returned no descriptors")
	}
	for _, d := range resp.GetChannelDescriptors() {
		if d.GetType() == "" {
			t.Fatalf("descriptor %q has empty type", d.GetName())
		}
		if d.GetDisplayName() == "" {
			t.Fatalf("descriptor %q has empty display_name", d.GetType())
		}
		if d.GetDescription() == "" {
			t.Fatalf("descriptor %q has empty description", d.GetType())
		}
		wantName := "projects/test/notificationChannelDescriptors/" + d.GetType()
		if d.GetName() != wantName {
			t.Fatalf("descriptor name = %q, want %q", d.GetName(), wantName)
		}
		if len(d.GetLabels()) == 0 {
			t.Fatalf("descriptor %q has no labels", d.GetType())
		}
	}
}

// TestGetNotificationChannelDescriptor checks a known type resolves and an
// unknown type is NotFound.
func TestGetNotificationChannelDescriptor(t *testing.T) {
	nc, cleanup := testChannelServer(t)
	defer cleanup()
	ctx := context.Background()

	got, err := nc.GetNotificationChannelDescriptor(ctx, &monitoringpb.GetNotificationChannelDescriptorRequest{
		Name: "projects/test/notificationChannelDescriptors/email",
	})
	if err != nil {
		t.Fatalf("get email descriptor: %v", err)
	}
	if got.GetType() != "email" {
		t.Fatalf("descriptor type = %q, want email", got.GetType())
	}
	if got.GetDisplayName() == "" || len(got.GetLabels()) == 0 {
		t.Fatalf("descriptor = %+v, want display_name and labels", got)
	}

	_, err = nc.GetNotificationChannelDescriptor(ctx, &monitoringpb.GetNotificationChannelDescriptorRequest{
		Name: "projects/test/notificationChannelDescriptors/does-not-exist",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("get unknown descriptor err = %v, want NotFound", err)
	}
}

// TestNotificationChannelVerification exercises the send/get-code/verify
// sequence and proves Verify flips a pre-seeded UNVERIFIED channel to VERIFIED
// (CreateNotificationChannel defaults to VERIFIED, so the public API cannot
// establish the negative case on its own).
func TestNotificationChannelVerification(t *testing.T) {
	store := monitoringstore.NewMemoryStore()
	if err := store.CreateNotificationChannel(context.Background(), "test", monitoringstore.NotificationChannel{
		ID:                 "channel-1",
		Type:               "email",
		DisplayName:        "Ops email",
		Labels:             map[string]string{"email_address": "ops@example.com"},
		VerificationStatus: int32(monitoringpb.NotificationChannel_UNVERIFIED),
	}); err != nil {
		t.Fatalf("seed channel: %v", err)
	}

	nc, cleanup := channelServerForStore(t, store)
	defer cleanup()
	ctx := context.Background()

	name := "projects/test/notificationChannels/channel-1"

	if _, err := nc.SendNotificationChannelVerificationCode(ctx, &monitoringpb.SendNotificationChannelVerificationCodeRequest{Name: name}); err != nil {
		t.Fatalf("send verification code: %v", err)
	}

	codeResp, err := nc.GetNotificationChannelVerificationCode(ctx, &monitoringpb.GetNotificationChannelVerificationCodeRequest{Name: name})
	if err != nil {
		t.Fatalf("get verification code: %v", err)
	}
	if codeResp.GetCode() == "" {
		t.Fatal("get verification code returned an empty code")
	}
	if codeResp.GetExpireTime() == nil {
		t.Fatal("get verification code returned no expire_time")
	}

	verified, err := nc.VerifyNotificationChannel(ctx, &monitoringpb.VerifyNotificationChannelRequest{Name: name, Code: codeResp.GetCode()})
	if err != nil {
		t.Fatalf("verify channel: %v", err)
	}
	if verified.GetVerificationStatus() != monitoringpb.NotificationChannel_VERIFIED {
		t.Fatalf("verify status = %v, want VERIFIED", verified.GetVerificationStatus())
	}

	// The status change is persisted, not just reflected in the response.
	stored, err := store.GetNotificationChannel(ctx, "test", "channel-1")
	if err != nil {
		t.Fatalf("reload channel: %v", err)
	}
	if stored.VerificationStatus != int32(monitoringpb.NotificationChannel_VERIFIED) {
		t.Fatalf("stored status = %d, want VERIFIED", stored.VerificationStatus)
	}
}

// TestSendNotificationChannelVerificationCodeMissing asserts a missing channel
// is NotFound rather than a blind success.
func TestSendNotificationChannelVerificationCodeMissing(t *testing.T) {
	nc, cleanup := testChannelServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := nc.SendNotificationChannelVerificationCode(ctx, &monitoringpb.SendNotificationChannelVerificationCodeRequest{
		Name: "projects/test/notificationChannels/missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("send to missing channel err = %v, want NotFound", err)
	}
}

// TestVerificationCodeIsDeterministic pins the code derivation to the channel
// id: repeated reads of the same channel yield the same non-empty code.
func TestVerificationCodeIsDeterministic(t *testing.T) {
	store := monitoringstore.NewMemoryStore()
	if err := store.CreateNotificationChannel(context.Background(), "test", monitoringstore.NotificationChannel{
		ID:          "abcdef01-2345-6789-abcd-ef0123456789",
		Type:        "email",
		DisplayName: "Ops email",
		Labels:      map[string]string{"email_address": "ops@example.com"},
	}); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	nc, cleanup := channelServerForStore(t, store)
	defer cleanup()
	ctx := context.Background()

	name := "projects/test/notificationChannels/abcdef01-2345-6789-abcd-ef0123456789"
	first, err := nc.GetNotificationChannelVerificationCode(ctx, &monitoringpb.GetNotificationChannelVerificationCodeRequest{Name: name})
	if err != nil {
		t.Fatalf("first get code: %v", err)
	}
	second, err := nc.GetNotificationChannelVerificationCode(ctx, &monitoringpb.GetNotificationChannelVerificationCodeRequest{Name: name})
	if err != nil {
		t.Fatalf("second get code: %v", err)
	}
	if !strings.HasPrefix(first.GetCode(), "JC-") {
		t.Fatalf("code = %q, want JC- prefix", first.GetCode())
	}
	if first.GetCode() != second.GetCode() {
		t.Fatalf("codes differ across reads: %q vs %q", first.GetCode(), second.GetCode())
	}
}
