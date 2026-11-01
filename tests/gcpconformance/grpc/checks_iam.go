package grpcconformance

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"time"

	iamadmin "cloud.google.com/go/iam/admin/apiv1"
	iamadminpb "cloud.google.com/go/iam/admin/apiv1/adminpb"
	iam "cloud.google.com/go/iam/apiv1"
	iampb "cloud.google.com/go/iam/apiv1/iampb"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// iamChecks covers the standalone google.iam.v1.IAMPolicy surface: the three
// RPCs registered once by grpcserver.NewIAMRouter and dispatched to whichever
// service owns the resource name. The router owns Pub/Sub topics/subscriptions
// and KMS resources, so each probe backs itself with its own run-unique Pub/Sub
// topic (projects/{project}/topics/{topic}) created and deleted in this file —
// it never depends on a fixture owned by another check.
func iamChecks() []Check {
	return []Check{
		{Service: "iam", RPC: "GetIamPolicy", Method: "GetIamPolicy", KeyField: "policy present, empty bindings by default", Run: checkIAMGetPolicy},
		{Service: "iam", RPC: "SetIamPolicy", Method: "SetIamPolicy", KeyField: "role+member binding round-trips via GetIamPolicy", Run: checkIAMSetPolicy},
		{Service: "iam", RPC: "TestIamPermissions", Method: "TestIamPermissions", KeyField: "requested permissions echoed back", Run: checkIAMTestPermissions},
	}
}

// iamKeyChecks covers the IAM Admin API's key surface
// (google.iam.admin.v1.IAM) through the official admin client, including
// projects.serviceAccounts.keys.upload (DC27).
func iamKeyChecks() []Check {
	return []Check{
		{Service: "iam", RPC: "UploadServiceAccountKey", Method: "UploadServiceAccountKey",
			KeyField: "USER_MANAGED/USER_PROVIDED key, publicKeyData on get, listed, deletable",
			Run:      checkIAMUploadKey},
	}
}

// newIAMAdminClient dials the IAM Admin RPC service at the emulator with
// insecure transport and no authentication, mirroring the other official
// clients in this suite.
func newIAMAdminClient(ctx context.Context, cfg Config) (*iamadmin.IamClient, error) {
	return iamadmin.NewIamClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// iamUploadCertificatePEM builds a self-signed X.509 v3 certificate around a
// fresh 2048-bit RSA public key. projects.serviceAccounts.keys.upload documents
// exactly that envelope for publicKeyData, and no private key is needed: the
// emulator stores the public material only.
func iamUploadCertificatePEM() ([]byte, *rsa.PublicKey, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "jaiscloud-grpc-conformance-upload"},
		NotBefore:             time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2120, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), &priv.PublicKey, nil
}

// checkIAMUploadKey uploads a caller-managed public key to a throwaway service
// account, then proves the key round-trips: get carries publicKeyData for the
// uploaded key, list includes it classified USER_MANAGED/USER_PROVIDED, and
// delete removes it.
func checkIAMUploadKey(ctx context.Context, cfg Config) (err error) {
	client, err := newIAMAdminClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM admin client: %w", err)
	}
	defer client.Close()

	parent := "projects/" + cfg.Project
	accountID := strings.ToLower(cfg.ResourceName("gcpc-sa-up"))
	sa, err := client.CreateServiceAccount(ctx, &iamadminpb.CreateServiceAccountRequest{
		Name:      parent,
		AccountId: accountID,
		ServiceAccount: &iamadminpb.ServiceAccount{
			DisplayName: "gRPC conformance key upload",
		},
	})
	if err != nil {
		return fmt.Errorf("CreateServiceAccount(%s/%s): %w", parent, accountID, err)
	}
	defer func() {
		derr := client.DeleteServiceAccount(ctx, &iamadminpb.DeleteServiceAccountRequest{Name: sa.GetName()})
		if derr != nil && status.Code(derr) != codes.NotFound && err == nil {
			err = fmt.Errorf("DeleteServiceAccount(%s): %w", sa.GetName(), derr)
		}
	}()

	certPEM, wantPub, err := iamUploadCertificatePEM()
	if err != nil {
		return fmt.Errorf("build upload certificate: %w", err)
	}
	uploaded, err := client.UploadServiceAccountKey(ctx, &iamadminpb.UploadServiceAccountKeyRequest{
		Name:          sa.GetName(),
		PublicKeyData: certPEM,
	})
	if err != nil {
		return fmt.Errorf("UploadServiceAccountKey(%s): %w", sa.GetName(), err)
	}
	if uploaded.GetKeyType() != iamadminpb.ListServiceAccountKeysRequest_USER_MANAGED {
		return fmt.Errorf("uploaded keyType = %v, want USER_MANAGED", uploaded.GetKeyType())
	}
	if uploaded.GetKeyOrigin() != iamadminpb.ServiceAccountKeyOrigin_USER_PROVIDED {
		return fmt.Errorf("uploaded keyOrigin = %v, want USER_PROVIDED", uploaded.GetKeyOrigin())
	}
	if uploaded.GetKeyAlgorithm() != iamadminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048 {
		return fmt.Errorf("uploaded keyAlgorithm = %v, want KEY_ALG_RSA_2048", uploaded.GetKeyAlgorithm())
	}
	if !strings.HasPrefix(uploaded.GetName(), sa.GetName()+"/keys/") {
		return fmt.Errorf("uploaded key name = %q, want a key under %s", uploaded.GetName(), sa.GetName())
	}
	if len(uploaded.GetPrivateKeyData()) != 0 || len(uploaded.GetPublicKeyData()) != 0 {
		return fmt.Errorf("upload response carried key material (private=%d public=%d bytes)",
			len(uploaded.GetPrivateKeyData()), len(uploaded.GetPublicKeyData()))
	}

	got, err := client.GetServiceAccountKey(ctx, &iamadminpb.GetServiceAccountKeyRequest{Name: uploaded.GetName()})
	if err != nil {
		return fmt.Errorf("GetServiceAccountKey(%s): %w", uploaded.GetName(), err)
	}
	block, _ := pem.Decode(got.GetPublicKeyData())
	if block == nil || block.Type != "PUBLIC KEY" {
		return fmt.Errorf("uploaded key publicKeyData is not a PUBLIC KEY PEM block")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse uploaded publicKeyData: %w", err)
	}
	if pub.(*rsa.PublicKey).N.Cmp(wantPub.N) != 0 {
		return fmt.Errorf("uploaded key publicKeyData does not describe the uploaded key")
	}

	list, err := client.ListServiceAccountKeys(ctx, &iamadminpb.ListServiceAccountKeysRequest{Name: sa.GetName()})
	if err != nil {
		return fmt.Errorf("ListServiceAccountKeys(%s): %w", sa.GetName(), err)
	}
	found := false
	for _, k := range list.GetKeys() {
		if k.GetName() == uploaded.GetName() {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("ListServiceAccountKeys(%s) dropped the uploaded key %q", sa.GetName(), uploaded.GetName())
	}

	if err := client.DeleteServiceAccountKey(ctx, &iamadminpb.DeleteServiceAccountKeyRequest{Name: uploaded.GetName()}); err != nil {
		return fmt.Errorf("DeleteServiceAccountKey(%s): %w", uploaded.GetName(), err)
	}
	if _, err := client.GetServiceAccountKey(ctx, &iamadminpb.GetServiceAccountKeyRequest{Name: uploaded.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetServiceAccountKey after delete = %v, want NotFound", err)
	}
	return nil
}

// newIAMPolicyClient dials the standalone IAMPolicy RPC service at the emulator
// with insecure transport and no authentication, mirroring the other official
// clients in this suite.
func newIAMPolicyClient(ctx context.Context, cfg Config) (*iam.IamPolicyClient, error) {
	return iam.NewIamPolicyClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

// iamTopicName builds a run-unique Pub/Sub topic resource name for one IAM
// probe. The distinct prefix keeps the three probes from sharing fixtures.
func iamTopicName(cfg Config, prefix string) string {
	return fmt.Sprintf("projects/%s/topics/%s", cfg.Project, cfg.ResourceName(prefix))
}

// iamCreateTopic creates the backing topic, tolerating AlreadyExists so a probe
// stays idempotent against a long-lived emulator.
func iamCreateTopic(ctx context.Context, cfg Config, topic string) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new pubsub client: %w", err)
	}
	defer client.Close()
	if _, err := client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateTopic(%s): %w", topic, err)
	}
	return nil
}

// iamDeleteTopic removes a probe's backing topic. A NotFound is tolerated so
// cleanup is idempotent.
func iamDeleteTopic(ctx context.Context, cfg Config, topic string) error {
	client, err := newPubSubClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new pubsub client: %w", err)
	}
	defer client.Close()
	if err := client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic}); err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("DeleteTopic(%s): %w", topic, err)
	}
	return nil
}

// checkIAMGetPolicy asserts GetIamPolicy on a resource the router owns returns a
// well-formed policy that is empty by default.
func checkIAMGetPolicy(ctx context.Context, cfg Config) (err error) {
	topic := iamTopicName(cfg, "gcpc-iam-get")
	if err := iamCreateTopic(ctx, cfg, topic); err != nil {
		return err
	}
	defer func() {
		if derr := iamDeleteTopic(ctx, cfg, topic); derr != nil && err == nil {
			err = derr
		}
	}()

	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: topic})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s): %w", topic, err)
	}
	if pol == nil {
		return fmt.Errorf("GetIamPolicy(%s) returned a nil policy", topic)
	}
	if len(pol.GetBindings()) != 0 {
		return fmt.Errorf("GetIamPolicy(%s) on a fresh topic returned %d binding(s), want 0", topic, len(pol.GetBindings()))
	}
	return nil
}

// checkIAMSetPolicy sets a single role+member binding with SetIamPolicy on the
// standalone IAMPolicy service, then reads it back with GetIamPolicy to prove
// the write round-trips through the router.
func checkIAMSetPolicy(ctx context.Context, cfg Config) (err error) {
	topic := iamTopicName(cfg, "gcpc-iam-set")
	if err := iamCreateTopic(ctx, cfg, topic); err != nil {
		return err
	}
	defer func() {
		if derr := iamDeleteTopic(ctx, cfg, topic); derr != nil && err == nil {
			err = derr
		}
	}()

	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	const (
		role   = "roles/pubsub.viewer"
		member = "allUsers"
	)
	set, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: topic,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: role, Members: []string{member}}},
		},
	})
	if err != nil {
		return fmt.Errorf("SetIamPolicy(%s): %w", topic, err)
	}
	if len(set.GetBindings()) != 1 || set.GetBindings()[0].GetRole() != role {
		return fmt.Errorf("SetIamPolicy(%s) returned bindings %v, want one %s binding", topic, set.GetBindings(), role)
	}

	got, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: topic})
	if err != nil {
		return fmt.Errorf("GetIamPolicy(%s) after SetIamPolicy: %w", topic, err)
	}
	if len(got.GetBindings()) != 1 {
		return fmt.Errorf("GetIamPolicy(%s) after SetIamPolicy returned %d binding(s), want 1", topic, len(got.GetBindings()))
	}
	b := got.GetBindings()[0]
	if b.GetRole() != role {
		return fmt.Errorf("GetIamPolicy(%s) binding role = %q, want %q", topic, b.GetRole(), role)
	}
	want := map[string]bool{member: true}
	for _, m := range b.GetMembers() {
		delete(want, m)
	}
	if len(want) != 0 {
		return fmt.Errorf("GetIamPolicy(%s) binding members = %v, want to contain %q", topic, b.GetMembers(), member)
	}
	return nil
}

// checkIAMTestPermissions asserts TestIamPermissions echoes back exactly the
// requested permission list on a resource owned by the router.
func checkIAMTestPermissions(ctx context.Context, cfg Config) (err error) {
	topic := iamTopicName(cfg, "gcpc-iam-test")
	if err := iamCreateTopic(ctx, cfg, topic); err != nil {
		return err
	}
	defer func() {
		if derr := iamDeleteTopic(ctx, cfg, topic); derr != nil && err == nil {
			err = derr
		}
	}()

	client, err := newIAMPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new IAM client: %w", err)
	}
	defer client.Close()

	want := []string{"pubsub.topics.get", "pubsub.topics.publish"}
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    topic,
		Permissions: want,
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions(%s): %w", topic, err)
	}
	got := map[string]bool{}
	for _, p := range resp.GetPermissions() {
		got[p] = true
	}
	if len(resp.GetPermissions()) != len(want) {
		return fmt.Errorf("TestIamPermissions(%s) returned %v, want exactly %v", topic, resp.GetPermissions(), want)
	}
	for _, p := range want {
		if !got[p] {
			return fmt.Errorf("TestIamPermissions(%s) returned %v, missing %q", topic, resp.GetPermissions(), p)
		}
	}
	return nil
}
