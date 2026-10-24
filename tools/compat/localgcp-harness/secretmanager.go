package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

var smClient secretmanagerpb.SecretManagerServiceClient

func initSecretManager() {
	conn, err := grpc.NewClient(grpcEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	smClient = secretmanagerpb.NewSecretManagerServiceClient(conn)
}

func smCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 20*time.Second)
}

func smCreate(ctx context.Context, project, id string, labels map[string]string) (*secretmanagerpb.Secret, error) {
	return smClient.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   "projects/" + project,
		SecretId: id,
		Secret:   &secretmanagerpb.Secret{Labels: labels},
	})
}

func runSecretManager() {
	initSecretManager()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"TestCreateAndGetSecret", smCreateAndGetSecret},
		{"TestCreateDuplicateSecret", smCreateDuplicateSecret},
		{"TestGetNonexistentSecret", smGetNonexistentSecret},
		{"TestListSecrets", smListSecrets},
		{"TestDeleteSecret", smDeleteSecret},
		{"TestAddAndAccessVersion", smAddAndAccessVersion},
		{"TestLatestVersionAlias", smLatestVersionAlias},
		{"TestListSecretVersions", smListSecretVersions},
		{"TestDisableVersion", smDisableVersion},
		{"TestEnableVersion", smEnableVersion},
		{"TestDestroyVersion", smDestroyVersion},
		{"TestVersionStateTransitions", smVersionStateTransitions},
		{"TestDeleteSecretDeletesVersions", smDeleteSecretDeletesVersions},
		{"TestUnimplementedRPC", smUnimplementedRPC},
		{"TestMultipleProjects", smMultipleProjects},
		{"TestGetSecretVersion", smGetSecretVersion},
	}
	for _, c := range cases {
		record("secretmanager", c.name, c.fn())
	}
}

func smCreateAndGetSecret() error {
	ctx, cancel := smCtx()
	defer cancel()
	id := "sm-create-" + suffix
	secret, err := smCreate(ctx, "test-project", id, map[string]string{"env": "test"})
	if err != nil {
		return err
	}
	name := "projects/test-project/secrets/" + id
	if secret.Name != name {
		return fmt.Errorf("got name %q", secret.Name)
	}
	if secret.Labels["env"] != "test" {
		return fmt.Errorf("labels lost: %v", secret.Labels)
	}
	if secret.CreateTime == nil {
		return fmt.Errorf("create_time missing")
	}
	got, err := smClient.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name})
	if err != nil {
		return err
	}
	if got.Name != secret.Name {
		return fmt.Errorf("get name mismatch")
	}
	return nil
}

func smCreateDuplicateSecret() error {
	ctx, cancel := smCtx()
	defer cancel()
	id := "sm-dup-" + suffix
	if _, err := smCreate(ctx, "test-project", id, nil); err != nil {
		return err
	}
	_, err := smCreate(ctx, "test-project", id, nil)
	return wantCode(err, codes.AlreadyExists)
}

func smGetNonexistentSecret() error {
	ctx, cancel := smCtx()
	defer cancel()
	_, err := smClient.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: "projects/test-project/secrets/nope-" + suffix})
	return wantCode(err, codes.NotFound)
}

func smListSecrets() error {
	ctx, cancel := smCtx()
	defer cancel()
	proj := "list-secrets-" + suffix
	for _, id := range []string{"alpha", "beta"} {
		if _, err := smCreate(ctx, proj, id, nil); err != nil {
			return err
		}
	}
	resp, err := smClient.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: "projects/" + proj})
	if err != nil {
		return err
	}
	if len(resp.Secrets) != 2 {
		return fmt.Errorf("expected 2 secrets, got %d", len(resp.Secrets))
	}
	if resp.TotalSize != 2 {
		return fmt.Errorf("expected total_size 2, got %d", resp.TotalSize)
	}
	return nil
}

func smDeleteSecret() error {
	ctx, cancel := smCtx()
	defer cancel()
	id := "sm-del-" + suffix
	if _, err := smCreate(ctx, "test-project", id, nil); err != nil {
		return err
	}
	name := "projects/test-project/secrets/" + id
	if _, err := smClient.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: name}); err != nil {
		return err
	}
	_, err := smClient.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name})
	return wantCode(err, codes.NotFound)
}

func smAddAndAccessVersion() error {
	ctx, cancel := smCtx()
	defer cancel()
	id := "sm-ver-" + suffix
	name := "projects/test-project/secrets/" + id
	if _, err := smCreate(ctx, "test-project", id, nil); err != nil {
		return err
	}
	v1, err := smClient.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: name, Payload: &secretmanagerpb.SecretPayload{Data: []byte("secret-value-1")},
	})
	if err != nil {
		return err
	}
	if v1.Name != name+"/versions/1" {
		return fmt.Errorf("got version %q", v1.Name)
	}
	if v1.State != secretmanagerpb.SecretVersion_ENABLED {
		return fmt.Errorf("state %s", v1.State)
	}
	resp, err := smClient.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: name + "/versions/1"})
	if err != nil {
		return err
	}
	if string(resp.Payload.Data) != "secret-value-1" {
		return fmt.Errorf("payload %q", resp.Payload.Data)
	}
	return nil
}

func smLatestVersionAlias() error {
	ctx, cancel := smCtx()
	defer cancel()
	id := "sm-latest-" + suffix
	name := "projects/test-project/secrets/" + id
	if _, err := smCreate(ctx, "test-project", id, nil); err != nil {
		return err
	}
	for i := 1; i <= 2; i++ {
		if _, err := smClient.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
			Parent: name, Payload: &secretmanagerpb.SecretPayload{Data: []byte(fmt.Sprintf("value-%d", i))},
		}); err != nil {
			return err
		}
	}
	resp, err := smClient.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: name + "/versions/latest"})
	if err != nil {
		return err
	}
	if string(resp.Payload.Data) != "value-2" {
		return fmt.Errorf("expected value-2, got %q", resp.Payload.Data)
	}
	if resp.Name != name+"/versions/2" {
		return fmt.Errorf("expected versions/2, got %s", resp.Name)
	}
	return nil
}

func smListSecretVersions() error {
	ctx, cancel := smCtx()
	defer cancel()
	id := "sm-listver-" + suffix
	name := "projects/test-project/secrets/" + id
	if _, err := smCreate(ctx, "test-project", id, nil); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		if _, err := smClient.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
			Parent: name, Payload: &secretmanagerpb.SecretPayload{Data: []byte(fmt.Sprintf("v%d", i))},
		}); err != nil {
			return err
		}
	}
	resp, err := smClient.ListSecretVersions(ctx, &secretmanagerpb.ListSecretVersionsRequest{Parent: name})
	if err != nil {
		return err
	}
	if len(resp.Versions) != 3 || resp.TotalSize != 3 {
		return fmt.Errorf("expected 3 versions/total 3, got %d/%d", len(resp.Versions), resp.TotalSize)
	}
	return nil
}

func smSecretWithVersion(ctx context.Context, id string) (string, error) {
	name := "projects/test-project/secrets/" + id
	if _, err := smCreate(ctx, "test-project", id, nil); err != nil {
		return "", err
	}
	if _, err := smClient.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: name, Payload: &secretmanagerpb.SecretPayload{Data: []byte("data")},
	}); err != nil {
		return "", err
	}
	return name + "/versions/1", nil
}

func smDisableVersion() error {
	ctx, cancel := smCtx()
	defer cancel()
	version, err := smSecretWithVersion(ctx, "sm-disable-"+suffix)
	if err != nil {
		return err
	}
	v, err := smClient.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{Name: version})
	if err != nil {
		return err
	}
	if v.State != secretmanagerpb.SecretVersion_DISABLED {
		return fmt.Errorf("state %s", v.State)
	}
	_, err = smClient.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: version})
	return wantCode(err, codes.FailedPrecondition)
}

func smEnableVersion() error {
	ctx, cancel := smCtx()
	defer cancel()
	version, err := smSecretWithVersion(ctx, "sm-enable-"+suffix)
	if err != nil {
		return err
	}
	if _, err := smClient.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{Name: version}); err != nil {
		return err
	}
	v, err := smClient.EnableSecretVersion(ctx, &secretmanagerpb.EnableSecretVersionRequest{Name: version})
	if err != nil {
		return err
	}
	if v.State != secretmanagerpb.SecretVersion_ENABLED {
		return fmt.Errorf("state %s", v.State)
	}
	resp, err := smClient.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: version})
	if err != nil {
		return err
	}
	if string(resp.Payload.Data) != "data" {
		return fmt.Errorf("payload %q", resp.Payload.Data)
	}
	return nil
}

func smDestroyVersion() error {
	ctx, cancel := smCtx()
	defer cancel()
	version, err := smSecretWithVersion(ctx, "sm-destroy-"+suffix)
	if err != nil {
		return err
	}
	v, err := smClient.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{Name: version})
	if err != nil {
		return err
	}
	if v.State != secretmanagerpb.SecretVersion_DESTROYED || v.DestroyTime == nil {
		return fmt.Errorf("state %s destroyTime %v", v.State, v.DestroyTime)
	}
	_, err = smClient.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: version})
	return wantCode(err, codes.FailedPrecondition)
}

func smVersionStateTransitions() error {
	ctx, cancel := smCtx()
	defer cancel()
	version, err := smSecretWithVersion(ctx, "sm-trans-"+suffix)
	if err != nil {
		return err
	}
	if _, err := smClient.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{Name: version}); err != nil {
		return err
	}
	if err := wantCode(secondErr(smClient.EnableSecretVersion(ctx, &secretmanagerpb.EnableSecretVersionRequest{Name: version})), codes.FailedPrecondition); err != nil {
		return fmt.Errorf("enable destroyed: %v", err)
	}
	if err := wantCode(secondErr(smClient.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{Name: version})), codes.FailedPrecondition); err != nil {
		return fmt.Errorf("disable destroyed: %v", err)
	}
	return nil
}

func secondErr[T any](_ T, err error) error { return err }

func smDeleteSecretDeletesVersions() error {
	ctx, cancel := smCtx()
	defer cancel()
	id := "sm-delver-" + suffix
	name, err := smSecretWithVersion(ctx, id)
	if err != nil {
		return err
	}
	secretName := "projects/test-project/secrets/" + id
	if _, err := smClient.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: secretName}); err != nil {
		return err
	}
	_, err = smClient.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: name})
	return wantCode(err, codes.NotFound)
}

func smUnimplementedRPC() error {
	ctx, cancel := smCtx()
	defer cancel()
	_, err := smClient.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{})
	// localgcp returns Unimplemented. jaiscloud implements UpdateSecret, so an
	// InvalidArgument/missing-field error is the compliant outcome; only a nil
	// error or a non-Unimplemented/InvalidArgument code is treated as failure.
	if err == nil {
		return fmt.Errorf("expected an error, got nil")
	}
	code := statusCode(err)
	if code == codes.InvalidArgument {
		return nil
	}
	return fmt.Errorf("expected Unimplemented (localgcp) or InvalidArgument (jaiscloud); got %s", code)
}

func smMultipleProjects() error {
	ctx, cancel := smCtx()
	defer cancel()
	a := "sm-proj-a-" + suffix
	b := "sm-proj-b-" + suffix
	for _, p := range []string{a, b} {
		if _, err := smCreate(ctx, p, "shared-name", nil); err != nil {
			return err
		}
	}
	resp, err := smClient.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: "projects/" + a})
	if err != nil {
		return err
	}
	if len(resp.Secrets) != 1 {
		return fmt.Errorf("expected 1 secret in proj-a, got %d", len(resp.Secrets))
	}
	return nil
}

func smGetSecretVersion() error {
	ctx, cancel := smCtx()
	defer cancel()
	version, err := smSecretWithVersion(ctx, "sm-getver-"+suffix)
	if err != nil {
		return err
	}
	v, err := smClient.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: version})
	if err != nil {
		return err
	}
	if v.State != secretmanagerpb.SecretVersion_ENABLED {
		return fmt.Errorf("state %s", v.State)
	}
	latest, err := smClient.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: versionPrefix(version) + "/versions/latest"})
	if err != nil {
		return err
	}
	if latest.Name != version {
		return fmt.Errorf("latest resolved to %s, want %s", latest.Name, version)
	}
	return nil
}

func versionPrefix(version string) string {
	// strip "/versions/N"
	if i := lastIndex(version, "/versions/"); i >= 0 {
		return version[:i]
	}
	return version
}

func lastIndex(s, sub string) int {
	for i := len(s) - len(sub); i >= 0; i-- {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
