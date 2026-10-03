package grpcconformance

import (
	"context"
	"fmt"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// secretManagerExtraChecks covers the Secret Manager control-plane RPCs beyond
// the create/get/list/addVersion/access/delete probes in
// checks_secretmanager.go: the SecretVersion lifecycle, UpdateSecret's
// update_mask, and the secret IAM surface.
//
// Every probe drives the official cloud.google.com/go/secretmanager client,
// whose methods map 1:1 onto the SecretManagerService RPCs (unlike, say, the
// Firestore high-level Doc helpers, which collapse onto Commit/BatchGetDocuments).
//
// Checks share one run-unique fixture secret (cfg.ResourceName) that every
// probe recreates idempotently, so the sequence is self-contained and safe
// against a long-lived emulator; the final probe deletes it. The base
// secretManagerChecks create their own separate secret, so the two groups
// never collide.
func secretManagerExtraChecks() []Check {
	return []Check{
		// ── version lifecycle ───────────────────────────────────────────────
		{Service: "secretmanager", RPC: "GetSecretVersion", Method: "GetSecretVersion", KeyField: "name/state=ENABLED/client_specified_payload_checksum", Run: checkSMXGetVersion},
		{Service: "secretmanager", RPC: "ListSecretVersions", Method: "ListSecretVersions", KeyField: "versions[].name contains fixture version", Run: checkSMXListVersions},
		{Service: "secretmanager", RPC: "EnableSecretVersion", Method: "EnableSecretVersion", KeyField: "state DISABLED->ENABLED", Run: checkSMXEnableVersion},
		{Service: "secretmanager", RPC: "DisableSecretVersion", Method: "DisableSecretVersion", KeyField: "state ENABLED->DISABLED", Run: checkSMXDisableVersion},
		{Service: "secretmanager", RPC: "DestroySecretVersion", Method: "DestroySecretVersion", KeyField: "state=DESTROYED + destroy_time", Run: checkSMXDestroyVersion},

		// ── IAM on the fixture secret ───────────────────────────────────────
		{Service: "secretmanager", RPC: "GetIamPolicy", Method: "GetIamPolicy", KeyField: "existing secret returns an empty policy", Run: checkSMXGetIamPolicy},
		{Service: "secretmanager", RPC: "SetIamPolicy", Method: "SetIamPolicy", KeyField: "binding round-trips through GetIamPolicy", Run: checkSMXSetIamPolicy},
		{Service: "secretmanager", RPC: "TestIamPermissions", Method: "TestIamPermissions", KeyField: "requested permissions echoed", Run: checkSMXTestIamPermissions},

		// ── metadata patch (update_mask) ────────────────────────────────────
		{Service: "secretmanager", RPC: "UpdateSecret (update_mask)", Method: "UpdateSecret", KeyField: "masked labels/annotations applied, unmasked field preserved", Run: checkSMXUpdateSecret},

		// ── cleanup ─────────────────────────────────────────────────────────
		{Service: "secretmanager", RPC: "DeleteSecret", Method: "DeleteSecret", KeyField: "fixture secret absent after delete", Run: checkSMXDeleteSecret},
	}
}

const (
	smxSecretID = "gcpc-grpc-smx-secret"
	smxPayload  = "grpc-conformance-secret-extra-payload"
)

// smxSecretName is the full resource name of the shared fixture secret.
func smxSecretName(cfg Config) string {
	return secretProject(cfg) + "/secrets/" + cfg.ResourceName(smxSecretID)
}

// smxEnsureSecret creates the shared fixture secret if it does not exist yet.
// The fixture carries a label and an annotation so UpdateSecret's mask
// semantics have both an updated field and a preserved field to assert.
func smxEnsureSecret(ctx context.Context, client *secretmanager.Client, cfg Config) error {
	_, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: smxSecretName(cfg)})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetSecret(fixture): %w", err)
	}
	_, err = client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   secretProject(cfg),
		SecretId: cfg.ResourceName(smxSecretID),
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
			Labels:      map[string]string{"env": "dev"},
			Annotations: map[string]string{"note": "fixture"},
		},
	})
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}
	if err != nil {
		return fmt.Errorf("CreateSecret(fixture): %w", err)
	}
	return nil
}

// smxAddVersion appends a fresh ENABLED version to the fixture secret.
func smxAddVersion(ctx context.Context, client *secretmanager.Client, cfg Config, payload string) (*secretmanagerpb.SecretVersion, error) {
	v, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  smxSecretName(cfg),
		Payload: &secretmanagerpb.SecretPayload{Data: []byte(payload)},
	})
	if err != nil {
		return nil, fmt.Errorf("AddSecretVersion: %w", err)
	}
	return v, nil
}

// ─── version lifecycle ───────────────────────────────────────────────────────

func checkSMXGetVersion(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	added, err := smxAddVersion(ctx, client, cfg, smxPayload)
	if err != nil {
		return err
	}
	got, err := client.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: added.GetName()})
	if err != nil {
		return fmt.Errorf("GetSecretVersion: %w", err)
	}
	if got.GetName() != added.GetName() {
		return fmt.Errorf("GetSecretVersion name = %q, want %q", got.GetName(), added.GetName())
	}
	if got.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		return fmt.Errorf("GetSecretVersion state = %v, want ENABLED", got.GetState())
	}
	if !got.GetClientSpecifiedPayloadChecksum() {
		return fmt.Errorf("GetSecretVersion client_specified_payload_checksum = false, want true")
	}
	if got.GetCreateTime() == nil {
		return fmt.Errorf("GetSecretVersion returned no create_time")
	}
	return nil
}

func checkSMXListVersions(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	added, err := smxAddVersion(ctx, client, cfg, smxPayload)
	if err != nil {
		return err
	}
	it := client.ListSecretVersions(ctx, &secretmanagerpb.ListSecretVersionsRequest{Parent: smxSecretName(cfg)})
	for {
		v, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListSecretVersions: %w", err)
		}
		if v.GetName() == added.GetName() {
			if v.GetState() != secretmanagerpb.SecretVersion_ENABLED {
				return fmt.Errorf("ListSecretVersions state = %v, want ENABLED", v.GetState())
			}
			return nil
		}
	}
	return fmt.Errorf("ListSecretVersions did not include %q", added.GetName())
}

func checkSMXEnableVersion(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	added, err := smxAddVersion(ctx, client, cfg, smxPayload)
	if err != nil {
		return err
	}
	disabled, err := client.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{Name: added.GetName()})
	if err != nil {
		return fmt.Errorf("DisableSecretVersion (fixture): %w", err)
	}
	if disabled.GetState() != secretmanagerpb.SecretVersion_DISABLED {
		return fmt.Errorf("fixture DisableSecretVersion state = %v, want DISABLED", disabled.GetState())
	}
	enabled, err := client.EnableSecretVersion(ctx, &secretmanagerpb.EnableSecretVersionRequest{Name: added.GetName()})
	if err != nil {
		return fmt.Errorf("EnableSecretVersion: %w", err)
	}
	if enabled.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		return fmt.Errorf("EnableSecretVersion state = %v, want ENABLED", enabled.GetState())
	}
	if enabled.GetName() != added.GetName() {
		return fmt.Errorf("EnableSecretVersion name = %q, want %q", enabled.GetName(), added.GetName())
	}
	return nil
}

func checkSMXDisableVersion(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	added, err := smxAddVersion(ctx, client, cfg, smxPayload)
	if err != nil {
		return err
	}
	if added.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		return fmt.Errorf("AddSecretVersion state = %v, want ENABLED", added.GetState())
	}
	disabled, err := client.DisableSecretVersion(ctx, &secretmanagerpb.DisableSecretVersionRequest{Name: added.GetName()})
	if err != nil {
		return fmt.Errorf("DisableSecretVersion: %w", err)
	}
	if disabled.GetState() != secretmanagerpb.SecretVersion_DISABLED {
		return fmt.Errorf("DisableSecretVersion state = %v, want DISABLED", disabled.GetState())
	}
	if disabled.GetName() != added.GetName() {
		return fmt.Errorf("DisableSecretVersion name = %q, want %q", disabled.GetName(), added.GetName())
	}
	return nil
}

func checkSMXDestroyVersion(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	added, err := smxAddVersion(ctx, client, cfg, smxPayload)
	if err != nil {
		return err
	}
	destroyed, err := client.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{Name: added.GetName()})
	if err != nil {
		return fmt.Errorf("DestroySecretVersion: %w", err)
	}
	if destroyed.GetState() != secretmanagerpb.SecretVersion_DESTROYED {
		return fmt.Errorf("DestroySecretVersion state = %v, want DESTROYED", destroyed.GetState())
	}
	if destroyed.GetDestroyTime() == nil {
		return fmt.Errorf("DestroySecretVersion returned no destroy_time")
	}
	if destroyed.GetName() != added.GetName() {
		return fmt.Errorf("DestroySecretVersion name = %q, want %q", destroyed.GetName(), added.GetName())
	}
	return nil
}

// ─── IAM ─────────────────────────────────────────────────────────────────────

func checkSMXGetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	pol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: smxSecretName(cfg)})
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	if len(pol.GetBindings()) != 0 {
		return fmt.Errorf("GetIamPolicy on a fresh secret returned %d binding(s), want 0", len(pol.GetBindings()))
	}
	return nil
}

func checkSMXSetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	const role = "roles/secretmanager.secretAccessor"
	set, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: smxSecretName(cfg),
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: role, Members: []string{"allUsers"}}},
		},
	})
	if err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	if len(set.GetBindings()) != 1 || set.GetBindings()[0].GetRole() != role {
		return fmt.Errorf("SetIamPolicy returned bindings %v, want one %s binding", set.GetBindings(), role)
	}
	got, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: smxSecretName(cfg)})
	if err != nil {
		return fmt.Errorf("GetIamPolicy after SetIamPolicy: %w", err)
	}
	if len(got.GetBindings()) != 1 || got.GetBindings()[0].GetRole() != role {
		return fmt.Errorf("GetIamPolicy after SetIamPolicy = %v, want persistence of the %s binding", got.GetBindings(), role)
	}
	return nil
}

func checkSMXTestIamPermissions(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	want := []string{"secretmanager.secrets.get", "secretmanager.versions.access"}
	resp, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    smxSecretName(cfg),
		Permissions: want,
	})
	if err != nil {
		return fmt.Errorf("TestIamPermissions: %w", err)
	}
	if len(resp.GetPermissions()) != len(want) {
		return fmt.Errorf("TestIamPermissions returned %v, want %v", resp.GetPermissions(), want)
	}
	return nil
}

// ─── metadata patch ──────────────────────────────────────────────────────────

// checkSMXUpdateSecret asserts the update_mask contract: a masked field is
// written, and an unmasked field present on the request Secret is left alone.
func checkSMXUpdateSecret(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	// Baseline fixture: labels {env:dev}, annotations {note:fixture}.
	base, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: smxSecretName(cfg)})
	if err != nil {
		return fmt.Errorf("GetSecret(fixture): %w", err)
	}
	if base.GetAnnotations()["note"] != "fixture" {
		return fmt.Errorf("fixture annotations = %v, want note=fixture", base.GetAnnotations())
	}

	// Mask "labels": annotations must survive untouched even though the
	// request Secret carries none.
	updated, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{
			Name:   smxSecretName(cfg),
			Labels: map[string]string{"env": "prod", "team": "core"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateSecret(labels): %w", err)
	}
	if got := updated.GetLabels()["env"]; got != "prod" {
		return fmt.Errorf("UpdateSecret labels[env] = %q, want prod", got)
	}
	if got := updated.GetLabels()["team"]; got != "core" {
		return fmt.Errorf("UpdateSecret labels[team] = %q, want core", got)
	}
	if got := updated.GetAnnotations()["note"]; got != "fixture" {
		return fmt.Errorf("UpdateSecret(labels) clobbered unmasked annotation note = %q, want fixture", got)
	}

	// Mask "annotations": the labels written above must survive untouched.
	updated, err = client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{
			Name:        smxSecretName(cfg),
			Annotations: map[string]string{"note": "updated", "owner": "conformance"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"annotations"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateSecret(annotations): %w", err)
	}
	if got := updated.GetAnnotations()["note"]; got != "updated" {
		return fmt.Errorf("UpdateSecret annotations[note] = %q, want updated", got)
	}
	if got := updated.GetAnnotations()["owner"]; got != "conformance" {
		return fmt.Errorf("UpdateSecret annotations[owner] = %q, want conformance", got)
	}
	if got := updated.GetLabels()["env"]; got != "prod" {
		return fmt.Errorf("UpdateSecret(annotations) clobbered unmasked label env = %q, want prod", got)
	}

	// The masked write must be persisted, not just echoed.
	persisted, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: smxSecretName(cfg)})
	if err != nil {
		return fmt.Errorf("GetSecret after UpdateSecret: %w", err)
	}
	if persisted.GetAnnotations()["note"] != "updated" || persisted.GetLabels()["team"] != "core" {
		return fmt.Errorf("UpdateSecret was not persisted: labels=%v annotations=%v",
			persisted.GetLabels(), persisted.GetAnnotations())
	}
	return nil
}

// ─── cleanup ─────────────────────────────────────────────────────────────────

func checkSMXDeleteSecret(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if err := smxEnsureSecret(ctx, client, cfg); err != nil {
		return err
	}
	if err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: smxSecretName(cfg)}); err != nil {
		return fmt.Errorf("DeleteSecret: %w", err)
	}
	if _, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: smxSecretName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetSecret after delete = %v, want NotFound", err)
	}
	return nil
}
