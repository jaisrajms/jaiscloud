//go:build gcp_persistence

package paritygrpc_test

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// kmsClient builds an official Cloud KMS client over the emulator's gRPC
// listener and returns a close func for both the client and its connection.
func (d *driver) kmsClient(ctx context.Context) (*kms.KeyManagementClient, func(), error) {
	conn, err := dialGRPC(d.target("KMS_EMULATOR_HOST"))
	if err != nil {
		return nil, nil, fmt.Errorf("kms dial: %w", err)
	}
	c, err := kms.NewKeyManagementClient(ctx, option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("kms client: %w", err)
	}
	return c, func() { c.Close(); conn.Close() }, nil
}

// seedKMS exercises the gRPC-only half of Cloud KMS: a RetiredResource left by
// deleting a fully-drained CryptoKey, plus an ImportJob. It creates a keyring,
// a symmetric crypto key (primary version 1), an extra version, and an import
// job; destroys and deletes every version; then deletes the key, which records
// the RetiredResource. The returned checks read all of that back after a restart
// and assert the reset cleared the keyring and import job.
func seedKMS(d *driver, suffix string) (func() error, func() error, func() error, error) {
	project := projectID()
	parent := "projects/" + project + "/locations/global"
	ringID := "parity-ring-" + suffix
	keyID := "parity-key-" + suffix
	jobID := "parity-import-" + suffix
	ringName := parent + "/keyRings/" + ringID
	keyName := ringName + "/cryptoKeys/" + keyID
	jobName := ringName + "/importJobs/" + jobID
	retiredName := parent + "/retiredResources/" + keyID

	seed := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		c, closeFn, err := d.kmsClient(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		// The destroy-window handling below freezes the emulator clock; always
		// restore wall time so later seeds/probes are unaffected, even on error.
		defer func() { _, _, _ = d.do("POST", "/_jaiscloud/clock", `{"mode":"real"}`) }()

		if _, err := c.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: ringID}); err != nil {
			return fmt.Errorf("CreateKeyRing: %w", err)
		}
		if _, err := c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
			Parent:      ringName,
			CryptoKeyId: keyID,
			CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		}); err != nil {
			return fmt.Errorf("CreateCryptoKey: %w", err)
		}
		extra, err := c.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{Parent: keyName})
		if err != nil {
			return fmt.Errorf("CreateCryptoKeyVersion: %w", err)
		}
		if _, err := c.CreateImportJob(ctx, &kmspb.CreateImportJobRequest{
			Parent:      ringName,
			ImportJobId: jobID,
			ImportJob: &kmspb.ImportJob{
				ImportMethod:    kmspb.ImportJob_RSA_OAEP_3072_SHA256,
				ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
			},
		}); err != nil {
			return fmt.Errorf("CreateImportJob: %w", err)
		}
		// The key can only be deleted once every version is gone, and each
		// version must reach DESTROYED before it can be deleted. Destroy only
		// schedules destruction (DESTROY_SCHEDULED + destroy_time), so advance
		// the emulator clock past the window to let the lazy promotion run.
		for _, version := range []string{keyName + "/cryptoKeyVersions/1", extra.GetName()} {
			scheduled, err := c.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: version})
			if err != nil {
				return fmt.Errorf("DestroyCryptoKeyVersion(%s): %w", version, err)
			}
			if dt := scheduled.GetDestroyTime(); dt != nil {
				body := fmt.Sprintf(`{"mode":"fixed","time":%q}`,
					dt.AsTime().Add(time.Second).UTC().Format(time.RFC3339Nano))
				if code, resp, err := d.do("POST", "/_jaiscloud/clock", body); err != nil || (code != http.StatusNoContent && code != http.StatusOK) {
					return fmt.Errorf("advance clock past destroy window: code=%d resp=%s err=%v", code, truncate(resp), err)
				}
			}
			op, err := c.DeleteCryptoKeyVersion(ctx, &kmspb.DeleteCryptoKeyVersionRequest{Name: version})
			if err != nil {
				return fmt.Errorf("DeleteCryptoKeyVersion(%s): %w", version, err)
			}
			if err := op.Wait(ctx); err != nil {
				return fmt.Errorf("DeleteCryptoKeyVersion(%s) wait: %w", version, err)
			}
		}
		op, err := c.DeleteCryptoKey(ctx, &kmspb.DeleteCryptoKeyRequest{Name: keyName})
		if err != nil {
			return fmt.Errorf("DeleteCryptoKey: %w", err)
		}
		if err := op.Wait(ctx); err != nil {
			return fmt.Errorf("DeleteCryptoKey wait: %w", err)
		}
		return nil
	}

	// verify runs the full post-delete observation set; it is used both
	// immediately after seeding and after the restart, so a survived-phase
	// failure can only mean the state was lost.
	verify := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, closeFn, err := d.kmsClient(ctx)
		if err != nil {
			return err
		}
		defer closeFn()

		if _, err := c.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: ringName}); err != nil {
			return fmt.Errorf("GetKeyRing: %w", err)
		}
		if _, err := c.GetImportJob(ctx, &kmspb.GetImportJobRequest{Name: jobName}); err != nil {
			return fmt.Errorf("GetImportJob: %w", err)
		}
		if _, err := c.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: keyName + "/cryptoKeyVersions/1"}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("GetCryptoKeyVersion after delete = %v, want NotFound", err)
		}
		if _, err := c.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: keyName}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("GetCryptoKey after delete = %v, want NotFound", err)
		}
		rr, err := c.GetRetiredResource(ctx, &kmspb.GetRetiredResourceRequest{Name: retiredName})
		if err != nil {
			return fmt.Errorf("GetRetiredResource: %w", err)
		}
		if rr.GetOriginalResource() != keyName {
			return fmt.Errorf("RetiredResource original_resource = %q, want %q", rr.GetOriginalResource(), keyName)
		}
		// A retired name must not be reusable.
		_, err = c.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
			Parent:      ringName,
			CryptoKeyId: keyID,
			CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
		})
		if code := status.Code(err); code != codes.AlreadyExists && code != codes.FailedPrecondition {
			return fmt.Errorf("CreateCryptoKey on a retired name = %v (code %v), want AlreadyExists or FailedPrecondition", err, code)
		}
		return nil
	}

	cleared := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c, closeFn, err := d.kmsClient(ctx)
		if err != nil {
			return err
		}
		defer closeFn()
		if _, err := c.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: ringName}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("GetKeyRing after reset = %v, want NotFound", err)
		}
		if _, err := c.GetImportJob(ctx, &kmspb.GetImportJobRequest{Name: jobName}); status.Code(err) != codes.NotFound {
			return fmt.Errorf("GetImportJob after reset = %v, want NotFound", err)
		}
		return nil
	}

	if err := seed(); err != nil {
		return nil, nil, nil, err
	}
	// Confirm the whole fixture landed before the restart, so a later
	// "survived" failure can only mean the state was lost.
	if err := verify(); err != nil {
		return nil, nil, nil, err
	}
	return verify, verify, cleared, nil
}
