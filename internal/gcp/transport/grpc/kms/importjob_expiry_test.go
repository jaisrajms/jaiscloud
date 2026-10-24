package kms

import (
	"bytes"
	"testing"

	kmscore "jaiscloud/internal/gcp/service/kms"

	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
)

// TestImportJobProtoOmitsWrappingKeyWhenExpired pins DC43 on the gRPC side: the
// ImportJob proto documents the wrapping public key as "Only returned if state
// is ACTIVE", so an EXPIRED job must leave public_key unset while still carrying
// the public_key_format the caller asked for (proto3-JSON clients see the format
// and no key, matching the REST renderer).
func TestImportJobProtoOmitsWrappingKeyWhenExpired(t *testing.T) {
	const pemKey = "-----BEGIN PUBLIC KEY-----\nMIIBfake\n-----END PUBLIC KEY-----\n"
	cases := []struct {
		name       string
		job        kmscore.ImportJob
		wantFormat kmspb.PublicKey_PublicKeyFormat
		wantKey    bool
	}{
		{
			name: "active PEM job publishes its key",
			job: kmscore.ImportJob{
				ImportMethod: "RSA_OAEP_3072_SHA256", State: "ACTIVE", PublicKeyPEM: pemKey,
			},
			wantFormat: kmspb.PublicKey_PEM, wantKey: true,
		},
		{
			name: "expired PEM job withholds its key",
			job: kmscore.ImportJob{
				ImportMethod: "RSA_OAEP_3072_SHA256", State: "EXPIRED", PublicKeyPEM: pemKey,
			},
			wantFormat: kmspb.PublicKey_PEM, wantKey: false,
		},
		{
			name: "active HPKE job publishes its raw key",
			job: kmscore.ImportJob{
				ImportMethod: "HPKE_KEM_XWING_HKDF_SHA256_AES_256_GCM", State: "ACTIVE",
				PublicKeyRaw: []byte{0x01, 0x02, 0x03},
			},
			wantFormat: kmspb.PublicKey_XWING_RAW_BYTES, wantKey: true,
		},
		{
			name: "expired HPKE job withholds its raw key",
			job: kmscore.ImportJob{
				ImportMethod: "HPKE_KEM_XWING_HKDF_SHA256_AES_256_GCM", State: "EXPIRED",
				PublicKeyRaw: []byte{0x01, 0x02, 0x03},
			},
			wantFormat: kmspb.PublicKey_XWING_RAW_BYTES, wantKey: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := importJobToProto(tc.job)
			if out.GetPublicKeyFormat() != tc.wantFormat {
				t.Fatalf("publicKeyFormat = %v, want %v", out.GetPublicKeyFormat(), tc.wantFormat)
			}
			if tc.wantKey {
				if out.GetPublicKey() == nil {
					t.Fatal("ACTIVE job must publish its wrapping public key")
				}
				if pem := out.GetPublicKey().GetPem(); pem != "" && pem != pemKey {
					t.Fatalf("publicKey.pem = %q, want the stored PEM", pem)
				}
				return
			}
			if out.GetPublicKey() != nil {
				t.Fatalf("EXPIRED job leaked its wrapping public key: %v", out.GetPublicKey())
			}
		})
	}

	// The expired shapes must not smuggle the key material through the format
	// or any other field.
	expired := importJobToProto(kmscore.ImportJob{
		ImportMethod: "RSA_OAEP_3072_SHA256", State: "EXPIRED", PublicKeyPEM: pemKey,
	})
	if expired.GetPublicKey() != nil || bytes.Contains([]byte(expired.String()), []byte("MIIBfake")) {
		t.Fatalf("EXPIRED job response still carries key material: %v", expired)
	}
}
