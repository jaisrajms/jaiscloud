package secretmanager

import (
	"testing"

	"jaiscloud/internal/gcp/store/kms"
)

// TestKmsKeyVersionName pins the derivation of the SecretVersion
// replicationStatus key version name (DC44), including the cases where it must
// be absent rather than approximated.
func TestKmsKeyVersionName(t *testing.T) {
	const key = "projects/p/locations/global/keyRings/r/cryptoKeys/k"
	tests := []struct {
		name    string
		keyName string
		wrapped []byte
		want    string
	}{
		{
			name:    "tagged wrapped DEK reports the wrap-time version",
			keyName: key,
			wrapped: kms.EncodeVersionedCiphertext("3", []byte("ct")),
			// Secret Manager spells the segment `versions`, not Cloud KMS's
			// `cryptoKeyVersions`.
			want: key + "/versions/3",
		},
		{
			name:    "no CMEK key means no status",
			keyName: "",
			wrapped: kms.EncodeVersionedCiphertext("1", []byte("ct")),
			want:    "",
		},
		{
			name:    "Google-managed version has no wrapped DEK",
			keyName: key,
			wrapped: nil,
			want:    "",
		},
		{
			name:    "untagged DEK is not turned into a bogus version",
			keyName: key,
			wrapped: []byte("raw-ciphertext-without-version-tag"),
			want:    "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := KmsKeyVersionName(tc.keyName, tc.wrapped); got != tc.want {
				t.Fatalf("KmsKeyVersionName() = %q, want %q", got, tc.want)
			}
		})
	}
}
