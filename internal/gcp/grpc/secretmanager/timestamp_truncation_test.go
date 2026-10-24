package secretmanager

import (
	"testing"
	"time"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

// TestSecretTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// server google-datetime instant to the microsecond the REST renderer
// (provider/secretmanager) emits.
func TestSecretTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	s := secretToProto("proj", secretmanagerstore.Secret{ID: "s1", CreateTime: instant})
	if got := s.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("secret.createTime = %v, want %v", got, want)
	}
	v := versionToProto("proj", secretmanagerstore.Version{
		SecretID: "s1", VersionID: "1", CreateTime: instant, DestroyTime: instant,
	}, nil)
	if got := v.GetCreateTime().AsTime(); !got.Equal(want) {
		t.Fatalf("version.createTime = %v, want %v", got, want)
	}
	if got := v.GetDestroyTime().AsTime(); !got.Equal(want) {
		t.Fatalf("version.destroyTime = %v, want %v", got, want)
	}
}
