package kms

import (
	"testing"
	"time"

	kmsstore "jaiscloud/internal/gcp/store/kms"

	kmscore "jaiscloud/internal/gcp/service/kms"
)

// TestKMSTimesMicrosecondTruncation pins TD48: the gRPC adapter truncates a
// server google-datetime instant to the microsecond the REST renderer
// (transport/rest/kms) emits, so the two transports agree.
func TestKMSTimesMicrosecondTruncation(t *testing.T) {
	instant := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC)
	check := func(name string, got time.Time) {
		t.Helper()
		if !got.Equal(want) {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}

	kr := keyRingToProto("proj", "global", kmsstore.KeyRing{ID: "kr1", CreateTime: instant})
	check("keyRing.createTime", kr.GetCreateTime().AsTime())

	k := cryptoKeyToProto("proj", kmsstore.CryptoKey{
		ID: "k1", Location: "global", KeyRingID: "kr1", PrimaryVersion: "1",
		Purpose: "ENCRYPT_DECRYPT", CreateTime: instant, NextRotationTime: instant,
	}, kmsstore.Version{Version: "1", CreateTime: instant})
	check("cryptoKey.createTime", k.GetCreateTime().AsTime())
	check("cryptoKey.nextRotationTime", k.GetNextRotationTime().AsTime())
	check("cryptoKey.primary.createTime", k.GetPrimary().GetCreateTime().AsTime())

	v := versionToProto("proj", "global", "kr1", "k1", kmsstore.Version{
		Version: "1", CreateTime: instant, ImportTime: instant,
		DestroyTime: instant, DestroyEventTime: instant, State: "DESTROYED",
	})
	check("version.createTime", v.GetCreateTime().AsTime())
	check("version.importTime", v.GetImportTime().AsTime())
	check("version.destroyEventTime", v.GetDestroyEventTime().AsTime())

	ij := importJobToProto(kmscore.ImportJob{CreateTime: instant, GenerateTime: instant, ExpireTime: instant})
	check("importJob.createTime", ij.GetCreateTime().AsTime())
	check("importJob.generateTime", ij.GetGenerateTime().AsTime())
	check("importJob.expireTime", ij.GetExpireTime().AsTime())

	rr := retiredResourceToProto(kmscore.RetiredResource{DeleteTime: instant})
	check("retiredResource.deleteTime", rr.GetDeleteTime().AsTime())
}
