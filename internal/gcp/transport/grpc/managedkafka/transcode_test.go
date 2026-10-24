package managedkafka

import (
	"testing"

	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
)

// TestPinnedProtoLacksBootstrapAddress is a canary for the upstream-proto gap
// tracked as J55 / AUD3-11.
//
// real GCP's REST Cluster schema exposes an output-only `bootstrapAddress`, and
// the emulator's REST transport serves it
// (internal/gcp/service/managedkafka/render.go). The gRPC transport cannot: the
// pinned cloud.google.com/go/managedkafka proto defines no
// Cluster.bootstrap_address, so a typed gRPC client has no field to receive it.
// The cross-transport parity suite records the difference as a written
// allowance (tests/gcpparity/testdata/parity-allowlist.json), closed as
// "no fix": no published proto defines the field (verified against the latest
// tagged module v1.2.0 and googleapis / google-cloud-go main).
//
// If this test fails, the published proto has gained the field: populate it in
// clusterToProto (from Cluster.BootstrapAddress, falling back to the synthesized
// BootstrapAddress), then drop the J55 / AUD3-11 allowance from the parity
// allowlist and update the plan rows.
func TestPinnedProtoLacksBootstrapAddress(t *testing.T) {
	if f := (&managedkafkapb.Cluster{}).ProtoReflect().Descriptor().Fields().ByName("bootstrap_address"); f != nil {
		t.Fatalf("pinned managedkafka proto now defines Cluster.bootstrap_address (field %d); "+
			"serve it in clusterToProto and remove the J55/AUD3-11 parity allowance", f.Number())
	}
}
