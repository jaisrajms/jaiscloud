//go:build gcp_conformance

package main

// Blank imports link the probe messages' compiled descriptors into the tool so
// protoregistry.GlobalFiles can resolve the scope's message names. They are
// explicit rather than relying on the import list of tests/gcpconformance,
// which the tool also links for the Discovery loader.
//
// Storage v2 is the only proto generated inside this repository
// (internal/gcp/grpc/storage/storagepb). The other probes come from the
// official client modules the emulator already depends on — importing the
// official storage client here would panic on duplicate file registration
// (same google.storage.v2 full names), so the in-repo package is used instead.
import (
	_ "cloud.google.com/go/firestore/apiv1/admin/adminpb"
	_ "cloud.google.com/go/functions/apiv2/functionspb"
	_ "cloud.google.com/go/kms/apiv1/kmspb"
	_ "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	_ "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	_ "jaiscloud/internal/gcp/grpc/storage/storagepb"
)
