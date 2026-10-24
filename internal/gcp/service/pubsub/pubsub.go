// Package pubsub is the transport-neutral core for the Cloud Pub/Sub surfaces
// both transports serve.
//
// Pub/Sub predates the dual-protocol core pattern: the REST provider
// (internal/gcp/provider/pubsub) and the gRPC service (internal/gcp/grpc/pubsub)
// each own most of their logic. This core holds the operations that were
// gRPC-only and had no REST route — subscription seek (the time and snapshot
// forms) and push-config modification — so a REST request and a gRPC request
// share one implementation of the ack-version (EOD2) invariants instead of
// drifting. It operates on the same shared stores as both transports
// (store.ResourceStore for control-plane metadata, pubsubstore.Messages for the
// published messages), so it reads and writes exactly the state the transports
// already use.
//
// The core is deliberately incremental: it grows as more of the service is
// moved behind it, rather than replacing the two transports at once.
package pubsub

import (
	"strings"

	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/store"
)

// Resource-type strings mirror the REST provider's / gRPC service's persisted
// types so every caller shares the same control-plane entries.
const (
	rtSubscription = "gcp_subscription"
	rtSnapshot     = "gcp_snapshot"
)

// Service is the transport-neutral Pub/Sub core for seek and push-config.
type Service struct {
	resources store.ResourceStore
	messages  pubsubstore.Messages
}

// NewService returns a core backed by the shared stores.
func NewService(resources store.ResourceStore, messages pubsubstore.Messages) *Service {
	return &Service{resources: resources, messages: messages}
}

// SplitSubscriptionName parses "projects/{p}/subscriptions/{s}" (or the
// relative "subscriptions/{s}"), returning the project and subscription id.
// project is empty when the name carries none.
func SplitSubscriptionName(name string) (project, sub string, ok bool) {
	parts := strings.Split(name, "/")
	switch len(parts) {
	case 4:
		if parts[0] == "projects" && parts[2] == "subscriptions" {
			return parts[1], parts[3], true
		}
	case 2:
		if parts[0] == "subscriptions" {
			return "", parts[1], true
		}
	}
	return "", "", false
}

// SplitSnapshotName parses "projects/{p}/snapshots/{s}".
func SplitSnapshotName(name string) (project, snap string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "snapshots" {
		return "", "", false
	}
	return parts[1], parts[3], true
}
