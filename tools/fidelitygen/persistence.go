//go:build gcp_conformance

package main

// persistentBackends records, per wire service, whether the emulator persists
// that service's state across a restart — i.e. it has both a memory and a
// Postgres backend plus a snapshot implementation.
//
// Derivation (from the repo, not hand-waved):
//
//   - internal/gcp/store/<dir>/: every directory carries memory.go,
//     postgres.go and snapshot.go (verified by inspection of the tree), so a
//     service backed by a dedicated store qualifies.
//   - The shared control-plane store internal/store (MemoryResourceStore +
//     PostgresResourceStore, both implementing Snapshot) backs the
//     metadata-only services; it is registered as the "resources"
//     snapshotter in cmd/jaiscloud-gcp/main.go, so those services qualify too.
//
// Service -> store lineage:
//
//	storage            -> internal/gcp/store/gcs
//	secretmanager      -> internal/gcp/store/secretmanager (+ kms)
//	kms                -> internal/gcp/store/kms
//	iam                -> internal/gcp/store/kms + internal/store (accounts)
//	pubsub             -> internal/gcp/store/pubsub (+ kms)
//	firestore          -> internal/gcp/store/firestore
//	firestoreadmin     -> internal/store (ResourceStore; composite-index state)
//	datastore          -> internal/gcp/store/datastore
//	functions          -> internal/gcp/store/functions
//	workflows          -> internal/gcp/store/workflows
//	workflowexecutions -> internal/gcp/store/workflows
//	dataproc           -> internal/gcp/store/dataproc
//	managedkafka       -> internal/gcp/store/managedkafka
//	metastore          -> internal/gcp/store/metastore
//	iceberg            -> internal/gcp/store/iceberg
//	bigquery           -> internal/gcp/store/bigquery
//	eventarc           -> internal/gcp/store/eventarc (+ workflows)
//	clouddns           -> internal/store (ResourceStore)
//	cloudsql           -> internal/store (ResourceStore)
//	compute            -> internal/store (ResourceStore)
//	memorystore        -> internal/store (ResourceStore)
//	serviceusage       -> internal/store (ResourceStore)
//	resourcemanager    -> internal/store (ResourceStore, via internal/gcp/policy)
//	logging            -> internal/gcp/store/logging
//	monitoring         -> internal/gcp/store/monitoring
//	scheduler          -> internal/gcp/store/scheduler
//	tasks              -> internal/gcp/store/tasks
//
// Every enumerated REST service therefore qualifies today, and the gRPC-only
// data services (datastore, logging) qualify via their own dedicated stores
// (firestoreadmin via the shared ResourceStore), so no service is listed
// false. The map is explicit rather than "true for
// everything" so a future service that lands without a store defaults to false
// and its mutating operations are downgraded until persistence parity exists.
// Truly stateless services are also listed true (persistence is N/A for them,
// not a gap).
//
// operations -> stateless: the google.longrunning.Operations stub keeps no
// operation registry (every operation is synchronously terminal), so
// DeleteOperation/CancelOperation have no state to persist and are not
// mutating-without-a-backend gaps.
var persistentBackends = map[string]bool{
	"storage":            true,
	"secretmanager":      true,
	"kms":                true,
	"iam":                true,
	"iamcredentials":     true,
	"pubsub":             true,
	"firestore":          true,
	"firestoreadmin":     true,
	"datastore":          true,
	"functions":          true,
	"workflows":          true,
	"workflowexecutions": true,
	"dataproc":           true,
	"container":          true,
	"run":                true,
	"managedkafka":       true,
	"metastore":          true,
	"iceberg":            true,
	"bigquery":           true,
	"eventarc":           true,
	"clouddns":           true,
	"cloudsql":           true,
	"compute":            true,
	"memorystore":        true,
	"serviceusage":       true,
	"resourcemanager":    true,
	"logging":            true,
	"monitoring":         true,
	"scheduler":          true,
	"tasks":              true,
	"operations":         true,
}
