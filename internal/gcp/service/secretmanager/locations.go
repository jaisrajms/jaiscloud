package secretmanager

// A user-managed replication policy names the Google Cloud regions that hold
// the secret's replicas; real GCP rejects a location it does not offer (the
// console only lists them, and the API answers INVALID_ARGUMENT for anything
// else). The emulator keeps a stable list of the regions Google Cloud offers
// rather than accepting any string, so a typo such as `us-east-1` fails at
// create instead of producing a secret no real client could address.

// replicaLocations is the set of Google Cloud regions a user-managed replica
// may name. `global` is deliberately absent: it is the automatic arm's
// location, not a replica location.
//
// The list is a snapshot of the regions Google Cloud offers. A newly launched
// region has to be added here before a client can target it, and a policy
// stored before that (or restored from a snapshot) cannot be updated through a
// replication-masked patch until it is — the alternative, accepting any string,
// would let a typo such as `us-east-1` create a secret no real client could
// address.
var replicaLocations = map[string]bool{
	"africa-south1":           true,
	"asia-east1":              true,
	"asia-east2":              true,
	"asia-northeast1":         true,
	"asia-northeast2":         true,
	"asia-northeast3":         true,
	"asia-south1":             true,
	"asia-south2":             true,
	"asia-southeast1":         true,
	"asia-southeast2":         true,
	"asia-southeast3":         true,
	"australia-southeast1":    true,
	"australia-southeast2":    true,
	"europe-central2":         true,
	"europe-north1":           true,
	"europe-north2":           true,
	"europe-southwest1":       true,
	"europe-west1":            true,
	"europe-west2":            true,
	"europe-west3":            true,
	"europe-west4":            true,
	"europe-west6":            true,
	"europe-west8":            true,
	"europe-west9":            true,
	"europe-west10":           true,
	"europe-west12":           true,
	"me-central1":             true,
	"me-central2":             true,
	"me-west1":                true,
	"northamerica-northeast1": true,
	"northamerica-northeast2": true,
	"northamerica-south1":     true,
	"southamerica-east1":      true,
	"southamerica-west1":      true,
	"us-central1":             true,
	"us-east1":                true,
	"us-east4":                true,
	"us-east5":                true,
	"us-south1":               true,
	"us-west1":                true,
	"us-west2":                true,
	"us-west3":                true,
	"us-west4":                true,
}

// validReplicaLoc is the region check for a user-managed replica.
func validReplicaLoc(location string) bool {
	return replicaLocations[location]
}
