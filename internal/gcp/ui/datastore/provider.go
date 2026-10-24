package datastoreui

import (
	"context"

	core "jaiscloud/internal/gcp/service/datastore"
)

// ProviderInterface is the subset of the Datastore core Service the console UI
// uses. The UI binds to the transport-neutral core directly (rather than a
// REST/gRPC adapter) so it reads and writes the same entity store through the
// same logic, and gets the __kind__/__property__ metadata synthesis for free.
type ProviderInterface interface {
	// RunQuery runs a structured query (including metadata kinds).
	RunQuery(ctx context.Context, project string, q *core.Query, txn []byte) (*core.QueryResult, error)
	// RunQueryGQL runs a GQL query.
	RunQueryGQL(ctx context.Context, project string, q core.GQLQuery, txn []byte, namespace, database string) (*core.QueryResult, error)
	// Lookup reads entities by key.
	Lookup(ctx context.Context, project string, keys []core.Key, txn []byte) (*core.LookupResult, error)
	// Commit applies mutations (used for upsert/delete from the console).
	Commit(ctx context.Context, project string, req *core.CommitRequest) (*core.CommitResponse, error)
}
