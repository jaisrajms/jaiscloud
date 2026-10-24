package firestoreui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *firestore.Provider used by the Firestore
// UI handlers. It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	// ListCollectionIds returns the top-level collection IDs of the database.
	ListCollectionIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Documents in a collection.
	ListDocuments(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DocumentsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateDocument(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DocumentsPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DocumentsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// RunQuery executes a StructuredQuery (newline-delimited JSON response).
	RunQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Composite indexes (the Firestore Admin surface). Indexes are addressed per
	// collection group; the list call accepts the "-" wildcard for the whole
	// database.
	CreateIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListIndexes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteIndex(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
