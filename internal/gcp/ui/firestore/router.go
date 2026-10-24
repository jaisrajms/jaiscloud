package firestoreui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Firestore UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/collections", h.ListCollections)

	// StructuredQuery runner (structured builder + raw query console).
	r.Post("/query", h.RunQuery)

	// Composite indexes (Firestore Admin surface). Without a collectionGroup the
	// list spans the whole database ("-" wildcard).
	r.Get("/indexes", h.ListIndexes)
	r.Post("/indexes", h.CreateIndex)
	r.Get("/indexes/{collectionGroup}/{indexId}", h.GetIndex)
	r.Delete("/indexes/{collectionGroup}/{indexId}", h.DeleteIndex)

	r.Get("/collections/{collection}/documents", h.ListDocuments)
	r.Post("/collections/{collection}/documents", h.CreateDocument)
	r.Get("/collections/{collection}/documents/{document}", h.GetDocument)
	r.Patch("/collections/{collection}/documents/{document}", h.UpdateDocument)
	r.Delete("/collections/{collection}/documents/{document}", h.DeleteDocument)
	r.Get("/collections/{collection}/documents/{document}/collections", h.ListSubcollections)

	return r
}
