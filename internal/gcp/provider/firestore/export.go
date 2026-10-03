package firestore

// Exported type aliases for the wire structs shared between the REST adapter
// (this package) and the gRPC transport (internal/gcp/grpc/firestore). The
// unexported canonical names remain the source of truth used throughout this
// package; the aliases let the gRPC transcoder construct the exact same values
// without duplicating the definitions or exposing the package internals
// piecemeal.

type (
	// PageParams carries cursor-pagination inputs to the Service list methods.
	PageParams = pageParams

	// StructuredQuery and its nested wire shapes (query.go).
	StructuredQuery    = structuredQuery
	CollectionSelector = collectionSelector
	FieldReference     = fieldReference
	FieldFilter        = fieldFilter
	CompositeFilter    = compositeFilter
	Filter             = filter
	UnaryFilter        = unaryFilter
	Order              = order
	Cursor             = cursor
	Projection         = projection

	// Write wire shapes (commit.go).
	WriteWire             = writeWire
	DocumentWire          = documentWire
	DocumentMaskWire      = documentMaskWire
	PreconditionWire      = preconditionWire
	FieldTransformWire    = fieldTransformWire
	DocumentTransformWire = documentTransformWire

	// Composite-index wire shapes (indexes.go), shared with the FirestoreAdmin
	// gRPC transport (internal/gcp/grpc/firestoreadmin).
	IndexDef   = indexDef
	IndexField = indexField
)

// NewPageParams returns pagination inputs for a Service list method. The
// pageParams fields are deliberately unexported, so the transport boundary
// builds the value here instead of reaching into the struct.
func NewPageParams(size int, token string) PageParams {
	return pageParams{size: size, token: token}
}
