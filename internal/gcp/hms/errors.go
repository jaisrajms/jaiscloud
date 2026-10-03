package hms

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
)

// DeclaredError is a Thrift exception declared in a method's `throws` clause.
// It is encoded as a field of the method's result struct (not as a
// TApplicationException), at the position given by the throws order.
type DeclaredError struct {
	FieldID int16  // position in the result struct (1-based throws order)
	Name    string // exception struct name, e.g. "MetaException"
	Message string
}

func (e *DeclaredError) Error() string { return e.Name + ": " + e.Message }

// AppError is a TApplicationException (method-missing / protocol error),
// encoded with message type EXCEPTION rather than REPLY.
type AppError struct {
	Type    int32 // e.g. thrift.UNKNOWN_METHOD, thrift.INTERNAL_ERROR
	Message string
}

func (e *AppError) Error() string { return e.Message }

// Declared exception names from hive_metastore.thrift.
const (
	excMetaException             = "MetaException"
	excNoSuchObjectException     = "NoSuchObjectException"
	excAlreadyExistsException    = "AlreadyExistsException"
	excInvalidObjectException    = "InvalidObjectException"
	excInvalidOperationException = "InvalidOperationException"
)

// ExceptionField returns the Field to place in a result struct for a declared
// exception: a STRUCT field (at the throws position) wrapping the exception
// struct, which itself carries a single `message` (field 1) string — the shape
// of every exception in hive_metastore.thrift.
func ExceptionField(e *DeclaredError) Field {
	return Field{ID: e.FieldID, V: StructV(NewBuilder().Str(1, e.Message).Build())}
}

// WriteAppException writes a TApplicationException as a struct with message
// (field 1) and type (field 2), matching the Thrift-defined exception shape.
func WriteAppException(ctx context.Context, p thrift.TProtocol, typ int32, msg string) error {
	return WriteStruct(ctx, p, NewBuilder().Str(1, msg).I32(2, typ).Build())
}
