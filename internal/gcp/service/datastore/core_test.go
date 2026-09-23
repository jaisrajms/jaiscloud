package datastore

import (
	"context"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

func TestValueEqualNumeric(t *testing.T) {
	int3 := dsstore.Value{IntegerValue: ptrInt64(3)}
	dbl3 := dsstore.Value{DoubleValue: ptrFloat64(3.0)}
	int5 := dsstore.Value{IntegerValue: ptrInt64(5)}

	if !valueEqual(int3, dbl3) {
		t.Fatal("valueEqual(int 3, double 3.0) = false")
	}
	if !valueEqual(dbl3, int3) {
		t.Fatal("valueEqual(double 3.0, int 3) = false")
	}
	if valueEqual(int3, int5) {
		t.Fatal("valueEqual(int 3, int 5) = true")
	}
	if valueEqual(int3, dsstore.Value{DoubleValue: ptrFloat64(3.5)}) {
		t.Fatal("valueEqual(int 3, double 3.5) = true")
	}

	if c, ok := valueCompare(int3, int5); !ok || c >= 0 {
		t.Fatalf("valueCompare(int 3, int 5) = %d, %v; want < 0", c, ok)
	}
	if c, ok := valueCompare(dbl3, int5); !ok || c >= 0 {
		t.Fatalf("valueCompare(double 3.0, int 5) = %d, %v; want < 0", c, ok)
	}
}

// TestResetClearsTransactions verifies that the core's Reset clears the
// in-memory transaction read-set registry: a handle open before Reset is no
// longer active afterwards.
func TestResetClearsTransactions(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "test")
	ctx := context.Background()

	txn, err := s.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := s.requireActive(txn); err != nil {
		t.Fatalf("transaction should be active before Reset: %v", err)
	}

	s.Reset(ctx)

	if err := s.requireActive(txn); err == nil {
		t.Fatal("transaction should be inactive after Reset")
	}
}

func ptrInt64(n int64) *int64       { return &n }
func ptrFloat64(f float64) *float64 { return &f }

// TestAllocateIDsRejectsEmptyKeyPath locks the preserved validation: an empty
// key path (no kind) cannot be completed with an allocated ID.
func TestAllocateIDsRejectsEmptyKeyPath(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "test")
	if _, err := s.AllocateIDs(context.Background(), "p", []Key{{}}); err == nil {
		t.Fatal("AllocateIDs with an empty key path should be rejected")
	}
}
