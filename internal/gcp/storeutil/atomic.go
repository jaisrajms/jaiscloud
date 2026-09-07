// Package storeutil provides the shared atomic read-modify-write primitive
// GCP memory stores use for their Update/Patch-style methods.
//
// An adversarial review across the GCP providers (storage, secretmanager,
// datastore, functions, workflows, dataproc, bigquery, managedkafka,
// monitoring, and the shared IAM-policy helper) found the same bug shape
// repeated in nearly every Update/Patch handler: Get() the current value,
// merge request fields into it in Go, then a SEPARATE, later Update() call
// writes the result — with no atomicity between the two. A second writer
// racing in between is silently overwritten (a lost update), and in at
// least one case (Secret Manager) this corrupted an internally-managed
// counter, not just a client-visible field. This mirrors the Firestore
// lost-update race fixed earlier by routing PatchDocument/buildUpdate
// through store.Commit's atomic read-set validation — this package
// generalizes that fix into one reusable primitive instead of hand-rolling
// it per store.
//
// AtomicUpdate is for in-memory stores (sync.RWMutex-guarded maps). Postgres
// stores need the equivalent guarantee too, but SQL specifics (table/column
// names) don't generalize into shared Go code the same way; the convention
// there is a Serializable-isolation transaction with `SELECT ... FOR UPDATE`
// on every row the mutation reads or writes, mirroring
// internal/gcp/store/firestore/postgres.go's Commit — see that file for the
// reference shape when adding this to a Postgres store.
package storeutil

import "sync"

// AtomicUpdate holds mu for the entire read-mutate-write sequence: get()
// reads the current value (and whether it exists), mutate computes the new
// value from it (or returns an error to abort without writing — a
// validation failure, a not-found condition, or a client-supplied
// precondition mismatch), and set() persists the result. Holding one lock
// across all three closes the race the Get-then-separate-Update pattern
// leaves open: no other Get/Create/Update/Delete guarded by the same mu can
// be observed or applied in the middle of this sequence.
//
// Keep get, mutate, and set fast and allocation-light — no I/O, no
// acquiring any other lock — since mu is held for their combined duration.
// On a mutate error, set is never called and the zero value of T is
// returned alongside the error.
func AtomicUpdate[T any](mu *sync.RWMutex, get func() (T, bool), mutate func(current T, exists bool) (T, error), set func(T)) (T, error) {
	mu.Lock()
	defer mu.Unlock()

	current, exists := get()
	next, err := mutate(current, exists)
	if err != nil {
		var zero T
		return zero, err
	}
	set(next)
	return next, nil
}
