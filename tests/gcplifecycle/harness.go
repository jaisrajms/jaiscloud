// Package gcplifecycle is the GCP lifecycle + ownership harness (FC5).
//
// The harness drives one parent resource through the documented lifecycle —
// create → get → list → update → blocked delete → delete → get(404) → recreate
// the same id → delete with children attached — over BOTH transports, and
// asserts on the resource stores (the state oracle), not only on responses.
//
// Ownership is declared, not discovered: every parent carries a manifest of its
// child collections and the policy the parent's delete applies to each one
// (cascade | retain | detach | reject-if-nonempty). A child collection that is
// not listed is a bug in the manifest, not an untested edge; the adapters'
// childCounts is the join between the manifest and the stores.
package gcplifecycle

import (
	"context"
	"errors"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"

	"jaiscloud/internal/model"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// serveGRPC starts an in-process gRPC server (the transport the harness drives
// gRPC steps through) and returns a client connection to it.
func serveGRPC(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(ln) }()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return conn
}

// transport names the adapter path a step is driven through.
type transport string

const (
	restT transport = "rest"
	grpcT transport = "grpc"
)

// Policy is the documented ownership policy a parent's delete applies to a
// child collection. The four values are the complete set real GCP uses for the
// pilot resources: GCS deletes a bucket's notifications with the bucket
// (cascade) but refuses a non-empty bucket (reject-if-nonempty); Pub/Sub keeps
// a topic's subscriptions and marks their topic deleted (detach); Firestore
// deletes a database's data and admin children (cascade) but keeps its backups
// (retain).
type Policy string

const (
	Cascade          Policy = "cascade"
	Retain           Policy = "retain"
	Detach           Policy = "detach"
	RejectIfNonEmpty Policy = "reject-if-nonempty"
	NoDelete         Policy = "no-delete"
)

// childSpec names one child collection and the policy its parent applies.
type childSpec struct {
	name   string
	policy Policy
}

// manifest declares the ownership contract of one parent resource. It is the
// table the harness asserts against; a policy that does not hold is a lifecycle
// defect, not a test-tuning problem.
type manifest struct {
	// parent is the canonical resource name fragment used in failure messages.
	parent string
	// policy is what deleting the parent does when the delete is legal.
	policy Policy
	// children lists every child collection the parent owns.
	children []childSpec
	// noUpdate marks a parent with no mutable field to exercise (the KMS key
	// ring is create/get/list only).
	noUpdate bool
	// recreateFails marks a parent whose id is deliberately retired on delete
	// (a deleted KMS crypto key cannot be re-created with the same name).
	recreateFails bool
	// note documents a deliberate deviation from the naive expectation.
	note string
}

// childCount is the oracle's view of one child collection: how many rows exist
// and how many are still attached to the (live) parent. Attached only matters
// for the detach policy.
type childCount struct {
	total    int
	attached int
}

// blockedResult is one documented failed-delete attempt: the error plus the
// status code the transport must answer.
type blockedResult struct {
	name     string
	err      error
	wantCode string
}

// adapter is one pilot's dual-transport driver. Implementations construct the
// same stores for both transports (the way cmd/jaiscloud-gcp wires them), so a
// step driven over REST is visible to a gRPC read and vice versa.
type adapter interface {
	name() string

	// create makes the parent `id` and returns an opaque identity token (etag,
	// uid, generation) so the harness can assert a recreate starts fresh.
	create(ctx context.Context, tr transport, id string) (identity string, err error)
	get(ctx context.Context, tr transport, id string) (found bool, err error)
	list(ctx context.Context, tr transport) (ids []string, err error)
	update(ctx context.Context, tr transport, id string) error
	remove(ctx context.Context, tr transport, id string) error

	// blockedDelete runs the pilot's documented failed-delete attempts against a
	// parent whose state must survive every attempt.
	blockedDelete(ctx context.Context, tr transport, id string) []blockedResult

	// populate creates one child of every manifest collection.
	populate(ctx context.Context, id string) error
	// counts reports the surviving children per manifest child name.
	counts(ctx context.Context, id string) (map[string]childCount, error)
	// clearRejecting removes the children whose policy is reject-if-nonempty so
	// a parent that refuses a non-empty delete can finally be deleted. Children
	// with every other policy stay in place for the cascade/retain/detach
	// assertion.
	clearRejecting(ctx context.Context, id string) error
}

// extraRunner is implemented by pilots with an edge the shared sequence does
// not cover (KMS crypto-key lifecycle, Dataproc recreate-while-DELETING,
// storage object recreate).
type extraRunner interface {
	extra(t *testing.T, ctx context.Context, tr transport, id string)
}

// runCase runs the lifecycle sequence over both transports, each on a fresh
// adapter so the transports are not observing each other's state.
func runCase(t *testing.T, m manifest, newAdapter func(t *testing.T) adapter) {
	t.Helper()
	for _, tr := range []transport{restT, grpcT} {
		tr := tr
		t.Run(string(tr), func(t *testing.T) {
			a := newAdapter(t)
			runSequence(t, m, a, tr)
		})
	}
}

func runSequence(t *testing.T, m manifest, a adapter, tr transport) {
	t.Helper()
	ctx := context.Background()
	id := "lc-" + strings.ReplaceAll(string(tr), "_", "-")

	t.Run("create/get/list", func(t *testing.T) {
		identity, err := a.create(ctx, tr, id)
		if err != nil {
			t.Fatalf("%s create(%s): %v", a.name(), id, err)
		}
		if _, err := a.create(ctx, tr, id); err == nil {
			t.Fatalf("%s create(%s) twice succeeded, want AlreadyExists", a.name(), id)
		} else if !isAlreadyExists(err) {
			t.Fatalf("%s duplicate create = %v, want AlreadyExists", a.name(), err)
		}
		assertFound(t, a, ctx, tr, id)
		assertListed(t, a, ctx, tr, id, true)
		if identity == "" {
			t.Logf("%s: create returned no identity token; recreate freshness not asserted", a.name())
		}
	})

	t.Run("update", func(t *testing.T) {
		if m.noUpdate {
			return
		}
		if err := a.update(ctx, tr, id); err != nil {
			t.Fatalf("%s update(%s): %v", a.name(), id, err)
		}
		assertFound(t, a, ctx, tr, id)
	})

	t.Run("blocked-delete-keeps-state", func(t *testing.T) {
		for _, res := range a.blockedDelete(ctx, tr, id) {
			if res.err == nil {
				t.Errorf("%s: blocked delete %q succeeded, want %s", a.name(), res.name, res.wantCode)
				continue
			}
			if code, httpStatus := codeOf(res.err); !codeMatches(code, httpStatus, res.wantCode) {
				t.Errorf("%s: blocked delete %q = %q/%d, want %s", a.name(), res.name, code, httpStatus, res.wantCode)
			}
		}
		assertFound(t, a, ctx, tr, id)
	})

	t.Run("delete-get-404", func(t *testing.T) {
		if m.policy == NoDelete {
			// No delete operation exists (Cloud KMS key rings): the resource
			// must still be readable, and the pilot's extra covers its children.
			assertFound(t, a, ctx, tr, id)
			return
		}
		if err := a.remove(ctx, tr, id); err != nil {
			t.Fatalf("%s delete(%s): %v", a.name(), id, err)
		}
		assertGone(t, a, ctx, tr, id)
		assertListed(t, a, ctx, tr, id, false)
	})

	t.Run("recreate-same-id", func(t *testing.T) {
		if m.policy == NoDelete {
			return
		}
		_, err := a.create(ctx, tr, id)
		if m.recreateFails {
			if !isAlreadyExists(err) {
				t.Fatalf("%s recreate(%s) = %v, want the documented AlreadyExists", a.name(), id, err)
			}
			return
		}
		if err != nil {
			t.Fatalf("%s recreate(%s) = %v, want success (a legal recreate must never answer ALREADY_EXISTS)", a.name(), id, err)
		}
		assertFound(t, a, ctx, tr, id)
	})

	t.Run("delete-with-children", func(t *testing.T) {
		if m.policy == NoDelete {
			return
		}
		if err := a.populate(ctx, id); err != nil {
			t.Fatalf("%s populate(%s): %v", a.name(), id, err)
		}
		before, err := a.counts(ctx, id)
		if err != nil {
			t.Fatalf("%s counts(%s): %v", a.name(), id, err)
		}
		rejects := false
		for _, c := range m.children {
			if c.policy == RejectIfNonEmpty && before[c.name].total > 0 {
				rejects = true
			}
			if before[c.name].total == 0 {
				t.Errorf("%s: child %q was not populated (the manifest and the adapter disagree)", a.name(), c.name)
			}
		}

		if rejects {
			err := a.remove(ctx, tr, id)
			if err == nil {
				t.Fatalf("%s delete(%s) with non-empty children succeeded, want the documented rejection", a.name(), id)
			}
			assertFound(t, a, ctx, tr, id) // state intact
		}

		if err := a.clearRejecting(ctx, id); err != nil {
			t.Fatalf("%s clearRejecting(%s): %v", a.name(), id, err)
		}
		if err := a.remove(ctx, tr, id); err != nil {
			t.Fatalf("%s delete(%s): %v", a.name(), id, err)
		}

		after, err := a.counts(ctx, id)
		if err != nil {
			t.Fatalf("%s counts after delete: %v", a.name(), err)
		}
		for _, c := range m.children {
			got := after[c.name]
			switch c.policy {
			case Cascade:
				if got.total != 0 {
					t.Errorf("%s: delete(%s) left %d %q row(s), want cascade to zero", a.name(), id, got.total, c.name)
				}
			case Retain:
				if got.total == 0 {
					t.Errorf("%s: delete(%s) removed retained %q rows", a.name(), id, c.name)
				}
			case Detach:
				if got.attached != 0 {
					t.Errorf("%s: delete(%s) left %d %q row(s) attached, want detach", a.name(), id, got.attached, c.name)
				}
				if got.total == 0 {
					t.Errorf("%s: delete(%s) removed detached %q rows, want retain-and-detach", a.name(), id, c.name)
				}
			case RejectIfNonEmpty:
				// Cleared before the delete; nothing to assert.
			}
		}
	})

	if ex, ok := a.(extraRunner); ok {
		t.Run("extra", func(t *testing.T) {
			ex.extra(t, ctx, tr, id)
		})
	}

	t.Run("race-delete-recreate", func(t *testing.T) {
		if m.policy == NoDelete {
			return
		}
		runRace(t, m, a, ctx, tr, id)
	})
}

// runRace hammers delete+create for the same id and then asserts that no child
// of a dead incarnation leaked into a live one. The invariant is policy-aware:
// cascade children must be gone, detach children must be detached, retained
// children may survive.
func runRace(t *testing.T, m manifest, a adapter, ctx context.Context, tr transport, id string) {
	t.Helper()
	// The preceding steps may have left the parent deleted (a cascade or a
	// detach delete) — bring it back before staging children for the race.
	if _, err := a.create(ctx, tr, id); err != nil && !isAlreadyExists(err) {
		t.Fatalf("%s create before race: %v", a.name(), err)
	}
	if err := a.populate(ctx, id); err != nil {
		t.Fatalf("%s populate before race: %v", a.name(), err)
	}
	if err := a.clearRejecting(ctx, id); err != nil {
		t.Fatalf("%s clearRejecting before race: %v", a.name(), err)
	}
	const rounds = 12
	var wg sync.WaitGroup
	errs := make(chan error, rounds*2)
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs <- a.remove(ctx, tr, id)
		}()
		go func() {
			defer wg.Done()
			_, err := a.create(ctx, tr, id)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		// A concurrent delete whose etag was read before another goroutine
		// re-created the parent is the precondition doing its job.
		if err == nil || isAlreadyExists(err) || isNotFound(err) || isFailedPrecondition(err) {
			continue
		}
		t.Errorf("%s: race delete/create error: %v", a.name(), err)
	}

	// The parent may be live (create won last) or absent (delete won last); the
	// ownership invariant holds either way.
	found, err := a.get(ctx, tr, id)
	if err != nil {
		t.Fatalf("%s: get after race: %v", a.name(), err)
	}
	_ = found
	after, err := a.counts(ctx, id)
	if err != nil {
		t.Fatalf("%s: counts after race: %v", a.name(), err)
	}
	for _, c := range m.children {
		got := after[c.name]
		switch c.policy {
		case Cascade:
			if got.total != 0 {
				t.Errorf("%s: race leaked %d cascade %q row(s) from a deleted incarnation", a.name(), got.total, c.name)
			}
		case Detach:
			if got.attached != 0 {
				t.Errorf("%s: race left %d %q row(s) attached to a re-created parent", a.name(), got.attached, c.name)
			}
		case Retain, RejectIfNonEmpty:
			// Unconstrained: retained rows are expected to outlive the parent,
			// and the race never populated a rejecting collection.
		}
	}
}

func assertFound(t *testing.T, a adapter, ctx context.Context, tr transport, id string) {
	t.Helper()
	found, err := a.get(ctx, tr, id)
	if err != nil {
		t.Fatalf("%s %s get(%s): %v", a.name(), tr, id, err)
	}
	if !found {
		t.Fatalf("%s %s get(%s) = not found, want found", a.name(), tr, id)
	}
}

func assertGone(t *testing.T, a adapter, ctx context.Context, tr transport, id string) {
	t.Helper()
	for _, read := range []transport{restT, grpcT} {
		found, err := a.get(ctx, read, id)
		if err != nil {
			t.Fatalf("%s %s get(%s) after delete: %v", a.name(), read, id, err)
		}
		if found {
			t.Fatalf("%s %s get(%s) after delete = found, want not found (read over %s after a %s delete)", a.name(), read, id, read, tr)
		}
	}
}

func assertListed(t *testing.T, a adapter, ctx context.Context, tr transport, id string, want bool) {
	t.Helper()
	for _, read := range []transport{restT, grpcT} {
		ids, err := a.list(ctx, read)
		if err != nil {
			t.Fatalf("%s %s list: %v", a.name(), read, err)
		}
		got := false
		for _, gotID := range ids {
			if gotID == id {
				got = true
			}
		}
		if got != want {
			sort.Strings(ids)
			t.Fatalf("%s %s list contains %q = %v, want %v (list: %v)", a.name(), read, id, got, want, ids)
		}
	}
}

// codeOf reduces both transports' errors to a status name + HTTP status.
func codeOf(err error) (string, int) {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		return pe.Code, pe.HTTPStatus
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.OK {
		return st.Code().String(), 0
	}
	return "", 0
}

func codeMatches(code string, httpStatus int, want string) bool {
	if want == "" {
		return true
	}
	if strings.EqualFold(code, want) {
		return true
	}
	// Transport-specific spellings of the same documented outcome.
	switch want {
	case "AlreadyExists":
		return httpStatus == 409 || strings.EqualFold(code, "Conflict")
	case "FailedPrecondition":
		return httpStatus == 409 || httpStatus == 400 || strings.EqualFold(code, "bucketNotEmpty")
	}
	return false
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	code, httpStatus := codeOf(err)
	return codeMatches(code, httpStatus, "AlreadyExists")
}

func isFailedPrecondition(err error) bool {
	if err == nil {
		return false
	}
	code, httpStatus := codeOf(err)
	return codeMatches(code, httpStatus, "FailedPrecondition")
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	code, httpStatus := codeOf(err)
	return strings.EqualFold(code, "NotFound") || httpStatus == 404
}
