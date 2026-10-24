# testdepth — the shared behavioural-depth kit

The assertion kit the per-service depth suites import (plan **DC2**,
`plan_docs/gcp-depth-closure-wave-plan.md`). It exists because the three
client-observable depth classes — **p4** paging, **p3** error envelopes and
**p2** contention — were re-derived per service, so a single depth cell cost a
whole session of copy-paste.

It is a separate Go module (`jaiscloud-gcp-testdepth`) because the SDK suites
live in per-service nested modules and a nested module is invisible to the root
module. Its dependencies are pinned to the **oldest** versions present in the
tree, so minimum-version selection in a consuming module picks its own newer
versions and the kit never forces a `go.sum` rewrite.

## Using it

A consuming module (`tests/integration/gcp/sdk-*`) already carries the wiring:

```
require jaiscloud-gcp-testdepth v0.0.0

replace jaiscloud-gcp-testdepth => ../testdepth
```

The `require`/`replace` pair is pre-wired by DC2 for every module the depth wave
imports into, so a session adding an assertion touches only its own test file.
`go mod tidy` keeps the pair once the test file imports the package.

```go
// package name: testdepth
import "jaiscloud-gcp-testdepth"
```

Run the consumer's suite with `-race`: the contention helper refuses to run a
contention assertion in a binary without the race detector, and the wiring above
therefore also added `-race` to that module's `test-gcp-integration` step and to
`make test-integration-gcp`.

## The three contracts

- **`AssertPaging(t, newLister, Paging[T]) []T`** — walks a List method to the
  end and asserts every resource appears exactly once, in the documented order
  across page boundaries, no page larger than the requested `pageSize`, an empty
  final token, the expected set (`Expect`), the filter's expected subset
  (`Include`), and that a second walk returned the same resources (sequence-wise
  when an order is documented, as a set otherwise). `Walk`/`CheckPages`/
  `CheckStable` are the pure halves. The empty-intermediate-page rule is **opt-in**
  (`RequireNonEmptyPages`): AIP-158 lets a list method return zero results on a
  page in the middle of a collection, so it is not part of the default contract.
  `AssertInvalidPageSize` covers the documented negative — and it is per
  Discovery, not universal: AIP-158 requires `INVALID_ARGUMENT` for a negative
  `pageSize` and coercion (never rejection) for an over-max one, but a service may
  document that a non-positive value is *ignored* (Cloud Logging), so pass the
  value and envelope that service documents.
- **`AssertError(t, err, ErrorSpec)`** — matches the typed error an official
  client surfaces (`*googleapi.Error` over REST, a gRPC status over gRPC) against
  the documented envelope. `SpecNotFound()`/`SpecAlreadyExists()`/`SpecAborted()`
  … are the catalogue: one spec asserts both transports, since the google.rpc
  status name (`NOT_FOUND`) is what the REST envelope carries as
  `error.status`. `FAILED_PRECONDITION` has **two** HTTP mappings — the
  convention's 400 (`SpecPreconditionFailed`) and the conditional-request 412
  (`SpecConditionalRequestFailed`) — so they are separate specs rather than one
  guess. `RunNegatives` runs a p3 table.
- **`Race(n, fn)` + `AssertOneWinner(t, errs, isConflict)`** — N writers released
  from one gate, exactly one winner, the documented conflict for the losers.
  `AssertOneWinner` requires the race detector (it fails rather than skipping), so
  a forgotten `-race` cannot silently weaken a contention test.

`StatusOf` classifies anything it cannot type and prints the Go type, so an
unknown client wrapper is diagnosable rather than silently mismatched.

## What it is not

- **Not a service suite.** It imports no service client package, so
  `tools/gcpstatus` never attributes its own tests to a service and it never
  becomes a `suites:` entry in a `docs/test-depth/*.yaml` record.
- **Not a real-GCP verifier.** It asserts what a local client must observe; the
  authority for the expected envelope stays the Discovery document/proto and the
  captured differentials (`tests/gcpdifferential`, `docs/gcp/testability.md` §7).
- **Not a scale harness.** `p5` (streaming, large payload, lineage) stays with
  the suite that owns the surface.
