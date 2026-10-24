# gcplint

`gcplint` is a static analyzer that enforces the GCP house conventions from
`CLAUDE.md` over the GCP tree. It is the GCP analogue of
`scripts/check_no_hardcoded_arn.sh` (AWS hardcoded ARNs) and
`tools/lint/paginationcheck` (pagination heuristic), and runs as part of
`make lint` (and the `test-gcp-unit` CI job).

## Rules

| rule | flags | fix |
|---|---|---|
| `clock` | `time.Now()` / `time.Since()` / `time.Until()` | use `clock.Now()` (business) or `clock.RealNow()` (infrastructure) |
| `clock-realnow` | a `clock.Now()` driving a wall-clock wait (poll-loop condition, a derived deadline compared inside a loop, or an argument to `time.Sleep`/`After`/`NewTimer`/`NewTicker`/`Tick`/`context.WithDeadline`) | use `clock.RealNow()` for infrastructure time |
| `resource-prefix` | a literal `projects/…`, `organizations/…` or `folders/…` used to **build** a name (`+` operand or `fmt.Sprintf`/`Sprint`/`Sprintln`/`strings.Join` argument) | build it with `internal/gcp/resource.ResourceID` |
| `store-sentinel` | `==` / `!=` against `store.ErrNotFound` / `store.ErrAlreadyExists` | use `errors.Is` |
| `unsafe-assert` | a `.(string)` type assertion not in comma-ok form | use `s, ok := x.(string)` |
| `reset-registration` | a mutating provider/service core that implements `Reset(context.Context)` but is never passed to `RegisterResetter`/`RegisterSnapshotter` | register it (or allowlist if it delegates to a registered store) |
| `aws-import` | a file under `internal/gcp` importing `internal/aws` | remove the import (cloud isolation) |

Construction-only for `resource-prefix` means parsing idioms
(`strings.HasPrefix`/`TrimPrefix`/`Split`) are not flagged.

## Usage

```sh
go run ./tools/lint/gcplint ./internal/gcp/... ./cmd/jaiscloud-gcp
```

Exit code 1 when violations are found, 0 otherwise. Diagnostics carry their rule
in the message prefix (`[rule] …`) and in the diagnostic category (`-json`).

## Suppression

- **Inline:** `//gcplint:ignore <rule>` on the offending line or the line above
  (or `//gcplint:ignore` alone for all rules). `//gcplint:ignore-file <rule>`
  anywhere in a file suppresses the whole file.
- **Allowlist file:** `tools/lint/gcplint/allowlist.txt` (override with
  `GCPLINT_ALLOWLIST`), one `<path>:<rule>` per line, each requiring a trailing
  `# reason`. Use this for cross-file rules (`reset-registration`) and for whole
  files of deliberate, documented deviations. Paths match by suffix.
