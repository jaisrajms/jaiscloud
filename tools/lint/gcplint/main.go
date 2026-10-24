// gcplint is a static analyzer that enforces the GCP conventions documented in
// CLAUDE.md over the GCP tree (internal/gcp/**). It is the GCP analogue of the
// AWS hardcoded-ARN guard (scripts/check_no_hardcoded_arn.sh) and the
// pagination heuristic (tools/lint/paginationcheck).
//
// Rules:
//
//	clock                — time.Now()/time.Since()/time.Until() instead of internal/clock
//	clock-realnow        — a clock.Now() driving a wall-clock wait/deadline instead of clock.RealNow()
//	resource-prefix      — a literal "projects/…"/"organizations/…"/"folders/…" used to build a
//	                       resource name instead of internal/gcp/resource.ResourceID
//	store-sentinel       — == / != against store.ErrNotFound / store.ErrAlreadyExists
//	unsafe-assert        — a .(string) type assertion not in comma-ok form
//	reset-registration   — a mutating provider/service type that implements Reset(context.Context)
//	                       but is never passed to RegisterResetter/RegisterSnapshotter
//	aws-import           — a file under internal/gcp importing internal/aws
//
// Usage:
//
//	go run ./tools/lint/gcplint ./internal/gcp/... ./cmd/jaiscloud-gcp
//
// Exit code 1 when violations are found, 0 otherwise.
//
// Suppression: a line carrying a violation may be annotated with an inline
// `//gcplint:ignore <rule>` comment (on the same or the preceding line); a
// `//gcplint:ignore-file <rule>` comment anywhere in a file suppresses the file.
// Cross-file rules (reset-registration) use the central allowlist file
// tools/lint/gcplint/allowlist.txt (override with GCPLINT_ALLOWLIST).
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	singlechecker.Main(Analyzer)
}
