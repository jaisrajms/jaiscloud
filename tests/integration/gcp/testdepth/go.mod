module jaiscloud-gcp-testdepth

go 1.26

// Dependency floors are deliberately the OLDEST versions present in the
// per-SDK test modules. A consumer's module picks the higher of the two by
// minimum-version selection, so the kit can never force a version bump (and a
// go.sum rewrite) on the suites that import it.
require (
	google.golang.org/api v0.219.0
	google.golang.org/grpc v1.70.0
)

require (
	golang.org/x/sys v0.29.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250124145028-65684f501c47 // indirect
	google.golang.org/protobuf v1.36.4 // indirect
)
