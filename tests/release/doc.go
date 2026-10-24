// Package release holds smoke tests for the GoReleaser artifacts.
//
// The tests only run with the `release` build tag and need a snapshot build in
// dist/ first:
//
//	pnpm --dir ui run build
//	goreleaser release --snapshot --clean --skip=publish,docker
//	go test -count=1 -tags release ./tests/release/...
//
// They assert the parts of the release contract a tagged run would otherwise
// only discover at publish time: the archive contents, the version stamped by
// `-X main.version`, the deb/rpm packages, and (when a local jaiscloud-gcp
// image is available) the image's REST health endpoint and embedded console.
//
// This file exists so the directory is a buildable package without the release
// tag; `go build ./...` would otherwise fail with "build constraints exclude
// all Go files".
package release
