package sdk

// Version is the version of this SDK, the one a plugin's go.mod requires:
//
//	require github.com/jaisrajms/jaiscloud/sdk v0.1.0
//
// The emulator reports it in `version` and in `/_jaiscloud/doctor` so that an author with only
// a binary knows what to require. An in-process plugin must be built against exactly the SDK
// source the emulator was built from, so the git tag sdk/<Version> must point at a commit whose
// sdk/ directory is identical to the emulator's. `make sdk-tag` checks that and creates the tag.
//
// Change Version in the same commit as any change to the SDK that is released, never a tag
// that already exists (the Go module proxy caches a tag for good).
const Version = "v0.1.0"
