package store

import "errors"

// Stubs mirroring jaiscloud/internal/store sentinels for the analyse test.
var (
	ErrNotFound      = errors.New("resource not found")
	ErrAlreadyExists = errors.New("resource already exists")
)
