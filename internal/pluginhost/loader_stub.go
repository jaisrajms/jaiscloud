//go:build !(cgo && (linux || darwin))

package pluginhost

import (
	"errors"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// InProcessSupported reports whether this build can load "so:" plugins. The Go
// plugin package exists only for cgo builds on linux and darwin.
func InProcessSupported() bool { return false }

func openSO(string) (sdk.Plugin, error) {
	return nil, errors.New("this build cannot load in-process plugins (CGO is disabled or the OS is unsupported): " +
		"use an http:// plugin, or build the emulator with `make build-plugins` (linux or darwin, CGO_ENABLED=1)")
}
