//go:build cgo && (linux || darwin)

package pluginhost

import (
	"fmt"
	goplugin "plugin"
	"strings"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// InProcessSupported reports whether this build can load "so:" plugins. The Go
// plugin package exists only for cgo builds on linux and darwin.
func InProcessSupported() bool { return true }

// openSO loads a Go plugin built with -buildmode=plugin and calls its exported
// New function. The error for a file that does not match this build says what this
// build is, because Go's own message (for example "different version of package
// internal/goarch") names a package, not the cause.
func openSO(path string) (p sdk.Plugin, err error) {
	defer func() {
		if r := recover(); r != nil {
			p, err = nil, fmt.Errorf("%s: the plugin panicked while loading: %v", path, r)
		}
	}()
	plug, err := goplugin.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w\n%s%s", path, err, mismatchHelp(), sdkHint(err))
	}
	sym, err := plug.Lookup("New")
	if err != nil {
		return nil, fmt.Errorf("%s has no exported New: write `func New() sdk.Plugin` in package main", path)
	}
	newFn, ok := sym.(func() sdk.Plugin)
	if !ok {
		return nil, fmt.Errorf("%s: New is a %T, want func() sdk.Plugin from the SDK this emulator was built with\n%s", path, sym, mismatchHelp())
	}
	return newFn(), nil
}

func mismatchHelp() string {
	return "This emulator is " + Build().String() + ". A plugin must be built with the same Go toolchain, " +
		"-trimpath, CGO_ENABLED=1 and the same SDK source (see docs/PLUGINS.md, \"In-process plugins\")."
}

// sdkHint adds the usual cause when the package that differs is the SDK itself: the
// emulator holds the SDK as a dependency, so a plugin built inside the SDK module (where
// it is the main module) has a package of the same name that Go treats as another one.
func sdkHint(err error) string {
	if !strings.Contains(err.Error(), "jaiscloud/sdk") {
		return ""
	}
	return "\nThe SDK package differs. Build the plugin as a module of its own that requires the SDK " +
		"(not inside the SDK module), from the same SDK source this emulator was built with."
}
