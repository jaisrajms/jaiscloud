//go:build !ui

package ui

import "io/fs"

// StaticFS returns nil when built without -tags ui (no embedded assets).
func StaticFS() (fs.FS, error) {
	return nil, nil
}
