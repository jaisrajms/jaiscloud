//go:build ui

package ui

import (
	"embed"
	"io/fs"
)

//go:embed dist
var embeddedFS embed.FS

// StaticFS returns the embedded dist/ directory as an fs.FS when built with -tags ui.
func StaticFS() (fs.FS, error) {
	return fs.Sub(embeddedFS, "dist")
}
