//go:build linux

package ui

// OpenBrowser opens url in the default Linux browser via xdg-open.
func OpenBrowser(url string) { runOpenCmd("xdg-open", url) }
