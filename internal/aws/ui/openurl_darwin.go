//go:build darwin

package ui

// OpenBrowser opens url in the default macOS browser.
func OpenBrowser(url string) { runOpenCmd("open", url) }
