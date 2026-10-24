//go:build windows

package ui

// OpenBrowser opens url in the default Windows browser.
func OpenBrowser(url string) { runOpenCmd("cmd", "/c", "start", url) }
