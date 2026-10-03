//go:build !linux

package kafka

import "os/exec"

// applySysProcAttr is a no-op off Linux, where syscall.SysProcAttr has no
// Pdeathsig field. The /proc start-time fingerprint that makes the pidfile
// sweep safe is unavailable there too, so those builds simply rely on a
// graceful StopCluster/Shutdown.
func applySysProcAttr(cmd *exec.Cmd) {}
