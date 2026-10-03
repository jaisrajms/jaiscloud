//go:build linux

package kafka

import (
	"os/exec"
	"syscall"
)

// applySysProcAttr arms Pdeathsig so the kernel kills the broker subprocess
// when the emulator process dies, covering the SIGKILL path where no Go
// cleanup runs. Note the signal is thread-scoped — it fires when the OS thread
// that started the child exits, which for a SIGKILLed emulator is the whole
// process. The pidfile start-time sweep in native.go is the authoritative
// startup reaper for a parent that died before its child did; Pdeathsig is a
// best-effort live-process complement, not the sole guarantee.
func applySysProcAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
