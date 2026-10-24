package ui

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"time"

	"github.com/mattn/go-isatty"
)

func runOpenCmd(name string, args ...string) {
	if !isatty.IsTerminal(os.Stdout.Fd()) {
		slog.Debug("--ui-open: not a TTY, skipping browser launch")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	if err := cmd.Run(); err != nil {
		slog.Warn("--ui-open: failed to open browser", "err", err)
	}
}
