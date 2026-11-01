package ecs

import (
	"bufio"
	"context"
	"io"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/logstream"
)

// StreamLogs reads lines from src and writes them to CloudWatch Logs using the awslogs config.
func StreamLogs(ctx context.Context, logsAPI logstream.Ingestor, cfg LogConfig, containerName, taskID string, src io.Reader) {
	if logsAPI == nil || cfg.LogDriver != "awslogs" {
		return
	}
	group := cfg.Options["awslogs-group"]
	prefix := cfg.Options["awslogs-stream-prefix"]
	if group == "" {
		return
	}
	if cfg.Options["awslogs-create-group"] == "true" {
		_ = logsAPI.InternalCreateLogGroup(ctx, group)
	}
	streamName := prefix + "/" + containerName + "/" + taskID
	scanner := bufio.NewScanner(src)
	var batch []logstream.Event
	for scanner.Scan() {
		line := scanner.Text()
		batch = append(batch, logstream.Event{Timestamp: clock.RealNow().UnixMilli(), Message: line})
		if len(batch) >= 10 {
			_ = logsAPI.InternalPutEvents(ctx, group, streamName, batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		// The last lines are the ones worth keeping after a stop; ctx may be done.
		_ = logsAPI.InternalPutEvents(context.WithoutCancel(ctx), group, streamName, batch)
	}
}
