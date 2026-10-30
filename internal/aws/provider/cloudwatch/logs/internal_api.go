package logs

import (
	"context"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/logstream"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// putLogEventsInternal serves PutLogEvents for origin=internal callers (the
// Lambda, ECS, EMR and Step Functions log streamers): the stream is created on
// demand and a missing group drops the batch, exactly like InternalPutEvents.
func (p *Provider) putLogEventsInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	raw, _ := nr.Params["logEvents"].([]any)
	events := make([]logstream.Event, 0, len(raw))
	for _, r := range raw {
		m, _ := r.(map[string]any)
		ts, _ := m["timestamp"].(float64)
		msg, _ := m["message"].(string)
		events = append(events, logstream.Event{Timestamp: int64(ts), Message: msg})
	}
	if err := p.InternalPutEvents(ctx, paramStr(nr.Params, "logGroupName"), paramStr(nr.Params, "logStreamName"), events); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// InternalPutEvents implements logstream.Ingestor for the Lambda log streamer.
// It creates the log stream if absent and appends the events without a NormalizedRequest.
func (p *Provider) InternalPutEvents(ctx context.Context, logGroupName, logStreamName string, events []logstream.Event) error {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	if _, ok := p.store.groups[logGroupName]; !ok {
		return nil // group not yet created; drop silently
	}

	streams := p.store.streams[logGroupName]
	if streams == nil {
		streams = make(map[string]*LogStream)
		p.store.streams[logGroupName] = streams
	}
	if _, ok := streams[logStreamName]; !ok {
		streams[logStreamName] = &LogStream{
			LogStreamName: logStreamName,
			CreationTime:  clock.Now().UnixMilli(),
		}
	}

	eventMap := p.store.events[logGroupName]
	if eventMap == nil {
		eventMap = make(map[string]*eventRing)
		p.store.events[logGroupName] = eventMap
	}
	ring := eventMap[logStreamName]
	if ring == nil {
		ring = newEventRing()
		eventMap[logStreamName] = ring
	}

	logEvents := make([]LogEvent, 0, len(events))
	now := clock.Now().UnixMilli()
	for _, e := range events {
		logEvents = append(logEvents, LogEvent{
			Timestamp:     e.Timestamp,
			Message:       e.Message,
			IngestionTime: now,
		})
	}
	ring.Append(logEvents)
	return nil
}

// InternalCreateLogGroup implements logstream.Ingestor for the Lambda log streamer.
// Creates the log group if it does not already exist (idempotent).
func (p *Provider) InternalCreateLogGroup(ctx context.Context, logGroupName string) error {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	if _, ok := p.store.groups[logGroupName]; ok {
		return nil
	}
	p.store.groups[logGroupName] = &LogGroup{
		LogGroupName: logGroupName,
		CreationTime: clock.Now().UnixMilli(),
	}
	return nil
}
