package logs

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// PutLogEvents ingests a batch of log events into a stream.
func (p *Provider) PutLogEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if nr.Origin == model.OriginInternal {
		return p.putLogEventsInternal(ctx, nr)
	}
	groupName := paramStr(nr.Params, "logGroupName")
	streamName := paramStr(nr.Params, "logStreamName")

	rawEvents, _ := nr.Params["logEvents"].([]any)
	if len(rawEvents) == 0 {
		return nil, logsErr("InvalidParameterException", "logEvents must contain at least 1 event", 400)
	}
	if len(rawEvents) > 10000 {
		return nil, logsErr("InvalidParameterException", "logEvents must not contain more than 10000 events", 400)
	}

	now := clock.Now().UnixMilli()
	events := make([]LogEvent, 0, len(rawEvents))
	var minTS, maxTS int64
	for i, raw := range rawEvents {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, logsErr("InvalidParameterException", fmt.Sprintf("logEvents[%d] is not an object", i), 400)
		}
		ts := int64(0)
		if tsRaw, ok := m["timestamp"]; ok {
			if f, ok := tsRaw.(float64); ok {
				ts = int64(f)
			}
		}
		msg, _ := m["message"].(string)
		if len(msg) > 262144 {
			return nil, logsErr("InvalidParameterException",
				fmt.Sprintf("logEvents[%d]: message exceeds 262144 bytes", i), 400)
		}
		events = append(events, LogEvent{
			Timestamp:     ts,
			Message:       msg,
			IngestionTime: now,
		})
		if i == 0 || ts < minTS {
			minTS = ts
		}
		if i == 0 || ts > maxTS {
			maxTS = ts
		}
	}

	p.store.mu.Lock()

	if _, err := p.verifyGroupExists(groupName); err != nil {
		p.store.mu.Unlock()
		return nil, err
	}

	streams := p.store.streams[groupName]
	if streams == nil {
		p.store.mu.Unlock()
		return nil, logsErr("ResourceNotFoundException", "The specified log stream does not exist: "+streamName, 400)
	}
	stream, ok := streams[streamName]
	if !ok {
		p.store.mu.Unlock()
		return nil, logsErr("ResourceNotFoundException", "The specified log stream does not exist: "+streamName, 400)
	}

	eventMap := p.store.events[groupName]
	if eventMap == nil {
		eventMap = make(map[string]*eventRing)
		p.store.events[groupName] = eventMap
	}
	ring := eventMap[streamName]
	if ring == nil {
		ring = newEventRing()
		eventMap[streamName] = ring
	}
	ring.Append(events)

	stream.LastIngestionTime = now
	if stream.FirstEventTimestamp == 0 {
		stream.FirstEventTimestamp = minTS
	}
	if maxTS > stream.LastEventTimestamp {
		stream.LastEventTimestamp = maxTS
	}
	for _, e := range events {
		stream.StoredBytes += int64(len(e.Message))
	}

	seqTokens := p.store.seqToken[groupName]
	if seqTokens == nil {
		seqTokens = make(map[string]int64)
		p.store.seqToken[groupName] = seqTokens
	}
	seqTokens[streamName]++
	counter := seqTokens[streamName]

	// Snapshot metric filters while holding the lock so goroutines below are safe.
	mfSnap := make([]*MetricFilter, 0)
	for _, mf := range p.store.metricFilters[groupName] {
		mfSnap = append(mfSnap, mf)
	}

	p.store.mu.Unlock()

	// Fan out to subscription filters (Lambda only) and metric filter extraction
	// without holding the store lock.
	accountID := nr.AccountID
	go p.dispatchSubscriptionFilters(ctx, accountID, groupName, streamName, events)
	go p.extractMetricFilters(ctx, groupName, mfSnap, events)

	return provider.OK(map[string]any{
		"nextSequenceToken": fmt.Sprintf("%056d", counter),
	}), nil
}

// GetLogEvents retrieves events from a single log stream with optional time range and pagination.
func (p *Provider) GetLogEvents(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	streamName := paramStr(nr.Params, "logStreamName")
	startTime := int64(paramInt(nr.Params, "startTime", 0))
	endTime := int64(paramInt(nr.Params, "endTime", 0))
	// startFromHead defaults to true
	startFromHead := true
	if v, ok := nr.Params["startFromHead"].(bool); ok {
		startFromHead = v
	}
	nextTokenIn := paramStr(nr.Params, "nextToken")
	limit := paramInt(nr.Params, "limit", 10000)
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}

	p.store.mu.RLock()
	defer p.store.mu.RUnlock()

	if _, err := p.verifyGroupExists(groupName); err != nil {
		return nil, err
	}

	streams := p.store.streams[groupName]
	if streams == nil || streams[streamName] == nil {
		return nil, logsErr("ResourceNotFoundException", "The specified log stream does not exist: "+streamName, 400)
	}

	eventMap := p.store.events[groupName]
	var allEvents []LogEvent
	if eventMap != nil {
		if ring := eventMap[streamName]; ring != nil {
			allEvents = ring.Slice()
		}
	}

	// Sort by timestamp ascending
	sort.Slice(allEvents, func(i, j int) bool {
		return allEvents[i].Timestamp < allEvents[j].Timestamp
	})

	// Filter by time range (0 means no bound)
	var filtered []LogEvent
	for _, e := range allEvents {
		if startTime != 0 && e.Timestamp < startTime {
			continue
		}
		if endTime != 0 && e.Timestamp >= endTime {
			continue
		}
		filtered = append(filtered, e)
	}

	// If startFromHead == false, reverse
	if !startFromHead {
		for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
			filtered[i], filtered[j] = filtered[j], filtered[i]
		}
	}

	// Parse nextToken to get offset
	offset := 0
	if nextTokenIn != "" {
		if strings.HasPrefix(nextTokenIn, "f/") {
			if n, err := strconv.Atoi(nextTokenIn[2:]); err == nil {
				offset = n
			}
		} else if strings.HasPrefix(nextTokenIn, "b/") {
			if n, err := strconv.Atoi(nextTokenIn[2:]); err == nil {
				offset = n
			}
		}
	}

	// Skip offset events
	if offset > len(filtered) {
		offset = len(filtered)
	}
	filtered = filtered[offset:]

	// Take up to limit
	page := filtered
	if len(page) > limit {
		page = page[:limit]
	}

	// Build output events
	outEvents := make([]any, 0, len(page))
	for _, e := range page {
		outEvents = append(outEvents, map[string]any{
			"timestamp":     e.Timestamp,
			"message":       e.Message,
			"ingestionTime": e.IngestionTime,
		})
	}

	newOffset := offset + len(page)
	return provider.OK(map[string]any{
		"events":            outEvents,
		"nextForwardToken":  fmt.Sprintf("f/%d", newOffset),
		"nextBackwardToken": fmt.Sprintf("b/%d", offset),
	}), nil
}

// filterToken is used to encode/decode pagination tokens for FilterLogEvents.
type filterToken struct {
	Group  string `json:"g"`
	Stream string `json:"s"`
	Offset int    `json:"o"`
}

// FilterLogEvents searches across multiple streams in a log group.
func (p *Provider) FilterLogEvents(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	groupName := paramStr(nr.Params, "logGroupName")
	streamPrefix := paramStr(nr.Params, "logStreamNamePrefix")
	filterPattern := paramStr(nr.Params, "filterPattern")
	startTime := int64(paramInt(nr.Params, "startTime", 0))
	endTime := int64(paramInt(nr.Params, "endTime", 0))
	nextTokenIn := paramStr(nr.Params, "nextToken")
	limit := paramInt(nr.Params, "limit", 10000)
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}

	// Parse logStreamNames
	var streamNames []string
	if rawNames, ok := nr.Params["logStreamNames"].([]any); ok {
		for _, v := range rawNames {
			if s, ok := v.(string); ok {
				streamNames = append(streamNames, s)
			}
		}
	}

	p.store.mu.RLock()
	defer p.store.mu.RUnlock()

	if _, err := p.verifyGroupExists(groupName); err != nil {
		return nil, err
	}

	// Resolve target streams
	streams := p.store.streams[groupName]
	var targetStreams []string
	if len(streamNames) > 0 {
		targetStreams = streamNames
	} else if streamPrefix != "" {
		for name := range streams {
			if strings.HasPrefix(name, streamPrefix) {
				targetStreams = append(targetStreams, name)
			}
		}
		sort.Strings(targetStreams)
	} else {
		for name := range streams {
			targetStreams = append(targetStreams, name)
		}
		sort.Strings(targetStreams)
	}

	// Parse pagination token
	var tok filterToken
	if nextTokenIn != "" {
		decoded, err := base64.StdEncoding.DecodeString(nextTokenIn)
		if err != nil {
			return nil, logsErr("InvalidParameterException", "Invalid nextToken", 400)
		}
		if err := json.Unmarshal(decoded, &tok); err != nil {
			return nil, logsErr("InvalidParameterException", "Invalid nextToken", 400)
		}
	}

	type annotatedEvent struct {
		LogStreamName string
		LogEvent
	}

	// Collect all matching events from all target streams
	var allEvents []annotatedEvent
	eventMap := p.store.events[groupName]
	for _, sName := range targetStreams {
		if eventMap == nil {
			continue
		}
		ring := eventMap[sName]
		if ring == nil {
			continue
		}
		evs := ring.Slice()
		sort.Slice(evs, func(i, j int) bool {
			return evs[i].Timestamp < evs[j].Timestamp
		})
		for _, e := range evs {
			// Time filter
			if startTime != 0 && e.Timestamp < startTime {
				continue
			}
			if endTime != 0 && e.Timestamp >= endTime {
				continue
			}
			allEvents = append(allEvents, annotatedEvent{LogStreamName: sName, LogEvent: e})
		}
	}

	// Sort merged events by timestamp ascending
	sort.Slice(allEvents, func(i, j int) bool {
		if allEvents[i].Timestamp != allEvents[j].Timestamp {
			return allEvents[i].Timestamp < allEvents[j].Timestamp
		}
		return allEvents[i].LogStreamName < allEvents[j].LogStreamName
	})

	// Apply filterPattern (supports JSON field filters and substring match; empty = all pass)
	if filterPattern != "" {
		matcher := compileLogFilter(filterPattern)
		var matched []annotatedEvent
		for _, e := range allEvents {
			if matcher(e.Message) {
				matched = append(matched, e)
			}
		}
		allEvents = matched
	}

	// Apply nextToken offset
	startOffset := 0
	if nextTokenIn != "" {
		startOffset = tok.Offset
	}
	if startOffset > len(allEvents) {
		startOffset = len(allEvents)
	}
	allEvents = allEvents[startOffset:]

	// Take up to limit
	page := allEvents
	if len(page) > limit {
		page = page[:limit]
	}

	// Build output events with eventId
	outEvents := make([]any, 0, len(page))
	for _, e := range page {
		h := sha256.New()
		h.Write([]byte(e.LogStreamName + strconv.FormatInt(e.Timestamp, 10) + e.Message))
		eventID := fmt.Sprintf("%x", h.Sum(nil))
		if len(eventID) > 40 {
			eventID = eventID[:40]
		}
		outEvents = append(outEvents, map[string]any{
			"logStreamName": e.LogStreamName,
			"timestamp":     e.Timestamp,
			"message":       e.Message,
			"ingestionTime": e.IngestionTime,
			"eventId":       eventID,
		})
	}

	// Build searchedLogStreams
	searchedStreams := make([]any, 0, len(targetStreams))
	for _, sName := range targetStreams {
		searchedStreams = append(searchedStreams, map[string]any{
			"logStreamName":      sName,
			"searchedCompletely": true,
		})
	}

	// Generate next token if more events remain
	result := map[string]any{
		"events":             outEvents,
		"searchedLogStreams": searchedStreams,
	}
	if len(allEvents) > limit {
		newTok := filterToken{
			Group:  groupName,
			Stream: "",
			Offset: startOffset + limit,
		}
		tokBytes, _ := json.Marshal(newTok)
		result["nextToken"] = base64.StdEncoding.EncodeToString(tokBytes)
	}

	return provider.OK(result), nil
}
