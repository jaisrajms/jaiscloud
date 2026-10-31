package logs

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

var logGroupNameRE = regexp.MustCompile(`^[\.\-_/#A-Za-z0-9]+$`)

// CreateLogGroup creates a new log group.
func (p *Provider) CreateLogGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := paramStr(nr.Params, "logGroupName")
	if name == "" {
		return nil, logsErr("InvalidParameterException", "logGroupName is required", 400)
	}
	if len(name) > 512 {
		return nil, logsErr("InvalidParameterException", "logGroupName must not exceed 512 characters", 400)
	}
	if !logGroupNameRE.MatchString(name) {
		return nil, logsErr("InvalidParameterException", "logGroupName contains invalid characters", 400)
	}

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	if _, exists := p.store.groups[name]; exists {
		return nil, logsErr("ResourceAlreadyExistsException", "The specified log group already exists: "+name, 400)
	}

	arn := nr.ResourceID("logs-group", name)
	g := &LogGroup{
		LogGroupName: name,
		CreationTime: clock.Now().UnixMilli(),
		Arn:          arn,
	}
	p.store.groups[name] = g

	// Process optional tags
	if tagsRaw, ok := nr.Params["tags"]; ok {
		if tagsMap, ok := tagsRaw.(map[string]any); ok {
			tags := make(map[string]string, len(tagsMap))
			for k, v := range tagsMap {
				if s, ok := v.(string); ok {
					tags[k] = s
				}
			}
			if len(tags) > 0 {
				p.store.tags[arn] = tags
			}
		}
	}

	return provider.OK(map[string]any{}), nil
}

// DeleteLogGroup deletes a log group and all its streams/events.
func (p *Provider) DeleteLogGroup(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := paramStr(nr.Params, "logGroupName")

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	g, err := p.verifyGroupExists(name)
	if err != nil {
		return nil, err
	}

	// Remove all streams, event rings, seq tokens for this group
	delete(p.store.streams, name)
	delete(p.store.events, name)
	delete(p.store.seqToken, name)
	delete(p.store.tags, g.Arn)
	delete(p.store.groups, name)

	return provider.OK(map[string]any{}), nil
}

// DescribeLogGroups lists log groups with optional prefix/pattern filtering and pagination.
func (p *Provider) DescribeLogGroups(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	prefix := paramStr(nr.Params, "logGroupNamePrefix")
	pattern := paramStr(nr.Params, "logGroupNamePattern")
	nextTokenIn := paramStr(nr.Params, "nextToken")
	limit := paramInt(nr.Params, "limit", 50)
	if limit <= 0 || limit > 50 {
		limit = 50
	}

	if prefix != "" && pattern != "" {
		return nil, logsErr("InvalidParameterException",
			"LogGroup name prefix and LogGroup name pattern are mutually exclusive parameters.", 400)
	}

	p.store.mu.RLock()
	defer p.store.mu.RUnlock()

	// Collect and filter
	var groups []*LogGroup
	for _, g := range p.store.groups {
		if prefix != "" && !strings.HasPrefix(g.LogGroupName, prefix) {
			continue
		}
		if pattern != "" && !strings.Contains(g.LogGroupName, pattern) {
			continue
		}
		groups = append(groups, g)
	}

	// Sort lexicographically
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].LogGroupName < groups[j].LogGroupName
	})

	// Apply nextToken cursor (skip groups where name <= nextToken)
	if nextTokenIn != "" {
		start := 0
		for start < len(groups) && groups[start].LogGroupName <= nextTokenIn {
			start++
		}
		groups = groups[start:]
	}

	// Paginate
	var nextToken string
	if len(groups) > limit {
		nextToken = groups[limit-1].LogGroupName
		groups = groups[:limit]
	}

	// Build response list
	out := make([]any, 0, len(groups))
	for _, g := range groups {
		item := map[string]any{
			"logGroupName":  g.LogGroupName,
			"creationTime":  g.CreationTime,
			"arn":           g.Arn,
			"storedBytes":   g.StoredBytes,
			"logGroupClass": "STANDARD",
		}
		if g.RetentionInDays != nil {
			item["retentionInDays"] = *g.RetentionInDays
		}
		out = append(out, item)
	}

	result := map[string]any{"logGroups": out}
	if nextToken != "" {
		result["nextToken"] = nextToken
	}
	return provider.OK(result), nil
}

// ─── retention policy ─────────────────────────────────────────────────────────

var validRetentionDays = map[int]bool{
	1: true, 3: true, 5: true, 7: true, 14: true, 30: true, 60: true, 90: true,
	120: true, 150: true, 180: true, 365: true, 400: true, 545: true, 731: true,
	1096: true, 1827: true, 2192: true, 2557: true, 2922: true, 3288: true, 3653: true,
}

// PutRetentionPolicy sets the retention policy on a log group.
func (p *Provider) PutRetentionPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := paramStr(nr.Params, "logGroupName")
	days := paramInt(nr.Params, "retentionInDays", 0)

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	g, err := p.verifyGroupExists(name)
	if err != nil {
		return nil, err
	}
	if !validRetentionDays[days] {
		return nil, logsErr("InvalidParameterException",
			"The specified retention period is not valid", 400)
	}
	g.RetentionInDays = &days
	return provider.OK(map[string]any{}), nil
}

// DeleteRetentionPolicy removes the retention policy from a log group.
func (p *Provider) DeleteRetentionPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := paramStr(nr.Params, "logGroupName")

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	g, err := p.verifyGroupExists(name)
	if err != nil {
		return nil, err
	}
	g.RetentionInDays = nil
	return provider.OK(map[string]any{}), nil
}

// TagLogGroup adds or updates tags on a log group.
func (p *Provider) TagLogGroup(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := paramStr(nr.Params, "logGroupName")

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	g, err := p.verifyGroupExists(name)
	if err != nil {
		return nil, err
	}

	if tagsRaw, ok := nr.Params["tags"]; ok {
		if tagsMap, ok := tagsRaw.(map[string]any); ok {
			existing := p.store.tags[g.Arn]
			if existing == nil {
				existing = make(map[string]string)
				p.store.tags[g.Arn] = existing
			}
			for k, v := range tagsMap {
				if s, ok := v.(string); ok {
					existing[k] = s
				}
			}
		}
	}
	return provider.OK(map[string]any{}), nil
}

// UntagLogGroup removes specified tag keys from a log group.
func (p *Provider) UntagLogGroup(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := paramStr(nr.Params, "logGroupName")

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	g, err := p.verifyGroupExists(name)
	if err != nil {
		return nil, err
	}

	if keysRaw, ok := nr.Params["tags"]; ok {
		if keys, ok := keysRaw.([]any); ok {
			existing := p.store.tags[g.Arn]
			if existing != nil {
				for _, k := range keys {
					if ks, ok := k.(string); ok {
						delete(existing, ks)
					}
				}
			}
		}
	}
	return provider.OK(map[string]any{}), nil
}

// ListTagsLogGroup returns the tags for a log group.
func (p *Provider) ListTagsLogGroup(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := paramStr(nr.Params, "logGroupName")

	p.store.mu.RLock()
	defer p.store.mu.RUnlock()

	g, err := p.verifyGroupExists(name)
	if err != nil {
		return nil, err
	}

	tags := p.store.tags[g.Arn]
	if tags == nil {
		tags = map[string]string{}
	}

	// Convert map[string]string → map[string]any for the response
	outTags := make(map[string]any, len(tags))
	for k, v := range tags {
		outTags[k] = v
	}
	return provider.OK(map[string]any{"tags": outTags}), nil
}

// ─── ARN-based tagging (4.11) ─────────────────────────────────────────────────

// groupByARN scans for a log group whose ARN matches. Must be called with at least a read lock.
func (s *memStore) groupByARN(arn string) *LogGroup {
	for _, g := range s.groups {
		if g.Arn == arn {
			return g
		}
	}
	return nil
}

// TagResource adds tags to a log group identified by its ARN.
func (p *Provider) TagResource(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := paramStr(nr.Params, "resourceArn")

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	g := p.store.groupByARN(arn)
	if g == nil {
		return nil, logsErr("ResourceNotFoundException", "The specified resource does not exist: "+arn, 400)
	}
	if p.store.tags[arn] == nil {
		p.store.tags[arn] = make(map[string]string)
	}
	if tagMap, ok := nr.Params["tags"].(map[string]any); ok {
		for k, v := range tagMap {
			p.store.tags[arn][k] = fmt.Sprint(v)
		}
	}
	return provider.OK(map[string]any{}), nil
}

// UntagResource removes tags from a log group identified by its ARN.
func (p *Provider) UntagResource(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := paramStr(nr.Params, "resourceArn")

	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	existing := p.store.tags[arn]
	if existing != nil {
		if keys, ok := nr.Params["tagKeys"].([]any); ok {
			for _, k := range keys {
				delete(existing, fmt.Sprint(k))
			}
		}
	}
	return provider.OK(map[string]any{}), nil
}

// ListTagsForResource returns the tags for a log group identified by its ARN.
func (p *Provider) ListTagsForResource(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := paramStr(nr.Params, "resourceArn")

	p.store.mu.RLock()
	defer p.store.mu.RUnlock()

	tags := p.store.tags[arn]
	out := make([]map[string]any, 0, len(tags))
	for k, v := range tags {
		out = append(out, map[string]any{"Key": k, "Value": v})
	}
	return provider.OK(map[string]any{"tags": out}), nil
}
