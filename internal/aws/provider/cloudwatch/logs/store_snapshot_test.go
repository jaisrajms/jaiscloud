package logs

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemStore_SnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newMemStore()

	// Populate state.
	s.groups["grp-a"] = &LogGroup{LogGroupName: "grp-a", CreationTime: 1000, RetentionInDays: intPtr(7)}
	s.groups["grp-b"] = &LogGroup{LogGroupName: "grp-b", CreationTime: 2000}

	s.streams["grp-a"] = map[string]*LogStream{
		"stream-1": {LogStreamName: "stream-1", CreationTime: 1001},
	}

	s.events["grp-a"] = map[string]*eventRing{
		"stream-1": newEventRing(),
	}
	s.events["grp-a"]["stream-1"].Append([]LogEvent{
		{Timestamp: 1001, Message: "msg-one"},
		{Timestamp: 1002, Message: "msg-two"},
	})

	s.tags["grp-a"] = map[string]string{"env": "test"}
	s.seqToken["grp-a"] = map[string]int64{"stream-1": 3}
	s.metricFilters["grp-a"] = map[string]*MetricFilter{
		"err-count": {FilterName: "err-count", FilterPattern: "ERROR"},
	}
	s.queryDefinitions["qd-1"] = &QueryDefinition{
		QueryDefinitionID: "qd-1",
		Name:              "find-errors",
		QueryString:       "fields @message",
	}

	// Snapshot.
	var buf bytes.Buffer
	require.NoError(t, s.Snapshot(ctx, &buf))

	// Restore into a fresh store.
	s2 := newMemStore()
	require.NoError(t, s2.Restore(ctx, &buf))

	// Verify groups.
	require.Len(t, s2.groups, 2)
	grpA := s2.groups["grp-a"]
	require.NotNil(t, grpA)
	assert.Equal(t, int64(1000), grpA.CreationTime)
	require.NotNil(t, grpA.RetentionInDays)
	assert.Equal(t, 7, *grpA.RetentionInDays)

	// Verify streams.
	require.Len(t, s2.streams["grp-a"], 1)
	assert.Equal(t, "stream-1", s2.streams["grp-a"]["stream-1"].LogStreamName)

	// Verify events are restored into a ring.
	ring := s2.events["grp-a"]["stream-1"]
	require.NotNil(t, ring)
	evs := ring.Slice()
	require.Len(t, evs, 2)
	assert.Equal(t, "msg-one", evs[0].Message)
	assert.Equal(t, "msg-two", evs[1].Message)

	// Verify tags.
	assert.Equal(t, "test", s2.tags["grp-a"]["env"])

	// Verify seqToken.
	assert.Equal(t, int64(3), s2.seqToken["grp-a"]["stream-1"])

	// Verify metric filter.
	mf := s2.metricFilters["grp-a"]["err-count"]
	require.NotNil(t, mf)
	assert.Equal(t, "ERROR", mf.FilterPattern)

	// Verify query definition.
	qd := s2.queryDefinitions["qd-1"]
	require.NotNil(t, qd)
	assert.Equal(t, "find-errors", qd.Name)
}

func TestMemStore_IsEmpty(t *testing.T) {
	ctx := context.Background()
	s := newMemStore()

	empty, err := s.IsEmpty(ctx)
	require.NoError(t, err)
	assert.True(t, empty)

	s.groups["grp"] = &LogGroup{LogGroupName: "grp"}
	empty, err = s.IsEmpty(ctx)
	require.NoError(t, err)
	assert.False(t, empty)
}

func TestMemStore_SnapshotThenReset(t *testing.T) {
	ctx := context.Background()
	s := newMemStore()
	s.groups["grp"] = &LogGroup{LogGroupName: "grp"}

	var buf bytes.Buffer
	require.NoError(t, s.Snapshot(ctx, &buf))

	s.Reset(ctx)
	empty, _ := s.IsEmpty(ctx)
	require.True(t, empty, "must be empty after reset")

	require.NoError(t, s.Restore(ctx, &buf))
	empty, _ = s.IsEmpty(ctx)
	assert.False(t, empty, "must not be empty after restore")
	assert.Equal(t, "grp", s.groups["grp"].LogGroupName)
}

func intPtr(v int) *int { return &v }
