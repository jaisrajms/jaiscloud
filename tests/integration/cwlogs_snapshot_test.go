package integration_test

// TestCWLogs_SnapshotRoundTrip verifies that CloudWatch Logs state (log groups,
// streams, events, retention, tags, subscription filters, metric filters, and
// saved query definitions) survives an export → reset → import cycle.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscwl "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCWLogs_SnapshotRoundTrip(t *testing.T) {
	skipIfNoServer(t)
	resetState(t)
	t.Cleanup(func() { resetState(t) })

	ctx := context.Background()
	c := newCWLClient(t)

	// ── 1. Create state ───────────────────────────────────────────────────────

	const groupApp = "/app/snapshot-test/service"
	const groupLambda = "/aws/lambda/snap-fn"
	const streamMain = "main"
	const streamRetry = "retry"
	const retentionDays = 7

	// Create two log groups.
	_, err := c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String(groupApp),
		Tags:         map[string]string{"env": "test", "owner": "snap"},
	})
	require.NoError(t, err, "create log group %s", groupApp)

	_, err = c.CreateLogGroup(ctx, &awscwl.CreateLogGroupInput{
		LogGroupName: aws.String(groupLambda),
	})
	require.NoError(t, err, "create log group %s", groupLambda)

	// Set retention on the first group.
	_, err = c.PutRetentionPolicy(ctx, &awscwl.PutRetentionPolicyInput{
		LogGroupName:    aws.String(groupApp),
		RetentionInDays: aws.Int32(retentionDays),
	})
	require.NoError(t, err, "set retention")

	// Create streams and put events.
	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(groupApp),
		LogStreamName: aws.String(streamMain),
	})
	require.NoError(t, err)

	_, err = c.CreateLogStream(ctx, &awscwl.CreateLogStreamInput{
		LogGroupName:  aws.String(groupApp),
		LogStreamName: aws.String(streamRetry),
	})
	require.NoError(t, err)

	now := time.Now().UnixMilli()
	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(groupApp),
		LogStreamName: aws.String(streamMain),
		LogEvents: []cwltypes.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String("INFO  service started")},
			{Timestamp: aws.Int64(now + 1), Message: aws.String("INFO  listening on :8080")},
		},
	})
	require.NoError(t, err, "put log events")

	_, err = c.PutLogEvents(ctx, &awscwl.PutLogEventsInput{
		LogGroupName:  aws.String(groupApp),
		LogStreamName: aws.String(streamRetry),
		LogEvents: []cwltypes.InputLogEvent{
			{Timestamp: aws.Int64(now + 2), Message: aws.String("WARN  retrying connection")},
		},
	})
	require.NoError(t, err, "put retry events")

	// Put a metric filter.
	_, err = c.PutMetricFilter(ctx, &awscwl.PutMetricFilterInput{
		LogGroupName:  aws.String(groupApp),
		FilterName:    aws.String("error-count"),
		FilterPattern: aws.String("ERROR"),
		MetricTransformations: []cwltypes.MetricTransformation{{
			MetricName:      aws.String("ErrorCount"),
			MetricNamespace: aws.String("App/Metrics"),
			MetricValue:     aws.String("1"),
		}},
	})
	require.NoError(t, err, "put metric filter")

	// Save a query definition.
	putQDOut, err := c.PutQueryDefinition(ctx, &awscwl.PutQueryDefinitionInput{
		Name:          aws.String("find-errors"),
		QueryString:   aws.String("fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc"),
		LogGroupNames: []string{groupApp},
	})
	require.NoError(t, err, "put query definition")
	savedQueryDefID := aws.ToString(putQDOut.QueryDefinitionId)
	require.NotEmpty(t, savedQueryDefID)

	// ── 2. Export, reset, import ──────────────────────────────────────────────

	snapshot := exportSnapshot(t)
	resetState(t)
	status := importSnapshot(t, snapshot)
	require.Equal(t, http.StatusOK, status, "import must return 200")

	// ── 3. Verify ─────────────────────────────────────────────────────────────

	t.Run("LogGroups_Exist", func(t *testing.T) {
		out, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
			LogGroupNamePrefix: aws.String("/app/snapshot-test"),
		})
		require.NoError(t, err)
		require.Len(t, out.LogGroups, 1)
		assert.Equal(t, groupApp, aws.ToString(out.LogGroups[0].LogGroupName))

		out2, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
			LogGroupNamePrefix: aws.String("/aws/lambda/snap-fn"),
		})
		require.NoError(t, err)
		require.Len(t, out2.LogGroups, 1)
		assert.Equal(t, groupLambda, aws.ToString(out2.LogGroups[0].LogGroupName))
	})

	t.Run("RetentionPolicy_Preserved", func(t *testing.T) {
		out, err := c.DescribeLogGroups(ctx, &awscwl.DescribeLogGroupsInput{
			LogGroupNamePrefix: aws.String(groupApp),
		})
		require.NoError(t, err)
		require.Len(t, out.LogGroups, 1)
		require.NotNil(t, out.LogGroups[0].RetentionInDays)
		assert.Equal(t, int32(retentionDays), aws.ToInt32(out.LogGroups[0].RetentionInDays))
	})

	t.Run("LogStreams_Exist", func(t *testing.T) {
		streamsOut, err := c.DescribeLogStreams(ctx, &awscwl.DescribeLogStreamsInput{
			LogGroupName: aws.String(groupApp),
		})
		require.NoError(t, err)
		names := make(map[string]bool)
		for _, s := range streamsOut.LogStreams {
			names[aws.ToString(s.LogStreamName)] = true
		}
		assert.True(t, names[streamMain], "main stream must exist after restore")
		assert.True(t, names[streamRetry], "retry stream must exist after restore")
	})

	t.Run("LogEvents_Survive", func(t *testing.T) {
		out, err := c.GetLogEvents(ctx, &awscwl.GetLogEventsInput{
			LogGroupName:  aws.String(groupApp),
			LogStreamName: aws.String(streamMain),
			StartFromHead: aws.Bool(true),
		})
		require.NoError(t, err)
		require.Len(t, out.Events, 2, "both events must survive restore")
		assert.Equal(t, "INFO  service started", aws.ToString(out.Events[0].Message))
		assert.Equal(t, "INFO  listening on :8080", aws.ToString(out.Events[1].Message))

		out2, err := c.GetLogEvents(ctx, &awscwl.GetLogEventsInput{
			LogGroupName:  aws.String(groupApp),
			LogStreamName: aws.String(streamRetry),
			StartFromHead: aws.Bool(true),
		})
		require.NoError(t, err)
		require.Len(t, out2.Events, 1)
		assert.Equal(t, "WARN  retrying connection", aws.ToString(out2.Events[0].Message))
	})

	t.Run("MetricFilter_Survives", func(t *testing.T) {
		out, err := c.DescribeMetricFilters(ctx, &awscwl.DescribeMetricFiltersInput{
			LogGroupName: aws.String(groupApp),
		})
		require.NoError(t, err)
		require.Len(t, out.MetricFilters, 1)
		assert.Equal(t, "error-count", aws.ToString(out.MetricFilters[0].FilterName))
		assert.Equal(t, "ERROR", aws.ToString(out.MetricFilters[0].FilterPattern))
	})

	t.Run("QueryDefinition_Survives", func(t *testing.T) {
		out, err := c.DescribeQueryDefinitions(ctx, &awscwl.DescribeQueryDefinitionsInput{})
		require.NoError(t, err)
		var found bool
		for _, qd := range out.QueryDefinitions {
			if aws.ToString(qd.QueryDefinitionId) == savedQueryDefID {
				found = true
				assert.Equal(t, "find-errors", aws.ToString(qd.Name))
				assert.Contains(t, aws.ToString(qd.QueryString), "filter @message like /ERROR/")
			}
		}
		assert.True(t, found, "saved query definition must survive restore")
	})

	t.Run("FilterLogEvents_Works", func(t *testing.T) {
		out, err := c.FilterLogEvents(ctx, &awscwl.FilterLogEventsInput{
			LogGroupName:  aws.String(groupApp),
			FilterPattern: aws.String("INFO"),
		})
		require.NoError(t, err)
		assert.Len(t, out.Events, 2, "FilterLogEvents must return the two INFO events")
	})
}
