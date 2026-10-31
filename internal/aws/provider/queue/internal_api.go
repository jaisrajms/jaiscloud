package queue

import (
	"context"
	"fmt"
	"strings"
)

// resolveQueueURLByName scans all SQS queues (cross-scope) and finds the one whose URL
// ends with the given queue name (last path segment). Returns the URL and its stored scope.
func (p *QueueProvider) resolveQueueURLByName(ctx context.Context, queueName string) (url, account, region string, err error) {
	entries, listErr := p.resources.List(ctx, "", "", "sqs_queues", "")
	if listErr != nil {
		return "", "", "", fmt.Errorf("esm: failed to list queues: %w", listErr)
	}
	for _, e := range entries {
		// The resource ID for SQS queues is the queue URL itself
		qURL := e.ID
		// Match last path segment
		idx := strings.LastIndex(qURL, "/")
		if idx >= 0 && qURL[idx+1:] == queueName {
			return qURL, e.Account, e.Region, nil
		}
	}
	return "", "", "", fmt.Errorf("esm: queue not found: %s", queueName)
}
