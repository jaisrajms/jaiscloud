package esm

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const (
	sqsPollWaitTimeSec   = 20
	sqsEmptyPollDelay    = 1 * time.Second
	maxConsecutiveErrors = 3
)

// While the service behind a call is unreachable (a plugin that is down or
// restarting) a poller waits outageBackoffMin, doubling up to outageBackoffMax.
// Variables so tests can shorten them.
var (
	outageBackoffMin = 1 * time.Second
	outageBackoffMax = 30 * time.Second
)

// outage tracks one poller's wait on an unreachable service. Such failures do
// not count towards maxConsecutiveErrors: the mapping did nothing wrong, and
// disabling it would leave every mapping off until someone re-enables it by hand
// after a plugin restart.
type outage struct{ n int }

// wait sleeps the backoff for the next consecutive outage and returns false if
// ctx ended first. The first call of an outage logs it; reset logs the recovery.
func (o *outage) wait(ctx context.Context, logger *slog.Logger, err error) bool {
	o.n++
	if o.n == 1 {
		logger.Warn("esm: service unreachable, backing off and keeping the mapping enabled", "err", err)
	}
	d := outageBackoffMin << min(o.n-1, 10)
	if d > outageBackoffMax {
		d = outageBackoffMax
	}
	return sleepCtx(ctx, d)
}

// reset reports whether an outage was in progress.
func (o *outage) reset() bool {
	was := o.n > 0
	o.n = 0
	return was
}

// runSQSPoller polls an SQS queue and invokes the Lambda function with batches of messages.
func (p *Provider) runSQSPoller(poller *esmPoller, esm EventSourceMapping) {
	logger := p.logger.With("esm_uuid", esm.UUID, "queue", esm.QueueName, "function", esm.FunctionName)
	logger.Info("esm: SQS poller started")

	var queueDown, fnDown outage
	for {
		// Check if poller has been cancelled
		select {
		case <-poller.ctx.Done():
			logger.Info("esm: SQS poller stopped")
			return
		default:
		}

		// Reload ESM to check if still enabled
		current, err := p.loadESM(poller.ctx, esm.UUID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				logger.Info("esm: SQS poller stopping - ESM deleted")
				return
			}
			logger.Warn("esm: failed to load ESM state", "err", err)
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}
		if current.State == ESMStateDisabled || current.State == ESMStateDeleting {
			logger.Info("esm: SQS poller stopping - ESM disabled or deleted")
			return
		}

		batchSize := current.BatchSize
		if batchSize <= 0 {
			batchSize = 10
		}

		// Receive messages from SQS
		messages, err := p.queueAPI.InternalReceive(poller.ctx, esm.EventSourceArn, batchSize, sqsPollWaitTimeSec)
		if err != nil {
			select {
			case <-poller.ctx.Done():
				return
			default:
			}
			if model.IsOutage(err) {
				if !queueDown.wait(poller.ctx, logger, err) {
					return
				}
				continue
			}
			logger.Warn("esm: SQS receive error", "err", err)
			current.ConsecutiveErrors++
			if current.ConsecutiveErrors >= maxConsecutiveErrors {
				logger.Error("esm: too many consecutive SQS errors, disabling ESM", "errors", current.ConsecutiveErrors)
				current.State = ESMStateDisabled
				current.StateTransitionReason = "PROBLEM"
				current.LastProcessingResult = "PROBLEM: " + err.Error()
				_ = p.persistESM(context.Background(), current)
				return
			}
			_ = p.persistESM(context.Background(), current)
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}

		if queueDown.reset() {
			logger.Info("esm: queue reachable again")
		}

		if len(messages) == 0 {
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}

		// Apply filter criteria (if configured) before invoking Lambda.
		filtered := applyESMFilterCriteria(messages, current.FilterCriteria)
		if len(filtered) == 0 {
			// All messages filtered out — delete them so they don't pile up.
			var handles []string
			for _, m := range messages {
				handles = append(handles, m.ReceiptHandle)
			}
			_ = p.queueAPI.InternalDeleteBatch(poller.ctx, esm.EventSourceArn, handles)
			continue
		}

		// Process the filtered batch (with bisect and DLQ support).
		succeeded, unreachable := p.processSQSBatch(poller.ctx, filtered, current, 0)

		if unreachable != nil {
			// The function's owner could not be reached: nothing was delivered or
			// dead-lettered, so the messages come back after their visibility timeout.
			if !fnDown.wait(poller.ctx, logger, unreachable) {
				return
			}
			continue
		}
		if fnDown.reset() {
			logger.Info("esm: function reachable again")
		}
		if succeeded {
			// Delete successfully processed messages from queue.
			var handles []string
			for _, m := range filtered {
				handles = append(handles, m.ReceiptHandle)
			}
			if delErr := p.queueAPI.InternalDeleteBatch(poller.ctx, esm.EventSourceArn, handles); delErr != nil {
				logger.Warn("esm: failed to delete SQS messages after successful invocation", "err", delErr)
			}
			current.ConsecutiveErrors = 0
			current.LastProcessingResult = "OK"
		} else {
			current.ConsecutiveErrors++
			current.LastProcessingResult = "PROBLEM: Lambda invocation failed"
			if current.ConsecutiveErrors >= maxConsecutiveErrors {
				logger.Error("esm: too many consecutive Lambda errors, disabling ESM", "errors", current.ConsecutiveErrors)
				current.State = ESMStateDisabled
				current.StateTransitionReason = "PROBLEM"
			}
		}
		_ = p.persistESM(context.Background(), current)

		if current.State == ESMStateDisabled {
			return
		}
	}
}

// processSQSBatch invokes Lambda with the given messages, recursively bisecting on error
// if BisectBatchOnFunctionError is configured.
// It returns whether all messages were processed successfully, and the outage
// error if the function's owner was unreachable (in which case nothing is
// bisected or sent to the failure destination, and the caller must neither delete
// nor count the batch).
func (p *Provider) processSQSBatch(ctx context.Context, msgs []InternalMessage, cfg EventSourceMapping, depth int) (ok bool, unreachable error) {
	if len(msgs) == 0 {
		return true, nil
	}

	payload := buildSQSEventPayload(msgs, cfg.EventSourceArn, cfg.Cloud, cfg.Region)
	_, invokeErr := p.invoker.InvokeInternal(ctx, cfg.FunctionName, payload)
	if invokeErr == nil {
		return true, nil
	}
	if model.IsOutage(invokeErr) {
		return false, invokeErr
	}

	// Invocation failed.
	p.logger.Warn("esm: Lambda invocation failed", "esm_uuid", cfg.UUID, "batch_size", len(msgs), "err", invokeErr)

	// Bisect if configured and batch is splittable.
	if cfg.BisectBatchOnFunctionError && len(msgs) > 1 {
		mid := len(msgs) / 2
		leftOK, leftDown := p.processSQSBatch(ctx, msgs[:mid], cfg, depth+1)
		if leftDown != nil {
			return false, leftDown // no point trying the other half against an unreachable owner
		}
		rightOK, rightDown := p.processSQSBatch(ctx, msgs[mid:], cfg, depth+1)
		return leftOK && rightOK, rightDown
	}

	// Send to DLQ (failure destination).
	p.sendToFailureDestination(ctx, cfg, msgs, invokeErr)
	return false, nil
}

// streamReadFailed handles an error from reading the stream: an unreachable
// service backs off (and is not counted against the mapping); anything else waits
// one poll interval. It returns false when ctx ended.
func (p *Provider) streamReadFailed(ctx context.Context, logger *slog.Logger, down *outage, err error) bool {
	if model.IsOutage(err) {
		return down.wait(ctx, logger, err)
	}
	logger.Warn("esm: reading the DynamoDB stream failed", "err", err)
	return sleepCtx(ctx, sqsEmptyPollDelay)
}

// runDynamoDBStreamsPoller polls DynamoDB Streams and invokes Lambda with batches of records.
func (p *Provider) runDynamoDBStreamsPoller(poller *esmPoller, esm EventSourceMapping) {
	logger := p.logger.With("esm_uuid", esm.UUID, "table", esm.TableName, "function", esm.FunctionName)
	logger.Info("esm: DynamoDB Streams poller started")

	cursor := esm.LastSequenceNumber

	var down outage
	for {
		// Check if poller has been cancelled
		select {
		case <-poller.ctx.Done():
			logger.Info("esm: DynamoDB Streams poller stopped")
			return
		default:
		}

		// Reload ESM to check if still enabled
		current, err := p.loadESM(poller.ctx, esm.UUID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				logger.Info("esm: DynamoDB Streams poller stopping - ESM deleted")
				return
			}
			logger.Warn("esm: failed to load ESM state", "err", err)
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}
		if current.State == ESMStateDisabled || current.State == ESMStateDeleting {
			logger.Info("esm: DynamoDB Streams poller stopping - ESM disabled or deleted")
			return
		}

		// Check if stream is still enabled
		streamInfo, ok, serr := p.streamStore.GetStreamInfo(poller.ctx, esm.TableName)
		if serr != nil && poller.ctx.Err() == nil {
			if !p.streamReadFailed(poller.ctx, logger, &down, serr) {
				return
			}
			continue
		}
		if !ok || !streamInfo.Enabled {
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}

		batchSize := current.BatchSize
		if batchSize <= 0 {
			batchSize = 100
		}

		// Get records from the stream
		records, _, rerr := p.streamStore.GetRecords(poller.ctx, esm.TableName, cursor)
		if rerr != nil && poller.ctx.Err() == nil {
			if !p.streamReadFailed(poller.ctx, logger, &down, rerr) {
				return
			}
			continue
		}
		if len(records) == 0 {
			if !sleepCtx(poller.ctx, sqsEmptyPollDelay) {
				return
			}
			continue
		}

		// Batch records
		batch := records
		if len(batch) > batchSize {
			batch = batch[:batchSize]
		}

		// Build payload and invoke
		payload := buildDynamoDBStreamsEventPayload(batch, esm.EventSourceArn, esm.Cloud, esm.Region)

		_, invokeErr := p.invoker.InvokeInternal(poller.ctx, esm.FunctionName, payload)
		if invokeErr != nil {
			select {
			case <-poller.ctx.Done():
				return
			default:
			}
			if model.IsOutage(invokeErr) {
				// Same batch again after the backoff; the cursor does not move.
				if !down.wait(poller.ctx, logger, invokeErr) {
					return
				}
				continue
			}
			logger.Warn("esm: Lambda invocation failed for DDB stream", "err", invokeErr)
			current.ConsecutiveErrors++
			current.LastProcessingResult = "PROBLEM: " + invokeErr.Error()

			if current.ConsecutiveErrors >= maxConsecutiveErrors {
				logger.Error("esm: too many consecutive Lambda errors, disabling DDB Streams ESM", "errors", current.ConsecutiveErrors)
				current.State = ESMStateDisabled
				current.StateTransitionReason = "PROBLEM"
				_ = p.persistESM(context.Background(), current)
				return
			}
			_ = p.persistESM(context.Background(), current)
			// Retry same batch on error (do NOT advance cursor)
			continue
		}

		if down.reset() {
			logger.Info("esm: service reachable again")
		}

		// Success: advance cursor to last processed record
		lastRecord := batch[len(batch)-1]
		cursor = lastRecord.SequenceNumber
		current.LastSequenceNumber = cursor
		current.ConsecutiveErrors = 0
		current.LastProcessingResult = "OK"
		_ = p.persistESM(context.Background(), current)
	}
}
