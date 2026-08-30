package ui

import (
	"context"

	"jaiscloud/internal/aws/arn"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

// uiNR constructs a NormalizedRequest for direct provider calls from UI handlers.
// MUST be used by every UI handler — never construct NormalizedRequest ad hoc.
//
// Critical: Port is cfg.Port (the wire port, 4566 by default) — NOT the UI port.
// SQS queue URLs, S3 endpoints, and Lambda URLs embed the wire port.
// Injecting the UI port produces malformed URLs that SDK clients cannot use.
//
// Critical: Clock is cfg.Clock — providers call nr.Clock.Now() for all business
// timestamps. A nil Clock causes a panic in every timestamp-bearing provider call.
func uiNR(_ context.Context, cfg *config.Config, service, action, region, accountID string) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:    service,
		Action:     action,
		Params:     make(map[string]any),
		Clock:      cfg.Clock,
		Region:     region,
		AccountID:  accountID,
		Port:       cfg.Port,         // wire port (4566), NOT ui port
		Cloud:      model.CloudAWS,
		ResourceID: arn.ResourceID(region, accountID),
	}
}

// uiNRWithParams is uiNR with pre-populated Params.
func uiNRWithParams(_ context.Context, cfg *config.Config, service, action, region, accountID string, params map[string]any) *model.NormalizedRequest {
	nr := uiNR(context.Background(), cfg, service, action, region, accountID)
	nr.Params = params
	return nr
}
