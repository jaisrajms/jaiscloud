package lambda

import (
	"context"
	"fmt"

	awsarn "jaiscloud/internal/aws/arn"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
)

// InvokeResult holds the result of an internal Lambda invocation.
type InvokeResult struct {
	StatusCode    int
	FunctionError string
	Payload       []byte
}

// InternalInvoker is the interface used by other providers to invoke Lambda functions.
type InternalInvoker interface {
	InternalInvoke(ctx context.Context, funcARNorName string, payload []byte, invocationType string) (*InvokeResult, error)
}

// RawInvoker is the interface used by providers that need fire-and-forget Lambda invocation.
type RawInvoker interface {
	InternalInvokeRaw(ctx context.Context, funcARNorName string, payload []byte) error
}

// InternalInvoke invokes a Lambda function synchronously or asynchronously.
// invocationType is "RequestResponse" or "Event".
// When funcARNorName is a full ARN, the account and region are extracted so the
// invocation routes to the correct per-account store (cross-account dispatch fix §11.1.5).
func (p *FunctionProvider) InternalInvoke(ctx context.Context, funcARNorName string, payload []byte, invocationType string) (*InvokeResult, error) {
	name, account, region := nameAccountRegionFromARN(funcARNorName)
	cfg, err := p.loadConfig(ctx, account, region, name)
	if err != nil {
		return nil, fmt.Errorf("lambda: function %q not found: %w", name, err)
	}
	if invocationType == "Event" {
		// Look up event invoke config for retry and DLQ settings.
		maxAttempts := defaultMaxAttempts
		var dlqARN string
		var maxAge int64
		if eiEntry, eiErr := p.resources.Get(ctx, account, region, resTypeEventInvoke, eiKey(name, "")); eiErr == nil {
			var eiCfg eventInvokeConfig
			if jsonErr := eiCfg.unmarshal(eiEntry.Data); jsonErr == nil {
				if eiCfg.MaximumRetryAttempts > 0 {
					maxAttempts = eiCfg.MaximumRetryAttempts
				}
				maxAge = int64(eiCfg.MaximumEventAgeInSeconds)
				if dc, ok := eiCfg.DestinationConfig["OnFailure"].(map[string]any); ok {
					dlqARN, _ = dc["Destination"].(string)
				}
			}
		}
		p.asyncQueue.Enqueue(asyncInvokeJob{
			funcARN:       cfg.FunctionArn,
			payload:       payload,
			maxAttempts:   maxAttempts,
			dlqARN:        dlqARN,
			createdAt:     clock.Now(),
			maxAgeSeconds: maxAge,
		})
		return &InvokeResult{StatusCode: 202}, nil
	}
	result, err := p.invokeConfig(ctx, cfg, payload)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// InternalGetFunction retrieves function configuration by name or ARN.
func (p *FunctionProvider) InternalGetFunction(ctx context.Context, nameOrARN string) (*functionConfig, error) {
	name, account, region := nameAccountRegionFromARN(nameOrARN)
	cfg, err := p.loadConfig(ctx, account, region, name)
	if err != nil {
		return nil, fmt.Errorf("lambda: function %q not found: %w", name, err)
	}
	return &cfg, nil
}

// nameAccountRegionFromARN extracts (functionName, account, region) from a Lambda
// ARN or bare function name. For bare names, account and region are "".
func nameAccountRegionFromARN(nameOrARN string) (name, account, region string) {
	if parsed, err := awsarn.Parse(nameOrARN); err == nil {
		account = parsed.AccountID
		region = parsed.Region
		name = parsed.Resource
		// Resource is "function:name" or "function:name:qualifier"
		if i := len("function:"); len(name) > i && name[:i] == "function:" {
			name = name[i:]
		}
		// Strip qualifier (:version or :alias)
		if j := len(name) - 1; j > 0 {
			for j > 0 && name[j] != ':' {
				j--
			}
			if j > 0 {
				name = name[:j]
			}
		}
		return
	}
	return nameOrARN, "", ""
}

// InternalInvokeRaw invokes a Lambda function asynchronously (fire-and-forget).
// Returns immediately after submitting the invocation.
func (p *FunctionProvider) InternalInvokeRaw(ctx context.Context, funcARNorName string, payload []byte) error {
	_, err := p.InternalInvoke(ctx, funcARNorName, payload, "Event")
	return err
}

// invokeConfig executes the given functionConfig with the provided payload and returns an InvokeResult.
func (p *FunctionProvider) invokeConfig(ctx context.Context, cfg functionConfig, payload []byte) (*InvokeResult, error) {
	raw, err := p.InvokeInternal(ctx, cfg.FunctionName, payload)
	if err != nil {
		return &InvokeResult{StatusCode: 200, FunctionError: err.Error()}, nil
	}
	return &InvokeResult{StatusCode: 200, Payload: raw}, nil
}

// invokeFunctionInternal serves Function.InvokeFunction for an internal origin
// (SNS, the event-source poller, CloudWatch, EventBridge targets, S3
// notifications, secret rotation, ...). The params use the same keys as the
// wire request: _function_name (name or ARN), _payload ([]byte) and
// _invocation_type ("RequestResponse" or "Event").
func (p *FunctionProvider) invokeFunctionInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["_function_name"].(string)
	payload, _ := nr.Params["_payload"].([]byte)
	invType, _ := nr.Params["_invocation_type"].(string)
	if invType == "" {
		invType = "RequestResponse"
	}
	res, err := p.InternalInvoke(ctx, name, payload, invType)
	if err != nil {
		return nil, err
	}
	data := map[string]any{}
	if len(res.Payload) > 0 {
		data["_payload"] = res.Payload
	}
	if res.FunctionError != "" {
		data["_function_error"] = res.FunctionError
	}
	return &model.ProviderResponse{HTTPStatus: res.StatusCode, Data: data}, nil
}
