package lambda

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

func blobKeyFor(account, funcName, versionID string) string {
	return fmt.Sprintf("lambda/%s/%s/%s/code.zip", account, funcName, versionID)
}

func layerBlobKey(account, layerName string, version int64) string {
	return fmt.Sprintf("lambda/layer/%s/%s/%d.zip", account, layerName, version)
}

func (p *FunctionProvider) storeCode(ctx context.Context, account, funcName, versionID string, zipBytes []byte) (sha256hex string, codeSize int64, blobKey string, err error) {
	sum := sha256.Sum256(zipBytes)
	sha256hex = hex.EncodeToString(sum[:])
	codeSize = int64(len(zipBytes))
	blobKey = blobKeyFor(account, funcName, versionID)
	err = p.blobs.Put(ctx, "lambda-code", blobKey, zipBytes)
	return
}

func (p *FunctionProvider) loadCode(ctx context.Context, account, funcName, versionID string) ([]byte, error) {
	key := blobKeyFor(account, funcName, versionID)
	data, err := p.blobs.Get(ctx, "lambda-code", key)
	if err != nil {
		return nil, fmt.Errorf("lambda: load code %s/%s/%s: %w", account, funcName, versionID, err)
	}
	return data, nil
}

func (p *FunctionProvider) storeLayerCode(ctx context.Context, account, layerName string, version int64, zipBytes []byte) (string, int64, string, error) {
	sum := sha256.Sum256(zipBytes)
	sha256hex := hex.EncodeToString(sum[:])
	codeSize := int64(len(zipBytes))
	key := layerBlobKey(account, layerName, version)
	err := p.blobs.Put(ctx, "lambda-code", key, zipBytes)
	return sha256hex, codeSize, key, err
}

func (p *FunctionProvider) loadLayerCode(ctx context.Context, account, layerName string, version int64) ([]byte, error) {
	key := layerBlobKey(account, layerName, version)
	data, err := p.blobs.Get(ctx, "lambda-code", key)
	if err != nil {
		return nil, fmt.Errorf("lambda: load layer code %s/%s/%d: %w", account, layerName, version, err)
	}
	return data, nil
}

// LoadCode implements the CodeLoader interface for executor injection.
func (p *FunctionProvider) LoadCode(ctx context.Context, account, funcName, version string) ([]byte, error) {
	return p.loadCode(ctx, account, funcName, version)
}

// GetFunctionCodeZip implements admin.LambdaCodeFetcher.
func (p *FunctionProvider) GetFunctionCodeZip(ctx context.Context, account, funcName, qualifier string) ([]byte, error) {
	return p.loadCode(ctx, account, funcName, qualifier)
}

// GetLayerCodeZip implements admin.LambdaCodeFetcher.
func (p *FunctionProvider) GetLayerCodeZip(ctx context.Context, account, layerName string, version int64) ([]byte, error) {
	return p.loadLayerCode(ctx, account, layerName, version)
}

// getFunctionCodeZipHandler serves Function.GetFunctionCodeZip. It is not an AWS
// API action: it is how the container executors and the admin code endpoint ask
// the Function service for the package they mount, so that a plugin that owns
// the function store can answer it. The zip comes back whole as "_body".
func (p *FunctionProvider) getFunctionCodeZipHandler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["functionName"].(string)
	qualifier, _ := nr.Params["qualifier"].(string)
	data, err := p.GetFunctionCodeZip(ctx, nr.AccountID, name, qualifier)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Function code not found: "+name, 404)
	}
	return provider.OK(map[string]any{"_body": data}), nil
}

// getLayerCodeZipHandler serves Function.GetLayerCodeZip (see above).
func (p *FunctionProvider) getLayerCodeZipHandler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["layerName"].(string)
	var version int64
	switch v := nr.Params["version"].(type) {
	case int64:
		version = v
	case int:
		version = int64(v)
	case float64:
		version = int64(v)
	}
	data, err := p.GetLayerCodeZip(ctx, nr.AccountID, name, version)
	if err != nil {
		return nil, model.NewProviderError("ResourceNotFoundException", "Layer code not found: "+name, 404)
	}
	return provider.OK(map[string]any{"_body": data}), nil
}
