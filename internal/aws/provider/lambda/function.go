// Package function implements the Lambda provider (FunctionProvider).
package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/logstream"
	"jaiscloud/internal/model"
	"jaiscloud/internal/pagination"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/reqctx"
	"jaiscloud/internal/store"
)

const (
	resTypeFunction = "lambda_functions"
	resTypeVersions = "lambda_versions"
	resTypeAliases  = "lambda_aliases"
	resTypeLayers   = "lambda_layers"
	resTypePolicies = "lambda_policies"
	resTypeURLs     = "lambda_urls"
)

// FunctionProvider handles all Lambda operations.
type FunctionProvider struct {
	resources          store.ResourceStore
	executor           lambdaexec.LambdaExecutor
	blobs              blobfs.BlobStore
	logsAPI            logstream.Ingestor
	concurrencyLimit   int64
	syncPayloadMax     int64
	asyncPayloadMax    int64
	responsePayloadMax int64
	activeInvocations  atomic.Int64
	asyncQueue         *AsyncQueue
}

// SetLogsAPI wires a CloudWatch Logs ingestor so every Lambda invocation writes
// START / END / REPORT platform log lines to /aws/lambda/{name}.
func (p *FunctionProvider) SetLogsAPI(l logstream.Ingestor) { p.logsAPI = l }

func New(resources store.ResourceStore) *FunctionProvider {
	p := &FunctionProvider{resources: resources, executor: &lambdaexec.MockExecutor{}, blobs: blobfs.NewMemoryBlobStore()}
	p.asyncQueue = NewAsyncQueue(p, nil)
	return p
}

func NewWithExecutor(resources store.ResourceStore, exec lambdaexec.LambdaExecutor) *FunctionProvider {
	p := &FunctionProvider{resources: resources, executor: exec, blobs: blobfs.NewMemoryBlobStore()}
	p.asyncQueue = NewAsyncQueue(p, nil)
	return p
}

func NewWithLimits(resources store.ResourceStore, exec lambdaexec.LambdaExecutor, cfg lambdaexec.LambdaConfig) *FunctionProvider {
	p := &FunctionProvider{
		resources:          resources,
		executor:           exec,
		blobs:              blobfs.NewMemoryBlobStore(),
		concurrencyLimit:   cfg.ConcurrencyLimit,
		syncPayloadMax:     cfg.SyncPayloadMax,
		asyncPayloadMax:    cfg.AsyncPayloadMax,
		responsePayloadMax: cfg.ResponsePayloadMax,
	}
	p.asyncQueue = NewAsyncQueue(p, nil)
	return p
}

// NewWithBlobs constructs a FunctionProvider with an explicit BlobStore (used in main.go for persistent mode).
func NewWithBlobs(resources store.ResourceStore, exec lambdaexec.LambdaExecutor, cfg lambdaexec.LambdaConfig, blobs blobfs.BlobStore) *FunctionProvider {
	p := NewWithLimits(resources, exec, cfg)
	p.blobs = blobs
	return p
}

// AsyncQueue returns the provider's async invocation queue (for worker registration).
func (p *FunctionProvider) AsyncQueue() *AsyncQueue {
	return p.asyncQueue
}

// SetAsyncSQSSend wires the SQS DLQ sender into the async queue.
func (p *FunctionProvider) SetAsyncSQSSend(fn asyncQueueSQSSend) {
	p.asyncQueue.sqsSend = fn
}

func (p *FunctionProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Core CRUD
		"Function.CreateFunction":              p.CreateFunction,
		"Function.GetFunction":                 p.GetFunction,
		"Function.GetFunctionConfiguration":    p.GetFunctionConfiguration,
		"Function.DeleteFunction":              p.DeleteFunction,
		"Function.ListFunctions":               p.ListFunctions,
		"Function.UpdateFunctionConfiguration": p.UpdateFunctionConfiguration,
		"Function.UpdateFunctionCode":          p.UpdateFunctionCode,
		"Function.InvokeFunction":              p.InvokeFunction,
		// Versions
		"Function.PublishVersion":         p.PublishVersion,
		"Function.ListVersionsByFunction": p.ListVersionsByFunction,
		// Aliases
		"Function.CreateAlias": p.CreateAlias,
		"Function.GetAlias":    p.GetAlias,
		"Function.UpdateAlias": p.UpdateAlias,
		"Function.DeleteAlias": p.DeleteAlias,
		"Function.ListAliases": p.ListAliases,
		// Layers
		"Function.ListLayers":          p.ListLayers,
		"Function.PublishLayerVersion": p.PublishLayerVersion,
		"Function.GetLayerVersion":     p.GetLayerVersion,
		"Function.ListLayerVersions":   p.ListLayerVersions,
		"Function.DeleteLayerVersion":  p.DeleteLayerVersion,
		// Tags
		"Function.TagResource":   p.TagResource,
		"Function.UntagResource": p.UntagResource,
		"Function.ListTags":      p.ListTags,
		// Permissions
		"Function.AddPermission":    p.AddPermission,
		"Function.RemovePermission": p.RemovePermission,
		"Function.GetPolicy":        p.GetPolicy,
		// Function URLs
		"Function.CreateFunctionUrlConfig": p.CreateFunctionUrlConfig,
		"Function.GetFunctionUrlConfig":    p.GetFunctionUrlConfig,
		"Function.UpdateFunctionUrlConfig": p.UpdateFunctionUrlConfig,
		"Function.DeleteFunctionUrlConfig": p.DeleteFunctionUrlConfig,
		// Concurrency
		"Function.PutFunctionConcurrency":    p.PutFunctionConcurrency,
		"Function.GetFunctionConcurrency":    p.GetFunctionConcurrency,
		"Function.DeleteFunctionConcurrency": p.DeleteFunctionConcurrency,
		// Account
		"Function.GetAccountSettings": p.GetAccountSettings,
		// Code signing configs
		"Function.CreateCodeSigningConfig":          p.CreateCodeSigningConfig,
		"Function.GetCodeSigningConfig":             p.GetCodeSigningConfig,
		"Function.UpdateCodeSigningConfig":          p.UpdateCodeSigningConfig,
		"Function.DeleteCodeSigningConfig":          p.DeleteCodeSigningConfig,
		"Function.ListCodeSigningConfigs":           p.ListCodeSigningConfigs,
		"Function.PutFunctionCodeSigningConfig":     p.PutFunctionCodeSigningConfig,
		"Function.GetFunctionCodeSigningConfig":     p.GetFunctionCodeSigningConfig,
		"Function.DeleteFunctionCodeSigningConfig":  p.DeleteFunctionCodeSigningConfig,
		"Function.ListFunctionsByCodeSigningConfig": p.ListFunctionsByCodeSigningConfig,
		// Provisioned concurrency
		"Function.PutProvisionedConcurrencyConfig":    p.PutProvisionedConcurrencyConfig,
		"Function.GetProvisionedConcurrencyConfig":    p.GetProvisionedConcurrencyConfig,
		"Function.DeleteProvisionedConcurrencyConfig": p.DeleteProvisionedConcurrencyConfig,
		"Function.ListProvisionedConcurrencyConfigs":  p.ListProvisionedConcurrencyConfigs,
		// Event invoke config
		"Function.PutFunctionEventInvokeConfig":    p.PutFunctionEventInvokeConfig,
		"Function.GetFunctionEventInvokeConfig":    p.GetFunctionEventInvokeConfig,
		"Function.UpdateFunctionEventInvokeConfig": p.UpdateFunctionEventInvokeConfig,
		"Function.DeleteFunctionEventInvokeConfig": p.DeleteFunctionEventInvokeConfig,
		"Function.ListFunctionEventInvokeConfigs":  p.ListFunctionEventInvokeConfigs,
		// Async invoke
		"Function.InvokeAsync": p.InvokeAsync,
	}
}

const (
	lambdaMinTimeoutSecs = 1
	lambdaMaxTimeoutSecs = 900
)

func validateLambdaTimeout(t int) error {
	if t < lambdaMinTimeoutSecs || t > lambdaMaxTimeoutSecs {
		return model.NewProviderError(
			"InvalidParameterValueException",
			fmt.Sprintf("1 validation error detected: Value '%d' at 'timeout' failed to satisfy constraint: Member must be between %d and %d (inclusive)",
				t, lambdaMinTimeoutSecs, lambdaMaxTimeoutSecs),
			400)
	}
	return nil
}

// ─── data models ──────────────────────────────────────────────────────────────

type functionConfig struct {
	FunctionName        string            `json:"FunctionName"`
	FunctionArn         string            `json:"FunctionArn"`
	Runtime             string            `json:"Runtime"`
	Role                string            `json:"Role"`
	Handler             string            `json:"Handler"`
	Description         string            `json:"Description"`
	Timeout             int               `json:"Timeout"`
	MemorySize          int               `json:"MemorySize"`
	State               string            `json:"State"`
	LastModified        string            `json:"LastModified"`
	RevisionId          string            `json:"RevisionId"`
	CodeSize            int64             `json:"CodeSize"`
	CodeSha256          string            `json:"CodeSha256,omitempty"`
	BlobKey             string            `json:"BlobKey,omitempty"`
	Environment         map[string]string `json:"Environment,omitempty"`
	Tags                map[string]string `json:"Tags,omitempty"`
	ReservedConcurrency *int              `json:"ReservedConcurrency,omitempty"`
	VersionCounter      int64             `json:"VersionCounter,omitempty"`
	Layers              []string          `json:"Layers,omitempty"` // layer version ARNs
}

type versionEntry struct {
	functionConfig
	Version string `json:"Version"`
}

type aliasEntry struct {
	FunctionName    string `json:"FunctionName"`
	Name            string `json:"Name"`
	FunctionVersion string `json:"FunctionVersion"`
	AliasArn        string `json:"AliasArn"`
	Description     string `json:"Description,omitempty"`
	RevisionId      string `json:"RevisionId"`
}

type layerEntry struct {
	LayerName          string   `json:"LayerName"`
	LayerArn           string   `json:"LayerArn"`
	VersionNumber      int64    `json:"VersionNumber"`
	VersionArn         string   `json:"VersionArn"`
	Description        string   `json:"Description,omitempty"`
	LicenseInfo        string   `json:"LicenseInfo,omitempty"`
	CompatibleRuntimes []string `json:"CompatibleRuntimes,omitempty"`
	CreatedDate        string   `json:"CreatedDate"`
	CodeSize           int64    `json:"CodeSize"`
}

type policyDocument struct {
	Version   string            `json:"Version"`
	Id        string            `json:"Id"`
	Statement []policyStatement `json:"Statement"`
}

type policyStatement struct {
	Sid       string         `json:"Sid"`
	Effect    string         `json:"Effect"`
	Principal map[string]any `json:"Principal"`
	Action    string         `json:"Action"`
	Resource  string         `json:"Resource"`
	Condition map[string]any `json:"Condition,omitempty"`
}

type urlConfig struct {
	FunctionArn      string `json:"FunctionArn"`
	FunctionUrl      string `json:"FunctionUrl"`
	AuthType         string `json:"AuthType"`
	CreatedTime      string `json:"CreatedTime"`
	LastModifiedTime string `json:"LastModifiedTime"`
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// extractFunctionName strips an ARN to a plain function name.
func extractFunctionName(name string) string {
	if strings.HasPrefix(name, "arn:") {
		parts := strings.Split(name, ":")
		if len(parts) >= 7 {
			return parts[6]
		}
	}
	return name
}

func parseLayerARNs(params map[string]any) []string {
	raw, ok := params["Layers"].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, v := range raw {
		if s, ok := v.(string); ok && s != "" {
			out = append(out, s)
		}
	}
	return out
}

func parseEnvVars(params map[string]any) map[string]string {
	env, ok := params["Environment"].(map[string]any)
	if !ok {
		return nil
	}
	vars, ok := env["Variables"].(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]string, len(vars))
	for k, v := range vars {
		result[k] = fmt.Sprintf("%v", v)
	}
	return result
}

func (p *FunctionProvider) functionARN(nr *model.NormalizedRequest, name string) string {
	return nr.ResourceID(model.RTLambdaFunction, name)
}

func (p *FunctionProvider) saveConfig(ctx context.Context, account, region string, cfg functionConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	entry := store.ResourceEntry{Type: resTypeFunction, ID: cfg.FunctionName, Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *FunctionProvider) loadConfig(ctx context.Context, account, region, name string) (functionConfig, error) {
	entry, err := p.resources.Get(ctx, account, region, resTypeFunction, name)
	if err != nil {
		return functionConfig{}, err
	}
	var cfg functionConfig
	return cfg, json.Unmarshal(entry.Data, &cfg)
}

// resolveConfig returns the functionConfig for (name, qualifier).
// qualifier="" or "$LATEST" → current config; numeric → version; else → alias.
func (p *FunctionProvider) resolveConfig(ctx context.Context, account, region, name, qualifier string) (functionConfig, string, error) {
	if qualifier == "" || qualifier == "$LATEST" {
		cfg, err := p.loadConfig(ctx, account, region, name)
		return cfg, "$LATEST", err
	}
	// Numeric version
	if _, err := strconv.ParseInt(qualifier, 10, 64); err == nil {
		cfg, err := p.loadVersion(ctx, account, region, name, qualifier)
		return cfg, qualifier, err
	}
	// Alias
	a, err := p.loadAlias(ctx, account, region, name, qualifier)
	if err != nil {
		return functionConfig{}, "", err
	}
	if a.FunctionVersion == "$LATEST" {
		cfg, err := p.loadConfig(ctx, account, region, name)
		return cfg, "$LATEST", err
	}
	cfg, err := p.loadVersion(ctx, account, region, name, a.FunctionVersion)
	return cfg, a.FunctionVersion, err
}

// cfgToWire converts a functionConfig to the wire map.
func cfgToWire(cfg functionConfig) map[string]any {
	var m map[string]any
	b, _ := json.Marshal(cfg)
	json.Unmarshal(b, &m)
	if cfg.Environment != nil {
		m["Environment"] = map[string]any{"Variables": cfg.Environment}
	}
	// Lambda API returns Layers as [{Arn, CodeSize}] objects, not plain strings.
	if len(cfg.Layers) > 0 {
		layerObjs := make([]map[string]any, 0, len(cfg.Layers))
		for _, arn := range cfg.Layers {
			layerObjs = append(layerObjs, map[string]any{"Arn": arn, "CodeSize": 0})
		}
		m["Layers"] = layerObjs
	}
	return m
}

// ─── Core CRUD ────────────────────────────────────────────────────────────────

func (p *FunctionProvider) CreateFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr.Params, "FunctionName")
	if name == "" {
		name = strParam(nr.Params, "_function_name")
	}
	if name == "" {
		return nil, model.NewProviderError("InvalidParameterValueException", "FunctionName is required", 400)
	}
	name = extractFunctionName(name)

	runtime := strParam(nr.Params, "Runtime")
	if runtime == "" {
		runtime = "provided"
	}
	timeout := 3
	if t, ok := nr.Params["Timeout"]; ok {
		switch v := t.(type) {
		case float64:
			timeout = int(v)
		case int:
			timeout = v
		}
	}
	if err := validateLambdaTimeout(timeout); err != nil {
		return nil, err
	}
	memSize := 128
	if m, ok := nr.Params["MemorySize"]; ok {
		switch v := m.(type) {
		case float64:
			memSize = int(v)
		case int:
			memSize = v
		}
	}

	var tags map[string]string
	if rawTags, ok := nr.Params["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(rawTags))
		for k, v := range rawTags {
			tags[k] = fmt.Sprint(v)
		}
	}

	cfg := functionConfig{
		FunctionName: name,
		FunctionArn:  p.functionARN(nr, name),
		Runtime:      runtime,
		Role:         strParam(nr.Params, "Role"),
		Handler:      strParam(nr.Params, "Handler"),
		Description:  strParam(nr.Params, "Description"),
		Timeout:      timeout,
		MemorySize:   memSize,
		State:        "Active",
		LastModified: clock.Now().Format(time.RFC3339),
		RevisionId:   "1",
		Environment:  parseEnvVars(nr.Params),
		Tags:         tags,
		Layers:       parseLayerARNs(nr.Params),
	}

	var zipBytes []byte
	if zf, ok := nr.Params["Code"].(map[string]any); ok {
		if b64, ok := zf["ZipFile"].(string); ok {
			zipBytes, _ = base64.StdEncoding.DecodeString(b64)
		}
	}
	if len(zipBytes) > 0 {
		sha256hex, codeSize, blobKey, err := p.storeCode(ctx, nr.AccountID, name, "$LATEST", zipBytes)
		if err != nil {
			return nil, fmt.Errorf("lambda: store code: %w", err)
		}
		cfg.CodeSha256 = sha256hex
		cfg.CodeSize = codeSize
		cfg.BlobKey = blobKey
	}

	if err := p.saveConfig(ctx, nr.AccountID, nr.Region, cfg); err != nil {
		return nil, model.NewProviderError("ResourceConflictException", "Function already exists", 409)
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: cfgToWire(cfg)}, nil
}

func (p *FunctionProvider) GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	qualifier := strParam(nr.Params, "Qualifier")
	cfg, resolvedVersion, err := p.resolveConfig(ctx, nr.AccountID, nr.Region, name, qualifier)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	wire := cfgToWire(cfg)
	wire["Version"] = resolvedVersion
	return provider.OK(map[string]any{
		"Configuration": wire,
		"Code":          map[string]any{"Location": ""},
		"Tags":          cfg.Tags,
	}), nil
}

func (p *FunctionProvider) GetFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	qualifier := strParam(nr.Params, "Qualifier")
	cfg, resolvedVersion, err := p.resolveConfig(ctx, nr.AccountID, nr.Region, name, qualifier)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	wire := cfgToWire(cfg)
	wire["Version"] = resolvedVersion
	return provider.OK(wire), nil
}

func (p *FunctionProvider) DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	cfg, cfgErr := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, resTypeFunction, name); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	if cfgErr == nil && cfg.BlobKey != "" {
		_ = p.blobs.Delete(ctx, "lambda-code", cfg.BlobKey)
	}
	p.executor.DeleteFunction(ctx, name)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListFunctions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, resTypeFunction, "")
	if err != nil {
		return nil, err
	}
	functions := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var cfg functionConfig
		if json.Unmarshal(e.Data, &cfg) == nil {
			functions = append(functions, cfgToWire(cfg))
		}
	}
	maxResults := 50
	if v, ok := nr.Params["MaxItems"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["Marker"].(string)
	page, next, pgErr := pagination.Paginate(functions, maxResults, token, "ListFunctions")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValueException", pgErr.Error(), 400)
	}
	data := map[string]any{"Functions": page}
	if next != "" {
		data["NextMarker"] = next
	}
	return provider.OK(data), nil
}

func (p *FunctionProvider) UpdateFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	if r := strParam(nr.Params, "Role"); r != "" {
		cfg.Role = r
	}
	if h := strParam(nr.Params, "Handler"); h != "" {
		cfg.Handler = h
	}
	if d := strParam(nr.Params, "Description"); d != "" {
		cfg.Description = d
	}
	if env := parseEnvVars(nr.Params); env != nil {
		cfg.Environment = env
	}
	if layers := parseLayerARNs(nr.Params); layers != nil {
		cfg.Layers = layers
	}
	if t, ok := nr.Params["Timeout"]; ok {
		var newTimeout int
		switch v := t.(type) {
		case float64:
			newTimeout = int(v)
		case int:
			newTimeout = v
		}
		if err := validateLambdaTimeout(newTimeout); err != nil {
			return nil, err
		}
		cfg.Timeout = newTimeout
	}
	cfg.LastModified = clock.Now().Format(time.RFC3339)
	if err := p.saveConfig(ctx, nr.AccountID, nr.Region, cfg); err != nil {
		return nil, err
	}
	return provider.OK(cfgToWire(cfg)), nil
}

func (p *FunctionProvider) UpdateFunctionCode(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	cfg.LastModified = clock.Now().Format(time.RFC3339)
	var zipBytes []byte
	if zf, ok := nr.Params["ZipFile"].(string); ok {
		zipBytes, _ = base64.StdEncoding.DecodeString(zf)
	}
	if len(zipBytes) > 0 {
		sha256hex, codeSize, blobKey, err := p.storeCode(ctx, nr.AccountID, cfg.FunctionName, "$LATEST", zipBytes)
		if err != nil {
			return nil, fmt.Errorf("lambda: store code: %w", err)
		}
		cfg.CodeSha256 = sha256hex
		cfg.CodeSize = codeSize
		cfg.BlobKey = blobKey
	}
	if err := p.saveConfig(ctx, nr.AccountID, nr.Region, cfg); err != nil {
		return nil, err
	}
	return provider.OK(cfgToWire(cfg)), nil
}

// ─── Invoke ───────────────────────────────────────────────────────────────────

// InvokeInternal invokes a Lambda function directly (used by ESM pollers).
func (p *FunctionProvider) InvokeInternal(ctx context.Context, functionName string, payload []byte) ([]byte, error) {
	cfg, err := p.loadConfig(ctx, "", "", functionName)
	if err != nil {
		return nil, fmt.Errorf("function %q not found: %w", functionName, err)
	}
	req := lambdaexec.InvokeRequest{
		FunctionName: cfg.FunctionName,
		Runtime:      cfg.Runtime,
		Handler:      cfg.Handler,
		MemoryMB:     cfg.MemorySize,
		TimeoutSecs:  cfg.Timeout,
		EnvVars:      cfg.Environment,
		Payload:      payload,
		Layers:       p.resolveLayerInfos(cfg.Layers),
	}
	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	invCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := p.executor.Invoke(invCtx, req)
	return result.Payload, err
}

// resolveLayerInfos maps layer version ARNs to LayerInfo structs by extracting
// the layer name and version from the ARN and looking up the blob key.
// ARN format: arn:aws:lambda:{region}:{account}:layer:{name}:{version}
func (p *FunctionProvider) resolveLayerInfos(layerARNs []string) []lambdaexec.LayerInfo {
	if len(layerARNs) == 0 {
		return nil
	}
	infos := make([]lambdaexec.LayerInfo, 0, len(layerARNs))
	for _, arn := range layerARNs {
		name, version := parseLayerARN(arn)
		if name == "" {
			continue
		}
		key := layerBlobKey("", name, version) // account not stored in blob key
		infos = append(infos, lambdaexec.LayerInfo{ARN: arn, BlobKey: key})
	}
	return infos
}

// parseLayerARN extracts (layerName, versionNumber) from a layer version ARN.
// arn:aws:lambda:{region}:{account}:layer:{name}:{version}
func parseLayerARN(arn string) (name string, version int64) {
	// Split by ":" — parts[6] = name, parts[7] = version
	parts := strings.Split(arn, ":")
	if len(parts) >= 8 {
		name = parts[6]
		fmt.Sscanf(parts[7], "%d", &version)
		return name, version
	}
	// Try to extract from the last two colon-separated segments
	if len(parts) >= 2 {
		name = parts[len(parts)-2]
		fmt.Sscanf(parts[len(parts)-1], "%d", &version)
		return name, version
	}
	return "", 0
}

func (p *FunctionProvider) Shutdown(_ context.Context) {
	if p.executor != nil {
		_ = p.executor.Close()
	}
}

func (p *FunctionProvider) InvokeFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	qualifier := strParam(nr.Params, "Qualifier")

	invType := strParam(nr.Params, "_invocation_type")
	isDryRun := strings.EqualFold(invType, "DryRun")
	isAsync := strings.EqualFold(invType, "Event")

	// DryRun: validate the function exists then return 204 without invoking.
	if isDryRun {
		if _, _, err := p.resolveConfig(ctx, nr.AccountID, nr.Region, name, qualifier); err != nil {
			return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
		}
		return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
	}

	cfg, _, err := p.resolveConfig(ctx, nr.AccountID, nr.Region, name, qualifier)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}

	payload, _ := nr.Params["_payload"].([]byte)

	maxReq := p.syncPayloadMax
	if isAsync {
		maxReq = p.asyncPayloadMax
	}
	if maxReq > 0 && int64(len(payload)) > maxReq {
		return nil, model.NewProviderError(
			"RequestEntityTooLargeException",
			fmt.Sprintf("Request must be smaller than %d bytes for the InvokeFunction operation", maxReq),
			413)
	}

	if isAsync {
		return &model.ProviderResponse{HTTPStatus: 202, Data: map[string]any{}}, nil
	}

	current := p.activeInvocations.Add(1)
	defer p.activeInvocations.Add(-1)

	if p.concurrencyLimit > 0 && current > p.concurrencyLimit {
		return nil, model.NewProviderError("TooManyRequestsException", "Rate Exceeded.", 429)
	}

	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	invCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	logType := strParam(nr.Params, "_log_type")

	req := lambdaexec.InvokeRequest{
		FunctionName: cfg.FunctionName,
		Runtime:      cfg.Runtime,
		Handler:      cfg.Handler,
		MemoryMB:     cfg.MemorySize,
		TimeoutSecs:  cfg.Timeout,
		EnvVars:      cfg.Environment,
		Payload:      payload,
		AccountID:    nr.AccountID,
		Layers:       p.resolveLayerInfos(cfg.Layers),
		LogType:      logType,
	}
	result, err := p.executor.Invoke(invCtx, req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(invCtx.Err(), context.DeadlineExceeded) {
			reqID := reqctx.GetRequestID(ctx)
			if reqID == "" {
				reqID = "00000000-0000-0000-0000-000000000000"
			}
			body := map[string]any{
				"errorMessage": fmt.Sprintf("%s %s Task timed out after %.2f seconds",
					clock.Now().Format("2006-01-02T15:04:05.000Z"),
					reqID,
					timeout.Seconds()),
				"errorType": "Runtime.TimeoutError",
			}
			timeoutBody, _ := json.Marshal(body)
			return &model.ProviderResponse{
				HTTPStatus: 200,
				Data:       map[string]any{"_function_error": "Unhandled", "_payload": timeoutBody},
			}, nil
		}
		return nil, model.NewProviderError("ServiceException", "invocation failed: "+err.Error(), 500)
	}

	if p.responsePayloadMax > 0 && int64(len(result.Payload)) > p.responsePayloadMax {
		body := map[string]any{
			"errorMessage": fmt.Sprintf("Response payload size (%d bytes) exceeded maximum allowed payload size (%d bytes).",
				len(result.Payload), p.responsePayloadMax),
			"errorType": "Function.ResponseSizeTooLarge",
		}
		oversizePayload, _ := json.Marshal(body)
		return &model.ProviderResponse{
			HTTPStatus: 200,
			Data:       map[string]any{"_function_error": "Unhandled", "_payload": oversizePayload},
		}, nil
	}

	// Write platform log lines (START/END/REPORT) to CloudWatch Logs and build LogTail.
	requestID := reqctx.GetRequestID(ctx)
	if requestID == "" {
		requestID = "00000000-0000-0000-0000-000000000000"
	}
	platformLines := buildPlatformLogs(requestID, cfg.MemorySize)
	if p.logsAPI != nil {
		logGroupName := "/aws/lambda/" + cfg.FunctionName
		date := clock.RealNow().Format("2006/01/02")
		logStreamName := fmt.Sprintf("%s/[$LATEST]%s", date, requestID)
		_ = p.logsAPI.InternalCreateLogGroup(ctx, logGroupName)
		events := make([]logstream.Event, len(platformLines))
		now := clock.RealNow().UnixMilli()
		for i, line := range platformLines {
			events[i] = logstream.Event{Timestamp: now, Message: line}
		}
		_ = p.logsAPI.InternalPutEvents(ctx, logGroupName, logStreamName, events)
	}

	respData := map[string]any{"_payload": result.Payload}
	if strings.EqualFold(logType, "Tail") {
		tail := result.LogTail
		if len(tail) == 0 {
			tail = []byte(strings.Join(platformLines, "\n") + "\n")
		}
		respData["LogResult"] = base64.StdEncoding.EncodeToString(tail)
	}

	return &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       respData,
	}, nil
}

// buildPlatformLogs returns Lambda-style START / END / REPORT lines for an invocation.
func buildPlatformLogs(requestID string, memMB int) []string {
	if memMB <= 0 {
		memMB = 128
	}
	return []string{
		fmt.Sprintf("START RequestId: %s Version: $LATEST", requestID),
		fmt.Sprintf("END RequestId: %s", requestID),
		fmt.Sprintf("REPORT RequestId: %s\tDuration: 1.00 ms\tBilled Duration: 1 ms\tMemory Size: %d MB\tMax Memory Used: %d MB",
			requestID, memMB, memMB),
	}
}

// ─── Versions ─────────────────────────────────────────────────────────────────

func versionKey(name, version string) string { return name + "#" + version }

func (p *FunctionProvider) loadVersion(ctx context.Context, account, region, name, version string) (functionConfig, error) {
	entry, err := p.resources.Get(ctx, account, region, resTypeVersions, versionKey(name, version))
	if err != nil {
		return functionConfig{}, err
	}
	var ve versionEntry
	if err := json.Unmarshal(entry.Data, &ve); err != nil {
		return functionConfig{}, err
	}
	return ve.functionConfig, nil
}

func (p *FunctionProvider) PublishVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	cfg.VersionCounter++
	versionStr := strconv.FormatInt(cfg.VersionCounter, 10)

	ve := versionEntry{functionConfig: cfg, Version: versionStr}
	data, _ := json.Marshal(ve)
	if err := p.resources.Upsert(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: resTypeVersions, ID: versionKey(name, versionStr), Data: data}); err != nil {
		return nil, fmt.Errorf("lambda: publish version: %w", err)
	}

	if err := p.saveConfig(ctx, nr.AccountID, nr.Region, cfg); err != nil {
		return nil, err
	}

	wire := cfgToWire(cfg)
	wire["Version"] = versionStr
	return provider.OK(wire), nil
}

func (p *FunctionProvider) ListVersionsByFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, resTypeVersions, name+"#")
	if err != nil {
		return nil, fmt.Errorf("lambda: list versions: %w", err)
	}
	// Also include $LATEST
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}

	versions := make([]map[string]any, 0, len(entries)+1)
	for _, e := range entries {
		var ve versionEntry
		if json.Unmarshal(e.Data, &ve) == nil {
			wire := cfgToWire(ve.functionConfig)
			wire["Version"] = ve.Version
			versions = append(versions, wire)
		}
	}
	// Sort by version number ascending
	sort.Slice(versions, func(i, j int) bool {
		vi, _ := strconv.ParseInt(fmt.Sprint(versions[i]["Version"]), 10, 64)
		vj, _ := strconv.ParseInt(fmt.Sprint(versions[j]["Version"]), 10, 64)
		return vi < vj
	})
	// Append $LATEST
	latestWire := cfgToWire(cfg)
	latestWire["Version"] = "$LATEST"
	versions = append(versions, latestWire)

	maxResults := 50
	if v, ok := nr.Params["MaxItems"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["Marker"].(string)
	page, next, pgErr := pagination.Paginate(versions, maxResults, token, "ListVersionsByFunction")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValueException", pgErr.Error(), 400)
	}
	data := map[string]any{"Versions": page}
	if next != "" {
		data["NextMarker"] = next
	}
	return provider.OK(data), nil
}

// ─── Aliases ──────────────────────────────────────────────────────────────────

func aliasKey(functionName, aliasName string) string { return functionName + "/" + aliasName }

func (p *FunctionProvider) loadAlias(ctx context.Context, account, region, functionName, aliasName string) (aliasEntry, error) {
	entry, err := p.resources.Get(ctx, account, region, resTypeAliases, aliasKey(functionName, aliasName))
	if err != nil {
		return aliasEntry{}, err
	}
	var a aliasEntry
	return a, json.Unmarshal(entry.Data, &a)
}

func (p *FunctionProvider) saveAlias(ctx context.Context, account, region string, a aliasEntry) error {
	data, _ := json.Marshal(a)
	entry := store.ResourceEntry{Type: resTypeAliases, ID: aliasKey(a.FunctionName, a.Name), Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *FunctionProvider) CreateAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	aliasName := strParam(nr.Params, "Name")
	if aliasName == "" {
		return nil, model.NewProviderError("ValidationException", "Name is required", 400)
	}
	funcVersion := strParam(nr.Params, "FunctionVersion")
	if funcVersion == "" {
		funcVersion = "$LATEST"
	}

	if _, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}

	a := aliasEntry{
		FunctionName:    name,
		Name:            aliasName,
		FunctionVersion: funcVersion,
		AliasArn:        nr.ResourceID(model.RTLambdaFunction, name+":"+aliasName),
		Description:     strParam(nr.Params, "Description"),
		RevisionId:      "1",
	}
	// Fail if alias already exists
	if _, err := p.loadAlias(ctx, nr.AccountID, nr.Region, name, aliasName); err == nil {
		return nil, model.NewProviderError("ResourceConflictException", "Alias already exists: "+aliasName, 409)
	}
	if err := p.saveAlias(ctx, nr.AccountID, nr.Region, a); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: aliasToWire(a)}, nil
}

func (p *FunctionProvider) GetAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	aliasName := strParam(nr.Params, "_alias_name")
	a, err := p.loadAlias(ctx, nr.AccountID, nr.Region, name, aliasName)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Alias not found: "+aliasName)
	}
	return provider.OK(aliasToWire(a)), nil
}

func (p *FunctionProvider) UpdateAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	aliasName := strParam(nr.Params, "_alias_name")
	a, err := p.loadAlias(ctx, nr.AccountID, nr.Region, name, aliasName)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Alias not found: "+aliasName)
	}
	if v := strParam(nr.Params, "FunctionVersion"); v != "" {
		a.FunctionVersion = v
	}
	if d := strParam(nr.Params, "Description"); d != "" {
		a.Description = d
	}
	if err := p.saveAlias(ctx, nr.AccountID, nr.Region, a); err != nil {
		return nil, err
	}
	return provider.OK(aliasToWire(a)), nil
}

func (p *FunctionProvider) DeleteAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	aliasName := strParam(nr.Params, "_alias_name")
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, resTypeAliases, aliasKey(name, aliasName)); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Alias not found: "+aliasName)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListAliases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, resTypeAliases, name+"/")
	if err != nil {
		return nil, fmt.Errorf("lambda: list aliases: %w", err)
	}
	aliases := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var a aliasEntry
		if json.Unmarshal(e.Data, &a) == nil {
			aliases = append(aliases, aliasToWire(a))
		}
	}
	maxResults := 50
	if v, ok := nr.Params["MaxItems"].(float64); ok && v > 0 {
		maxResults = int(v)
	}
	token, _ := nr.Params["Marker"].(string)
	page, next, pgErr := pagination.Paginate(aliases, maxResults, token, "ListAliases")
	if pgErr != nil {
		return nil, model.NewProviderError("InvalidParameterValueException", pgErr.Error(), 400)
	}
	data := map[string]any{"Aliases": page}
	if next != "" {
		data["NextMarker"] = next
	}
	return provider.OK(data), nil
}

func aliasToWire(a aliasEntry) map[string]any {
	return map[string]any{
		"FunctionName":    a.FunctionName,
		"Name":            a.Name,
		"FunctionVersion": a.FunctionVersion,
		"AliasArn":        a.AliasArn,
		"Description":     a.Description,
		"RevisionId":      a.RevisionId,
	}
}

// ─── Layers ───────────────────────────────────────────────────────────────────

func layerVersionKey(layerName string, version int64) string {
	return fmt.Sprintf("%s#%d", layerName, version)
}

func (p *FunctionProvider) layerARN(nr *model.NormalizedRequest, layerName string) string {
	return nr.ResourceID(model.RTLambdaFunction, "layer:"+layerName)
}

func (p *FunctionProvider) nextLayerVersion(ctx context.Context, account, region, layerName string) (int64, error) {
	entries, err := p.resources.List(ctx, account, region, resTypeLayers, layerName+"#")
	if err != nil {
		return 0, err
	}
	var max int64
	for _, e := range entries {
		var le layerEntry
		if json.Unmarshal(e.Data, &le) == nil && le.VersionNumber > max {
			max = le.VersionNumber
		}
	}
	return max + 1, nil
}

func (p *FunctionProvider) PublishLayerVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	layerName := strParam(nr.Params, "_layer_name")
	if layerName == "" {
		return nil, model.NewProviderError("ValidationException", "LayerName is required", 400)
	}
	version, err := p.nextLayerVersion(ctx, nr.AccountID, nr.Region, layerName)
	if err != nil {
		return nil, fmt.Errorf("lambda: layer version: %w", err)
	}

	var runtimes []string
	if raw, ok := nr.Params["CompatibleRuntimes"].([]any); ok {
		for _, r := range raw {
			if s, ok := r.(string); ok {
				runtimes = append(runtimes, s)
			}
		}
	}

	layerARN := p.layerARN(nr, layerName)
	le := layerEntry{
		LayerName:          layerName,
		LayerArn:           layerARN,
		VersionNumber:      version,
		VersionArn:         fmt.Sprintf("%s:%d", layerARN, version),
		Description:        strParam(nr.Params, "Description"),
		LicenseInfo:        strParam(nr.Params, "LicenseInfo"),
		CompatibleRuntimes: runtimes,
		CreatedDate:        clock.Now().Format(time.RFC3339),
	}
	if zf, ok := nr.Params["Content"].(map[string]any); ok {
		if b64, ok := zf["ZipFile"].(string); ok {
			if zipBytes, err := base64.StdEncoding.DecodeString(b64); err == nil && len(zipBytes) > 0 {
				_, codeSize, _, _ := p.storeLayerCode(ctx, nr.AccountID, layerName, version, zipBytes)
				le.CodeSize = codeSize
			}
		}
	}
	data, _ := json.Marshal(le)
	if err := p.resources.Create(ctx, nr.AccountID, nr.Region, store.ResourceEntry{Type: resTypeLayers, ID: layerVersionKey(layerName, version), Data: data}); err != nil {
		return nil, fmt.Errorf("lambda: publish layer: %w", err)
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: layerToWire(le)}, nil
}

func (p *FunctionProvider) GetLayerVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	layerName := strParam(nr.Params, "_layer_name")
	versionStr := strParam(nr.Params, "_layer_version")
	version, _ := strconv.ParseInt(versionStr, 10, 64)

	entry, err := p.resources.Get(ctx, nr.AccountID, nr.Region, resTypeLayers, layerVersionKey(layerName, version))
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Layer version not found: "+layerName+":"+versionStr)
	}
	var le layerEntry
	if err := json.Unmarshal(entry.Data, &le); err != nil {
		return nil, err
	}
	return provider.OK(layerToWire(le)), nil
}

func (p *FunctionProvider) DeleteLayerVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	layerName := strParam(nr.Params, "_layer_name")
	versionStr := strParam(nr.Params, "_layer_version")
	version, _ := strconv.ParseInt(versionStr, 10, 64)

	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, resTypeLayers, layerVersionKey(layerName, version)); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Layer version not found")
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListLayerVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	layerName := strParam(nr.Params, "_layer_name")
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, resTypeLayers, layerName+"#")
	if err != nil {
		return nil, fmt.Errorf("lambda: list layer versions: %w", err)
	}
	versions := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		var le layerEntry
		if json.Unmarshal(e.Data, &le) == nil {
			versions = append(versions, layerToWire(le))
		}
	}
	sort.Slice(versions, func(i, j int) bool {
		vi, _ := versions[i]["Version"].(float64)
		vj, _ := versions[j]["Version"].(float64)
		return vi > vj // descending (latest first)
	})
	return provider.OK(map[string]any{"LayerVersions": versions}), nil
}

func (p *FunctionProvider) ListLayers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, nr.Region, resTypeLayers, "")
	if err != nil {
		return nil, fmt.Errorf("lambda: list layers: %w", err)
	}
	// Deduplicate to latest version per layer
	latest := map[string]layerEntry{}
	for _, e := range entries {
		var le layerEntry
		if json.Unmarshal(e.Data, &le) == nil {
			if existing, ok := latest[le.LayerName]; !ok || le.VersionNumber > existing.VersionNumber {
				latest[le.LayerName] = le
			}
		}
	}
	layers := make([]map[string]any, 0, len(latest))
	for _, le := range latest {
		layers = append(layers, map[string]any{
			"LayerName":             le.LayerName,
			"LayerArn":              le.LayerArn,
			"LatestMatchingVersion": layerToWire(le),
		})
	}
	return provider.OK(map[string]any{"Layers": layers}), nil
}

func layerToWire(le layerEntry) map[string]any {
	return map[string]any{
		"LayerName":          le.LayerName,
		"LayerArn":           le.LayerArn,
		"Version":            le.VersionNumber,
		"LayerVersionArn":    le.VersionArn,
		"Description":        le.Description,
		"LicenseInfo":        le.LicenseInfo,
		"CompatibleRuntimes": le.CompatibleRuntimes,
		"CreatedDate":        le.CreatedDate,
	}
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

// functionNameFromARN extracts the function name from an ARN or returns as-is.
func functionNameFromARN(arn string) string {
	// arn:aws:lambda:region:account:function:name
	parts := strings.Split(arn, ":")
	for i, p := range parts {
		if p == "function" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return arn
}

func (p *FunctionProvider) TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "_resource_arn")
	name := functionNameFromARN(arn)
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	if cfg.Tags == nil {
		cfg.Tags = make(map[string]string)
	}
	if tags, ok := nr.Params["Tags"].(map[string]any); ok {
		for k, v := range tags {
			cfg.Tags[k] = fmt.Sprint(v)
		}
	}
	if err := p.saveConfig(ctx, nr.AccountID, nr.Region, cfg); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) UntagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "_resource_arn")
	name := functionNameFromARN(arn)
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	// Tag keys come as repeated query params: ?tagKeys=k1&tagKeys=k2
	// Already copied into params by the codec's query param loop (only first value).
	// The SDK actually sends: DELETE /tags/{arn}?tagKeys=k1&tagKeys=k2
	// We need to extract all tagKeys values from the raw request.
	if raw := nr.Raw; raw != nil {
		for _, k := range raw.URL.Query()["tagKeys"] {
			delete(cfg.Tags, k)
		}
	}
	if err := p.saveConfig(ctx, nr.AccountID, nr.Region, cfg); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) ListTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	arn := strParam(nr.Params, "_resource_arn")
	name := functionNameFromARN(arn)
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	tags := cfg.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return provider.OK(map[string]any{"Tags": tags}), nil
}

// ─── Permissions ──────────────────────────────────────────────────────────────

func (p *FunctionProvider) loadPolicy(ctx context.Context, account, region, name string) (policyDocument, error) {
	entry, err := p.resources.Get(ctx, account, region, resTypePolicies, name)
	if err != nil {
		return policyDocument{Version: "2012-10-17", Id: name}, nil // empty policy
	}
	var doc policyDocument
	return doc, json.Unmarshal(entry.Data, &doc)
}

func (p *FunctionProvider) savePolicy(ctx context.Context, account, region, name string, doc policyDocument) error {
	data, _ := json.Marshal(doc)
	entry := store.ResourceEntry{Type: resTypePolicies, ID: name, Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *FunctionProvider) AddPermission(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	statementID := strParam(nr.Params, "StatementId")
	if statementID == "" {
		return nil, model.NewProviderError("ValidationException", "StatementId is required", 400)
	}
	if _, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}

	principal := strParam(nr.Params, "Principal")
	action := strParam(nr.Params, "Action")
	functionARN := p.functionARN(nr, name)

	stmt := policyStatement{
		Sid:    statementID,
		Effect: "Allow",
		Principal: map[string]any{
			"Service": principal,
		},
		Action:   action,
		Resource: functionARN,
	}
	if src := strParam(nr.Params, "SourceArn"); src != "" {
		stmt.Condition = map[string]any{
			"ArnLike": map[string]any{"AWS:SourceArn": src},
		}
	}

	doc, err := p.loadPolicy(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	// Remove existing statement with same Sid
	filtered := doc.Statement[:0]
	for _, s := range doc.Statement {
		if s.Sid != statementID {
			filtered = append(filtered, s)
		}
	}
	doc.Statement = append(filtered, stmt)
	if err := p.savePolicy(ctx, nr.AccountID, nr.Region, name, doc); err != nil {
		return nil, err
	}

	stmtJSON, _ := json.Marshal(stmt)
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{
		"Statement": string(stmtJSON),
	}}, nil
}

func (p *FunctionProvider) RemovePermission(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	statementID := strParam(nr.Params, "_statement_id")

	doc, err := p.loadPolicy(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	filtered := doc.Statement[:0]
	for _, s := range doc.Statement {
		if s.Sid != statementID {
			filtered = append(filtered, s)
		}
	}
	doc.Statement = filtered
	if err := p.savePolicy(ctx, nr.AccountID, nr.Region, name, doc); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func (p *FunctionProvider) GetPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	if _, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	doc, err := p.loadPolicy(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, err
	}
	if len(doc.Statement) == 0 {
		return nil, model.NewProviderError("ResourceNotFoundException", "No policy found for function: "+name, 404)
	}
	policyJSON, _ := json.Marshal(doc)
	return provider.OK(map[string]any{
		"Policy":     string(policyJSON),
		"RevisionId": "1",
	}), nil
}

// ─── Function URLs ─────────────────────────────────────────────────────────────

func (p *FunctionProvider) loadURLConfig(ctx context.Context, account, region, name string) (urlConfig, error) {
	entry, err := p.resources.Get(ctx, account, region, resTypeURLs, name)
	if err != nil {
		return urlConfig{}, err
	}
	var uc urlConfig
	return uc, json.Unmarshal(entry.Data, &uc)
}

func (p *FunctionProvider) saveURLConfig(ctx context.Context, account, region, name string, uc urlConfig) error {
	data, _ := json.Marshal(uc)
	entry := store.ResourceEntry{Type: resTypeURLs, ID: name, Data: data}
	return p.resources.Upsert(ctx, account, region, entry)
}

func (p *FunctionProvider) CreateFunctionUrlConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	if _, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	if _, err := p.loadURLConfig(ctx, nr.AccountID, nr.Region, name); err == nil {
		return nil, model.NewProviderError("ResourceConflictException", "Function URL config already exists", 409)
	}
	authType := strParam(nr.Params, "AuthType")
	if authType == "" {
		authType = "NONE"
	}
	now := clock.Now().Format(time.RFC3339)
	uc := urlConfig{
		FunctionArn:      p.functionARN(nr, name),
		FunctionUrl:      fmt.Sprintf("https://%s.lambda-url.us-east-1.on.aws/", name),
		AuthType:         authType,
		CreatedTime:      now,
		LastModifiedTime: now,
	}
	if err := p.saveURLConfig(ctx, nr.AccountID, nr.Region, name, uc); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: urlToWire(uc)}, nil
}

func (p *FunctionProvider) GetFunctionUrlConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	uc, err := p.loadURLConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function URL config not found: "+name)
	}
	return provider.OK(urlToWire(uc)), nil
}

func (p *FunctionProvider) UpdateFunctionUrlConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	uc, err := p.loadURLConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function URL config not found: "+name)
	}
	if a := strParam(nr.Params, "AuthType"); a != "" {
		uc.AuthType = a
	}
	uc.LastModifiedTime = clock.Now().Format(time.RFC3339)
	if err := p.saveURLConfig(ctx, nr.AccountID, nr.Region, name, uc); err != nil {
		return nil, err
	}
	return provider.OK(urlToWire(uc)), nil
}

func (p *FunctionProvider) DeleteFunctionUrlConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	if err := p.resources.Delete(ctx, nr.AccountID, nr.Region, resTypeURLs, name); err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function URL config not found: "+name)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

func urlToWire(uc urlConfig) map[string]any {
	return map[string]any{
		"FunctionArn":      uc.FunctionArn,
		"FunctionUrl":      uc.FunctionUrl,
		"AuthType":         uc.AuthType,
		"CreatedTime":      uc.CreatedTime,
		"LastModifiedTime": uc.LastModifiedTime,
	}
}

// ─── Concurrency ──────────────────────────────────────────────────────────────

func (p *FunctionProvider) PutFunctionConcurrency(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	var concurrency int
	switch v := nr.Params["ReservedConcurrentExecutions"].(type) {
	case float64:
		concurrency = int(v)
	case int:
		concurrency = v
	}
	cfg.ReservedConcurrency = &concurrency
	if err := p.saveConfig(ctx, nr.AccountID, nr.Region, cfg); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"ReservedConcurrentExecutions": concurrency}), nil
}

func (p *FunctionProvider) GetFunctionConcurrency(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	if cfg.ReservedConcurrency == nil {
		return provider.OK(map[string]any{}), nil
	}
	return provider.OK(map[string]any{"ReservedConcurrentExecutions": *cfg.ReservedConcurrency}), nil
}

func (p *FunctionProvider) DeleteFunctionConcurrency(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := extractFunctionName(strParam(nr.Params, "_function_name"))
	cfg, err := p.loadConfig(ctx, nr.AccountID, nr.Region, name)
	if err != nil {
		return nil, provider.StoreNotFoundError(err, "ResourceNotFoundException", "Function not found: "+name)
	}
	cfg.ReservedConcurrency = nil
	if err := p.saveConfig(ctx, nr.AccountID, nr.Region, cfg); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── Account settings ─────────────────────────────────────────────────────────

func (p *FunctionProvider) GetAccountSettings(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{
		"AccountLimit": map[string]any{
			"TotalCodeSize":                  80530636800,
			"CodeSizeUnzipped":               262144000,
			"CodeSizeZipped":                 52428800,
			"ConcurrentExecutions":           1000,
			"UnreservedConcurrentExecutions": 1000,
		},
		"AccountUsage": map[string]any{
			"TotalCodeSize": 0,
			"FunctionCount": 0,
		},
	}), nil
}
