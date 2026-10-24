// Package model holds the shared request/response types that flow between the
// gateway, adapter, and provider layers. It has no dependencies on any other
// internal package, which prevents import cycles.
package model

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"jaiscloud/internal/clock"
)

// KeyEncryptor is the narrow interface that providers use to interact with KMS.
// It lives in the model package to prevent import cycles: providers import
// model (not the key package), and the key package constructs implementations
// that are injected at startup.
type KeyEncryptor interface {
	// Encrypt encrypts plaintext using the given KMS key and optional encryption context.
	Encrypt(ctx context.Context, keyID string, pt []byte, encCtx map[string]string) ([]byte, error)
	// Decrypt decrypts ciphertext produced by Encrypt.
	Decrypt(ctx context.Context, keyID string, ct []byte, encCtx map[string]string) ([]byte, error)
	// GenerateDataKey generates a new data key under keyID.
	// Returns (plaintextDEK, encryptedDEK, error). Callers must zero ptDEK after use.
	GenerateDataKey(ctx context.Context, keyID string, bits int) (ptDEK, ctDEK []byte, err error)
}

// NoopKeyEncryptor is a pass-through KeyEncryptor used in memory mode (no real KMS).
// Encrypt returns the plaintext unchanged; Decrypt returns ciphertext unchanged.
// This is intentional for development — not production use.
type NoopKeyEncryptor struct{}

func (NoopKeyEncryptor) Encrypt(_ context.Context, _ string, pt []byte, _ map[string]string) ([]byte, error) {
	return pt, nil
}

func (NoopKeyEncryptor) Decrypt(_ context.Context, _ string, ct []byte, _ map[string]string) ([]byte, error) {
	return ct, nil
}

func (NoopKeyEncryptor) GenerateDataKey(_ context.Context, _ string, bits int) ([]byte, []byte, error) {
	key := make([]byte, bits/8)
	return key, key, nil
}

// Cloud identifies which cloud provider handled a request.
type Cloud string

const (
	CloudAWS   Cloud = "aws"
	CloudAzure Cloud = "azure"
	CloudGCP   Cloud = "gcp"
)

// NormalizedRequest is the cloud-agnostic representation of an API call.
// Each cloud adapter decodes the wire-format request into this struct.
type NormalizedRequest struct {
	Service string         // "sqs", "s3", ...
	Action  string         // "CreateQueue", "SendMessage", ...
	Params  map[string]any // decoded action-specific parameters

	// Injected by the gateway after codec decode
	Clock     clock.Clock
	Region    string
	AccountID string
	Port      int
	Cloud     Cloud // which cloud adapter handled this request (aws, azure, gcp)

	// AccessKey is the raw SigV4/SigV2 access-key-id extracted from the
	// Authorization header or presigned URL. Empty for anonymous requests.
	// Used by STS GetCallerIdentity and session-store lookups (§5.3, §9.3).
	AccessKey string

	// SourceArn carries the ARN of the resource that originated a cross-resource
	// fan-out call (e.g. the SNS topic ARN when SNS delivers to SQS). Propagated
	// via synthesised NormalizedRequest for in-process cross-resource dispatch (§11.0).
	SourceArn string

	// ServicePrincipal identifies the AWS service making a cross-resource call
	// (e.g. "sns.amazonaws.com"). Mirrors LocalStack's request_metadata propagation.
	ServicePrincipal string

	// ResourceID constructs a cloud-specific resource identifier from an abstract
	// resource type and logical name. Injected by the gateway; providers must call
	// this instead of formatting cloud-specific IDs (ARNs, Azure resource IDs, etc.)
	// directly. Example: nr.ResourceID("dynamodb-table", "my-table")
	ResourceID func(resourceType, name string) string

	// Per-request metadata (e.g. protocol variant)
	meta map[string]string

	// Original HTTP request (for headers, remote addr, etc.)
	Raw *http.Request
}

func (nr *NormalizedRequest) SetMeta(key, value string) {
	if nr.meta == nil {
		nr.meta = make(map[string]string)
	}
	nr.meta[key] = value
}

func (nr *NormalizedRequest) GetMeta(key string) string {
	if nr.meta == nil {
		return ""
	}
	return nr.meta[key]
}

// ProviderResponse is the cloud-agnostic response from a provider operation.
// The codec layer serialises it into the cloud-specific wire format.
type ProviderResponse struct {
	HTTPStatus int
	Data       map[string]any
}

// ProviderError is a structured error returned by a provider operation.
// The codec layer translates it into the cloud-specific wire format.
type ProviderError struct {
	Code       string // canonical code, e.g. "NotFound", "InvalidParameter"
	Message    string
	HTTPStatus int
	// Status, when non-empty, overrides the codec-derived google.rpc status
	// string in the error envelope (e.g. "FAILED_PRECONDITION" on HTTP 400,
	// "ABORTED" on HTTP 409). GCP codecs honor it; AWS/Azure ignore it.
	Status string
	// Data carries additional structured fields merged into the error body by
	// codecs (e.g. Reason, Type, LimitType for throttle errors). nil is safe.
	Data map[string]any
	// RetryAfter, when > 0, is the suggested client backoff for a retryable
	// error. The GCP transport surfaces it as a google.rpc.RetryInfo detail and
	// a Retry-After header (see internal/gcp/throttle); AWS/Azure ignore it.
	RetryAfter time.Duration
	// Details carries structured google.rpc detail protos merged into the GCP
	// error envelope's "details" array (REST) and the gRPC status details. nil
	// is safe. AWS/Azure ignore it.
	Details []ErrorDetail
}

// ErrorDetail is a structured detail attached to a ProviderError. The GCP
// transports render it as a google.rpc.* message; ErrorDetail currently models
// google.rpc.ErrorInfo (the shape Pub/Sub exactly-once failures use).
type ErrorDetail struct {
	// Type is the fully-qualified proto name without the URL prefix, e.g.
	// "google.rpc.ErrorInfo".
	Type string
	// Reason, Domain and Metadata are the google.rpc.ErrorInfo fields.
	Reason   string
	Domain   string
	Metadata map[string]string
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("%s: %s (HTTP %d)", e.Code, e.Message, e.HTTPStatus)
}

// WithData attaches structured fields to the error for codec-level serialization.
// Returns the receiver so callers can chain: NewProviderError(...).WithData(...)
func (e *ProviderError) WithData(data map[string]any) *ProviderError {
	e.Data = data
	return e
}

func NewProviderError(code, message string, httpStatus int) *ProviderError {
	return &ProviderError{Code: code, Message: message, HTTPStatus: httpStatus}
}
