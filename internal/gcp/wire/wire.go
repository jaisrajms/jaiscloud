// Package wire holds the reserved keys shared between GCP codecs and providers.
//
// GCP media APIs need to pass raw bytes between the decode path (adapter) and
// the provider, and resumable uploads need to surface a Location header from the
// provider back to the codec. These reserved keys are that contract.
package wire

const (
	// MediaKey carries raw object bytes: []byte in NormalizedRequest.Params
	// (codec → provider) or in ProviderResponse.Data (provider → codec).
	MediaKey = "media"
	// ContentTypeKey carries the object's Content-Type as a string.
	ContentTypeKey = "contentType"
	// LocationKey carries a resumable-upload Location header value as a string
	// in ProviderResponse.Data (provider → codec). Namespaced with a prefix that
	// cannot collide with a GCS resource field (which uses "location" for buckets).
	LocationKey = "jaiscloud:resumeLocation"
	// RangeKey carries the "Range: bytes=0-N" header value for 308 Resume
	// Incomplete responses (provider → codec).
	RangeKey = "jaiscloud:range"
	// StreamKey carries an io.Reader for a streaming upload body (codec →
	// provider). When present, the provider streams to PutStream instead of
	// buffering the full body in memory.
	StreamKey = "jaiscloud:stream"
	// BaseURLKey carries the request's absolute base URL ("scheme://host") as a
	// string in NormalizedRequest.Params (codec → provider). Resumable uploads
	// use it to build the absolute Location header the SDK expects.
	BaseURLKey = "jaiscloud:baseURL"
	// No308Key carries a bool in NormalizedRequest.Params (codec → provider)
	// indicating the client set "X-GUploader-No-308: yes". In that case the
	// provider signals "resume incomplete" with HTTP 200 + the
	// X-Http-Status-Code-Override header instead of a literal 308.
	No308Key = "jaiscloud:no308"
	// StatusOverrideKey carries the X-Http-Status-Code-Override header value as
	// a string in ProviderResponse.Data (provider → codec).
	StatusOverrideKey = "jaiscloud:statusOverride"
	// CSEKKey carries the base64 "x-goog-encryption-key" header value as a string
	// in NormalizedRequest.Params (codec → provider) for customer-supplied
	// encryption keys.
	CSEKKey = "x-goog-encryption-key"
	// CSEKKeySHA256 carries the base64 "x-goog-encryption-key-sha256" header
	// value as a string in NormalizedRequest.Params (codec → provider).
	CSEKKeySHA256 = "x-goog-encryption-key-sha256"
	// RawJSONKey carries a json.RawMessage in ProviderResponse.Data that the codec
	// emits verbatim (used by server-streaming REST methods — Firestore runQuery
	// and batchGet — whose response body is newline-delimited JSON, one JSON
	// object per line, not a JSON array or a single object).
	RawJSONKey = "jaiscloud:rawJSON"
	// MetaHeadersKey carries custom object metadata extracted from x-goog-meta-*
	// request headers (codec → provider), as map[string]string keyed by the
	// canonicalised metadata key.
	MetaHeadersKey = "jaiscloud:metaHeaders"
	// HeadersKey carries extra response headers (provider → codec) as
	// map[string]string. The codec merges them into the response header set, so
	// media downloads can surface x-goog-hash / x-goog-generation /
	// x-goog-meta-* etc. alongside the streamed bytes.
	HeadersKey = "jaiscloud:headers"
)
