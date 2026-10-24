// Package ui is the cloud-neutral JaisCloud UI core: it serves the embedded
// React portal, the meta/services/accounts endpoints, the SSE event stream and
// the admin/scene static assets. Cloud-specific service APIs are contributed
// by a Registrar implementation (see internal/aws/ui for the AWS registrar).
package ui

// Tiers describe how faithfully a service is emulated, ordered by depth:
// full > shape > metadata. Every cloud's catalog (internal/aws/ui for AWS/Azure,
// internal/gcp/ui for GCP) must use exactly these values with this meaning — the
// rule applies across clouds, not per-cloud taste:
//
//   - TierFull — the real data plane and semantics, gated against the real
//     cloud. The only tier that may claim behaviour, not just shape.
//   - TierShape — a wire-conformant control plane plus a partial behaviour;
//     coverage and semantics are incomplete, so behaviour must be verified
//     against the real cloud service.
//   - TierMetadata — wire protocol and resource CRUD only; nothing executes.
//
// The consoles render full as no badge, and the others as "Shape only" /
// "Metadata only" (ui/src/lib/tier.ts). Do not add a value or a fourth meaning
// without updating every catalog and those shared labels.
const (
	TierFull     = "full"
	TierMetadata = "metadata"
	TierShape    = "shape"
)

// ServiceChild is a sub-page of a service (e.g. S3 → Buckets).
type ServiceChild struct {
	Label string `json:"label"`
	Path  string `json:"path"`
}

// ServiceDescriptor describes one service the UI can render. The navigation
// menu is built from these; a service appears only when its provider is wired.
type ServiceDescriptor struct {
	ID       string         `json:"id"`
	Label    string         `json:"label"`
	Category string         `json:"category"`
	RootPath string         `json:"rootPath"`
	Children []ServiceChild `json:"children"`
	Tier     string         `json:"tier"`
	Note     string         `json:"note,omitempty"`
	// Engine is the execution backend for an engine-capable service (Dataproc,
	// Cloud Run, Functions, Managed Kafka) and is nil for every other service.
	Engine *Engine `json:"engine,omitempty"`
}

// EngineMode is one execution backend an engine-capable service can run on.
type EngineMode struct {
	// Name is the backend: "mock", "docker", "k8s", or "native".
	Name string `json:"name"`
	// Supported reports whether this build can run that backend.
	Supported bool `json:"supported"`
	// Note is a short caveat (e.g. why a backend is unsupported).
	Note string `json:"note,omitempty"`
}

// Engine describes the execution backend behind an engine-capable service. It
// is the implementation axis, deliberately separate from Tier (the
// behavioural-depth axis): docker and k8s are interchangeable implementations
// of one executor seam, so Tier never depends on the active Mode. Only the
// mock-vs-real distinction changes depth.
type Engine struct {
	// Active reports whether a real (non-mock) backend is configured.
	Active bool `json:"active"`
	// Mode is the active backend name, empty when none is configured (the
	// service runs its mock/no-engine path).
	Mode string `json:"mode,omitempty"`
	// Source names where the configured mode came from (an environment variable,
	// or "default"), for the admin Runtime view.
	Source string `json:"source,omitempty"`
	// Modes lists every backend with its support state and caveat, driving the
	// console's availability matrix.
	Modes []EngineMode `json:"modes"`
}

// ServicesResponse is the payload for GET /api/ui/v1/services.
type ServicesResponse struct {
	Services []ServiceDescriptor `json:"services"`
}

// MetaResponse is the payload for GET /api/ui/v1/meta.
type MetaResponse struct {
	Cloud      string `json:"cloud"`
	Region     string `json:"region"`
	AccountId  string `json:"accountId"`
	Mode       string `json:"mode"` // "memory" | "postgres" | "ephemeral"
	Version    string `json:"version"`
	UIVersion  string `json:"uiVersion"`
	InstanceId string `json:"instanceId"`
	// BootID identifies this process start; it changes on every restart (unlike
	// the persisted InstanceId) so the browser can invalidate cached data.
	BootID string `json:"bootId"`
}

// AccountsResponse is the payload for GET /api/ui/v1/meta/accounts.
type AccountsResponse struct {
	Accounts []string `json:"accounts"`
}
