// Package functionsui serves the Cloud Functions console UI API. Handlers call
// the transport-neutral Functions core directly (in-process) rather than over
// the wire.
//
// The console is location-optional: the function list aggregates every resource
// in the project across all locations (no location picker) and shows the
// location as a read-only field. Detail/actions link back with the location
// carried from the list row, so a resource id never has to be disambiguated by
// hand.
//
// Real Cloud Functions exposes no execution resource (invocation history lives
// in Cloud Logging), so the "Executions" tab here renders the emulator's
// persisted event-delivery records — an emulator-only diagnostic, not a parity
// surface. The Test action maps onto the real functions.call method.
package functionsui

// Function is the console rendering of a Cloud Functions function, flattened
// across locations for the list page.
type Function struct {
	ID                            string            `json:"id"`
	Location                      string            `json:"location"`
	Name                          string            `json:"name"`
	Status                        string            `json:"status"`
	URL                           string            `json:"url,omitempty"`
	Runtime                       string            `json:"runtime,omitempty"`
	EntryPoint                    string            `json:"entryPoint,omitempty"`
	TriggerType                   string            `json:"triggerType,omitempty"` // "http" | "event"
	EventTrigger                  *EventTrigger     `json:"eventTrigger,omitempty"`
	AvailableMemoryMB             int               `json:"availableMemoryMB,omitempty"`
	Timeout                       string            `json:"timeout,omitempty"`
	TimeoutSeconds                int               `json:"timeoutSeconds,omitempty"`
	MinInstanceCount              int               `json:"minInstanceCount,omitempty"`
	MaxInstanceCount              int               `json:"maxInstanceCount,omitempty"`
	MaxInstanceRequestConcurrency int               `json:"maxInstanceRequestConcurrency,omitempty"`
	AvailableCPU                  string            `json:"availableCpu,omitempty"`
	Revision                      int               `json:"revision,omitempty"`
	SourceArchiveURL              string            `json:"sourceArchiveUrl,omitempty"`
	SourceUploadURL               string            `json:"sourceUploadUrl,omitempty"`
	SourceSize                    int64             `json:"sourceSize,omitempty"`
	SourceSHA256                  string            `json:"sourceSha256,omitempty"`
	Description                   string            `json:"description,omitempty"`
	Labels                        map[string]string `json:"labels,omitempty"`
	EnvironmentVariables          map[string]string `json:"environmentVariables,omitempty"`
	CreateTime                    string            `json:"createTime,omitempty"`
	UpdateTime                    string            `json:"updateTime,omitempty"`
}

// EventTrigger is the console rendering of a function's event trigger. It
// carries only fields real Cloud Functions exposes on the function
// (eventType/resource from the v1 shape, retryPolicy/trigger from v2); the
// emulator-internal backing subscription is deliberately omitted.
type EventTrigger struct {
	EventType   string `json:"eventType,omitempty"`
	Resource    string `json:"resource,omitempty"`
	Retry       bool   `json:"retry,omitempty"`
	RetryPolicy string `json:"retryPolicy,omitempty"`
	Trigger     string `json:"trigger,omitempty"`
}

// ListFunctionsResponse is the response for GET /functions.
type ListFunctionsResponse struct {
	Functions []Function `json:"functions"`
	Total     int        `json:"total"`
}

// CreateInput is the create-function form body. Location, id, and runtime are
// required. Source is optional and supplied as a gs:// source archive URL, an
// inline source file (SourceInline/SourceFilename), which the server packages
// into an archive, or the storageSource returned by the generateUploadUrl flow
// (SourceBucket/SourceObject) after the browser PUTs the archive to the upload
// URL.
type CreateInput struct {
	ID            string `json:"id"`
	Location      string `json:"location"`
	Runtime       string `json:"runtime"`
	EntryPoint    string `json:"entryPoint"`
	Description   string `json:"description"`
	TriggerType   string `json:"triggerType"` // "http" (default) | "event"
	EventType     string `json:"eventType,omitempty"`
	EventResource string `json:"eventResource,omitempty"`
	Retry         bool   `json:"retry,omitempty"`

	AvailableMemoryMB             int    `json:"availableMemoryMB,omitempty"`
	Timeout                       string `json:"timeout,omitempty"`
	MinInstanceCount              int    `json:"minInstanceCount,omitempty"`
	MaxInstanceCount              int    `json:"maxInstanceCount,omitempty"`
	MaxInstanceRequestConcurrency int    `json:"maxInstanceRequestConcurrency,omitempty"`
	AvailableCPU                  string `json:"availableCpu,omitempty"`

	SourceArchiveURL string `json:"sourceArchiveUrl,omitempty"`
	SourceInline     string `json:"sourceInline,omitempty"`
	SourceFilename   string `json:"sourceFilename,omitempty"`
	SourceBucket     string `json:"sourceBucket,omitempty"`
	SourceObject     string `json:"sourceObject,omitempty"`

	Labels               map[string]string `json:"labels,omitempty"`
	EnvironmentVariables map[string]string `json:"environmentVariables,omitempty"`
}

// Delivery is the console rendering of one persisted event-delivery record.
// Real Cloud Functions does not expose delivery records; the emulator persists
// them so retries and dead-letter outcomes are observable.
type Delivery struct {
	ID              string            `json:"id"`
	FunctionID      string            `json:"functionId"`
	Source          string            `json:"source,omitempty"`
	EventType       string            `json:"eventType,omitempty"`
	Resource        string            `json:"resource,omitempty"`
	EventID         string            `json:"eventId,omitempty"`
	Status          string            `json:"status"`
	Attempts        int               `json:"attempts"`
	Error           string            `json:"error,omitempty"`
	Result          string            `json:"result,omitempty"`
	DeadLetterTopic string            `json:"deadLetterTopic,omitempty"`
	Attributes      map[string]string `json:"attributes,omitempty"`
	CreateTime      string            `json:"createTime,omitempty"`
	UpdateTime      string            `json:"updateTime,omitempty"`
}

// ListDeliveriesResponse is the response for GET /functions/{location}/{function}/deliveries.
type ListDeliveriesResponse struct {
	Deliveries []Delivery `json:"deliveries"`
	Total      int        `json:"total"`
}

// CallInput is the body of POST /functions/{location}/{function}/call.
type CallInput struct {
	Data string `json:"data"`
}

// UploadInput is the body of POST /functions/upload. The location determines
// the GCS bucket (gcf-v2-sources-{project}-{location}) the archive is uploaded
// to, matching real Cloud Functions' generateUploadUrl.
type UploadInput struct {
	Location string `json:"location"`
}

// SourceUploadTarget is the POST /functions/upload response: the storageSource
// (bucket/object) the browser then PUTs the zip to and echoes back on create.
// This is the v2 generateUploadUrl contract; the console transfers the bytes
// through its own API origin (the UI server is a separate listener from the
// emulator gateway), so the target is addressed here rather than as an
// absolute GCS URL.
type SourceUploadTarget struct {
	Bucket string `json:"bucket"`
	Object string `json:"object"`
}

// CallResponse is the synchronous result of a function invocation. An executor
// failure is reported in Error with a nil transport error, matching real Cloud
// Functions, which returns the function error in-band.
type CallResponse struct {
	ExecutionID string `json:"executionId"`
	Result      string `json:"result,omitempty"`
	Error       string `json:"error,omitempty"`
}
