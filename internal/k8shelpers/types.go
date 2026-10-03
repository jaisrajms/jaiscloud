package k8shelpers

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// JobHandle uniquely identifies a submitted k8s Job and its pod.
// Returned by SubmitJob; consumed by WaitTerminal, TailLogs, Cancel.
type JobHandle struct {
	JobID     string // provider-owned job id
	Namespace string
	JobName   string // k8s Job object name
	JobUID    types.UID
	PodName   string // populated after pod is scheduled
	PodUID    types.UID
	CreatedAt time.Time
}

// Final is the terminal result of a single-pod k8s Job observed by
// WaitTerminal. This is pod-level terminal state — not Spark-level.
// sparkhelpers wraps this with Spark-specific classification.
type Final struct {
	Succeeded bool
	Cancelled bool
	ExitCode  int32
	Reason    string // K8s-reported reason (OOMKilled, Error, Completed)
	Message   string // truncated last-container-termination-message
	StartTime time.Time
	EndTime   time.Time
}

// LogKind selects which set of container logs to tail.
type LogKind int

const (
	LogKindInit LogKind = iota
	LogKindMain
	LogKindSidecar
	LogKindAll
	// LogKindMainRaw tails only the main container with no line prefix. It is
	// used when the stream is persisted as the job's own output (e.g. Dataproc
	// driver output) rather than multiplexed to a terminal, where the "[main] "
	// marker LogKindMain adds would be noise.
	LogKindMainRaw
)

// Snapshot is persisted to store.ResourceStore when a job terminates,
// so Describe-style APIs answer correctly after the k8s Job is GC'd.
type Snapshot struct {
	JobID      string
	State      string // provider-specific state string
	Reason     string
	Message    string
	StartTime  time.Time
	EndTime    time.Time
	ExitCode   int32
	LogURIs    map[string]string // e.g. {"stdout": "s3://...", "stderr": "s3://..."}
	CallerMeta map[string]string // provider-specific key/values
}

// OwnerRefHint lets a provider declare what owner object the helper
// should attach to pods it creates.
type OwnerRefHint struct {
	APIVersion string
	Kind       string
	Name       string
	UID        types.UID
}

// SubmitJobRequest is the input to SubmitJob.
type SubmitJobRequest struct {
	Namespace               string
	JobName                 string
	Spec                    corev1.PodSpec // fully-resolved pod spec
	Labels                  map[string]string
	Annotations             map[string]string
	Parallelism             int32
	BackoffLimit            int32
	TTLSecondsAfterFinished *int32
	ActiveDeadlineSeconds   *int64
	OwnerRef                *OwnerRefHint
}
