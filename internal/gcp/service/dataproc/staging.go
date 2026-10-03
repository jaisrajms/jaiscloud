package dataproc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// BlobSink writes bytes into the emulated GCS so the Dataproc core can stage a
// job's driver output and control files without importing the storage provider
// (no provider→provider dependency). It is implemented by the GCS provider and
// injected by main.go; a nil sink keeps driver-output URIs advertised but does
// not materialize the objects (unit tests / mock deployments).
type BlobSink interface {
	// EnsureBucket creates the bucket if it does not already exist. It is
	// idempotent and safe to call concurrently.
	EnsureBucket(ctx context.Context, project, bucket, location string) error
	// PutObjectBytes writes (or replaces) an object in the bucket.
	PutObjectBytes(ctx context.Context, project, bucket, object, contentType string, data []byte) error
}

// WithBlobSink wires the GCS object sink used to materialize driver output.
func WithBlobSink(s BlobSink) Option { return func(svc *Service) { svc.blobSink = s } }

// stagingBucketDefaultPrefix is the base name of the Cloud Storage bucket the
// emulator provisions for a cluster's ephemeral data, matching real Dataproc's
// "dataproc-staging-*" convention.
const stagingBucketDefaultPrefix = "dataproc-staging"

// maxStagingBucketName is the GCS bucket-name length limit.
const maxStagingBucketName = 63

// metainfoPrefix is the object prefix real Dataproc uses for a job's driver
// output and control files under its staging bucket.
const metainfoPrefix = "google-cloud-dataproc-metainfo"

// driverOutputAttemptSuffix is appended to a job's driverOutputResourceUri to
// get the actual stdout object. Real Dataproc's driverOutputResourceUri is a
// prefix and the driver output is attempt-indexed (`<uri>.000000000`); every
// official client sample and `gcloud dataproc jobs wait` append this suffix
// before reading, so the emulator stages the bytes there too.
const driverOutputAttemptSuffix = ".000000000"

// defaultStagingBucket derives a stable, GCS-valid default staging bucket for a
// project+region. Real Dataproc creates a region-scoped bucket with a random
// suffix; the emulator derives the suffix deterministically from the
// project+region so the same bucket is reused across clusters and survives a
// restart (mirroring the AWS S3 bucket-name helper).
func defaultStagingBucket(project, region string) string {
	raw := stagingBucketDefaultPrefix + "-" + project + "-" + region
	sum := sha256.Sum256([]byte(project + "/" + region))
	suffix := hex.EncodeToString(sum[:])[:8]
	name := sanitizeBucketName(raw)
	if max := maxStagingBucketName - len(suffix) - 1; len(name) > max {
		name = strings.TrimRight(name[:max], "-_.")
	}
	if name == "" {
		name = stagingBucketDefaultPrefix
	}
	return name + "-" + suffix
}

// sanitizeBucketName lowercases s and replaces every character GCS does not
// allow in a bucket name, collapses the consecutive dots GCS rejects, then
// trims the leading/trailing separators GCS rejects.
func sanitizeBucketName(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	name := b.String()
	// GCS forbids adjacent dots (".."); collapse runs.
	for strings.Contains(name, "..") {
		name = strings.ReplaceAll(name, "..", ".")
	}
	return strings.Trim(name, "-_.")
}

// stagingBucketForCluster resolves the bucket a cluster stages its jobs'
// ephemeral data in. Precedence: an explicit GCE configBucket, then the
// caller-supplied config.tempBucket, then the GKE virtualClusterConfig
// stagingBucket, then the emulator's derived default.
func stagingBucketForCluster(c dpstore.Cluster) string {
	if cfg := mustJSONMap(c.Config); cfg != nil {
		if b := bucketField(cfg, "configBucket"); b != "" {
			return b
		}
		if b := bucketField(cfg, "tempBucket"); b != "" {
			return b
		}
	}
	if vcc := mustJSONMap(c.VirtualClusterConfig); vcc != nil {
		if b := bucketField(vcc, "stagingBucket"); b != "" {
			return b
		}
	}
	return defaultStagingBucket(c.ProjectID, c.Region)
}

// bucketField reads a bucket name from a config map, tolerating a "gs://"
// prefix and surrounding whitespace.
func bucketField(m map[string]any, key string) string {
	s, _ := m[key].(string)
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "gs://")
	return strings.TrimRight(s, "/")
}

// driverOutputObject is the object holding a job's driver stdout/stderr.
func driverOutputObject(clusterUUID, jobID string) string {
	return metainfoPrefix + "/" + clusterUUID + "/jobs/" + jobID + "/driveroutput"
}

// driverControlFilesPrefix is the directory the job's control files live under
// (DriverControlFilesURI in real GCP is a directory prefix ending in "/").
func driverControlFilesPrefix(clusterUUID, jobID string) string {
	return metainfoPrefix + "/" + clusterUUID + "/jobs/" + jobID + "/"
}

// gsURI renders a gs:// URI for a bucket + object.
func gsURI(bucket, object string) string { return "gs://" + bucket + "/" + object }

// splitGSURI splits a gs://bucket/object URI. ok is false for a malformed URI.
func splitGSURI(uri string) (bucket, object string, ok bool) {
	rest, found := strings.CutPrefix(uri, "gs://")
	if !found {
		return "", "", false
	}
	bucket, object, found = strings.Cut(rest, "/")
	if !found || bucket == "" || object == "" {
		return "", "", false
	}
	return bucket, object, true
}

// ensureDriverOutputURIs fills a job's driver-output URIs when they were not
// allocated at submit (e.g. a job rehydrated from an older snapshot, or one
// inserted directly by a test). It falls back to the derived default staging
// bucket and the job's own UUID. No-op when the URIs are already set.
func ensureDriverOutputURIs(project, region string, j *dpstore.Job) {
	if j.DriverOutputResourceURI != "" {
		return
	}
	uuid := j.JobUUID
	if uuid == "" {
		uuid = j.JobID
	}
	bucket := defaultStagingBucket(project, region)
	j.DriverOutputResourceURI = gsURI(bucket, driverOutputObject(uuid, j.JobID))
	j.DriverControlFilesURI = gsURI(bucket, driverControlFilesPrefix(uuid, j.JobID))
}

// prepareDriverOutput allocates a job's driver-output/control-file URIs and
// provisions the staging bucket. It is best-effort: an object-store failure is
// logged and never fails the submit (the URIs are still advertised and the
// output is retried at terminal state). URIs are allocated at submit time so
// they survive the cluster being deleted before the job completes.
func (s *Service) prepareDriverOutput(ctx context.Context, c dpstore.Cluster, j *dpstore.Job) {
	bucket := stagingBucketForCluster(c)
	if s.blobSink != nil {
		if err := s.blobSink.EnsureBucket(ctx, c.ProjectID, bucket, c.Region); err != nil {
			slog.Warn("dataproc: ensure staging bucket failed",
				"bucket", bucket, "cluster", c.Name, "err", err)
		}
	}
	j.DriverOutputResourceURI = gsURI(bucket, driverOutputObject(c.ClusterUUID, j.JobID))
	j.DriverControlFilesURI = gsURI(bucket, driverControlFilesPrefix(c.ClusterUUID, j.JobID))
}

// materializeDriverOutput writes a terminal job's driver output and control
// file to the staging bucket its URI points at. output is the captured driver
// stdout/stderr (empty in mock mode or on an early failure, in which case a
// small synthetic line is written so the advertised URI resolves). It is
// best-effort: a write failure is logged, not surfaced.
func (s *Service) materializeDriverOutput(ctx context.Context, j dpstore.Job, output []byte) {
	if s.blobSink == nil || j.DriverOutputResourceURI == "" {
		return
	}
	bucket, object, ok := splitGSURI(j.DriverOutputResourceURI)
	if !ok {
		slog.Warn("dataproc: malformed driverOutputResourceUri", "job", j.JobID, "uri", j.DriverOutputResourceURI)
		return
	}
	if len(output) == 0 {
		output = []byte("Dataproc job " + j.JobID + " finished with state " + j.Status.State + "\n")
	}
	// The advertised URI is a prefix: real readers fetch <uri>.000000000.
	outputObject := object + driverOutputAttemptSuffix
	if err := s.blobSink.PutObjectBytes(ctx, j.ProjectID, bucket, outputObject, "text/plain; charset=utf-8", output); err != nil {
		slog.Warn("dataproc: write driver output failed",
			"job", j.JobID, "bucket", bucket, "object", outputObject, "err", err)
	}
	// The control file lives under DriverControlFilesURI (a directory prefix);
	// derive its key from that URI so a custom/legacy output path cannot
	// misplace it.
	if cBucket, cPrefix, cok := splitGSURI(j.DriverControlFilesURI); cok {
		control := cPrefix + "drivercontrol"
		controlBody := []byte(driverControlSummary(j))
		if err := s.blobSink.PutObjectBytes(ctx, j.ProjectID, cBucket, control, "text/plain; charset=utf-8", controlBody); err != nil {
			slog.Warn("dataproc: write driver control file failed",
				"job", j.JobID, "bucket", cBucket, "object", control, "err", err)
		}
	}
}

// driverControlSummary is the emulator-defined control-file body: the job's
// placement and terminal state, mirroring the metadata a Dataproc control file
// carries.
func driverControlSummary(j dpstore.Job) string {
	return "jobId: " + j.JobID + "\nclusterName: " + j.PlacementClusterName +
		"\nstate: " + j.Status.State + "\n"
}
