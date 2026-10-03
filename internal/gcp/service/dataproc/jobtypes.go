package dataproc

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/sparkgcp"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/sparkhelpers"
)

// JobInput carries the caller-supplied fields of a job submission.
type JobInput struct {
	JobID                string
	PlacementClusterName string
	Type                 string
	TypeJob              json.RawMessage
	Labels               map[string]string
	// Scheduling is the job's restart policy (dataproc.v1.Job.scheduling). Nil
	// means the field was omitted (the API default: no restarts).
	Scheduling *dpstore.JobScheduling
}

// jobSchedulingFromMap reads <job>.scheduling, returning nil when absent or
// when both counters are zero. An all-zero policy is the API default "no
// restarts" and is stored as unset, so an explicit `scheduling: {}` reads back
// the same on every backend instead of only in the memory store.
func jobSchedulingFromMap(jobBody map[string]any) *dpstore.JobScheduling {
	s, ok := jobBody["scheduling"].(map[string]any)
	if !ok {
		return nil
	}
	perHour := schedulingCounter(s, "maxFailuresPerHour")
	total := schedulingCounter(s, "maxFailuresTotal")
	if perHour == 0 && total == 0 {
		return nil
	}
	return &dpstore.JobScheduling{MaxFailuresPerHour: perHour, MaxFailuresTotal: total}
}

// schedulingCounter reads a JobScheduling counter. A non-integral or
// out-of-int32 value is surfaced as -1 rather than silently wrapping to 0 (the
// "no restarts" default), so the range validation rejects it.
func schedulingCounter(m map[string]any, key string) int32 {
	raw, ok := m[key]
	if !ok {
		return 0
	}
	f, ok := raw.(float64)
	if !ok {
		if i, ok := raw.(int); ok {
			f = float64(i)
		} else {
			return -1
		}
	}
	if f != math.Trunc(f) || f < math.MinInt32 || f > math.MaxInt32 {
		return -1
	}
	return int32(f)
}

// longRunningPropertyPrefixes are the Spark configuration prefixes that mark a
// job as long-running (streaming) for the emulator's purposes. This is an
// emulator approximation: real GCP has no streaming marker — a streaming job is
// simply one whose driver never exits (see
// plan_docs/gcp-dataproc-streaming-wave-plan.md §5).
var longRunningPropertyPrefixes = []string{"spark.sql.streaming.", "spark.streaming."}

// isLongRunningJob reports whether a type-job's properties indicate a streaming
// job that must not be auto-settled to DONE in mock mode.
func isLongRunningJob(typeJob map[string]any) bool {
	props, _ := typeJob["properties"].(map[string]any)
	for k := range props {
		for _, p := range longRunningPropertyPrefixes {
			if strings.HasPrefix(k, p) {
				return true
			}
		}
	}
	return false
}

// JobInputFromMap builds a JobInput from the Discovery/proto Job map (the
// nested "job" object of a SubmitJob request).
func JobInputFromMap(jobBody map[string]any) JobInput {
	in := JobInput{
		Labels:     bodyStringMap(jobBody, "labels"),
		Scheduling: jobSchedulingFromMap(jobBody),
	}
	if ref, ok := jobBody["reference"].(map[string]any); ok {
		in.JobID = bodyString(ref, "jobId")
	}
	if placement, ok := jobBody["placement"].(map[string]any); ok {
		in.PlacementClusterName = bodyString(placement, "clusterName")
	}
	jobType, typeJob := extractJobType(jobBody)
	in.Type = jobType
	if jobType != "" && typeJob != nil {
		if data, err := json.Marshal(typeJob); err == nil {
			in.TypeJob = data
		}
	}
	return in
}

// jobTypeKeys is the set of Dataproc oneof type-job field names in precedence
// order. Only Spark-family types run; hadoopJob/hiveJob/pigJob/prestoJob/
// trinoJob/flinkJob are unsupported.
var jobTypeKeys = []string{
	"sparkJob", "pysparkJob", "sparkSqlJob", "sparkRJob",
	"hadoopJob", "hiveJob", "pigJob",
	"prestoJob", "trinoJob", "flinkJob",
}

// extractJobType returns the type-job field name and its body, or ("", nil)
// when the job carries no recognised type.
func extractJobType(body map[string]any) (string, map[string]any) {
	for _, k := range jobTypeKeys {
		if m, ok := body[k].(map[string]any); ok {
			return k, m
		}
	}
	return "", nil
}

// unsupportedJobTypes are job types the emulator does not run.
var unsupportedJobTypes = map[string]bool{
	"hadoopJob": true,
	"hiveJob":   true,
	"pigJob":    true,
	"prestoJob": true,
	"trinoJob":  true,
	"flinkJob":  true,
}

// jobToEntryPoint maps a Dataproc type-job body to a sparkhelpers.EntryPoint.
// Returns an error for unsupported (fail-loud) or malformed job types.
func jobToEntryPoint(jobType string, typeJob map[string]any) (sparkhelpers.EntryPoint, []string, error) {
	switch jobType {
	case "sparkJob":
		mainJar := bodyString(typeJob, "mainJarFileUri")
		mainClass := bodyString(typeJob, "mainClass")
		if mainJar == "" && mainClass == "" {
			return nil, nil, fmt.Errorf("sparkJob requires mainJarFileUri or mainClass")
		}
		ep := sparkhelpers.JarEntryPoint{
			JarURI:      mainJar,
			MainClass:   mainClass,
			JarFileURIs: bodyStringSlice(typeJob, "jarFileUris"),
		}
		return ep, bodyStringSlice(typeJob, "args"), nil
	case "pysparkJob":
		mainPy := bodyString(typeJob, "mainPythonFileUri")
		if mainPy == "" {
			return nil, nil, fmt.Errorf("pysparkJob requires mainPythonFileUri")
		}
		ep := sparkhelpers.PythonEntryPoint{
			MainPythonFile: mainPy,
			PyFiles:        bodyStringSlice(typeJob, "pythonFileUris"),
			JarFileURIs:    bodyStringSlice(typeJob, "jarFileUris"),
		}
		return ep, bodyStringSlice(typeJob, "args"), nil
	case "sparkRJob":
		mainR := bodyString(typeJob, "mainRFileUri")
		if mainR == "" {
			return nil, nil, fmt.Errorf("sparkRJob requires mainRFileUri")
		}
		ep := sparkhelpers.REntryPoint{MainRFile: mainR}
		return ep, bodyStringSlice(typeJob, "args"), nil
	case "sparkSqlJob":
		// spark-sql is a spark-submit wrapper, so this is semantically Spark
		// SQL (unlike hiveJob, which stays fail-loud as HJ2). queryFileUri is
		// passed to `-f` and queryList.queries to `-e`; both accept HCFS/gs://
		// URIs via the wired GCS connector. scriptVariables map to --hivevar
		// (the SQL CLI applies these as SET-equivalent variables).
		// SparkSqlJob.query_file_uri and query_list form a proto oneof, so
		// exactly one must be set.
		queries := queryListQueries(typeJob)
		_, hasQueryList := typeJob["queryList"].(map[string]any)
		fileURI := bodyString(typeJob, "queryFileUri")
		switch {
		case fileURI != "" && hasQueryList:
			return nil, nil, fmt.Errorf("sparkSqlJob must set only one of queryFileUri or queryList")
		case fileURI == "" && !hasQueryList:
			return nil, nil, fmt.Errorf("sparkSqlJob requires queryFileUri or queryList.queries")
		case fileURI == "" && len(queries) == 0:
			return nil, nil, fmt.Errorf("sparkSqlJob queryList.queries must not be empty")
		}
		ep := sparkhelpers.SqlEntryPoint{
			Queries:     queries,
			FileURI:     fileURI,
			JarFileURIs: bodyStringSlice(typeJob, "jarFileUris"),
			HiveVars:    bodyStringMap(typeJob, "scriptVariables"),
		}
		return ep, nil, nil
	default:
		return nil, nil, fmt.Errorf("job type %q is not supported by the emulator", jobType)
	}
}

// queryListQueries reads <typeJob>.queryList.queries.
func queryListQueries(typeJob map[string]any) []string {
	ql, _ := typeJob["queryList"].(map[string]any)
	if ql == nil {
		return nil
	}
	return bodyStringSlice(ql, "queries")
}

// propertiesToConfArgs converts a type-job properties map into "--conf k=v"
// flags. Caller properties that collide with the emulator's GCS connector
// wiring (sparkgcp.ProtectedSparkConf) are dropped: the emulator injects those
// via ExtraSparkConfs, and a caller-supplied override would break gs:// access
// against the local emulator. Every other property is passed through verbatim.
func propertiesToConfArgs(typeJob map[string]any) []string {
	props, _ := typeJob["properties"].(map[string]any)
	if props == nil {
		return nil
	}
	var out []string
	for k, v := range props {
		if sparkgcp.ProtectedSparkConf(k) {
			continue
		}
		if s, ok := v.(string); ok {
			out = append(out, "--conf", k+"="+s)
		}
	}
	return out
}

// jobToStore builds the store Job from a SubmitJob input.
func jobToStore(project, region string, in JobInput) dpstore.Job {
	jobID := in.JobID
	if jobID == "" {
		jobID = randomHex(16)
	}
	now := clock.Now().UTC()
	return dpstore.Job{
		ProjectID:            project,
		Region:               region,
		JobID:                jobID,
		PlacementClusterName: in.PlacementClusterName,
		Type:                 in.Type,
		TypeJob:              in.TypeJob,
		Labels:               in.Labels,
		Scheduling:           in.Scheduling,
		LongRunning:          isLongRunningJob(mustJSONMap(in.TypeJob)),
		// A submitted job is born PENDING and advances through SETUP_DONE/
		// RUNNING to a terminal state (see jobstate.go).
		Status:     dpstore.JobStatus{State: jobStatePending, StateStartTime: now},
		JobUUID:    randomHex(32),
		CreateTime: now,
	}
}
