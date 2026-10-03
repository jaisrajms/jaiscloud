package dataproc

import (
	"encoding/json"
	"log/slog"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// protojsonOpts tolerates unknown fields so a REST-created resource body (whose
// Discovery shape may carry fields the proto snapshot does not yet have) still
// transcodes, and so the "@type" key in a persisted operation metadata is
// dropped.
var protojsonOpts = protojson.UnmarshalOptions{DiscardUnknown: true}

// protojsonToMap renders a proto message to a Discovery-shaped map (camelCase
// field names), the inverse of protojson.Unmarshal.
func protojsonToMap(m proto.Message) map[string]any {
	if m == nil {
		return nil
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil
	}
	return out
}

// clusterToProto renders a stored cluster as the proto Cluster. The Discovery
// JSON renderer is the single source of truth for the shape, so the gRPC and
// REST surfaces cannot drift.
func clusterToProto(c dpstore.Cluster) *dataprocpb.Cluster {
	out := &dataprocpb.Cluster{}
	b, err := json.Marshal(core.ClusterJSON(c))
	if err != nil {
		return out
	}
	_ = protojsonOpts.Unmarshal(b, out)
	return out
}

// jobToProto renders a stored job as the proto Job. SparkJob.driver is a proto
// oneof while the REST Discovery shape allows both mainJarFileUri and
// mainClass, so the Discovery map is sanitized first (see sanitizeJobOneofs).
// If the type-job still cannot be transcoded the scalar fields are preserved
// rather than silently dropped.
func jobToProto(j dpstore.Job) *dataprocpb.Job {
	data := core.JobJSON(j)
	sanitizeJobOneofs(data)
	b, err := json.Marshal(data)
	if err != nil {
		return jobScalarsToProto(j)
	}
	out := &dataprocpb.Job{}
	if err := protojsonOpts.Unmarshal(b, out); err != nil {
		slog.Warn("dataproc: job proto transcode failed; returning scalar fields", "job", j.JobID, "err", err)
		return jobScalarsToProto(j)
	}
	return out
}

// sanitizeJobOneofs rewrites a Discovery job map so it satisfies the proto
// oneofs. SparkJob's driver is a oneof (main_jar_file_uri XOR main_class), but
// the REST Discovery schema (and this emulator) allows both; the proto documents
// the faithful encoding — move the jar into jarFileUris and keep mainClass as
// the driver (see SparkJob's field docs).
func sanitizeJobOneofs(data map[string]any) {
	sj, ok := data["sparkJob"].(map[string]any)
	if !ok {
		return
	}
	jar, jarOK := sj["mainJarFileUri"].(string)
	if !jarOK || jar == "" {
		return
	}
	if class, classOK := sj["mainClass"].(string); !classOK || class == "" {
		return
	}
	uris, _ := sj["jarFileUris"].([]any)
	sj["jarFileUris"] = append(uris, jar)
	delete(sj, "mainJarFileUri")
}

// jobScalarsToProto renders the identity/status fields of a job without the
// type-job oneof. It is the fallback when a full protojson transcode fails.
func jobScalarsToProto(j dpstore.Job) *dataprocpb.Job {
	out := &dataprocpb.Job{
		Reference:               &dataprocpb.JobReference{ProjectId: j.ProjectID, JobId: j.JobID},
		Placement:               &dataprocpb.JobPlacement{ClusterName: j.PlacementClusterName, ClusterUuid: j.PlacementClusterUUID},
		Status:                  jobStatusToProto(j.Status),
		Labels:                  j.Labels,
		JobUuid:                 j.JobUUID,
		Done:                    jobIsTerminal(j.Status.State),
		DriverOutputResourceUri: j.DriverOutputResourceURI,
		DriverControlFilesUri:   j.DriverControlFilesURI,
	}
	for _, s := range j.StatusHistory {
		out.StatusHistory = append(out.StatusHistory, jobStatusToProto(s))
	}
	if j.Scheduling != nil {
		out.Scheduling = &dataprocpb.JobScheduling{
			MaxFailuresPerHour: j.Scheduling.MaxFailuresPerHour,
			MaxFailuresTotal:   j.Scheduling.MaxFailuresTotal,
		}
	}
	return out
}

// jobIsTerminal reports whether a job state is terminal (done).
func jobIsTerminal(state string) bool {
	switch state {
	case "DONE", "ERROR", "CANCELLED":
		return true
	}
	return false
}

// jobStateMatcherFromProto maps the ListJobsRequest.JobStateMatcher enum onto
// the core's transport-neutral JobStateMatcher. The zero value (ALL) and any
// unknown enum both mean "match every job".
func jobStateMatcherFromProto(m dataprocpb.ListJobsRequest_JobStateMatcher) core.JobStateMatcher {
	switch m {
	case dataprocpb.ListJobsRequest_ACTIVE:
		return core.JobStateMatcherActive
	case dataprocpb.ListJobsRequest_NON_ACTIVE:
		return core.JobStateMatcherNonActive
	default:
		return core.JobStateMatcherAll
	}
}

// jobStatusToProto maps a stored JobStatus onto the proto JobStatus.
func jobStatusToProto(s dpstore.JobStatus) *dataprocpb.JobStatus {
	out := &dataprocpb.JobStatus{
		State:          dataprocpb.JobStatus_State(dataprocpb.JobStatus_State_value[s.State]),
		Details:        s.Details,
		StateStartTime: timestamppb.New(s.StateStartTime),
	}
	if s.Substate != "" {
		out.Substate = dataprocpb.JobStatus_Substate(dataprocpb.JobStatus_Substate_value[s.Substate])
	}
	return out
}

// operationToProto renders a stored operation as the proto
// google.longrunning.Operation. Metadata and response are packed as typed Any
// messages: JobMetadata for a submit, ClusterOperationMetadata otherwise, and
// the response is Empty for a delete, Job for a submit, Cluster otherwise.
func operationToProto(op dpstore.Operation) (*longrunningpb.Operation, error) {
	meta := operationMetadataProto(op)
	if op.Metadata != "" {
		_ = protojsonOpts.Unmarshal([]byte(op.Metadata), meta)
	}
	metaAny, err := anypb.New(meta)
	if err != nil {
		return nil, err
	}
	out := &longrunningpb.Operation{
		Name:     core.OperationName(op.ProjectID, op.Region, op.ID),
		Metadata: metaAny,
		Done:     op.Done,
	}
	if op.Done && op.Response != "" {
		resp := operationResponseProto(op)
		if err := protojsonOpts.Unmarshal([]byte(op.Response), resp); err != nil {
			return nil, err
		}
		respAny, err := anypb.New(resp)
		if err != nil {
			return nil, err
		}
		out.Result = &longrunningpb.Operation_Response{Response: respAny}
	}
	return out, nil
}

func operationMetadataProto(op dpstore.Operation) proto.Message {
	switch op.Verb {
	case "submit":
		return &dataprocpb.JobMetadata{}
	case "workflow":
		return &dataprocpb.WorkflowMetadata{}
	default:
		return &dataprocpb.ClusterOperationMetadata{}
	}
}

func operationResponseProto(op dpstore.Operation) proto.Message {
	switch op.Verb {
	case "delete", "workflow":
		return &emptypb.Empty{}
	case "submit":
		return &dataprocpb.Job{}
	default:
		return &dataprocpb.Cluster{}
	}
}

// clusterInputFromProto builds a core ClusterInput from a request Cluster.
func clusterInputFromProto(c *dataprocpb.Cluster) core.ClusterInput {
	return core.ClusterInputFromMap(protojsonToMap(c))
}

// jobInputFromProto builds a core JobInput from a request Job.
func jobInputFromProto(j *dataprocpb.Job) core.JobInput {
	return core.JobInputFromMap(protojsonToMap(j))
}
