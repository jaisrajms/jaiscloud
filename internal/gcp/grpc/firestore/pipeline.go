package firestore

import (
	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

// ExecutePipeline implements the server-streaming Firestore.ExecutePipeline RPC.
// The pipeline engine itself lives in the transport-neutral provider package
// (firestoreprovider.Service.RunPipeline, shared with the REST
// `documents.executePipeline` handler); this adapter only resolves the project
// and database from the request and encodes the proto response.
func (s *Service) ExecutePipeline(req *firestorepb.ExecutePipelineRequest, stream firestorepb.Firestore_ExecutePipelineServer) error {
	ctx := stream.Context()
	project, database, _, ok := splitParent(req.GetDatabase())
	if !ok {
		return mapError(model.NewProviderError("InvalidArgument", "invalid database resource name", 400))
	}
	if project == "" {
		project = s.resolveProject(ctx)
	}
	res, err := s.svc.RunPipeline(ctx, project, database,
		req.GetStructuredPipeline().GetPipeline().GetStages(),
		req.GetTransaction(), req.GetNewTransaction() != nil)
	if err != nil {
		return mapError(err)
	}
	resp := &firestorepb.ExecutePipelineResponse{
		ExecutionTime: timestamp.Proto(res.ExecutionTime),
		Results:       encodeDocuments(res.Documents),
		Transaction:   res.Transaction,
	}
	return stream.Send(resp)
}
