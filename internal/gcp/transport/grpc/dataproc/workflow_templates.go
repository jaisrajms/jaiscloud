package dataproc

import (
	"context"
	"encoding/json"
	"strings"

	dataprocpb "cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"

	core "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// --- WorkflowTemplateService ---

// workflowTemplateToProto renders a stored workflow template as the proto
// WorkflowTemplate. The Discovery JSON renderer is the single source of truth
// for the shape, so gRPC and REST cannot drift.
func workflowTemplateToProto(t dpstore.WorkflowTemplate) *dataprocpb.WorkflowTemplate {
	out := &dataprocpb.WorkflowTemplate{}
	b, err := json.Marshal(core.WorkflowTemplateJSON(t))
	if err != nil {
		return out
	}
	_ = protojsonOpts.Unmarshal(b, out)
	return out
}

// parseWorkflowTemplateName splits a canonical template name
// (projects/{p}/regions/{r}/workflowTemplates/{id}) or a bare parent
// (projects/{p}/regions/{r}) into its parts. Missing parts are "".
func parseWorkflowTemplateName(name string) (project, region, id string) {
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	for i := 0; i+1 < len(parts); i++ {
		switch parts[i] {
		case "projects":
			project = parts[i+1]
		case "regions":
			region = parts[i+1]
		case "workflowTemplates":
			id = parts[i+1]
		}
	}
	return project, region, id
}

// resolveTemplateScope resolves the owning project/region/id from an explicit
// name or a parent, falling back to the configured default project.
func (s *Service) resolveTemplateScope(_ context.Context, name string) (string, string, string) {
	project, region, id := parseWorkflowTemplateName(name)
	if project == "" {
		project = s.defaultProj
	}
	return project, region, id
}

func (s *Service) CreateWorkflowTemplate(ctx context.Context, req *dataprocpb.CreateWorkflowTemplateRequest) (*dataprocpb.WorkflowTemplate, error) {
	project, region, _ := s.resolveTemplateScope(ctx, req.GetParent())
	t, err := s.core.CreateWorkflowTemplate(ctx, project, region, core.WorkflowTemplateInput{
		ID:         req.GetTemplate().GetId(),
		Definition: protojsonToMap(req.GetTemplate()),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return workflowTemplateToProto(t), nil
}

func (s *Service) GetWorkflowTemplate(ctx context.Context, req *dataprocpb.GetWorkflowTemplateRequest) (*dataprocpb.WorkflowTemplate, error) {
	project, region, id := s.resolveTemplateScope(ctx, req.GetName())
	t, err := s.core.GetWorkflowTemplate(ctx, project, region, id, req.GetVersion())
	if err != nil {
		return nil, mapError(err)
	}
	return workflowTemplateToProto(t), nil
}

func (s *Service) ListWorkflowTemplates(ctx context.Context, req *dataprocpb.ListWorkflowTemplatesRequest) (*dataprocpb.ListWorkflowTemplatesResponse, error) {
	project, region, _ := s.resolveTemplateScope(ctx, req.GetParent())
	page, next, err := s.core.ListWorkflowTemplates(ctx, project, region, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := &dataprocpb.ListWorkflowTemplatesResponse{NextPageToken: next}
	for _, t := range page {
		out.Templates = append(out.Templates, workflowTemplateToProto(t))
	}
	return out, nil
}

func (s *Service) UpdateWorkflowTemplate(ctx context.Context, req *dataprocpb.UpdateWorkflowTemplateRequest) (*dataprocpb.WorkflowTemplate, error) {
	tmpl := req.GetTemplate()
	project, region, id := s.resolveTemplateScope(ctx, tmpl.GetName())
	if id == "" {
		id = tmpl.GetId()
	}
	t, err := s.core.UpdateWorkflowTemplate(ctx, project, region, core.WorkflowTemplateInput{
		ID:         id,
		Version:    tmpl.GetVersion(),
		Definition: protojsonToMap(tmpl),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return workflowTemplateToProto(t), nil
}

func (s *Service) DeleteWorkflowTemplate(ctx context.Context, req *dataprocpb.DeleteWorkflowTemplateRequest) (*emptypb.Empty, error) {
	project, region, id := s.resolveTemplateScope(ctx, req.GetName())
	if err := s.core.DeleteWorkflowTemplate(ctx, project, region, id, req.GetVersion()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) InstantiateWorkflowTemplate(ctx context.Context, req *dataprocpb.InstantiateWorkflowTemplateRequest) (*longrunningpb.Operation, error) {
	project, region, id := s.resolveTemplateScope(ctx, req.GetName())
	op, err := s.core.InstantiateWorkflowTemplate(ctx, project, region, id, req.GetVersion(), req.GetParameters())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}

func (s *Service) InstantiateInlineWorkflowTemplate(ctx context.Context, req *dataprocpb.InstantiateInlineWorkflowTemplateRequest) (*longrunningpb.Operation, error) {
	project, region, _ := s.resolveTemplateScope(ctx, req.GetParent())
	op, err := s.core.InstantiateInlineWorkflowTemplate(ctx, project, region, protojsonToMap(req.GetTemplate()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op)
}
