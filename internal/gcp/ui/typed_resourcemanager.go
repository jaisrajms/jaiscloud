package ui

import (
	"context"
	"strings"

	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
)

// typedResourceManager serves the Resource Manager and project-picker consoles
// through the plugin chain when a plugin covers the key (see typed.go). It embeds
// the core, so the console-only discovery call (ListObservedProjects, which reads
// the registry of projects seen on the wire and is not a Resource Manager action)
// and anything else stays on the built-in.
//
// The REST API is v1: a project is {projectId, projectNumber, name (the display
// name), lifecycleState, parent {type,id}, labels, createTime}, so with a plugin in
// the path the console shows those fields.
type typedResourceManager struct {
	*resourcemanagercore.Service
	c typedCaller
}

func routeResourceManager(c typedCaller, core *resourcemanagercore.Service) typedResourceManager {
	return typedResourceManager{Service: core, c: c}
}

// decodeV1Project turns a v1 Project into the core's model.
func decodeV1Project(m map[string]any) resourcemanagercore.Project {
	p := resourcemanagercore.Project{
		ProjectID:     strField(m, "projectId"),
		ProjectNumber: strField(m, "projectNumber"),
		DisplayName:   strField(m, "name"),
		State:         strField(m, "lifecycleState"),
		CreateTime:    timeField(m, "createTime"),
		Labels:        strMapField(m, "labels"),
	}
	if par, ok := m["parent"].(map[string]any); ok {
		typ, id := strField(par, "type"), strField(par, "id")
		if typ != "" && id != "" {
			if !strings.HasSuffix(typ, "s") {
				typ += "s"
			}
			p.Parent = typ + "/" + id
		}
	}
	return p
}

// operationProject reads the project a create operation carries.
func operationProject(m map[string]any) resourcemanagercore.Project {
	if r, ok := m["response"].(map[string]any); ok {
		return decodeV1Project(r)
	}
	return decodeV1Project(m)
}

func (p typedResourceManager) ListProjects(ctx context.Context, pageSize int, pageToken string, showDeleted bool, filter string) ([]resourcemanagercore.Project, string, error) {
	params := map[string]any{}
	if pageSize > 0 {
		params["pageSize"] = float64(pageSize)
	}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	if filter != "" {
		params["filter"] = filter
	}
	data, covered, err := p.c.do(ctx, "ResourceManager.ProjectList", "", "", params, nil)
	if !covered {
		return p.Service.ListProjects(ctx, pageSize, pageToken, showDeleted, filter)
	}
	if err != nil {
		return nil, "", err
	}
	var out []resourcemanagercore.Project
	for _, m := range items(data, "projects") {
		pr := decodeV1Project(m)
		if !showDeleted && pr.State == "DELETE_REQUESTED" {
			continue // the v1 list keeps them; the console hides them unless asked
		}
		out = append(out, pr)
	}
	next, _ := data["nextPageToken"].(string)
	return out, next, nil
}

func (p typedResourceManager) GetProject(ctx context.Context, project string) (resourcemanagercore.Project, error) {
	data, covered, err := p.c.do(ctx, "ResourceManager.ProjectGet", project, "", map[string]any{"project": project}, nil)
	if !covered {
		return p.Service.GetProject(ctx, project)
	}
	if err != nil {
		return resourcemanagercore.Project{}, err
	}
	return decodeV1Project(data), nil
}

func (p typedResourceManager) CreateProject(ctx context.Context, in resourcemanagercore.CreateProjectInput) (resourcemanagercore.Project, resourcemanagercore.Operation, error) {
	body := map[string]any{"projectId": in.ProjectID, "name": in.DisplayName}
	if in.Labels != nil {
		body["labels"] = in.Labels
	}
	if typ, id, ok := strings.Cut(in.Parent, "/"); ok {
		body["parent"] = map[string]any{"type": strings.TrimSuffix(typ, "s"), "id": id}
	}
	data, covered, err := p.c.do(ctx, "ResourceManager.ProjectCreate", in.ProjectID, "", map[string]any{"body": body}, nil)
	if !covered {
		return p.Service.CreateProject(ctx, in)
	}
	if err != nil {
		return resourcemanagercore.Project{}, resourcemanagercore.Operation{}, err
	}
	proj := operationProject(data)
	done, _ := data["done"].(bool)
	return proj, resourcemanagercore.Operation{Name: strField(data, "name"), Verb: "create", Project: proj, Done: done}, nil
}

// settle runs a v1 delete or undelete, which answer Empty, and reads the project back
// so the console sees its new state.
func (p typedResourceManager) settle(ctx context.Context, key, verb, project string, direct func() (resourcemanagercore.Project, resourcemanagercore.Operation, error)) (resourcemanagercore.Project, resourcemanagercore.Operation, error) {
	_, covered, err := p.c.do(ctx, key, project, "", map[string]any{"project": project}, nil)
	if !covered {
		return direct()
	}
	if err != nil {
		return resourcemanagercore.Project{}, resourcemanagercore.Operation{}, err
	}
	proj, err := p.GetProject(ctx, project)
	if err != nil {
		return resourcemanagercore.Project{}, resourcemanagercore.Operation{}, err
	}
	return proj, resourcemanagercore.Operation{Verb: verb, Project: proj, Done: true}, nil
}

func (p typedResourceManager) DeleteProject(ctx context.Context, project string) (resourcemanagercore.Project, resourcemanagercore.Operation, error) {
	return p.settle(ctx, "ResourceManager.ProjectDelete", "delete", project, func() (resourcemanagercore.Project, resourcemanagercore.Operation, error) {
		return p.Service.DeleteProject(ctx, project)
	})
}

func (p typedResourceManager) UndeleteProject(ctx context.Context, project string) (resourcemanagercore.Project, resourcemanagercore.Operation, error) {
	return p.settle(ctx, "ResourceManager.ProjectUndelete", "undelete", project, func() (resourcemanagercore.Project, resourcemanagercore.Operation, error) {
		return p.Service.UndeleteProject(ctx, project)
	})
}
