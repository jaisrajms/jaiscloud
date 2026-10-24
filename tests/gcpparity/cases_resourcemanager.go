//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// resourceManagerScenario compares the Cloud Resource Manager project surface
// over REST and gRPC. The emulator serves the legacy v1 project API over REST
// (cloudresourcemanager.googleapis.com/v1: projectId, projectNumber, name =
// displayName, lifecycleState, parent {type,id}) while the proto-defined gRPC
// surface is v3 (google.cloud.resourcemanager.v3.Projects: name =
// "projects/{id}", displayName, state, parent string, etag, lifecycle times),
// both over one transport-neutral core. The two schemas are the deliberate
// v1/v3 alignment from the dual-protocol effort, so a full-body diff needs the
// resourceManagerProjection logical projection below (AUD3-5).
//
// The scenario creates a run-unique project, reads it back over both transports
// (get + list), updates it, deletes and undeletes it, and cross-diffs the
// create/update mutation responses on per-transport twins. Get/List/DELETE
// are read back over both transports so a field one adapter drops or invents is
// caught symmetrically.
func resourceManagerScenario() Scenario {
	// A short prefix keeps the run-unique project id within the v1/v3 grammar
	// (6-30 chars; cfg.Suffix is 12 hex chars, and a mutation-parity step folds
	// in "s<N>-<side>-" on top of the prefix).
	id := func(e *Env) string { return e.Resource("rm") }
	name := func(e *Env) string { return "projects/" + id(e) }
	restPath := func(e *Env) string { return "/v1/projects/" + id(e) }

	newClient := func(ctx context.Context, e *Env) (*resourcemanager.ProjectsClient, error) {
		return resourcemanager.NewProjectsClient(ctx, e.GRPCClientOptions()...)
	}

	// createGRPC returns the settled Project (the create operation is done in
	// the default synchronous LRO mode); createREST unwraps the v1 create
	// operation's response (the Any-typed Project) so a Create diff compares
	// like against like.
	createGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		op, err := c.CreateProject(ctx, &resourcemanagerpb.CreateProjectRequest{
			Project: &resourcemanagerpb.Project{
				ProjectId:   id(e),
				DisplayName: "Parity Project",
				Parent:      "organizations/123",
				Labels:      map[string]string{"env": "parity"},
			},
		})
		if err != nil {
			return nil, err
		}
		return op.Wait(ctx)
	}
	createREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		body := fmt.Sprintf(`{"projectId":%q,"name":"Parity Project","parent":{"type":"organization","id":"123"},"labels":{"env":"parity"}}`, id(e))
		return e.RestOperationResource(ctx, http.MethodPost, "/v1/projects", body)
	}

	getGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.GetProject(ctx, &resourcemanagerpb.GetProjectRequest{Name: name(e)})
	}
	getREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodGet, restPath(e), "")
	}

	// The v1 list keeps DELETE_REQUESTED projects visible until deletion
	// completes, so both transports list with ShowDeleted set.
	listGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		it := c.ListProjects(ctx, &resourcemanagerpb.ListProjectsRequest{
			Parent:      "projects/" + e.Cfg.Project,
			ShowDeleted: true,
		})
		out := &resourcemanagerpb.ListProjectsResponse{}
		for {
			p, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			out.Projects = append(out.Projects, p)
		}
		return out, nil
	}
	listREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodGet, "/v1/projects", "")
	}

	updateGRPC := func(ctx context.Context, e *Env) (protoMessage, error) {
		c, err := newClient(ctx, e)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		op, err := c.UpdateProject(ctx, &resourcemanagerpb.UpdateProjectRequest{
			Project: &resourcemanagerpb.Project{
				Name:        name(e),
				DisplayName: "Parity Updated",
				Labels:      map[string]string{"env": "parity", "stage": "updated"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name", "labels"}},
		})
		if err != nil {
			return nil, err
		}
		return op.Wait(ctx)
	}
	updateREST := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		return e.Rest(ctx, http.MethodPut, restPath(e), `{"name":"Parity Updated","labels":{"env":"parity","stage":"updated"}}`)
	}
	// The update mutation-parity steps stage their twin first (the create half
	// is discarded) so both transports diff an update response, mirroring the
	// storage-bucket update parity.
	updateGRPCMutation := func(ctx context.Context, e *Env) (protoMessage, error) {
		if _, err := createGRPC(ctx, e); err != nil {
			return nil, err
		}
		return updateGRPC(ctx, e)
	}
	updateRESTMutation := func(ctx context.Context, e *Env) (json.RawMessage, error) {
		if _, err := createREST(ctx, e); err != nil {
			return nil, err
		}
		return updateREST(ctx, e)
	}

	createCanonical := func(ctx context.Context, e *Env) error {
		_, err := createGRPC(ctx, e)
		return err
	}
	updateCanonical := func(ctx context.Context, e *Env) error {
		_, err := updateGRPC(ctx, e)
		return err
	}
	deleteCanonical := func(ctx context.Context, e *Env) error {
		c, err := newClient(ctx, e)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: name(e)})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	undeleteCanonical := func(ctx context.Context, e *Env) error {
		c, err := newClient(ctx, e)
		if err != nil {
			return err
		}
		defer c.Close()
		op, err := c.UndeleteProject(ctx, &resourcemanagerpb.UndeleteProjectRequest{Name: name(e)})
		if err != nil {
			return err
		}
		_, err = op.Wait(ctx)
		return err
	}
	// deleteTwin removes one side's twin after a mutation-parity step,
	// dispatching on the transport the harness is currently driving (Side).
	deleteTwin := func(ctx context.Context, e *Env) error {
		if e.Side == "grpc" {
			c, err := newClient(ctx, e)
			if err != nil {
				return err
			}
			defer c.Close()
			op, err := c.DeleteProject(ctx, &resourcemanagerpb.DeleteProjectRequest{Name: name(e)})
			if err != nil {
				return err
			}
			_, err = op.Wait(ctx)
			return err
		}
		return e.RestDelete(ctx, restPath(e))
	}

	return Scenario{Service: "resourcemanager", Steps: []Step{
		{Op: "CreateProject", Mutate: createCanonical},
		{Op: "GetProject", GRPC: getGRPC, REST: getREST, Project: resourceManagerProjection},
		{Op: "ListProjects", Scope: true, GRPC: listGRPC, REST: listREST, Project: resourceManagerProjection},
		{Op: "UpdateProject", Mutate: updateCanonical, GRPC: getGRPC, REST: getREST, Project: resourceManagerProjection},
		{Op: "DeleteProject", Mutate: deleteCanonical, GRPC: getGRPC, REST: getREST, Project: resourceManagerProjection},
		{Op: "UndeleteProject", Mutate: undeleteCanonical, GRPC: getGRPC, REST: getREST, Project: resourceManagerProjection},
		{
			Op: "CreateProject (parity)",
			Mutation: &MutationParity{
				GRPC: createGRPC, REST: createREST, Cleanup: deleteTwin, Project: resourceManagerProjection,
			},
		},
		{
			Op: "UpdateProject (parity)",
			Mutation: &MutationParity{
				GRPC: updateGRPCMutation, REST: updateRESTMutation, Cleanup: deleteTwin, Project: resourceManagerProjection,
			},
		},
	}}
}

// resourceManagerProjection canonicalizes a Cloud Resource Manager project body
// — a bare Project, a projects.list envelope, or a project nested anywhere in a
// response — into one logical form shared by the v1 REST JSON and the v3
// protojson rendering, leaving any genuine logical difference intact. It walks
// the body recursively so every project object is handled.
//
//   - `name` is overloaded: v1 carries the display name while v3 carries the
//     resource name. The display name moves to `displayName` and `name` is
//     normalized to "projects/{projectId}" on both sides (which also keeps
//     list-scoping, keyed on `name`, working across transports).
//   - `lifecycleState` (v1) → `state` (v3).
//   - `parent`: the v1 ResourceId `{type,id}` → the v3 "organizations/{id}" /
//     "folders/{id}" string.
//   - v1-only / v3-only fields with no counterpart in the other schema:
//     `projectNumber` (v1 has it; the v3 proto does not), `etag`, `updateTime`
//     and `deleteTime` (v3 has them; the v1 Project schema does not).
func resourceManagerProjection(raw json.RawMessage) (json.RawMessage, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	projectResourceManagerBody(v)
	return json.Marshal(v)
}

func projectResourceManagerBody(v any) {
	switch t := v.(type) {
	case map[string]any:
		if isResourceManagerProject(t) {
			canonicalizeResourceManagerProject(t)
			return
		}
		for _, e := range t {
			projectResourceManagerBody(e)
		}
	case []any:
		for _, e := range t {
			projectResourceManagerBody(e)
		}
	}
}

// isResourceManagerProject reports whether m is a project resource: it carries a
// projectId plus one of the two name forms. The operation envelope's own `name`
// ("operations/…") has no projectId, so it is never mistaken for a project.
func isResourceManagerProject(m map[string]any) bool {
	if _, ok := m["projectId"].(string); !ok {
		return false
	}
	if _, ok := m["name"].(string); ok {
		return true
	}
	_, ok := m["displayName"].(string)
	return ok
}

func canonicalizeResourceManagerProject(m map[string]any) {
	// v1: `name` is the display name and there is no `displayName`; move it
	// before overwriting `name` with the resource name.
	if _, hasDisplay := m["displayName"]; !hasDisplay {
		if n, ok := m["name"].(string); ok && n != "" {
			m["displayName"] = n
		}
	}
	if id, ok := m["projectId"].(string); ok && id != "" {
		// The emulator addresses a project by its id ("projects/{id}") on both
		// transports; normalizing `name` here (rather than trusting either
		// side's rendering) also keeps list-scoping, keyed on `name`, aligned.
		m["name"] = "projects/" + id
	}
	if ls, ok := m["lifecycleState"]; ok {
		if _, exists := m["state"]; !exists {
			m["state"] = ls
		}
		delete(m, "lifecycleState")
	}
	if p, ok := m["parent"].(map[string]any); ok {
		typ, _ := p["type"].(string)
		pid, _ := p["id"].(string)
		if typ != "" && pid != "" {
			if !strings.HasSuffix(typ, "s") {
				typ += "s"
			}
			m["parent"] = typ + "/" + pid
		}
	}
	// v1-only (projectNumber) and v3-only (etag/updateTime/deleteTime) fields
	// the other schema cannot represent.
	delete(m, "projectNumber")
	delete(m, "etag")
	delete(m, "updateTime")
	delete(m, "deleteTime")
}
