package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// Cloud Monitoring dashboards (google.monitoring.dashboard.v1.Dashboard) are a
// separate REST-only v1 API from the v3 MetricService/ServiceMonitoringService
// surface: the official v1 Discovery document
// (https://monitoring.googleapis.com/$discovery/rest?version=v1, revision
// 20261022) declares projects.dashboards.{list,get,create,patch,delete} and no
// gRPC service. The dashboard document itself (layout/widgets/filters/labels)
// is opaque to the emulator: it is stored and returned verbatim, and only the
// server-managed fields (name, etag) and the required displayName are
// interpreted. Dashboards live in the shared ResourceStore so they survive the
// same snapshot/reset lifecycle as every other control-plane resource.
const dashboardResourceType = "gcp_monitoring_dashboard"

// Dashboard is one stored Cloud Monitoring dashboard. Document is the whole
// dashboard JSON as the client sent it, with the server-managed name and etag
// set by the emulator. Name/DisplayName/Etag are decoded for validation and
// comparison; the transports emit Document.
type Dashboard struct {
	ID          string
	Name        string
	DisplayName string
	Etag        string
	Document    json.RawMessage
}

// ListDashboards returns one page of the project's dashboards plus the
// next-page token. Dashboards are listed ordered by id (the shared paging
// cursor convention); pageSize follows the Discovery document's int32 binding:
// zero/absent takes the 1000 default and a negative value is INVALID_ARGUMENT.
func (s *Service) ListDashboards(ctx context.Context, project string, pageSize int, pageToken string) ([]Dashboard, string, error) {
	if err := s.requireResourceStore(); err != nil {
		return nil, "", err
	}
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, dashboardResourceType, "")
	if err != nil {
		return nil, "", err
	}
	page, next, err := paging.Apply(entries, pageParams(pageSize, pageToken))
	if err != nil {
		return nil, "", err
	}
	out := make([]Dashboard, 0, len(page))
	for _, e := range page {
		d, err := dashboardFromEntry(e)
		if err != nil {
			return nil, "", err
		}
		out = append(out, d)
	}
	return out, next, nil
}

// GetDashboard returns one dashboard by id. A missing dashboard is NOT_FOUND.
func (s *Service) GetDashboard(ctx context.Context, project, id string) (Dashboard, error) {
	if err := s.requireResourceStore(); err != nil {
		return Dashboard{}, err
	}
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, dashboardResourceType, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return Dashboard{}, dashboardNotFound(id)
		}
		return Dashboard{}, err
	}
	return dashboardFromEntry(e)
}

// CreateDashboard stores a new dashboard. Per the official "Create a dashboard"
// doc (https://cloud.google.com/monitoring/dashboards/api-dashboard) the body's
// name is optional: an empty name gets a server-generated id, while a supplied
// name must be "projects/{parent-project}/dashboards/{id}" for the request's
// parent project (INVALID_ARGUMENT otherwise). displayName is required and
// non-empty. The etag is server-managed: a client-supplied one (which the
// Discovery document says "should not be passed during dashboard creation") is
// replaced with a freshly minted etag.
func (s *Service) CreateDashboard(ctx context.Context, project string, body json.RawMessage, validateOnly bool) (Dashboard, error) {
	if err := s.requireResourceStore(); err != nil {
		return Dashboard{}, err
	}
	fields, err := decodeDashboardFields(body)
	if err != nil {
		return Dashboard{}, err
	}
	if _, err := dashboardDisplayName(fields); err != nil {
		return Dashboard{}, err
	}
	id := uuid.NewString()
	if name, present, err := dashboardRequestName(fields); err != nil {
		return Dashboard{}, err
	} else if present && name != "" {
		nameProject, nameID, ok := SplitDashboardName(name)
		if !ok || nameProject != project || nameID == "" {
			return Dashboard{}, invalidArgument("dashboard name must be projects/" + project + "/dashboards/{dashboard_id}")
		}
		id = nameID
	}
	doc, err := buildDashboard(project, id, fields, newDashboardEtag())
	if err != nil {
		return Dashboard{}, err
	}
	if validateOnly {
		return doc, nil
	}
	err = s.resources.Create(ctx, project, store.GlobalRegion, store.ResourceEntry{
		Type: dashboardResourceType, ID: id, Data: doc.Document,
	})
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return Dashboard{}, model.NewProviderError("AlreadyExists", "dashboard already exists", 409)
		}
		return Dashboard{}, err
	}
	return doc, nil
}

// UpdateDashboard replaces a dashboard's whole document (dashboards.patch has
// no updateMask: "Replaces an existing custom dashboard with a new definition").
// The body's name is the path-bound resource name: an absent/empty name is
// filled from the path (the official update example supplies only "the revised
// Dashboard object and the etag value", and the Terraform provider's
// google_monitoring_dashboard update sends the dashboard JSON with only its old
// etag), while a non-empty name that disagrees with the path is
// INVALID_ARGUMENT. Optimistic concurrency follows the Discovery document's
// etag contract ("An etag is returned in the response to GetDashboard, and
// users are expected to put that etag in the request to UpdateDashboard"): a
// request carrying a non-empty etag must match the stored one (else 409
// ABORTED), every persisted update mints a fresh etag, and get/list return the
// current stored etag. A missing dashboard is NOT_FOUND.
func (s *Service) UpdateDashboard(ctx context.Context, project, id string, body json.RawMessage, validateOnly bool) (Dashboard, error) {
	if err := s.requireResourceStore(); err != nil {
		return Dashboard{}, err
	}
	fields, err := decodeDashboardFields(body)
	if err != nil {
		return Dashboard{}, err
	}
	pathName := DashboardName(project, id)
	name, _, err := dashboardRequestName(fields)
	if err != nil {
		return Dashboard{}, err
	}
	if name != "" && name != pathName {
		return Dashboard{}, invalidArgument("dashboard name must match the request path name " + pathName)
	}
	if _, err := dashboardDisplayName(fields); err != nil {
		return Dashboard{}, err
	}
	reqEtag, err := dashboardRequestEtag(fields)
	if err != nil {
		return Dashboard{}, err
	}
	if validateOnly {
		// Preview only: read the stored document for the etag precondition and
		// return the projected replacement without persisting anything.
		stored, err := s.GetDashboard(ctx, project, id)
		if err != nil {
			return Dashboard{}, err
		}
		if err := checkDashboardEtag(reqEtag, stored.Etag); err != nil {
			return Dashboard{}, err
		}
		return buildDashboard(project, id, fields, stored.Etag)
	}
	var result Dashboard
	_, err = s.resources.UpsertAtomic(ctx, project, store.GlobalRegion, dashboardResourceType, id,
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			if !exists {
				return current, dashboardNotFound(id)
			}
			stored, err := dashboardFromEntry(current)
			if err != nil {
				return current, err
			}
			if err := checkDashboardEtag(reqEtag, stored.Etag); err != nil {
				return current, err
			}
			doc, err := buildDashboard(project, id, fields, newDashboardEtag())
			if err != nil {
				return current, err
			}
			result = doc
			return store.ResourceEntry{Type: dashboardResourceType, ID: id, Data: doc.Document}, nil
		})
	if err != nil {
		return Dashboard{}, err
	}
	return result, nil
}

// DeleteDashboard deletes one dashboard. A missing dashboard is NOT_FOUND.
func (s *Service) DeleteDashboard(ctx context.Context, project, id string) error {
	if err := s.requireResourceStore(); err != nil {
		return err
	}
	err := s.resources.Delete(ctx, project, store.GlobalRegion, dashboardResourceType, id)
	if errors.Is(err, store.ErrNotFound) {
		return dashboardNotFound(id)
	}
	return err
}

// requireResourceStore fails loud rather than fabricating success when the core
// was built without the shared ResourceStore. The production wiring always
// supplies it (cmd/jaiscloud-gcp, WithResources).
func (s *Service) requireResourceStore() error {
	if s.resources == nil {
		return model.NewProviderError("Unimplemented", "dashboards require the shared resource store", 501)
	}
	return nil
}

// pageParams builds the shared cursor-pagination parameter map used by the
// dashboards list. A non-zero pageSize is forwarded as-is — including a
// negative value, which the shared pager rejects with INVALID_ARGUMENT
// (AIP-158); zero means "unset" and takes the Discovery default.
func pageParams(pageSize int, pageToken string) map[string]any {
	params := map[string]any{}
	if pageSize != 0 {
		params["pageSize"] = pageSize
	}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	return params
}

// decodeDashboardFields decodes a dashboard body into its top-level fields while
// preserving every value as raw JSON, so the layout/widgets/filters are stored
// verbatim and only name/displayName/etag are interpreted.
func decodeDashboardFields(body json.RawMessage) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, invalidArgument("dashboard body is required")
	}
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(trimmed, &fields); err != nil || fields == nil {
		return nil, invalidArgument("dashboard body must be a JSON object")
	}
	return fields, nil
}

// dashboardRequestName returns the body's name field, reporting whether it was
// present. A non-string name is INVALID_ARGUMENT.
func dashboardRequestName(fields map[string]json.RawMessage) (string, bool, error) {
	raw, ok := fields["name"]
	if !ok {
		return "", false, nil
	}
	var name string
	if err := json.Unmarshal(raw, &name); err != nil {
		return "", true, invalidArgument("dashboard name must be a string")
	}
	return name, true, nil
}

// dashboardDisplayName validates the required displayName field: the Discovery
// schema marks Dashboard.displayName "Required", so a missing, non-string, or
// empty value is INVALID_ARGUMENT.
func dashboardDisplayName(fields map[string]json.RawMessage) (string, error) {
	raw, ok := fields["displayName"]
	if !ok {
		return "", invalidArgument("dashboard display_name is required")
	}
	var name string
	if err := json.Unmarshal(raw, &name); err != nil {
		return "", invalidArgument("dashboard display_name must be a string")
	}
	if name == "" {
		return "", invalidArgument("dashboard display_name is required")
	}
	return name, nil
}

// dashboardRequestEtag returns the request's etag precondition ("" when absent).
func dashboardRequestEtag(fields map[string]json.RawMessage) (string, error) {
	raw, ok := fields["etag"]
	if !ok {
		return "", nil
	}
	var etag string
	if err := json.Unmarshal(raw, &etag); err != nil {
		return "", invalidArgument("dashboard etag must be a string")
	}
	return etag, nil
}

// checkDashboardEtag enforces the Discovery-documented optimistic concurrency
// contract: an absent/empty request etag is not a precondition, a matching one
// passes, and a stale one is 409 ABORTED.
func checkDashboardEtag(reqEtag, storedEtag string) error {
	if reqEtag == "" || reqEtag == storedEtag {
		return nil
	}
	return model.NewProviderError("Aborted", "dashboard etag is stale; read the dashboard again and retry", 409)
}

// buildDashboard assembles the stored document: the client's body verbatim, with
// the server-managed name and etag fields set.
func buildDashboard(project, id string, fields map[string]json.RawMessage, etag string) (Dashboard, error) {
	name := DashboardName(project, id)
	nameRaw, err := json.Marshal(name)
	if err != nil {
		return Dashboard{}, err
	}
	etagRaw, err := json.Marshal(etag)
	if err != nil {
		return Dashboard{}, err
	}
	// Copy so the caller's map is not mutated.
	doc := make(map[string]json.RawMessage, len(fields)+2)
	for k, v := range fields {
		doc[k] = v
	}
	doc["name"] = nameRaw
	doc["etag"] = etagRaw
	data, err := json.Marshal(doc)
	if err != nil {
		return Dashboard{}, model.NewProviderError("Internal", err.Error(), 500)
	}
	displayName, err := dashboardDisplayName(doc)
	if err != nil {
		return Dashboard{}, err
	}
	return Dashboard{ID: id, Name: name, DisplayName: displayName, Etag: etag, Document: data}, nil
}

// dashboardFromEntry decodes a stored dashboard entry.
func dashboardFromEntry(e store.ResourceEntry) (Dashboard, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &fields); err != nil || fields == nil {
		return Dashboard{}, model.NewProviderError("Internal", "stored dashboard is not a JSON object", 500)
	}
	name, _, err := dashboardRequestName(fields)
	if err != nil {
		return Dashboard{}, model.NewProviderError("Internal", "stored dashboard has an invalid name", 500)
	}
	displayName, err := dashboardDisplayName(fields)
	if err != nil {
		return Dashboard{}, model.NewProviderError("Internal", "stored dashboard has an invalid display_name", 500)
	}
	etag, err := dashboardRequestEtag(fields)
	if err != nil {
		return Dashboard{}, model.NewProviderError("Internal", "stored dashboard has an invalid etag", 500)
	}
	if name == "" {
		name = DashboardName(e.Account, e.ID)
	}
	return Dashboard{ID: e.ID, Name: name, DisplayName: displayName, Etag: etag, Document: e.Data}, nil
}

// newDashboardEtag mints a fresh opaque etag for a created or updated dashboard.
// The value is opaque to clients (the Discovery document only requires that it
// round-trips through get/patch).
func newDashboardEtag() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}

// dashboardNotFound is the canonical NOT_FOUND error for a dashboard.
func dashboardNotFound(id string) error {
	return notFound("dashboard not found: " + id)
}
