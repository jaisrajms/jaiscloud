package functions

import "jaiscloud/internal/gcp/paging"

// functionRegions is the synthesized set of locations the emulator advertises
// for functions.location discovery. Real Cloud Functions is available in more
// regions; this is a stable subset. Region discovery is a shared, project-wide
// surface: the bare /v1/projects/{p}/locations[/{l}] paths are owned by the
// Memorystore detector and return the same google.cloud.location.Location
// records, so a Cloud Functions SDK locations.list/get resolves through that
// handler. These records back the Function.ListLocations/GetLocation handlers
// for direct dispatch.
var functionRegions = []string{
	"asia-east1",
	"asia-east2",
	"asia-northeast1",
	"asia-northeast2",
	"asia-northeast3",
	"asia-south1",
	"asia-southeast1",
	"asia-southeast2",
	"australia-southeast1",
	"europe-central2",
	"europe-north1",
	"europe-west1",
	"europe-west2",
	"europe-west3",
	"europe-west6",
	"northamerica-northeast1",
	"southamerica-east1",
	"us-central1",
	"us-east1",
	"us-east4",
	"us-west1",
	"us-west2",
	"us-west3",
	"us-west4",
}

// LocationJSON renders a google.cloud.location.Location for a region.
func LocationJSON(project, location string) map[string]any {
	return map[string]any{
		"name":        resourceID(project)("cloud-function-location", location),
		"locationId":  location,
		"displayName": location,
	}
}

// ListLocations returns the synthesized function region set, honoring
// pageSize/pageToken via the shared paging helper.
func (s *Service) ListLocations(project string, pageSize int, pageToken string) ([]map[string]any, string) {
	locations := make([]map[string]any, 0, len(functionRegions))
	for _, r := range functionRegions {
		locations = append(locations, LocationJSON(project, r))
	}
	page, next := paging.Page(locations, func(m map[string]any) string { return m["locationId"].(string) }, pageParams(pageSize, pageToken))
	return page, next
}

// GetLocation returns a synthesized google.cloud.location.Location for a region.
func (s *Service) GetLocation(project, location string) (map[string]any, error) {
	if location == "" {
		return nil, invalidArgument("missing location")
	}
	return LocationJSON(project, location), nil
}
