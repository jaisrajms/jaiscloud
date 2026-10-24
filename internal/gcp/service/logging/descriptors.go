package logging

import (
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/rescatalog"
)

// ListMonitoredResourceDescriptors returns a page of the canonical monitored
// resource descriptor catalog shared with Cloud Monitoring
// (internal/gcp/rescatalog). The descriptors carry type/displayName/
// description/labels; unlike Monitoring, the Logging surface does not set the
// descriptor resource name (real Cloud Logging leaves `name` unset), so the
// neutral type has no Name field.
//
// pageSize/pageToken are honored via the shared cursor pager. The Logging
// descriptors surface carries no `parent` or `filter`, so the catalog is global
// and unfiltered.
func (s *Service) ListMonitoredResourceDescriptors(pageSize int, pageToken string) ([]MonitoredResourceDescriptor, string, error) {
	entries := rescatalog.Catalog()
	all := make([]MonitoredResourceDescriptor, 0, len(entries))
	for _, e := range entries {
		all = append(all, descriptorFromCatalog(e))
	}
	// monitoredResourceDescriptors.list's Discovery document says non-positive
	// page sizes are ignored (a documented exception to AIP-158), so a negative
	// size takes the default rather than raising InvalidArgument.
	if pageSize < 0 {
		pageSize = 0
	}
	page, next, err := paging.Page(all, func(d MonitoredResourceDescriptor) string { return d.Type },
		map[string]any{"pageSize": pageSize, "pageToken": pageToken})
	if err != nil {
		return nil, "", err
	}
	return page, next, nil
}

// descriptorFromCatalog converts a canonical catalog descriptor to the
// transport-neutral form (no protobuf types).
func descriptorFromCatalog(d rescatalog.Descriptor) MonitoredResourceDescriptor {
	labels := d.Labels()
	out := MonitoredResourceDescriptor{
		Type:        d.Type(),
		DisplayName: d.DisplayName(),
		Description: d.Description(),
		Labels:      make([]MonitoredResourceLabel, 0, len(labels)),
	}
	for _, l := range labels {
		out.Labels = append(out.Labels, MonitoredResourceLabel{
			Key:         l.Key,
			ValueType:   l.ValueType,
			Description: l.Description,
		})
	}
	return out
}
