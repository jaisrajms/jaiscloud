// Package computeui serves the Compute Engine UI API. Handlers call the Compute
// Engine provider directly (in-process) rather than over the wire.
//
// The console browses the instances the provider already stores: the list is
// the provider's aggregated (cross-zone) view flattened into one row per
// instance, and the detail page reads a single zonal instance. Start/stop
// return the provider's compute#operation envelope.
package computeui

// Instance is the UI summary of a Compute Engine instance, flattened across
// zones for the list page.
type Instance struct {
	Name              string            `json:"name"`
	Zone              string            `json:"zone"`
	Status            string            `json:"status"`
	MachineType       string            `json:"machineType"`
	CPUPlatform       string            `json:"cpuPlatform,omitempty"`
	InternalIP        string            `json:"internalIp,omitempty"`
	ExternalIP        string            `json:"externalIp,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
}

// ListInstancesResponse is the response for GET /instances.
type ListInstancesResponse struct {
	Instances []Instance `json:"instances"`
	Total     int        `json:"total"`
}

// InstanceDetail is the full detail for GET /instances/{zone}/{instance}. The
// composition fields the console edits or displays verbatim (metadata, disks,
// network interfaces) are passed through as opaque provider JSON.
type InstanceDetail struct {
	Instance
	ID                string         `json:"id,omitempty"`
	SelfLink          string         `json:"selfLink,omitempty"`
	Description       string         `json:"description,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
	Disks             []any          `json:"disks,omitempty"`
	NetworkInterfaces []any          `json:"networkInterfaces,omitempty"`
}
