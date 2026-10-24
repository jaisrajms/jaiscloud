package loggingui

// MetricRequest is the UI body for creating or updating a logs-based metric. It
// mirrors the configurable fields of the Discovery LogMetric; output-only fields
// (resourceName, metricDescriptor.name/type/description, timestamps) are ignored.
type MetricRequest struct {
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	Filter           string            `json:"filter,omitempty"`
	Disabled         bool              `json:"disabled,omitempty"`
	ValueExtractor   string            `json:"valueExtractor,omitempty"`
	LabelExtractors  map[string]string `json:"labelExtractors,omitempty"`
	MetricDescriptor map[string]any    `json:"metricDescriptor,omitempty"`
	BucketOptions    map[string]any    `json:"bucketOptions,omitempty"`
}

// body renders the request as the map[string]any the provider's body reader
// expects.
func (m MetricRequest) body() map[string]any {
	out := map[string]any{
		"name":        m.Name,
		"description": m.Description,
		"filter":      m.Filter,
		"disabled":    m.Disabled,
	}
	if m.ValueExtractor != "" {
		out["valueExtractor"] = m.ValueExtractor
	}
	if len(m.LabelExtractors) > 0 {
		out["labelExtractors"] = m.LabelExtractors
	}
	if m.MetricDescriptor != nil {
		out["metricDescriptor"] = m.MetricDescriptor
	}
	if m.BucketOptions != nil {
		out["bucketOptions"] = m.BucketOptions
	}
	return out
}

// SinkRequest is the UI body for creating or updating a log sink. Exclusions are
// intentionally omitted: the UI manages resource-level exclusions separately.
type SinkRequest struct {
	Name            string `json:"name,omitempty"`
	Destination     string `json:"destination,omitempty"`
	Filter          string `json:"filter,omitempty"`
	Description     string `json:"description,omitempty"`
	Disabled        bool   `json:"disabled,omitempty"`
	IncludeChildren bool   `json:"includeChildren,omitempty"`
}

func (s SinkRequest) body() map[string]any {
	out := map[string]any{
		"name":        s.Name,
		"destination": s.Destination,
		"filter":      s.Filter,
		"description": s.Description,
		"disabled":    s.Disabled,
	}
	if s.IncludeChildren {
		out["includeChildren"] = true
	}
	return out
}

// ExclusionRequest is the UI body for creating or updating a resource exclusion.
type ExclusionRequest struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Filter      string `json:"filter,omitempty"`
	Disabled    bool   `json:"disabled,omitempty"`
}

func (e ExclusionRequest) body() map[string]any {
	return map[string]any{
		"name":        e.Name,
		"description": e.Description,
		"filter":      e.Filter,
		"disabled":    e.Disabled,
	}
}
