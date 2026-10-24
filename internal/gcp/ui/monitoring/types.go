package monitoringui

// AlertPolicyRequest is the UI body for creating or updating an alert policy. It
// mirrors the configurable fields of the Discovery AlertPolicy; the server fills
// the name/id.
type AlertPolicyRequest struct {
	DisplayName          string            `json:"displayName,omitempty"`
	Documentation        map[string]any    `json:"documentation,omitempty"`
	Conditions           []map[string]any  `json:"conditions,omitempty"`
	Combiner             string            `json:"combiner,omitempty"`
	Enabled              *bool             `json:"enabled,omitempty"`
	NotificationChannels []string          `json:"notificationChannels,omitempty"`
	UserLabels           map[string]string `json:"userLabels,omitempty"`
}

func (a AlertPolicyRequest) body() map[string]any {
	out := map[string]any{}
	if a.DisplayName != "" {
		out["displayName"] = a.DisplayName
	}
	if a.Documentation != nil {
		out["documentation"] = a.Documentation
	}
	if len(a.Conditions) > 0 {
		out["conditions"] = toAnySlice(a.Conditions)
	}
	if a.Combiner != "" {
		out["combiner"] = a.Combiner
	}
	if a.Enabled != nil {
		out["enabled"] = *a.Enabled
	}
	if len(a.NotificationChannels) > 0 {
		out["notificationChannels"] = toAnySlice(a.NotificationChannels)
	}
	if len(a.UserLabels) > 0 {
		out["userLabels"] = a.UserLabels
	}
	return out
}

// NotificationChannelRequest is the UI body for creating or updating a
// notification channel. Labels carry the type-specific configuration.
type NotificationChannelRequest struct {
	Type        string            `json:"type,omitempty"`
	DisplayName string            `json:"displayName,omitempty"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	UserLabels  map[string]string `json:"userLabels,omitempty"`
	Enabled     *bool             `json:"enabled,omitempty"`
}

func (c NotificationChannelRequest) body() map[string]any {
	out := map[string]any{}
	if c.Type != "" {
		out["type"] = c.Type
	}
	if c.DisplayName != "" {
		out["displayName"] = c.DisplayName
	}
	if c.Description != "" {
		out["description"] = c.Description
	}
	if len(c.Labels) > 0 {
		out["labels"] = c.Labels
	}
	if len(c.UserLabels) > 0 {
		out["userLabels"] = c.UserLabels
	}
	if c.Enabled != nil {
		out["enabled"] = *c.Enabled
	}
	return out
}

// toAnySlice widens a generic slice into the []any the provider body readers use.
func toAnySlice[T any](in []T) []any {
	out := make([]any, 0, len(in))
	for i := range in {
		out = append(out, in[i])
	}
	return out
}
