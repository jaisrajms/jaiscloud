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

// ServiceRequest is the UI body for creating or updating a Service Monitoring
// service. It mirrors the configurable fields of the Discovery Service,
// including the identifier oneof (exactly one may be set) and the separate
// basicService/telemetry fields; the server fills the name/id.
type ServiceRequest struct {
	DisplayName           string            `json:"displayName,omitempty"`
	UserLabels            map[string]string `json:"userLabels,omitempty"`
	Custom                map[string]any    `json:"custom,omitempty"`
	AppEngine             map[string]any    `json:"appEngine,omitempty"`
	CloudEndpoints        map[string]any    `json:"cloudEndpoints,omitempty"`
	ClusterIstio          map[string]any    `json:"clusterIstio,omitempty"`
	MeshIstio             map[string]any    `json:"meshIstio,omitempty"`
	IstioCanonicalService map[string]any    `json:"istioCanonicalService,omitempty"`
	CloudRun              map[string]any    `json:"cloudRun,omitempty"`
	GKENamespace          map[string]any    `json:"gkeNamespace,omitempty"`
	GKEWorkload           map[string]any    `json:"gkeWorkload,omitempty"`
	GKEService            map[string]any    `json:"gkeService,omitempty"`
	BasicService          map[string]any    `json:"basicService,omitempty"`
	Telemetry             map[string]any    `json:"telemetry,omitempty"`
}

func (s ServiceRequest) body() map[string]any {
	out := map[string]any{}
	if s.DisplayName != "" {
		out["displayName"] = s.DisplayName
	}
	if len(s.UserLabels) > 0 {
		out["userLabels"] = s.UserLabels
	}
	for key, value := range map[string]map[string]any{
		"custom":                s.Custom,
		"appEngine":             s.AppEngine,
		"cloudEndpoints":        s.CloudEndpoints,
		"clusterIstio":          s.ClusterIstio,
		"meshIstio":             s.MeshIstio,
		"istioCanonicalService": s.IstioCanonicalService,
		"cloudRun":              s.CloudRun,
		"gkeNamespace":          s.GKENamespace,
		"gkeWorkload":           s.GKEWorkload,
		"gkeService":            s.GKEService,
		"basicService":          s.BasicService,
		"telemetry":             s.Telemetry,
	} {
		if value != nil {
			out[key] = value
		}
	}
	return out
}

// ServiceLevelObjectiveRequest is the UI body for creating or updating an SLO.
// It mirrors the configurable fields of the Discovery
// ServiceLevelObjective; the server fills the name/id and service. The core is
// the validation authority: a goal outside (0, 0.9999], a missing SLI, or zero
// or two periods are rejected there and surface unchanged through WriteError.
type ServiceLevelObjectiveRequest struct {
	DisplayName           string            `json:"displayName,omitempty"`
	Goal                  float64           `json:"goal,omitempty"`
	ServiceLevelIndicator map[string]any    `json:"serviceLevelIndicator,omitempty"`
	RollingPeriod         string            `json:"rollingPeriod,omitempty"`
	CalendarPeriod        string            `json:"calendarPeriod,omitempty"`
	UserLabels            map[string]string `json:"userLabels,omitempty"`
}

func (s ServiceLevelObjectiveRequest) body() map[string]any {
	out := map[string]any{}
	if s.DisplayName != "" {
		out["displayName"] = s.DisplayName
	}
	if s.Goal != 0 {
		out["goal"] = s.Goal
	}
	if s.ServiceLevelIndicator != nil {
		out["serviceLevelIndicator"] = s.ServiceLevelIndicator
	}
	if s.RollingPeriod != "" {
		out["rollingPeriod"] = s.RollingPeriod
	}
	if s.CalendarPeriod != "" {
		out["calendarPeriod"] = s.CalendarPeriod
	}
	if len(s.UserLabels) > 0 {
		out["userLabels"] = s.UserLabels
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
