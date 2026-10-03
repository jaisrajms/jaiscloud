package monitoring

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
)

// ProjectFromResourceName extracts the project id from "projects/{p}" or a
// longer "projects/{p}/..." name, returning "" when the name is not
// project-scoped.
func ProjectFromResourceName(name string) string {
	if name == "" {
		return ""
	}
	parts := strings.Split(name, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	return ""
}

// MetricDescriptorName is the Cloud Monitoring resource name for a metric type.
func MetricDescriptorName(project, typ string) string {
	return resource.ResourceID(project)("metric-descriptor", typ)
}

// SplitMetricDescriptorName parses "projects/{p}/metricDescriptors/{type}",
// where {type} may itself contain slashes (e.g.
// "custom.googleapis.com/invoice/paid/amount").
func SplitMetricDescriptorName(name string) (project, typ string, ok bool) {
	const prefix = "projects/"
	const marker = "metricDescriptors/"
	if !strings.HasPrefix(name, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(name, prefix)
	project, after, found := strings.Cut(rest, "/")
	if !found || !strings.HasPrefix(after, marker) {
		return "", "", false
	}
	typ = strings.TrimPrefix(after, marker)
	if typ == "" {
		return "", "", false
	}
	return project, typ, true
}

// AlertPolicyName is the Cloud Monitoring resource name for an alert policy id.
func AlertPolicyName(project, id string) string {
	return resource.ResourceID(project)("alert-policy", id)
}

// SplitAlertPolicyName parses "projects/{p}/alertPolicies/{id}".
func SplitAlertPolicyName(name string) (project, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "alertPolicies" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// MonitoredResourceDescriptorName is the Cloud Monitoring resource name for a
// canonical monitored resource type.
func MonitoredResourceDescriptorName(project, typ string) string {
	return resource.ResourceID(project)("monitored-resource-descriptor", typ)
}

// SplitMonitoredResourceDescriptorName parses
// "projects/{p}/monitoredResourceDescriptors/{type}". Monitored resource types
// contain no slashes, so the name has exactly four segments.
func SplitMonitoredResourceDescriptorName(name string) (project, typ string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "monitoredResourceDescriptors" {
		return "", "", false
	}
	if parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// ServiceName is the Cloud Monitoring resource name for a Service Monitoring
// service id: "projects/{p}/services/{id}".
func ServiceName(project, id string) string {
	return resource.ResourceID(project)("monitoring-service", id)
}

// SplitServiceName parses "projects/{p}/services/{id}". Service ids contain no
// slashes, so the name has exactly four segments.
func SplitServiceName(name string) (project, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "services" {
		return "", "", false
	}
	if parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// ServiceLevelObjectiveName is the Cloud Monitoring resource name for an SLO:
// "projects/{p}/services/{service}/serviceLevelObjectives/{id}".
func ServiceLevelObjectiveName(project, serviceID, id string) string {
	return resource.ResourceID(project)("monitoring-slo", serviceID+"/"+id)
}

// SplitServiceLevelObjectiveName parses
// "projects/{p}/services/{service}/serviceLevelObjectives/{id}".
func SplitServiceLevelObjectiveName(name string) (project, serviceID, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "services" || parts[4] != "serviceLevelObjectives" {
		return "", "", "", false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

// NotificationChannelName is the Cloud Monitoring resource name for a
// notification channel id.
func NotificationChannelName(project, id string) string {
	return resource.ResourceID(project)("notification-channel", id)
}

// SplitNotificationChannelName parses "projects/{p}/notificationChannels/{id}".
func SplitNotificationChannelName(name string) (project, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "notificationChannels" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// SplitNotificationChannelDescriptorName parses
// "projects/{p}/notificationChannelDescriptors/{type}". Channel types contain
// no slashes, so the name has exactly four segments.
func SplitNotificationChannelDescriptorName(name string) (project, typ string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "notificationChannelDescriptors" {
		return "", "", false
	}
	if parts[1] == "" || parts[3] == "" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// NotificationChannelDescriptorName is the Cloud Monitoring resource name for a
// notification channel type descriptor.
func NotificationChannelDescriptorName(project, typ string) string {
	return resource.ResourceID(project)("notification-channel-descriptor", typ)
}
