package monitoring

import (
	"sort"

	"jaiscloud/internal/gcp/rescatalog"
)

// MonitoredResourceDescriptors returns the canonical catalog of well-known
// monitored resource descriptors (rescatalog), scoped to project and sorted by
// type.
func MonitoredResourceDescriptors(project string) []MonitoredResourceDescriptor {
	entries := rescatalog.Catalog()
	out := make([]MonitoredResourceDescriptor, 0, len(entries))
	for _, e := range entries {
		out = append(out, monitoredResourceDescriptorFromCatalog(project, e))
	}
	return out
}

// LookupMonitoredResourceDescriptor returns the canonical descriptor for typ,
// or ok=false when the type is not in the catalog.
func LookupMonitoredResourceDescriptor(project, typ string) (MonitoredResourceDescriptor, bool) {
	e, ok := rescatalog.Lookup(typ)
	if !ok {
		return MonitoredResourceDescriptor{}, false
	}
	return monitoredResourceDescriptorFromCatalog(project, e), true
}

func monitoredResourceDescriptorFromCatalog(project string, e rescatalog.Descriptor) MonitoredResourceDescriptor {
	labels := e.Labels()
	out := MonitoredResourceDescriptor{
		Name:        MonitoredResourceDescriptorName(project, e.Type()),
		Type:        e.Type(),
		DisplayName: e.DisplayName(),
		Description: e.Description(),
		Labels:      make([]LabelDescriptor, 0, len(labels)),
	}
	for _, l := range labels {
		out.Labels = append(out.Labels, LabelDescriptor{Key: l.Key, ValueType: l.ValueType, Description: l.Description})
	}
	return out
}

// ─── notification channel descriptor catalog ─────────────────────────────────

// notificationChannelDescriptor is one entry in the static catalog of
// well-known notification channel types. Unlike monitored resource descriptors
// (shared with Logging via rescatalog), this catalog is Monitoring-only. Channel
// types are globally published by Cloud Monitoring and carry no per-project
// state, so the emulator returns the catalog without persisting it.
type notificationChannelDescriptor struct {
	typ         string
	displayName string
	description string
	labels      []LabelDescriptor
}

// notificationChannelCatalog is the fixed set of notification channel types the
// emulator advertises. It mirrors the shape (not the exhaustive list) of the
// descriptors Cloud Monitoring publishes.
var notificationChannelCatalog = []notificationChannelDescriptor{
	{
		typ:         "email",
		displayName: "Email",
		description: "A notification channel that sends email to one or more addresses.",
		labels:      []LabelDescriptor{{Key: "email_address", ValueType: "STRING", Description: "The email address to send notifications to."}},
	},
	{
		typ:         "sms",
		displayName: "SMS",
		description: "A notification channel that sends an SMS text message to one or more phone numbers.",
		labels:      []LabelDescriptor{{Key: "number", ValueType: "STRING", Description: "The phone number to send notifications to, in E.164 format."}},
	},
	{
		typ:         "pubsub",
		displayName: "Pub/Sub",
		description: "A notification channel that publishes notifications to a Google Cloud Pub/Sub topic.",
		labels:      []LabelDescriptor{{Key: "topic", ValueType: "STRING", Description: "The full resource name of the Pub/Sub topic, projects/{project}/topics/{topic}."}},
	},
	{
		typ:         "webhook_tokenauth",
		displayName: "Webhook (token auth)",
		description: "A notification channel that POSTs a JSON payload to an HTTPS endpoint, authenticated with a URL token.",
		labels:      []LabelDescriptor{{Key: "url", ValueType: "STRING", Description: "The HTTPS URL to POST notifications to."}},
	},
	{
		typ:         "slack",
		displayName: "Slack",
		description: "A notification channel that posts notifications to a Slack workspace via a webhook.",
		labels: []LabelDescriptor{
			{Key: "channel_name", ValueType: "STRING", Description: "The Slack channel to post to."},
			{Key: "auth_token", ValueType: "STRING", Description: "The Slack authentication token."},
		},
	},
}

// NotificationChannelDescriptors returns the static catalog of notification
// channel descriptors, scoped to project and sorted by type.
func NotificationChannelDescriptors(project string) []NotificationChannelDescriptor {
	out := make([]NotificationChannelDescriptor, 0, len(notificationChannelCatalog))
	for _, e := range notificationChannelCatalog {
		out = append(out, notificationChannelDescriptorFromCatalog(project, e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// LookupNotificationChannelDescriptor returns the descriptor for typ, or
// ok=false when the type is not in the catalog.
func LookupNotificationChannelDescriptor(project, typ string) (NotificationChannelDescriptor, bool) {
	for _, e := range notificationChannelCatalog {
		if e.typ == typ {
			return notificationChannelDescriptorFromCatalog(project, e), true
		}
	}
	return NotificationChannelDescriptor{}, false
}

func notificationChannelDescriptorFromCatalog(project string, e notificationChannelDescriptor) NotificationChannelDescriptor {
	labels := make([]LabelDescriptor, 0, len(e.labels))
	for _, l := range e.labels {
		labels = append(labels, LabelDescriptor{Key: l.Key, ValueType: l.ValueType, Description: l.Description})
	}
	return NotificationChannelDescriptor{
		Name:        NotificationChannelDescriptorName(project, e.typ),
		Type:        e.typ,
		DisplayName: e.displayName,
		Description: e.description,
		Labels:      labels,
	}
}
