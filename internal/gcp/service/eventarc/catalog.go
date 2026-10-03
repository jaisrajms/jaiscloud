package eventarc

import (
	"context"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
)

// EventType is one catalogued provider event type.
type EventType struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

// Provider is one catalogued Eventarc provider.
type Provider struct {
	ID          string
	DisplayName string
	EventTypes  []EventType
}

// catalog is the fixed set of real, catalogued Eventarc providers surfaced by
// the emulator. Deliberately small: only real provider IDs and event types are
// used (no invented providers). Providers are read-only discovery.
var catalog = []Provider{
	{
		ID:          "pubsub.googleapis.com",
		DisplayName: "Cloud Pub/Sub",
		EventTypes: []EventType{
			{Type: "google.cloud.pubsub.topic.v1.messagePublished", Description: "A message is published to a Pub/Sub topic."},
		},
	},
	{
		ID:          "storage.googleapis.com",
		DisplayName: "Cloud Storage",
		EventTypes: []EventType{
			{Type: "google.cloud.storage.object.v1.finalized", Description: "An object is finalized (created or overwritten) in Cloud Storage."},
			{Type: "google.cloud.storage.object.v1.deleted", Description: "An object is deleted in Cloud Storage."},
		},
	},
}

// ListProviders returns a cursor page of the catalogued providers in a
// location.
func (s *Service) ListProviders(ctx context.Context, project, location string, pageSize int, pageToken string) ([]Provider, string, error) {
	_ = ctx
	if location == "" {
		return nil, "", invalidArgument("missing location")
	}
	page, next := paging.Page(catalog, func(d Provider) string { return d.ID }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// GetProvider returns one catalogued provider.
func (s *Service) GetProvider(ctx context.Context, project, location, providerID string) (Provider, error) {
	_ = ctx
	if location == "" || providerID == "" {
		return Provider{}, invalidArgument("missing location or provider id")
	}
	for _, d := range catalog {
		if d.ID == providerID {
			return d, nil
		}
	}
	return Provider{}, model.NewProviderError("NotFound", "provider not found: "+providerID, 404)
}

// Catalog returns the static provider catalog (for the transports' tests).
func Catalog() []Provider { return catalog }
