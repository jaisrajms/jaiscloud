// Package eventarcui serves the Eventarc UI API. Handlers call the
// transport-neutral Eventarc core directly (in-process) rather than over the
// wire.
//
// The console is location-optional: the trigger and channel lists aggregate
// every resource in the project across all locations (no location picker) and
// show the location as a read-only field. Detail/actions link back with the
// location carried from the list row, so a resource id never has to be
// disambiguated by hand.
package eventarcui

import "encoding/json"

// EventFilter is one Eventarc event filter (attribute/operator/value).
type EventFilter struct {
	Attribute string `json:"attribute"`
	Operator  string `json:"operator,omitempty"`
	Value     string `json:"value"`
}

// Trigger is the console rendering of an Eventarc trigger, flattened across
// locations for the list page. Config carries the verbatim stored trigger body
// so the edit form's JSON escape hatch can round-trip fields the structured
// form does not model (e.g. a gke/httpEndpoint destination).
type Trigger struct {
	Name                        string            `json:"name"`
	Location                    string            `json:"location"`
	UID                         string            `json:"uid,omitempty"`
	Etag                        string            `json:"etag,omitempty"`
	CreateTime                  string            `json:"createTime,omitempty"`
	UpdateTime                  string            `json:"updateTime,omitempty"`
	Labels                      map[string]string `json:"labels,omitempty"`
	DestinationType             string            `json:"destinationType,omitempty"`
	Destination                 string            `json:"destination,omitempty"`
	DestinationRegion           string            `json:"destinationRegion,omitempty"`
	EventFilters                []EventFilter     `json:"eventFilters,omitempty"`
	ServiceAccount              string            `json:"serviceAccount,omitempty"`
	Channel                     string            `json:"channel,omitempty"`
	EventDataContentType        string            `json:"eventDataContentType,omitempty"`
	TransportPubsubTopic        string            `json:"transportPubsubTopic,omitempty"`
	TransportPubsubSubscription string            `json:"transportPubsubSubscription,omitempty"`
	Config                      json.RawMessage   `json:"config,omitempty"`
}

// ListTriggersResponse is the response for GET /triggers.
type ListTriggersResponse struct {
	Triggers []Trigger `json:"triggers"`
	Total    int       `json:"total"`
}

// TriggerInput is the create/update trigger form body. Location is required on
// create and ignored on update (the path carries it).
//
// Config is the JSON escape hatch: when non-empty it is used verbatim as the
// trigger body and the structured fields above are ignored. The structured
// fields cover the common destinations (cloudFunction/workflow/cloudRun) and
// filters; a gke/httpEndpoint destination is expressed through Config.
type TriggerInput struct {
	Name                 string            `json:"name"`
	Location             string            `json:"location"`
	DestinationType      string            `json:"destinationType"`
	Destination          string            `json:"destination,omitempty"`
	DestinationRegion    string            `json:"destinationRegion,omitempty"`
	ServiceAccount       string            `json:"serviceAccount,omitempty"`
	Channel              string            `json:"channel,omitempty"`
	EventDataContentType string            `json:"eventDataContentType,omitempty"`
	EventFilters         []EventFilter     `json:"eventFilters,omitempty"`
	TransportPubsubTopic string            `json:"transportPubsubTopic,omitempty"`
	Labels               map[string]string `json:"labels,omitempty"`
	Config               json.RawMessage   `json:"config,omitempty"`
}

// Channel is the console rendering of an Eventarc channel, flattened across
// locations for the list page.
type Channel struct {
	Name            string            `json:"name"`
	Location        string            `json:"location"`
	UID             string            `json:"uid,omitempty"`
	Etag            string            `json:"etag,omitempty"`
	ActivationToken string            `json:"activationToken,omitempty"`
	PubsubTopic     string            `json:"pubsubTopic,omitempty"`
	State           string            `json:"state,omitempty"`
	CreateTime      string            `json:"createTime,omitempty"`
	UpdateTime      string            `json:"updateTime,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Provider        string            `json:"provider,omitempty"`
	CryptoKeyName   string            `json:"cryptoKeyName,omitempty"`
	Config          json.RawMessage   `json:"config,omitempty"`
}

// ListChannelsResponse is the response for GET /channels.
type ListChannelsResponse struct {
	Channels []Channel `json:"channels"`
	Total    int       `json:"total"`
}

// ChannelInput is the create/update channel form body. Config is the JSON
// escape hatch, used verbatim when non-empty.
type ChannelInput struct {
	Name          string            `json:"name"`
	Location      string            `json:"location"`
	Provider      string            `json:"provider,omitempty"`
	CryptoKeyName string            `json:"cryptoKeyName,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	Config        json.RawMessage   `json:"config,omitempty"`
}
