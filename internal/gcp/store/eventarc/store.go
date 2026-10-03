// Package eventarc provides the Eventarc store. Eventarc is trigger-based (not
// the AWS EventBridge "bus + rule + pattern + target" fan-out model): a Trigger
// routes events from a source (a Pub/Sub topic or a Channel) to a destination
// (Cloud Run service, Cloud Functions v2, Workflows, GKE) with optional
// eventFilters, and a Channel is the 3rd-party-source registration record.
//
// This is metadata-only CRUD — the emulator never stands up an event-delivery
// engine. A Trigger/Channel is a stored metadata record with the canonical name
// projects/{project}/locations/{location}/triggers/{trigger} (or .../channels/
// {channel}). The verbatim request body is stored as JSON so read-back echoes
// what the caller sent; output-only fields (name/uid/etag/times, and a
// channel's activation token) are overlaid by the provider on read.
package eventarc

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNoSuchTrigger = errors.New("NoSuchTrigger")
	ErrNoSuchChannel = errors.New("NoSuchChannel")
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// Trigger is a stored Eventarc trigger. Config holds the wire request body
// verbatim (destination, transport, eventFilters, serviceAccount, channel,
// eventDataContentType, ...) so read-back echoes what the caller sent; UID and
// Etag are server-assigned output-only values generated at create time and kept
// stable for the resource's lifetime.
type Trigger struct {
	ProjectID  string            `json:"projectId"`
	Location   string            `json:"location"`
	Name       string            `json:"name"` // trigger id (last segment of name)
	Config     json.RawMessage   `json:"config,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	UID        string            `json:"uid,omitempty"`
	Etag       string            `json:"etag,omitempty"`
	CreateTime time.Time         `json:"createTime"`
	UpdateTime time.Time         `json:"updateTime"`
}

// Channel is a stored Eventarc channel. Config holds the wire request body
// verbatim (provider, cryptoKeyName, ...). UID, Etag, and ActivationToken are
// server-assigned output-only values; UID/ActivationToken are generated at
// create time and kept stable, while Etag is a deterministic content checksum
// recomputed on every mutation for optimistic concurrency control. pubsubTopic
// and state are synthesized on read.
type Channel struct {
	ProjectID       string            `json:"projectId"`
	Location        string            `json:"location"`
	Name            string            `json:"name"` // channel id (last segment of name)
	Config          json.RawMessage   `json:"config,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	UID             string            `json:"uid,omitempty"`
	Etag            string            `json:"etag,omitempty"`
	ActivationToken string            `json:"activationToken,omitempty"`
	CreateTime      time.Time         `json:"createTime"`
	UpdateTime      time.Time         `json:"updateTime"`
}

// Store is the Eventarc store.
type Store interface {
	CreateTrigger(ctx context.Context, projectID, location string, t Trigger) error
	GetTrigger(ctx context.Context, projectID, location, id string) (Trigger, error)
	// UpdateTriggerAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current trigger and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetTrigger followed by
	// a standalone write, this is atomic with respect to concurrent updates on
	// the same trigger, so two concurrent PATCH requests merging different
	// fields can't lose one or the other.
	UpdateTriggerAtomic(ctx context.Context, projectID, location, id string, mutate func(Trigger) (Trigger, error)) (Trigger, error)
	DeleteTrigger(ctx context.Context, projectID, location, id string) error
	// DeleteTriggerAtomic performs a locked get-check-delete cycle: guard
	// receives the current trigger and returns an error to abort without
	// deleting. The read and the delete happen under one lock, so an etag
	// precondition checked in guard can't be invalidated by a concurrent
	// update landing between the check and the delete.
	DeleteTriggerAtomic(ctx context.Context, projectID, location, id string, guard func(Trigger) error) error
	ListTriggers(ctx context.Context, projectID, location string) ([]Trigger, error)
	// ListTriggersAllLocations returns every trigger for a project across all
	// locations, for event delivery: a Pub/Sub event carries no Eventarc
	// location, so every trigger subscribing to its source topic must be
	// considered.
	ListTriggersAllLocations(ctx context.Context, projectID string) ([]Trigger, error)

	CreateChannel(ctx context.Context, projectID, location string, c Channel) error
	GetChannel(ctx context.Context, projectID, location, id string) (Channel, error)
	// UpdateChannelAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current channel and returns the version to persist, or an
	// error to abort without writing. See UpdateTriggerAtomic.
	UpdateChannelAtomic(ctx context.Context, projectID, location, id string, mutate func(Channel) (Channel, error)) (Channel, error)
	DeleteChannel(ctx context.Context, projectID, location, id string) error
	// DeleteChannelAtomic performs a locked get-check-delete cycle: guard
	// receives the current channel and returns an error to abort without
	// deleting. See DeleteTriggerAtomic.
	DeleteChannelAtomic(ctx context.Context, projectID, location, id string, guard func(Channel) error) error
	ListChannels(ctx context.Context, projectID, location string) ([]Channel, error)

	Reset(ctx context.Context)
}
