package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"jaiscloud/internal/clock"
	pubsubstore "jaiscloud/internal/gcp/store/pubsub"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// SnapshotMeta is the persisted form of a Snapshot resource. Backlog captures
// the source subscription's unacked messages at creation so a later Seek to the
// snapshot can faithfully restore the ack state (including messages acked
// afterwards) — without it, snapshot-based Seek would silently diverge from
// real Pub/Sub, which redelivers the snapshot backlog.
//
// The JSON field set is part of the persisted format: both transports store and
// decode this type, so the tags must not change without a migration.
type SnapshotMeta struct {
	Name         string                `json:"name"`
	Topic        string                `json:"topic"`
	Subscription string                `json:"subscription"`
	CreatedAt    time.Time             `json:"createdAt"`
	ExpireTime   time.Time             `json:"expireTime"`
	Labels       map[string]string     `json:"labels,omitempty"`
	Backlog      []pubsubstore.Message `json:"backlog,omitempty"`
}

// GetSnapshotMeta loads and decodes a snapshot resource.
func (s *Service) GetSnapshotMeta(ctx context.Context, project, snap string) (SnapshotMeta, error) {
	var meta SnapshotMeta
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSnapshot, snap)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return meta, notFound("snapshot not found")
		}
		return meta, err
	}
	if err := json.Unmarshal(e.Data, &meta); err != nil {
		return meta, internal("corrupt snapshot metadata")
	}
	return meta, nil
}

// Seek repositions a subscription to a point in time, or to the state captured
// by a snapshot. Exactly one of at/snapshot must be set.
//
// It validates the subscription first (NotFound), then dispatches:
//   - at != nil: mark every retained message published before at as acknowledged
//     (deleted) and make the rest visible again.
//   - snapshot != "": restore the snapshot backlog; the snapshot must exist and
//     belong to the subscription's topic (FailedPrecondition otherwise).
func (s *Service) Seek(ctx context.Context, project, sub string, at *time.Time, snapshot string) error {
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtSubscription, sub)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return notFound("subscription not found")
		}
		return err
	}
	var meta map[string]any
	json.Unmarshal(e.Data, &meta)

	switch {
	case at != nil:
		return s.seekToTime(ctx, sub, *at)
	case snapshot != "":
		snapProject, snapID, ok := SplitSnapshotName(snapshot)
		if !ok || snapProject != project {
			return invalidArgument("invalid snapshot name")
		}
		snap, err := s.GetSnapshotMeta(ctx, project, snapID)
		if err != nil {
			return err
		}
		subTopic, _ := meta["topic"].(string)
		if snap.Topic != subTopic {
			return &model.ProviderError{
				Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
				Message: "snapshot topic " + snap.Topic + " does not match subscription topic " + subTopic,
			}
		}
		return s.restoreSnapshot(ctx, sub, snap)
	default:
		return invalidArgument("seek target (time or snapshot) is required")
	}
}

// seekToTime marks the subscription's retained messages published before t as
// acknowledged (deleted) and makes those published at/after t visible again.
// Already-acked messages are gone, so they are not restored (proto-documented).
// Making a message visible again invalidates outstanding ack IDs, so every
// retained message's delivery version is advanced (EOD2). The versions are
// advanced *before* the messages are made visible, so a concurrent Pull cannot
// claim a message and be handed an ID that the bump immediately invalidates.
func (s *Service) seekToTime(ctx context.Context, queue string, t time.Time) error {
	msgs, err := s.messages.List(ctx, queue)
	if err != nil {
		return err
	}
	var retained []string
	for _, m := range msgs {
		if m.PublishTime.Before(t) {
			_ = s.messages.Delete(ctx, queue, m.MessageID)
			continue
		}
		retained = append(retained, m.MessageID)
	}
	if err := s.messages.BumpDeliveryVersions(ctx, queue, retained); err != nil {
		return err
	}
	for _, id := range retained {
		_ = s.messages.ModifyAckDeadline(ctx, queue, []string{id}, 0, clock.Now())
	}
	return nil
}

// restoreSnapshot resets the subscription to the state captured by snap: the
// snapshot backlog is restored as unacknowledged, plus every message still
// retained in the subscription that was published at/after the snapshot was
// created. Messages acked before the snapshot are not restored.
//
// A Seek invalidates outstanding ack IDs: each restored message gets a delivery
// version greater than both its snapshot version and its version in the store at
// Seek time, so an ack ID issued before the Seek no longer names the current
// delivery (EOD2). This covers a message still present at Seek; a message
// superseded after the snapshot and acked before the Seek is restored from the
// snapshot version (see the EOD6 deferral in the EOD wave plan).
func (s *Service) restoreSnapshot(ctx context.Context, queue string, snap SnapshotMeta) error {
	current, err := s.messages.List(ctx, queue)
	if err != nil {
		return err
	}
	currentVersions := make(map[string]int, len(current))
	for _, m := range current {
		currentVersions[m.MessageID] = m.DeliveryVersion
	}
	for _, m := range current {
		_ = s.messages.Delete(ctx, queue, m.MessageID)
	}
	restore := func(m pubsubstore.Message) error {
		m.Subscription = queue
		m.VisibleAt = time.Time{}
		// A seek restores the backlog as unacknowledged, so clear any retained
		// ordered ack state on the message.
		m.Acked = false
		m.AckPending = false
		if v, ok := currentVersions[m.MessageID]; ok && v > m.DeliveryVersion {
			m.DeliveryVersion = v
		}
		m.DeliveryVersion++
		return s.messages.Put(ctx, m)
	}
	for _, m := range snap.Backlog {
		if err := restore(m); err != nil {
			return err
		}
	}
	for _, m := range current {
		if m.PublishTime.Before(snap.CreatedAt) {
			continue
		}
		if err := restore(m); err != nil {
			return err
		}
	}
	return nil
}

// ─── errors ───────────────────────────────────────────────────────────────────

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func notFound(msg string) error {
	return model.NewProviderError("NotFound", msg, 404)
}

func internal(msg string) error {
	return model.NewProviderError("Internal", msg, 500)
}
