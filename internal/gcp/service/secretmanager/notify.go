// Event notifications shared by the Secret Manager REST provider and gRPC
// service (DC64).
//
// A secret may name up to 10 Pub/Sub topics; Secret Manager publishes a message
// to every one of them whenever an operation modifies the secret or one of its
// versions ("Get, List, and Access calls don't result in message
// publications"). The message carries the event metadata as Pub/Sub attributes
// — always `eventType`, `dataFormat: JSON_API_V1`, `secretId` (the secret's full
// resource name) and `timestamp`, plus `versionId` on the four version events
// and `deleteType` on a delete — and the changed resource as data, JSON-encoded
// as the API's proto3 JSON mapping renders it. `TOPIC_CONFIGURED` is the
// bodyless test message sent whenever a secret is created or updated *with* a
// topic list, before the SECRET_CREATE/SECRET_UPDATE message, to every topic on
// the secret (including ones already present).
//
// Delivery is best-effort here, as it is for GCS object notifications: real
// Secret Manager retries a failed publish, while the emulator must never turn a
// notification failure (a deleted topic, a quota) into a failed control-plane
// call.
package secretmanager

import (
	"context"
	"time"

	"jaiscloud/internal/gcp/timestamp"
)

// Event types Secret Manager publishes. Every documented type now has a
// trigger: SECRET_VERSION_DESTROY_SCHEDULED fires when a version is destroyed on
// a secret with a destruction delay, and SECRET_DELETE carries
// deleteType: EXPIRATION for an automatically expired secret (DC70).
const (
	EventSecretCreate                  = "SECRET_CREATE"
	EventSecretUpdate                  = "SECRET_UPDATE"
	EventSecretDelete                  = "SECRET_DELETE"
	EventSecretVersionAdd              = "SECRET_VERSION_ADD"
	EventSecretVersionEnable           = "SECRET_VERSION_ENABLE"
	EventSecretVersionDisable          = "SECRET_VERSION_DISABLE"
	EventSecretVersionDestroy          = "SECRET_VERSION_DESTROY"
	EventSecretVersionDestroyScheduled = "SECRET_VERSION_DESTROY_SCHEDULED"
	EventSecretRotate                  = "SECRET_ROTATE"
	EventTopicConfigured               = "TOPIC_CONFIGURED"
)

// SECRET_DELETE's documented deleteType values: a user-initiated delete or the
// secret's automatic expiration.
const (
	DeleteTypeRequested  = "REQUESTED"
	DeleteTypeExpiration = "EXPIRATION"
)

// DataFormatJSONAPIV1 is the only data format the API documents: data is the
// changed resource in the JSON API's proto3 JSON mapping.
const DataFormatJSONAPIV1 = "JSON_API_V1"

// VersionStateEvent maps a version lifecycle transition (DESTROYED/DISABLED/
// ENABLED) to the notification type it publishes.
func VersionStateEvent(state string) string {
	switch state {
	case "DESTROYED":
		return EventSecretVersionDestroy
	case "DISABLED":
		return EventSecretVersionDisable
	default:
		return EventSecretVersionEnable
	}
}

// Publisher is the Pub/Sub publish path the notifications ride on (the Pub/Sub
// provider's PublishEvent). A nil Publisher disables delivery.
type Publisher interface {
	PublishEvent(ctx context.Context, accountID, topic string, data []byte, attributes map[string]string) (string, error)
}

// eventAttributes is the attribute set every notification carries.
func eventAttributes(eventType, secretName string, at time.Time) map[string]string {
	return map[string]string{
		"eventType":  eventType,
		"dataFormat": DataFormatJSONAPIV1,
		"secretId":   secretName,
		"timestamp":  timestamp.Canonical(at),
	}
}

// SecretEventAttributes is the attribute set for an event about a secret
// itself (create/update/delete/rotate).
func SecretEventAttributes(eventType, secretName string, at time.Time) map[string]string {
	return eventAttributes(eventType, secretName, at)
}

// DeleteEventAttributes is SECRET_DELETE's attribute set. deleteType is
// REQUESTED for a user-initiated delete and EXPIRATION when the secret reached
// its expireTime.
func DeleteEventAttributes(secretName, deleteType string, at time.Time) map[string]string {
	attrs := eventAttributes(EventSecretDelete, secretName, at)
	attrs["deleteType"] = deleteType
	return attrs
}

// VersionEventAttributes is the attribute set for an event about one secret
// version, which adds the version's full resource name.
func VersionEventAttributes(eventType, secretName, versionName string, at time.Time) map[string]string {
	attrs := eventAttributes(eventType, secretName, at)
	attrs["versionId"] = versionName
	return attrs
}

// Notify publishes data with attrs to each of the secret's topics. Every topic
// gets its own copy (as with a real publish), and failures are ignored: the
// control-plane operation has already succeeded.
func Notify(ctx context.Context, pub Publisher, accountID string, topics []string, data []byte, attrs map[string]string) {
	if pub == nil {
		return
	}
	for _, topic := range topics {
		_, _ = pub.PublishEvent(ctx, accountID, topic, data, attrs)
	}
}

// NotifyTopicConfigured sends the TOPIC_CONFIGURED test message: no data and no
// attributes beyond eventType, to every topic on the secret.
func NotifyTopicConfigured(ctx context.Context, pub Publisher, accountID string, topics []string) {
	if pub == nil {
		return
	}
	attrs := map[string]string{"eventType": EventTopicConfigured}
	for _, topic := range topics {
		_, _ = pub.PublishEvent(ctx, accountID, topic, nil, attrs)
	}
}
