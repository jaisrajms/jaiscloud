package eventarc

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/eventing"
	"jaiscloud/internal/gcp/resource"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
)

// OperationMetadataType is the @type of the Eventarc v1 OperationMetadata.
const OperationMetadataType = "type.googleapis.com/google.cloud.eventarc.v1.OperationMetadata"

// Any type URLs for the resources a done operation can carry as its response.
const (
	TriggerTypeURL = "type.googleapis.com/google.cloud.eventarc.v1.Trigger"
	ChannelTypeURL = "type.googleapis.com/google.cloud.eventarc.v1.Channel"
)

// Operation is the transport-neutral description of a completed Eventarc
// long-running operation. Eventarc operations are never persisted — every
// control-plane mutation completes synchronously — so this is built on demand
// and rendered by each transport.
type Operation struct {
	Location   string
	ID         string
	Verb       string
	Target     string
	CreateTime time.Time
}

// newOperation builds a synchronous (done) operation for a mutation.
func newOperation(location, verb, target string) Operation {
	return Operation{
		Location:   location,
		ID:         randomHex(12),
		Verb:       verb,
		Target:     target,
		CreateTime: clock.Now().UTC(),
	}
}

// OperationJSON renders an Operation as the google.longrunning.Operation wire
// map. response is the Any-wrapped typed response body (see OperationResponse)
// or nil for a response-less operation.
func OperationJSON(op Operation, project string, response map[string]any) map[string]any {
	if response == nil {
		response = map[string]any{}
	}
	return map[string]any{
		"name":     OperationName(project, op.Location, op.ID),
		"metadata": operationMetadata(op),
		"done":     true,
		"response": response,
	}
}

// OperationResponse wraps a rendered resource body as the Any JSON a
// google.longrunning.Operation response carries, adding the required @type
// discriminator. body is copied, never mutated.
func OperationResponse(typeURL string, body map[string]any) map[string]any {
	out := make(map[string]any, len(body)+1)
	out["@type"] = typeURL
	for k, v := range body {
		out[k] = v
	}
	return out
}

// operationMetadata renders the eventarc.v1.OperationMetadata for an operation.
// The emulator completes every control-plane mutation synchronously, so
// createTime and endTime are the same instant.
func operationMetadata(op Operation) map[string]any {
	return map[string]any{
		"@type":                 OperationMetadataType,
		"createTime":            formatTimestamp(op.CreateTime),
		"endTime":               formatTimestamp(op.CreateTime),
		"target":                op.Target,
		"verb":                  op.Verb,
		"requestedCancellation": false,
		"apiVersion":            "v1",
	}
}

// formatTimestamp renders a business timestamp as the RFC3339Nano string the
// Discovery JSON shape (and protojson) uses.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// randomHex returns n hex characters of crypto/rand randomness. It is used only
// for ephemeral operation ids, never for a resource's stable output-only uid.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// contentEtag derives a deterministic OCC etag from a resource's mutable
// content. It is recomputed on every create/update so a caller presenting a
// pre-mutation etag is rejected (409 ABORTED) on the next write.
func contentEtag(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
		h.Write([]byte{0})
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func triggerEtag(t eventarcstore.Trigger) string {
	labels, _ := json.Marshal(t.Labels)
	return contentEtag(t.Config, labels)
}

func channelEtag(c eventarcstore.Channel) string {
	labels, _ := json.Marshal(c.Labels)
	return contentEtag(c.Config, labels, []byte(c.ActivationToken))
}

// TriggerJSON renders a stored Trigger as the eventarc.v1.Trigger wire map. The
// stored request body (destination/transport/eventFilters/...) is echoed
// verbatim; name/uid/etag/createTime/updateTime are overlaid as output-only.
//
// It is the single source of truth for the derived fields: the gRPC transport
// also drives its proto rendering from this map (via protojson), so the two
// transports cannot drift.
func TriggerJSON(project string, t eventarcstore.Trigger) map[string]any {
	out := map[string]any{}
	if len(t.Config) > 0 {
		_ = json.Unmarshal(t.Config, &out)
	}
	out["name"] = TriggerName(project, t.Location, t.Name)
	out["uid"] = t.UID
	out["etag"] = t.Etag
	out["createTime"] = formatTimestamp(t.CreateTime)
	out["updateTime"] = formatTimestamp(t.UpdateTime)
	// transport.pubsub.subscription is output-only: the short id of the
	// platform-provisioned subscription that carries the trigger's transport and
	// holds its deadLetterPolicy. The emulator provisions one for every trigger
	// whose destination is a Cloud Functions function.
	if topic := triggerTransportTopic(out); topic != "" {
		if dest := bodyMap(out, "destination"); dest != nil {
			if cf, _ := dest["cloudFunction"].(string); cf != "" {
				transport := bodyMap(out, "transport")
				pubsub := bodyMap(transport, "pubsub")
				if pubsub != nil {
					pubsub["subscription"] = resource.ResourceID(project)("pubsub-subscription", eventing.EventarcSubscriptionID(t.Location, t.Name))
				}
			}
		}
	}
	labels := t.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// ChannelJSON renders a stored Channel as the eventarc.v1.Channel wire map. The
// stored request body (provider/cryptoKeyName/...) is echoed verbatim;
// name/uid/etag/createTime/updateTime are overlaid and the output-only
// pubsubTopic, activationToken, and state are synthesized.
func ChannelJSON(project string, c eventarcstore.Channel) map[string]any {
	out := map[string]any{}
	if len(c.Config) > 0 {
		_ = json.Unmarshal(c.Config, &out)
	}
	out["name"] = ChannelName(project, c.Location, c.Name)
	out["uid"] = c.UID
	out["etag"] = c.Etag
	out["createTime"] = formatTimestamp(c.CreateTime)
	out["updateTime"] = formatTimestamp(c.UpdateTime)
	out["activationToken"] = c.ActivationToken
	out["pubsubTopic"] = resource.ResourceID(project)("pubsub-topic", "jc-eventarc-channel-"+c.Name)
	// A freshly created channel has no provider Connection yet, so it is
	// PENDING; it only becomes ACTIVE once a SaaS provider connects. The
	// emulator never delivers events or connects providers.
	out["state"] = "PENDING"
	labels := c.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	out["labels"] = labels
	return out
}

// ProviderJSON renders a catalogued provider as the eventarc.v1.Provider wire
// map.
func ProviderJSON(project, location string, d Provider) map[string]any {
	eventTypes := make([]any, 0, len(d.EventTypes))
	for _, et := range d.EventTypes {
		eventTypes = append(eventTypes, map[string]any{"type": et.Type, "description": et.Description})
	}
	return map[string]any{
		"name":        ProviderName(project, location, d.ID),
		"displayName": d.DisplayName,
		"eventTypes":  eventTypes,
	}
}
