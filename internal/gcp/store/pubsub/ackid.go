package pubsub

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

// Exactly-once acknowledgement-ID versioning (EOD1).
//
// Real GCP versions the ack ID per delivery: subscribing with
// enableExactlyOnceDelivery makes Acknowledge/ModifyAckDeadline reject a
// *superseded* (a later delivery has a newer ID) or *expired* (the ack deadline
// has lapsed) ack ID with INVALID_ARGUMENT, while a plain subscription keeps
// accepting any decodable ID. A Seek that makes a message visible again also
// invalidates outstanding ack IDs (a version advance, EOD2). The emulator
// encodes a per-message *delivery version* into the opaque wire ack ID so both
// transports can honour that contract. That version is separate from
// Message.DeliveryAttempt, which drives dead-letter accounting and must not be
// advanced by a Seek. Message IDs are numeric (store NextID), so the version is
// the final path segment and the message ID the middle one:
// "subscription/messageID/deliveryVersion".
//
// EncodeAckID renders the opaque wire ack ID for a delivery.
func EncodeAckID(subscription, messageID string, deliveryVersion int) string {
	raw := subscription + "/" + messageID + "/" + strconv.Itoa(deliveryVersion)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeAckID reverses EncodeAckID. ok is false for a malformed ID (bad base64,
// missing segments, or a non-numeric version). Message IDs are numeric
// (store.NextID), so the middle segment is unambiguous and the store's
// last-slash stripping of "subscription/messageID" stays correct.
func DecodeAckID(s string) (subscription, messageID string, deliveryVersion int, ok bool) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", "", 0, false
	}
	text := string(raw)
	first := strings.IndexByte(text, '/')
	if first <= 0 {
		return "", "", 0, false
	}
	last := strings.LastIndexByte(text, '/')
	if last <= first {
		return "", "", 0, false
	}
	version, err := strconv.Atoi(text[last+1:])
	if err != nil {
		return "", "", 0, false
	}
	return text[:first], text[first+1 : last], version, true
}

// AckIDCurrent reports whether the ack ID minted for deliveryVersion still names
// the message's current delivery whose ack deadline has not lapsed. A message
// that is visible — never claimed, or explicitly released by a nack
// (VisibleAt zero) — is not treated as expired: only a lapsed deadline
// (VisibleAt set and in the past) invalidates the ID.
func AckIDCurrent(m Message, deliveryVersion int, now time.Time) bool {
	if m.DeliveryVersion != deliveryVersion {
		return false
	}
	if !m.VisibleAt.IsZero() && !now.Before(m.VisibleAt) {
		return false
	}
	return true
}

// The wire contract real GCP returns for an exactly-once ack/modack whose ID is
// superseded or expired: INVALID_ARGUMENT plus a google.rpc.ErrorInfo detail
// that maps each bad ack ID to PERMANENT_FAILURE_INVALID_ACK_ID. Both the REST
// envelope and the gRPC status carry it, and the official clients (Go's
// AcknowledgeStatusInvalidAckID, Python's INVALID_ACK_ID, Node's AckResponses
// .Invalid) resolve the per-message result from that metadata.
const (
	// EODAckFailureMessage is the real-GCP INVALID_ARGUMENT message.
	EODAckFailureMessage = "Some acknowledgement ids in the request were invalid. This could be because the acknowledgement ids have expired or the acknowledgement ids were malformed."
	// EODAckFailureReason is the google.rpc.ErrorInfo.reason.
	EODAckFailureReason = "EXACTLY_ONCE_ACKID_FAILURE"
	// EODAckFailureDomain is the google.rpc.ErrorInfo.domain.
	EODAckFailureDomain = "pubsub.googleapis.com"
	// InvalidAckIDValue is the per-ack-ID ErrorInfo metadata value.
	InvalidAckIDValue = "PERMANENT_FAILURE_INVALID_ACK_ID"
)

// EODAckFailureMetadata maps each rejected ack ID (the wire value, as sent) to
// InvalidAckIDValue for the ErrorInfo detail.
func EODAckFailureMetadata(invalidAckIDs []string) map[string]string {
	out := make(map[string]string, len(invalidAckIDs))
	for _, id := range invalidAckIDs {
		out[id] = InvalidAckIDValue
	}
	return out
}
