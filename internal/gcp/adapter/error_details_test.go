package gcp

import (
	"encoding/json"
	"testing"

	"jaiscloud/internal/model"
)

// TestJSONCodecEncodeErrorDetails verifies the GCP error envelope carries the
// ProviderError.Details array (the shape Pub/Sub exactly-once ack failures use).
func TestJSONCodecEncodeErrorDetails(t *testing.T) {
	perr := model.NewProviderError("InvalidArgument", "bad ack ids", 400)
	perr.Details = []model.ErrorDetail{{
		Type:     "google.rpc.ErrorInfo",
		Reason:   "EXACTLY_ONCE_ACKID_FAILURE",
		Domain:   "pubsub.googleapis.com",
		Metadata: map[string]string{"ack-1": "PERMANENT_FAILURE_INVALID_ACK_ID"},
	}}
	status, _, body := (&JSONCodec{}).EncodeError(nil, perr)
	if status != 400 {
		t.Fatalf("status = %d, want 400", status)
	}
	var env struct {
		Error struct {
			Code    int              `json:"code"`
			Status  string           `json:"status"`
			Details []map[string]any `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if env.Error.Status != "INVALID_ARGUMENT" {
		t.Fatalf("status = %q, want INVALID_ARGUMENT", env.Error.Status)
	}
	if len(env.Error.Details) != 1 {
		t.Fatalf("details = %v, want 1 entry", env.Error.Details)
	}
	d := env.Error.Details[0]
	if d["@type"] != "type.googleapis.com/google.rpc.ErrorInfo" ||
		d["reason"] != "EXACTLY_ONCE_ACKID_FAILURE" ||
		d["domain"] != "pubsub.googleapis.com" {
		t.Fatalf("detail = %v", d)
	}
	md, _ := d["metadata"].(map[string]any)
	if md["ack-1"] != "PERMANENT_FAILURE_INVALID_ACK_ID" {
		t.Fatalf("metadata = %v", md)
	}
}
