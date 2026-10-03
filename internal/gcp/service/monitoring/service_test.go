package monitoring

import (
	"encoding/json"
	"testing"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func TestNormalizeMaskPath(t *testing.T) {
	cases := map[string]string{
		"display_name":          "display_name",
		"displayName":           "display_name",
		"notificationChannels":  "notification_channels",
		"userLabels":            "user_labels",
		"documentation.content": "documentation",
		"documentation":         "documentation",
		"conditions":            "conditions",
	}
	for in, want := range cases {
		if got := normalizeMaskPath(in); got != want {
			t.Errorf("normalizeMaskPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestApplyAlertPolicyMaskCamelCaseAndSnake(t *testing.T) {
	stored := monitoringstore.AlertPolicy{
		DisplayName: "old", Combiner: 1, Conditions: []json.RawMessage{json.RawMessage(`{"a":1}`)},
		NotificationChannels: []string{"projects/p/notificationChannels/1"},
	}
	incoming := monitoringstore.AlertPolicy{DisplayName: "new", Combiner: 2}

	got, err := applyAlertPolicyMask(stored, incoming, []string{"displayName"})
	if err != nil {
		t.Fatalf("camelCase mask: %v", err)
	}
	if got.DisplayName != "new" || got.Combiner != 1 {
		t.Fatalf("camelCase merge = %+v", got)
	}

	got, err = applyAlertPolicyMask(stored, incoming, []string{"display_name", "combiner"})
	if err != nil {
		t.Fatalf("snake_case mask: %v", err)
	}
	if got.DisplayName != "new" || got.Combiner != 2 {
		t.Fatalf("snake_case merge = %+v", got)
	}

	if _, err := applyAlertPolicyMask(stored, incoming, []string{"bogusField"}); err == nil {
		t.Fatal("unknown mask path should be rejected")
	}
}

func TestApplyNotificationChannelMaskCamelCase(t *testing.T) {
	stored := monitoringstore.NotificationChannel{Type: "email", DisplayName: "old", Labels: map[string]string{"a": "b"}}
	incoming := monitoringstore.NotificationChannel{DisplayName: "new"}
	got, err := applyNotificationChannelMask(stored, incoming, []string{"displayName"})
	if err != nil {
		t.Fatalf("mask: %v", err)
	}
	if got.DisplayName != "new" || got.Type != "email" || got.Labels["a"] != "b" {
		t.Fatalf("merge = %+v", got)
	}
}
