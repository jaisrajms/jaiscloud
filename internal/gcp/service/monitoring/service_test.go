package monitoring

import (
	"context"
	"encoding/json"
	"testing"

	"jaiscloud/internal/model"

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

// TestApplyAlertPolicyMaskStrategyAndSeverity covers the two fields TGM5 added
// to the stored policy: the whole alertStrategy object is replaced by any
// nested mask path (the normaliser collapses alertStrategy.autoClose), severity
// sets the enum string, while the still-unsupported read-only fields keep
// answering 501 and an unknown path stays a 400.
func TestApplyAlertPolicyMaskStrategyAndSeverity(t *testing.T) {
	stored := monitoringstore.AlertPolicy{
		DisplayName:   "old",
		AlertStrategy: json.RawMessage(`{"autoClose":"18000s","notificationRateLimit":{"period":"300s"}}`),
		Severity:      "WARNING",
		UserLabels:    map[string]string{"team": "platform"},
	}
	incoming := monitoringstore.AlertPolicy{
		AlertStrategy: json.RawMessage(`{"autoClose":"36000s","notificationRateLimit":{"period":"600s"}}`),
		Severity:      "CRITICAL",
		UserLabels:    map[string]string{"team": "sre"},
	}

	// A nested camelCase path applies the whole strategy object.
	got, err := applyAlertPolicyMask(stored, incoming, []string{"alertStrategy.autoClose"})
	if err != nil {
		t.Fatalf("alertStrategy.autoClose mask: %v", err)
	}
	if string(got.AlertStrategy) != string(incoming.AlertStrategy) || got.DisplayName != "old" || got.Severity != "WARNING" {
		t.Fatalf("alertStrategy merge = %+v (strategy %s)", got, got.AlertStrategy)
	}

	// The gRPC snake_case spelling works too.
	got, err = applyAlertPolicyMask(stored, incoming, []string{"alert_strategy"})
	if err != nil {
		t.Fatalf("alert_strategy mask: %v", err)
	}
	if string(got.AlertStrategy) != string(incoming.AlertStrategy) {
		t.Fatalf("snake_case alert_strategy merge = %+v", got)
	}

	// severity sets the stored enum name and leaves the strategy alone.
	got, err = applyAlertPolicyMask(stored, incoming, []string{"severity"})
	if err != nil {
		t.Fatalf("severity mask: %v", err)
	}
	if got.Severity != "CRITICAL" || string(got.AlertStrategy) != string(stored.AlertStrategy) {
		t.Fatalf("severity merge = %+v", got)
	}

	// userLabels + alertStrategy in one mask, the module's mutate shape.
	got, err = applyAlertPolicyMask(stored, incoming, []string{"userLabels", "alertStrategy"})
	if err != nil {
		t.Fatalf("userLabels+alertStrategy mask: %v", err)
	}
	if got.UserLabels["team"] != "sre" || string(got.AlertStrategy) != string(incoming.AlertStrategy) {
		t.Fatalf("combined merge = %+v", got)
	}

	// An empty incoming strategy clears the stored one.
	got, err = applyAlertPolicyMask(stored, monitoringstore.AlertPolicy{}, []string{"alertStrategy"})
	if err != nil {
		t.Fatalf("clearing mask: %v", err)
	}
	if len(got.AlertStrategy) != 0 {
		t.Fatalf("cleared strategy = %s", got.AlertStrategy)
	}

	// validity is a real read-only field the emulator does not store: 501.
	_, err = applyAlertPolicyMask(stored, incoming, []string{"validity"})
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.HTTPStatus != 501 {
		t.Fatalf("validity mask err = %v, want 501", err)
	}

	// A path naming no field is a client error.
	_, err = applyAlertPolicyMask(stored, incoming, []string{"bogusField"})
	perr, ok = err.(*model.ProviderError)
	if !ok || perr.HTTPStatus != 400 {
		t.Fatalf("unknown mask err = %v, want 400", err)
	}
}

// TestServiceAlertPolicyStrategyLifecycle drives the transport-neutral core so
// the create -> get -> list -> masked update -> clear lifecycle of alertStrategy
// and severity is covered without either transport.
func TestServiceAlertPolicyStrategyLifecycle(t *testing.T) {
	ctx := context.Background()
	store := monitoringstore.NewMemoryStore()
	svc := NewService(store, "test")

	first := json.RawMessage(`{"autoClose":"18000s","notificationRateLimit":{"period":"300s"},"notificationChannelStrategy":[{"notificationChannelNames":["projects/test/notificationChannels/nc1"],"renotifyInterval":"600s"}]}`)
	created, err := svc.CreateAlertPolicy(ctx, "test", monitoringstore.AlertPolicy{
		DisplayName:          "Uptime",
		Combiner:             1,
		NotificationChannels: []string{"projects/test/notificationChannels/nc1"},
		UserLabels:           map[string]string{"team": "platform"},
		AlertStrategy:        first,
		Severity:             "WARNING",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := svc.GetAlertPolicy(ctx, "test", created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.AlertStrategy) != string(first) || got.Severity != "WARNING" {
		t.Fatalf("get = %+v (strategy %s)", got, got.AlertStrategy)
	}

	page, total, _, err := svc.ListAlertPolicies(ctx, "test", 0, "")
	if err != nil || total != 1 || len(page) != 1 {
		t.Fatalf("list = %+v total=%d err=%v", page, total, err)
	}
	if string(page[0].AlertStrategy) != string(first) || page[0].Severity != "WARNING" {
		t.Fatalf("listed = %+v (strategy %s)", page[0], page[0].AlertStrategy)
	}

	// The provider's mutate shape: userLabels + alertStrategy in one mask, with
	// a changed autoClose/rate limit.
	second := json.RawMessage(`{"autoClose":"36000s","notificationRateLimit":{"period":"600s"}}`)
	updated, err := svc.UpdateAlertPolicy(ctx, "test", created.ID, monitoringstore.AlertPolicy{
		UserLabels:    map[string]string{"team": "sre"},
		AlertStrategy: second,
		Severity:      "CRITICAL",
	}, []string{"userLabels", "alertStrategy", "severity"})
	if err != nil {
		t.Fatalf("masked update: %v", err)
	}
	if string(updated.AlertStrategy) != string(second) || updated.UserLabels["team"] != "sre" || updated.Severity != "CRITICAL" {
		t.Fatalf("updated = %+v (strategy %s)", updated, updated.AlertStrategy)
	}
	if updated.DisplayName != "Uptime" || len(updated.NotificationChannels) != 1 {
		t.Fatalf("masked update clobbered unmasked fields: %+v", updated)
	}

	// Clearing both the strategy and the labels converges to an empty policy.
	cleared, err := svc.UpdateAlertPolicy(ctx, "test", created.ID, monitoringstore.AlertPolicy{}, []string{"alertStrategy", "userLabels", "severity"})
	if err != nil {
		t.Fatalf("clearing update: %v", err)
	}
	if len(cleared.AlertStrategy) != 0 || len(cleared.UserLabels) != 0 || cleared.Severity != "" {
		t.Fatalf("cleared = %+v", cleared)
	}

	// validity keeps failing loud through the service surface.
	if _, err := svc.UpdateAlertPolicy(ctx, "test", created.ID, monitoringstore.AlertPolicy{}, []string{"validity"}); err == nil {
		t.Fatal("validity mask should be rejected")
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
