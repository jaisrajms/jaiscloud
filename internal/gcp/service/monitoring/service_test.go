package monitoring

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"jaiscloud/internal/clock"
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

// TestServiceAlertPolicyAuditAndConditionNames covers FC2-18/FC2-20 in the
// transport-neutral core: an alert policy carries the server create/update
// clock its creationRecord/mutationRecord are rendered from, and every
// condition is named by the server. A name supplied at create is ignored
// (alert.proto: "do not include the name field"); a name supplied on update
// identifies an existing condition and is preserved.
func TestServiceAlertPolicyAuditAndConditionNames(t *testing.T) {
	ctx := context.Background()
	createdAt := time.Date(2031, 2, 3, 4, 5, 6, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: createdAt})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	svc := NewService(monitoringstore.NewMemoryStore(), "test")
	created, err := svc.CreateAlertPolicy(ctx, "test", monitoringstore.AlertPolicy{
		DisplayName: "CPU",
		Conditions: []json.RawMessage{
			json.RawMessage(`{"displayName":"a","name":"projects/other/alertPolicies/x/conditions/999"}`),
			json.RawMessage(`{"displayName":"b"}`),
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	wantNames := []string{
		"projects/test/alertPolicies/" + created.ID + "/conditions/0",
		"projects/test/alertPolicies/" + created.ID + "/conditions/1",
	}
	if got := conditionNames(t, created.Conditions); !slices.Equal(got, wantNames) {
		t.Fatalf("create condition names = %v, want %v (a client-supplied name must be ignored)", got, wantNames)
	}
	if !created.CreateTime.Equal(createdAt) || !created.UpdateTime.Equal(createdAt) {
		t.Fatalf("created times = %v/%v, want %v", created.CreateTime, created.UpdateTime, createdAt)
	}

	// A get serves the stored names unchanged.
	got, err := svc.GetAlertPolicy(ctx, "test", created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if names := conditionNames(t, got.Conditions); !slices.Equal(names, wantNames) {
		t.Fatalf("get condition names = %v, want %v", names, wantNames)
	}

	// An update keeps the existing conditions' names and names the condition the
	// caller added without one; the create time is immutable and the mutation
	// time advances.
	updatedAt := createdAt.Add(time.Hour)
	clock.SetGlobalClock(clock.FixedClock{T: updatedAt})
	updated, err := svc.UpdateAlertPolicy(ctx, "test", created.ID, monitoringstore.AlertPolicy{
		DisplayName: "CPU (updated)",
		Conditions: []json.RawMessage{
			json.RawMessage(`{"displayName":"a","name":"` + wantNames[0] + `"}`),
			json.RawMessage(`{"displayName":"b","name":"` + wantNames[1] + `"}`),
			json.RawMessage(`{"displayName":"c"}`),
		},
	}, nil)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !updated.CreateTime.Equal(createdAt) {
		t.Fatalf("updated createTime = %v, want %v (create time is immutable)", updated.CreateTime, createdAt)
	}
	if !updated.UpdateTime.Equal(updatedAt) {
		t.Fatalf("updated updateTime = %v, want %v", updated.UpdateTime, updatedAt)
	}
	wantGot := append(append([]string{}, wantNames...), "projects/test/alertPolicies/"+created.ID+"/conditions/2")
	if names := conditionNames(t, updated.Conditions); !slices.Equal(names, wantGot) {
		t.Fatalf("updated condition names = %v, want %v", names, wantGot)
	}

	// A masked update that does not touch conditions leaves them and the
	// condition names alone, while the mutation time advances.
	maskedAt := updatedAt.Add(time.Hour)
	clock.SetGlobalClock(clock.FixedClock{T: maskedAt})
	masked, err := svc.UpdateAlertPolicy(ctx, "test", created.ID, monitoringstore.AlertPolicy{DisplayName: "renamed"}, []string{"displayName"})
	if err != nil {
		t.Fatalf("masked update: %v", err)
	}
	if names := conditionNames(t, masked.Conditions); !slices.Equal(names, wantGot) {
		t.Fatalf("masked update condition names = %v, want %v", names, wantGot)
	}
	if !masked.CreateTime.Equal(createdAt) || !masked.UpdateTime.Equal(maskedAt) {
		t.Fatalf("masked times = %v/%v, want %v/%v", masked.CreateTime, masked.UpdateTime, createdAt, maskedAt)
	}

	// An unnamed condition never takes a name another condition in the same
	// request already claims.
	reordered, err := svc.UpdateAlertPolicy(ctx, "test", created.ID, monitoringstore.AlertPolicy{
		Conditions: []json.RawMessage{
			json.RawMessage(`{"displayName":"new"}`),
			json.RawMessage(`{"displayName":"a","name":"` + wantNames[0] + `"}`),
		},
	}, []string{"conditions"})
	if err != nil {
		t.Fatalf("reorder update: %v", err)
	}
	reorderedNames := conditionNames(t, reordered.Conditions)
	if reorderedNames[1] != wantNames[0] {
		t.Fatalf("named condition lost its name: %v", reorderedNames)
	}
	if reorderedNames[0] == wantNames[0] || reorderedNames[0] == "" {
		t.Fatalf("unnamed condition reused a claimed name: %v", reorderedNames)
	}

	// A name that does not belong to this policy, or a duplicate of one that
	// another condition already claims, is replaced by a server name.
	foreign, err := svc.UpdateAlertPolicy(ctx, "test", created.ID, monitoringstore.AlertPolicy{
		Conditions: []json.RawMessage{
			json.RawMessage(`{"displayName":"x","name":"projects/other/alertPolicies/x/conditions/999"}`),
			json.RawMessage(`{"displayName":"y","name":"` + wantNames[0] + `"}`),
			json.RawMessage(`{"displayName":"z","name":"` + wantNames[0] + `"}`),
		},
	}, []string{"conditions"})
	if err != nil {
		t.Fatalf("foreign name update: %v", err)
	}
	foreignNames := conditionNames(t, foreign.Conditions)
	if foreignNames[1] != wantNames[0] {
		t.Fatalf("valid supplied name was not preserved: %v", foreignNames)
	}
	if foreignNames[0] == "projects/other/alertPolicies/x/conditions/999" || foreignNames[0] == "" {
		t.Fatalf("foreign condition name was accepted: %v", foreignNames)
	}
	if foreignNames[2] == wantNames[0] {
		t.Fatalf("duplicate condition name was accepted: %v", foreignNames)
	}
	seen := map[string]bool{}
	for _, n := range foreignNames {
		if seen[n] {
			t.Fatalf("duplicate condition name after update: %v", foreignNames)
		}
		seen[n] = true
	}
}

// TestServiceAlertPolicyHealsLegacyConditionNames proves a policy stored before
// FC2-20 (no condition names, as an upgraded database or an old snapshot holds
// it) still reads back with the server-assigned names, without mutating the
// store's copy.
func TestServiceAlertPolicyHealsLegacyConditionNames(t *testing.T) {
	ctx := context.Background()
	store := monitoringstore.NewMemoryStore()
	if err := store.CreateAlertPolicy(ctx, "test", monitoringstore.AlertPolicy{
		ID:          "legacy",
		DisplayName: "legacy",
		Conditions: []json.RawMessage{
			json.RawMessage(`{"displayName":"a"}`),
			json.RawMessage(`{"displayName":"b"}`),
		},
	}); err != nil {
		t.Fatalf("seed legacy policy: %v", err)
	}

	svc := NewService(store, "test")
	want := []string{
		"projects/test/alertPolicies/legacy/conditions/0",
		"projects/test/alertPolicies/legacy/conditions/1",
	}
	for pass := 0; pass < 2; pass++ {
		got, err := svc.GetAlertPolicy(ctx, "test", "legacy")
		if err != nil {
			t.Fatalf("get pass %d: %v", pass, err)
		}
		if names := conditionNames(t, got.Conditions); !slices.Equal(names, want) {
			t.Fatalf("get pass %d condition names = %v, want %v", pass, names, want)
		}
	}
	page, _, _, err := svc.ListAlertPolicies(ctx, "test", 0, "")
	if err != nil || len(page) != 1 {
		t.Fatalf("list = %+v, %v", page, err)
	}
	if names := conditionNames(t, page[0].Conditions); !slices.Equal(names, want) {
		t.Fatalf("list condition names = %v, want %v", names, want)
	}

	// The heal is a read-path rendering: the stored record is untouched.
	stored, err := store.GetAlertPolicy(ctx, "test", "legacy")
	if err != nil {
		t.Fatalf("stored get: %v", err)
	}
	if names := conditionNames(t, stored.Conditions); names[0] != "" || names[1] != "" {
		t.Fatalf("read-path heal mutated the store: %v", names)
	}
}

// conditionNames decodes the server-assigned name of every stored condition.
func conditionNames(t *testing.T, conds []json.RawMessage) []string {
	t.Helper()
	out := make([]string, 0, len(conds))
	for _, raw := range conds {
		var c struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatalf("decode condition %s: %v", raw, err)
		}
		out = append(out, c.Name)
	}
	return out
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
