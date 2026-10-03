package monitoring

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"jaiscloud/internal/model"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func TestCreateServiceGeneratesIDAndValidates(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	svc, err := s.CreateService(ctx, "test", "", monitoringstore.Service{
		DisplayName: "Checkout",
		Identifier:  json.RawMessage(`{"cloudRun":{"serviceName":"checkout"}}`),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if svc.ID == "" {
		t.Fatal("server-generated id is empty")
	}

	if _, err := s.CreateService(ctx, "test", "Bad_ID", monitoringstore.Service{Identifier: json.RawMessage(`{"custom":{}}`)}); err == nil {
		t.Fatal("invalid service id should be rejected")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 400 {
		t.Fatalf("invalid id err = %v, want 400", err)
	}

	if _, err := s.CreateService(ctx, "test", "no-identifier", monitoringstore.Service{DisplayName: "x"}); err == nil {
		t.Fatal("service without an identifier should be rejected")
	}
	if _, err := s.CreateService(ctx, "test", "empty-identifier", monitoringstore.Service{Identifier: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("empty identifier object should be rejected")
	}
	// basicService is a valid identity on its own (not part of the oneof).
	if _, err := s.CreateService(ctx, "test", "basic-svc", monitoringstore.Service{
		BasicService: json.RawMessage(`{"basicService":{"serviceType":"CLOUD_RUN"}}`),
	}); err != nil {
		t.Fatalf("basic-service-only create: %v", err)
	}

	if _, err := s.CreateService(ctx, "test", svc.ID, monitoringstore.Service{Identifier: json.RawMessage(`{"custom":{}}`)}); err == nil {
		t.Fatal("duplicate service id should be rejected")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 409 {
		t.Fatalf("duplicate err = %v, want 409", err)
	}
}

func TestUpdateServiceMask(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	created, err := s.CreateService(ctx, "test", "svc", monitoringstore.Service{
		DisplayName: "old",
		Identifier:  json.RawMessage(`{"cloudRun":{"serviceName":"checkout"}}`),
		UserLabels:  map[string]string{"a": "b"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// A masked displayName update preserves the identifier and labels.
	got, err := s.UpdateService(ctx, "test", created.ID, monitoringstore.Service{DisplayName: "new"}, []string{"displayName"})
	if err != nil {
		t.Fatalf("masked update: %v", err)
	}
	if got.DisplayName != "new" || len(got.Identifier) == 0 || got.UserLabels["a"] != "b" {
		t.Fatalf("masked merge = %+v", got)
	}

	// A mask on an identifier root replaces the identifier.
	got, err = s.UpdateService(ctx, "test", created.ID, monitoringstore.Service{
		Identifier: json.RawMessage(`{"gkeService":{"serviceName":"checkout"}}`),
	}, []string{"gkeService"})
	if err != nil {
		t.Fatalf("identifier mask: %v", err)
	}
	if string(got.Identifier) != `{"gkeService":{"serviceName":"checkout"}}` {
		t.Fatalf("identifier = %s", got.Identifier)
	}

	// An unsupported path is a 501 UnsupportedOperation.
	if _, err := s.UpdateService(ctx, "test", created.ID, monitoringstore.Service{}, []string{"bogus"}); err == nil {
		t.Fatal("unsupported mask path should error")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 501 {
		t.Fatalf("unsupported mask err = %v, want 501", err)
	}

	// A full replace that drops the identifier is rejected.
	if _, err := s.UpdateService(ctx, "test", created.ID, monitoringstore.Service{DisplayName: "x"}, nil); err == nil {
		t.Fatal("full replace without identifier should be rejected")
	}
}

func TestServiceLevelObjectiveValidation(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	_, _ = s.CreateService(ctx, "test", "svc", monitoringstore.Service{Identifier: json.RawMessage(`{"custom":{}}`)})

	valid := monitoringstore.ServiceLevelObjective{
		ServiceLevelIndicator: json.RawMessage(`{"basicSli":{"availability":{}}}`),
		Goal:                  0.99, RollingPeriod: 24 * time.Hour,
	}
	if _, err := s.CreateServiceLevelObjective(ctx, "test", "svc", "slo", valid); err != nil {
		t.Fatalf("valid create: %v", err)
	}

	// Missing parent service.
	if _, err := s.CreateServiceLevelObjective(ctx, "test", "nope", "", valid); err == nil {
		t.Fatal("missing parent should error")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 404 {
		t.Fatalf("missing parent err = %v, want 404", err)
	}

	cases := map[string]monitoringstore.ServiceLevelObjective{
		"no indicator":         {Goal: 0.9, RollingPeriod: 24 * time.Hour},
		"zero goal":            {ServiceLevelIndicator: valid.ServiceLevelIndicator, RollingPeriod: 24 * time.Hour},
		"goal > 0.9999":        {ServiceLevelIndicator: valid.ServiceLevelIndicator, Goal: 1.0, RollingPeriod: 24 * time.Hour},
		"no period":            {ServiceLevelIndicator: valid.ServiceLevelIndicator, Goal: 0.9},
		"both periods":         {ServiceLevelIndicator: valid.ServiceLevelIndicator, Goal: 0.9, RollingPeriod: 24 * time.Hour, CalendarPeriod: 4},
		"rolling not a day":    {ServiceLevelIndicator: valid.ServiceLevelIndicator, Goal: 0.9, RollingPeriod: time.Hour},
		"rolling > 30 days":    {ServiceLevelIndicator: valid.ServiceLevelIndicator, Goal: 0.9, RollingPeriod: 31 * 24 * time.Hour},
		"calendar unsupported": {ServiceLevelIndicator: valid.ServiceLevelIndicator, Goal: 0.9, CalendarPeriod: 5},
	}
	for name, slo := range cases {
		if _, err := s.CreateServiceLevelObjective(ctx, "test", "svc", "", slo); err == nil {
			t.Errorf("%s: should be rejected", name)
		}
	}
	// An SLO id may contain uppercase, underscore, colon, and dot.
	if _, err := s.CreateServiceLevelObjective(ctx, "test", "svc", "SLO.v1_beta", monitoringstore.ServiceLevelObjective{
		ServiceLevelIndicator: valid.ServiceLevelIndicator, Goal: 0.9, CalendarPeriod: 2,
	}); err != nil {
		t.Fatalf("valid SLO id rejected: %v", err)
	}
}

func TestUpdateServiceLevelObjectiveMaskSwitchesPeriod(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	_, _ = s.CreateService(ctx, "test", "svc", monitoringstore.Service{Identifier: json.RawMessage(`{"custom":{}}`)})
	created, err := s.CreateServiceLevelObjective(ctx, "test", "svc", "slo", monitoringstore.ServiceLevelObjective{
		ServiceLevelIndicator: json.RawMessage(`{"basicSli":{"availability":{}}}`),
		Goal:                  0.9, RollingPeriod: 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Masking calendar_period switches the SLO's period oneof.
	got, err := s.UpdateServiceLevelObjective(ctx, "test", "svc", created.ID, monitoringstore.ServiceLevelObjective{
		CalendarPeriod: 4,
	}, []string{"calendarPeriod"})
	if err != nil {
		t.Fatalf("switch period: %v", err)
	}
	if got.CalendarPeriod != 4 || got.RollingPeriod != 0 {
		t.Fatalf("period = calendar %d rolling %v, want calendar only", got.CalendarPeriod, got.RollingPeriod)
	}
}

func TestCompileServiceFilter(t *testing.T) {
	mesh := monitoringstore.Service{Identifier: json.RawMessage(`{"meshIstio":{"meshUid":"123","serviceName":"checkout"}}`)}
	basic := monitoringstore.Service{BasicService: json.RawMessage(`{"basicService":{"serviceType":"CLOUD_RUN"}}`)}
	custom := monitoringstore.Service{Identifier: json.RawMessage(`{"custom":{}}`)}

	for _, tc := range []struct {
		filter string
		svc    monitoringstore.Service
		want   bool
	}{
		{``, mesh, true},
		{`identifier_case = "MESH_ISTIO"`, mesh, true},
		{`identifier_case = "CUSTOM"`, mesh, false},
		{`mesh_istio.mesh_uid = "123"`, mesh, true},
		{`mesh_istio.mesh_uid = "999"`, mesh, false},
		{`mesh_istio.service_name = "checkout"`, mesh, true},
		{`basic_service.service_type = "CLOUD_RUN"`, basic, true},
		{`identifier_case = "CUSTOM"`, custom, true},
	} {
		f, err := compileServiceFilter(tc.filter)
		if err != nil {
			t.Fatalf("compile %q: %v", tc.filter, err)
		}
		if got := f.match(tc.svc); got != tc.want {
			t.Errorf("filter %q match = %v, want %v", tc.filter, got, tc.want)
		}
	}

	for _, bad := range []string{`bogus = "x"`, `display_name = "x"`, `identifier_case = "NOPE"`, `mesh_istio.mesh_uid = starts_with("1")`} {
		if _, err := compileServiceFilter(bad); err == nil {
			t.Errorf("filter %q should be rejected", bad)
		}
	}
}

func TestCompileEqualityFilterForSLO(t *testing.T) {
	f, err := compileEqualityFilter(`display_name = "checkout"`, sloFilterKeys)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !matchEqualityFilter(f, map[string]string{"display_name": "checkout"}) {
		t.Fatal("filter should match")
	}
	if _, err := compileEqualityFilter(`bogus = "x"`, sloFilterKeys); err == nil {
		t.Fatal("unknown key should be rejected")
	}
}

func TestIsJSONEmpty(t *testing.T) {
	for _, raw := range []string{"", "null", "{}", "[]", "  {}  "} {
		if !isJSONEmpty(json.RawMessage(raw)) {
			t.Errorf("isJSONEmpty(%q) = false, want true", raw)
		}
	}
	for _, raw := range []string{`{"custom":{}}`, `{"a":1}`} {
		if isJSONEmpty(json.RawMessage(raw)) {
			t.Errorf("isJSONEmpty(%q) = true, want false", raw)
		}
	}
}
