package monitoringui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
	monitoringprovider "jaiscloud/internal/gcp/transport/rest/monitoring"
)

// The SLO console is a seam over the existing Monitoring core and REST
// provider. This test drives the UI routes against a real provider + in-memory
// core, so a wrong NormalizedRequest parameter (not just a wrong mock call)
// fails the round trip.
func TestSLOConsole_RoundTripThroughRealProvider(t *testing.T) {
	provider := monitoringprovider.NewProvider(
		core.NewService(monitoringstore.NewMemoryStore(), "test-project"),
		"test-project",
	)
	router := BuildRouter(provider, testCfg())

	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		var r *http.Request
		if body != "" {
			r = httptest.NewRequest(method, path, strings.NewReader(body))
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}

	w := request(http.MethodPost, "/services?serviceId=checkout",
		`{"displayName":"Checkout","custom":{"displayName":"Checkout"}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create service status = %d: %s", w.Code, w.Body.String())
	}

	w = request(http.MethodGet, "/services", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list services status = %d: %s", w.Code, w.Body.String())
	}
	var services struct {
		Services []struct {
			Name string `json:"name"`
		} `json:"services"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &services); err != nil {
		t.Fatalf("decode services: %v", err)
	}
	if len(services.Services) != 1 || services.Services[0].Name != "projects/test-project/services/checkout" {
		t.Fatalf("services = %+v", services.Services)
	}

	w = request(http.MethodPost, "/services/checkout/serviceLevelObjectives?serviceLevelObjectiveId=availability",
		`{"displayName":"99.9% availability","goal":0.999,"rollingPeriod":"86400s","serviceLevelIndicator":{"basicSli":{"availability":{}}}}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("create SLO status = %d: %s", w.Code, w.Body.String())
	}
	const sloPath = "/services/checkout/serviceLevelObjectives/availability"
	var slo struct {
		Name           string  `json:"name"`
		Goal           float64 `json:"goal"`
		RollingPeriod  string  `json:"rollingPeriod"`
		CalendarPeriod string  `json:"calendarPeriod"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &slo); err != nil {
		t.Fatalf("decode SLO: %v", err)
	}
	if slo.Name != "projects/test-project/services/checkout/serviceLevelObjectives/availability" ||
		slo.Goal != 0.999 || slo.RollingPeriod != "86400s" {
		t.Fatalf("created SLO = %+v", slo)
	}

	w = request(http.MethodPatch, sloPath+"?updateMask=goal", `{"goal":0.995}`)
	if w.Code != http.StatusOK {
		t.Fatalf("update SLO status = %d: %s", w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &slo); err != nil {
		t.Fatalf("decode updated SLO: %v", err)
	}
	if slo.Goal != 0.995 || slo.RollingPeriod != "86400s" {
		t.Fatalf("updated SLO = %+v", slo)
	}

	w = request(http.MethodGet, sloPath, "")
	if w.Code != http.StatusOK {
		t.Fatalf("get SLO status = %d: %s", w.Code, w.Body.String())
	}

	w = request(http.MethodDelete, sloPath, "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete SLO status = %d: %s", w.Code, w.Body.String())
	}
	w = request(http.MethodGet, sloPath, "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("get deleted SLO status = %d, want 404: %s", w.Code, w.Body.String())
	}

	w = request(http.MethodDelete, "/services/checkout", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete service status = %d: %s", w.Code, w.Body.String())
	}
}
