package monitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"jaiscloud/internal/model"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"

	"github.com/google/uuid"
)

// serviceIDPattern is the pattern Cloud Monitoring requires for a client
// supplied service id ("[a-z0-9\-]+"). A server-generated uuid satisfies it.
var serviceIDPattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// sloIDPattern is the pattern Cloud Monitoring requires for a client supplied
// service-level-objective id ("^[a-zA-Z0-9-_:.]+$"). A server-generated uuid
// satisfies it.
var sloIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_:.-]+$`)

// ─── Service Monitoring: services ─────────────────────────────────────────────

// ListServices returns the services in a project that satisfy the (equality
// only) filter, plus the unpaged match count.
func (s *Service) ListServices(ctx context.Context, project, filter string, pageSize int, pageToken string) ([]monitoringstore.Service, int, string, error) {
	f, err := compileServiceFilter(filter)
	if err != nil {
		return nil, 0, "", invalidArgument("invalid filter: " + err.Error())
	}
	all, err := s.store.ListServices(ctx, project)
	if err != nil {
		return nil, 0, "", mapStoreError(err)
	}
	matching := make([]monitoringstore.Service, 0, len(all))
	for _, svc := range all {
		if f.match(svc) {
			matching = append(matching, svc)
		}
	}
	page, next := pageSlice(matching, pageSize, pageToken)
	return page, len(matching), next, nil
}

// GetService returns one service by id.
func (s *Service) GetService(ctx context.Context, project, id string) (monitoringstore.Service, error) {
	svc, err := s.store.GetService(ctx, project, id)
	if err != nil {
		return monitoringstore.Service{}, mapStoreError(err)
	}
	return svc, nil
}

// CreateService stores a new service. id is the request's service_id
// (query/param), or "" to auto-generate one (real Cloud Monitoring generates an
// id when service_id is omitted).
func (s *Service) CreateService(ctx context.Context, project, id string, svc monitoringstore.Service) (monitoringstore.Service, error) {
	if id == "" {
		id = uuid.NewString()
	}
	if !serviceIDPattern.MatchString(id) {
		return monitoringstore.Service{}, invalidArgument("service id must match [a-z0-9-]+")
	}
	if err := validateServiceIdentifier(svc); err != nil {
		return monitoringstore.Service{}, err
	}
	svc.ID = id
	if err := s.store.CreateService(ctx, project, svc); err != nil {
		return monitoringstore.Service{}, mapStoreError(err)
	}
	return svc, nil
}

// UpdateService merges an incoming service into the stored one. An empty/nil
// update mask is a full replace; a non-empty mask merges field-by-field inside
// the store's locked section so a concurrent masked update touching different
// fields cannot be lost. The route uses PATCH, so DELETE-only fields do not
// apply.
func (s *Service) UpdateService(ctx context.Context, project, id string, incoming monitoringstore.Service, updateMask []string) (monitoringstore.Service, error) {
	incoming.ID = id
	svc, err := s.store.UpdateServiceAtomic(ctx, project, id, func(stored monitoringstore.Service) (monitoringstore.Service, error) {
		next := stored
		if len(updateMask) == 0 {
			next = incoming
		} else {
			var merr error
			next, merr = applyServiceMask(stored, incoming, updateMask)
			if merr != nil {
				return stored, merr
			}
		}
		if err := validateServiceIdentifier(next); err != nil {
			return stored, err
		}
		return next, nil
	})
	if err != nil {
		return monitoringstore.Service{}, mapStoreError(err)
	}
	return svc, nil
}

// DeleteService deletes a service and every SLO it owns.
func (s *Service) DeleteService(ctx context.Context, project, id string) error {
	return mapStoreError(s.store.DeleteService(ctx, project, id))
}

// validateServiceIdentifier rejects a service with no identity. A service is
// identified either by the identifier oneof (custom, cloudRun, clusterIstio,
// ...) or, for a basic service, by the separate basicService field (service
// type + service labels) — those are the two documented service definitions
// ("Constructs in the API").
func validateServiceIdentifier(svc monitoringstore.Service) error {
	if isJSONEmpty(svc.Identifier) && isJSONEmpty(svc.BasicService) {
		return invalidArgument("service must specify an identifier or a basic_service")
	}
	return nil
}

// serviceIdentifierMaskRoots are the normalized (snake_case) roots of the
// Service identifier oneof. A mask on any of them replaces the identifier.
// basic_service is a separate field, not part of the oneof.
var serviceIdentifierMaskRoots = map[string]bool{
	"custom":                  true,
	"app_engine":              true,
	"cloud_endpoints":         true,
	"cluster_istio":           true,
	"mesh_istio":              true,
	"istio_canonical_service": true,
	"cloud_run":               true,
	"gke_namespace":           true,
	"gke_workload":            true,
	"gke_service":             true,
}

// applyServiceMask merges an incoming service into the stored service according
// to the field paths in updateMask. Paths are normalized the same way as
// applyAlertPolicyMask (camelCase or snake_case); an unsupported path is a 501
// UnsupportedOperation, matching the rest of the emulator.
func applyServiceMask(stored, incoming monitoringstore.Service, updateMask []string) (monitoringstore.Service, error) {
	for _, raw := range updateMask {
		path := normalizeMaskPath(raw)
		switch {
		case path == "display_name":
			stored.DisplayName = incoming.DisplayName
		case path == "user_labels":
			stored.UserLabels = incoming.UserLabels
		case path == "telemetry":
			stored.Telemetry = incoming.Telemetry
		case path == "basic_service":
			stored.BasicService = incoming.BasicService
		case serviceIdentifierMaskRoots[path]:
			stored.Identifier = incoming.Identifier
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+raw, 501)
		}
	}
	return stored, nil
}

// ─── Service Monitoring: service level objectives ─────────────────────────────

// ListServiceLevelObjectives returns the SLOs of a service that satisfy the
// (equality only) filter, plus the unpaged match count.
func (s *Service) ListServiceLevelObjectives(ctx context.Context, project, serviceID, filter string, pageSize int, pageToken string) ([]monitoringstore.ServiceLevelObjective, int, string, error) {
	if _, err := s.store.GetService(ctx, project, serviceID); err != nil {
		return nil, 0, "", mapStoreError(err)
	}
	f, err := compileEqualityFilter(filter, sloFilterKeys)
	if err != nil {
		return nil, 0, "", invalidArgument("invalid filter: " + err.Error())
	}
	all, err := s.store.ListServiceLevelObjectives(ctx, project, serviceID)
	if err != nil {
		return nil, 0, "", mapStoreError(err)
	}
	matching := make([]monitoringstore.ServiceLevelObjective, 0, len(all))
	for _, slo := range all {
		if matchEqualityFilter(f, map[string]string{"display_name": slo.DisplayName}) {
			matching = append(matching, slo)
		}
	}
	page, next := pageSlice(matching, pageSize, pageToken)
	return page, len(matching), next, nil
}

// GetServiceLevelObjective returns one SLO by service + id. The view argument is
// accepted but not folded (see the package/plan deferral): the indicator is
// returned as stored for DEFAULT, FULL, and EXPLICIT alike.
func (s *Service) GetServiceLevelObjective(ctx context.Context, project, serviceID, id string) (monitoringstore.ServiceLevelObjective, error) {
	slo, err := s.store.GetServiceLevelObjective(ctx, project, serviceID, id)
	if err != nil {
		return monitoringstore.ServiceLevelObjective{}, mapStoreError(err)
	}
	return slo, nil
}

// CreateServiceLevelObjective stores a new SLO under an existing service. id is
// the request's service_level_objective_id, or "" to auto-generate one.
func (s *Service) CreateServiceLevelObjective(ctx context.Context, project, serviceID, id string, slo monitoringstore.ServiceLevelObjective) (monitoringstore.ServiceLevelObjective, error) {
	if _, err := s.store.GetService(ctx, project, serviceID); err != nil {
		return monitoringstore.ServiceLevelObjective{}, mapStoreError(err)
	}
	if id == "" {
		id = uuid.NewString()
	}
	if !sloIDPattern.MatchString(id) {
		return monitoringstore.ServiceLevelObjective{}, invalidArgument("service level objective id must match [a-zA-Z0-9-_:.]+")
	}
	slo.ID = id
	slo.ServiceID = serviceID
	if err := validateServiceLevelObjective(slo); err != nil {
		return monitoringstore.ServiceLevelObjective{}, err
	}
	if err := s.store.CreateServiceLevelObjective(ctx, project, slo); err != nil {
		return monitoringstore.ServiceLevelObjective{}, mapStoreError(err)
	}
	return slo, nil
}

// UpdateServiceLevelObjective merges an incoming SLO into the stored one. An
// empty/nil update mask is a full replace; a non-empty mask merges
// field-by-field. The merged result is re-validated (goal range and exactly one
// period) so a masked update cannot produce an invalid SLO.
func (s *Service) UpdateServiceLevelObjective(ctx context.Context, project, serviceID, id string, incoming monitoringstore.ServiceLevelObjective, updateMask []string) (monitoringstore.ServiceLevelObjective, error) {
	incoming.ID = id
	incoming.ServiceID = serviceID
	slo, err := s.store.UpdateServiceLevelObjectiveAtomic(ctx, project, serviceID, id, func(stored monitoringstore.ServiceLevelObjective) (monitoringstore.ServiceLevelObjective, error) {
		next := stored
		if len(updateMask) == 0 {
			next = incoming
		} else {
			var merr error
			next, merr = applyServiceLevelObjectiveMask(stored, incoming, updateMask)
			if merr != nil {
				return stored, merr
			}
		}
		if err := validateServiceLevelObjective(next); err != nil {
			return stored, err
		}
		return next, nil
	})
	if err != nil {
		return monitoringstore.ServiceLevelObjective{}, mapStoreError(err)
	}
	return slo, nil
}

// DeleteServiceLevelObjective deletes one SLO.
func (s *Service) DeleteServiceLevelObjective(ctx context.Context, project, serviceID, id string) error {
	return mapStoreError(s.store.DeleteServiceLevelObjective(ctx, project, serviceID, id))
}

// validateServiceLevelObjective enforces the request-level invariants real
// Cloud Monitoring checks: an SLI, a goal in (0, 0.9999], and exactly one
// period. A rolling period must be a positive integer multiple of one day no
// larger than 30 days; a calendar period must be one of DAY, WEEK, FORTNIGHT,
// or MONTH (the enum values 1-4).
func validateServiceLevelObjective(slo monitoringstore.ServiceLevelObjective) error {
	if isJSONEmpty(slo.ServiceLevelIndicator) {
		return invalidArgument("service level objective must specify a service_level_indicator")
	}
	if slo.Goal <= 0 || slo.Goal > 0.9999 {
		return invalidArgument("service level objective goal must be in the range (0, 0.9999]")
	}
	hasRolling := slo.RollingPeriod != 0
	hasCalendar := slo.CalendarPeriod != 0
	switch {
	case !hasRolling && !hasCalendar:
		return invalidArgument("service level objective must specify exactly one of rolling_period or calendar_period")
	case hasRolling && hasCalendar:
		return invalidArgument("service level objective may specify only one of rolling_period or calendar_period")
	}
	if hasRolling {
		const day = 24 * time.Hour
		if slo.RollingPeriod < 0 || slo.RollingPeriod%day != 0 || slo.RollingPeriod > 30*day {
			return invalidArgument("rolling_period must be an integer multiple of 1 day no larger than 30 days")
		}
	}
	if hasCalendar {
		switch slo.CalendarPeriod {
		case calendarPeriodDay, calendarPeriodWeek, calendarPeriodFortnight, calendarPeriodMonth:
		default:
			return invalidArgument("calendar_period must be one of DAY, WEEK, FORTNIGHT, or MONTH")
		}
	}
	return nil
}

// google.monitoring.v3.ServiceLevelObjective.CalendarPeriod numeric values.
const (
	calendarPeriodDay       int32 = 1
	calendarPeriodWeek      int32 = 2
	calendarPeriodFortnight int32 = 3
	calendarPeriodMonth     int32 = 4
)

// applyServiceLevelObjectiveMask merges an incoming SLO into the stored SLO
// according to the field paths in updateMask.
func applyServiceLevelObjectiveMask(stored, incoming monitoringstore.ServiceLevelObjective, updateMask []string) (monitoringstore.ServiceLevelObjective, error) {
	for _, raw := range updateMask {
		switch normalizeMaskPath(raw) {
		case "display_name":
			stored.DisplayName = incoming.DisplayName
		case "goal":
			stored.Goal = incoming.Goal
		case "service_level_indicator":
			stored.ServiceLevelIndicator = incoming.ServiceLevelIndicator
		case "rolling_period":
			stored.RollingPeriod = incoming.RollingPeriod
			// A masked set of rolling_period clears calendar_period: the two
			// form the SLO's period oneof, so the incoming request's period
			// selection wins.
			stored.CalendarPeriod = incoming.CalendarPeriod
		case "calendar_period":
			stored.CalendarPeriod = incoming.CalendarPeriod
			stored.RollingPeriod = incoming.RollingPeriod
		case "user_labels":
			stored.UserLabels = incoming.UserLabels
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+raw, 501)
		}
	}
	return stored, nil
}

// ─── filter helpers (equality subset) ─────────────────────────────────────────

// sloFilterKeys are the filter keys ListServiceLevelObjectives accepts. The
// Discovery document does not publish the SLO filter grammar, so the emulator
// supports a display_name equality subset (documented) and fails closed on
// anything else.
var sloFilterKeys = map[string]bool{"display_name": true}

// identifierCaseJSONKey maps a Service filter's identifier_case enum name to the
// JSON key of the identifier it selects. basic_service is included because a
// basic service is identified by its basicService field.
var identifierCaseJSONKey = map[string]string{
	"CUSTOM":                  "custom",
	"APP_ENGINE":              "appEngine",
	"CLOUD_ENDPOINTS":         "cloudEndpoints",
	"CLUSTER_ISTIO":           "clusterIstio",
	"MESH_ISTIO":              "meshIstio",
	"ISTIO_CANONICAL_SERVICE": "istioCanonicalService",
	"CLOUD_RUN":               "cloudRun",
	"GKE_NAMESPACE":           "gkeNamespace",
	"GKE_WORKLOAD":            "gkeWorkload",
	"GKE_SERVICE":             "gkeService",
	"BASIC_SERVICE":           "basicService",
}

// serviceFilter is the subset of the ListServices filter grammar the emulator
// supports: `identifier_case = "<ENUM>"` and `<identifier_type>.<attr> = "value"`
// clauses joined by AND, matching the documented service-identifier filter
// (e.g. `identifier_case = "MESH_ISTIO"` or `mesh_istio.mesh_uid = "123"`). Any
// other key or operator fails closed.
type serviceFilter struct {
	identifierCase string            // expected identity JSON key ("" = unset)
	attrs          map[string]string // camelCase dotted JSON path → expected leaf
}

// compileServiceFilter parses the documented ListServices filter subset.
func compileServiceFilter(filter string) (serviceFilter, error) {
	f := serviceFilter{attrs: map[string]string{}}
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return f, nil
	}
	for _, raw := range strings.Split(filter, " AND ") {
		clause := strings.TrimSpace(raw)
		if clause == "" {
			return f, fmt.Errorf("empty filter clause")
		}
		key, val, ok := ParseEquality(clause)
		if !ok {
			return f, fmt.Errorf("unsupported filter clause %q (only key = \"value\" is supported)", clause)
		}
		if key == "identifier_case" {
			jsonKey, ok := identifierCaseJSONKey[val]
			if !ok {
				return f, fmt.Errorf("unsupported identifier_case %q", val)
			}
			f.identifierCase = jsonKey
			continue
		}
		first, _, _ := strings.Cut(key, ".")
		if _, ok := identifierCaseJSONKey[strings.ToUpper(first)]; !ok {
			return f, fmt.Errorf("unsupported filter key %q", key)
		}
		segments := strings.Split(key, ".")
		for i, seg := range segments {
			segments[i] = snakeToLowerCamel(seg)
		}
		f.attrs[strings.Join(segments, ".")] = val
	}
	return f, nil
}

// match reports whether a service satisfies every clause.
func (f serviceFilter) match(svc monitoringstore.Service) bool {
	identity := serviceIdentityJSON(svc)
	if f.identifierCase != "" {
		if _, ok := identity[f.identifierCase]; !ok {
			return false
		}
	}
	for path, want := range f.attrs {
		got, ok := lookupJSONPath(identity, path)
		if !ok || got != want {
			return false
		}
	}
	return true
}

// serviceIdentityJSON merges the identifier-oneof and basicService JSON objects
// into one map keyed by their protojson field names.
func serviceIdentityJSON(svc monitoringstore.Service) map[string]any {
	out := map[string]any{}
	for _, raw := range []json.RawMessage{svc.Identifier, svc.BasicService} {
		if isJSONEmpty(raw) {
			continue
		}
		var m map[string]any
		if json.Unmarshal(raw, &m) == nil {
			for k, v := range m {
				out[k] = v
			}
		}
	}
	return out
}

// lookupJSONPath resolves a dotted path against a nested JSON object, returning
// the leaf's string form.
func lookupJSONPath(obj map[string]any, path string) (string, bool) {
	var cur any = obj
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = m[seg]
		if !ok {
			return "", false
		}
	}
	switch v := cur.(type) {
	case string:
		return v, true
	case nil:
		return "", false
	default:
		return fmt.Sprint(v), true
	}
}

// snakeToLowerCamel converts a snake_case identifier segment to lowerCamelCase
// (e.g. "mesh_istio" → "meshIstio").
func snakeToLowerCamel(s string) string {
	var b strings.Builder
	upper := false
	for _, r := range s {
		if r == '_' {
			upper = true
			continue
		}
		if upper && r >= 'a' && r <= 'z' {
			r -= 'a' - 'A'
			upper = false
		}
		b.WriteRune(r)
	}
	return b.String()
}

// compileEqualityFilter parses a conjunction of `key = "value"` clauses over an
// allowlist of keys. An empty filter matches everything; an unknown key or a
// non-equality operator fails closed with an error rather than silently
// matching everything.
func compileEqualityFilter(filter string, allowed map[string]bool) (map[string]string, error) {
	out := map[string]string{}
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return out, nil
	}
	for _, raw := range strings.Split(filter, " AND ") {
		clause := strings.TrimSpace(raw)
		if clause == "" {
			return nil, fmt.Errorf("empty filter clause")
		}
		key, val, ok := ParseEquality(clause)
		if !ok {
			return nil, fmt.Errorf("unsupported filter clause %q (only key = \"value\" is supported)", clause)
		}
		if !allowed[key] {
			return nil, fmt.Errorf("unsupported filter key %q", key)
		}
		out[key] = val
	}
	return out, nil
}

// matchEqualityFilter reports whether every accepted key's value equals the
// corresponding actual value.
func matchEqualityFilter(filter, actual map[string]string) bool {
	for key, want := range filter {
		if actual[key] != want {
			return false
		}
	}
	return true
}

// isJSONEmpty reports whether an opaque JSON payload is absent or an empty
// object/array. protojson renders an unset message as "null" and a set-but-empty
// oneof as "{}"; neither counts as "present" for identifier/indicator
// validation.
func isJSONEmpty(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s == "" || s == "null" || s == "{}" || s == "[]"
}
