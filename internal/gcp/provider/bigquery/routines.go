package bigquery

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// resourceIDRE is the identifier rule the Discovery document states for
// RoutineReference.routineId, RowAccessPolicyReference.policyId and
// ModelReference.modelId: letters (a-z, A-Z), numbers (0-9) or underscores.
var resourceIDRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// validateResourceID enforces the identifier rule and the per-resource maximum
// length. field is the wire field name so the rejection names what was wrong.
func validateResourceID(field, id string, maxLen int) error {
	if id == "" {
		return invalidArgument(field + " is required")
	}
	if len(id) > maxLen {
		return invalidArgument(field + " must be at most " + strconv.Itoa(maxLen) + " characters")
	}
	if !resourceIDRE.MatchString(id) {
		return invalidArgument(field + " must contain only letters, numbers, or underscores")
	}
	return nil
}

// resourceEtag derives the opaque etag the routine and row-access-policy
// resources carry. Unlike a bare last-modified millisecond (datasets/tables),
// it hashes the stored content together with the update instant, so an update
// that changes a field (or lands in the same millisecond as the previous one)
// always yields a new etag. The content is canonicalized through a
// map-marshal, so the memory store (raw bytes) and Postgres (JSONB, which
// reorders keys) produce the same value.
func resourceEtag(config json.RawMessage, updated time.Time) string {
	var v any
	if len(config) > 0 {
		_ = json.Unmarshal(config, &v)
	}
	canon, _ := json.Marshal(v)
	h := fnv.New64a()
	_, _ = h.Write(canon)
	_, _ = h.Write([]byte(strconv.FormatInt(updated.UnixNano(), 10)))
	return strconv.FormatUint(h.Sum64(), 16)
}

// --- Wire rendering ---

// routineMap renders the full Routine resource. The stored config is the wire
// body verbatim; the computed members (the reference, etag and the two
// timestamps) are always derived from the stored record so a client cannot
// spoof them. Routine has no `kind` member in the Discovery schema, so none is
// synthesized (unlike Dataset/Table/Job).
func routineMap(projectID string, r bqstore.Routine) map[string]any {
	out := map[string]any{}
	if len(r.Config) > 0 {
		_ = json.Unmarshal(r.Config, &out)
	}
	out["routineReference"] = map[string]any{
		"projectId": projectID, "datasetId": r.DatasetID, "routineId": r.RoutineID,
	}
	out["etag"] = resourceEtag(r.Config, r.UpdateTime)
	out["creationTime"] = millis(r.CreateTime)
	out["lastModifiedTime"] = millis(r.UpdateTime)
	return out
}

// routineSummaryMap projects a routine onto the ListRoutinesResponse item the
// Discovery document defines: unless read_mask is set in the request, only
// etag, project_id, dataset_id, routine_id, routine_type, creation_time,
// last_modified_time, language and remote_function_options are populated. The
// emulator does not implement read_mask, so it always emits that subset.
func routineSummaryMap(projectID string, r bqstore.Routine) map[string]any {
	full := routineMap(projectID, r)
	out := map[string]any{}
	for _, k := range []string{
		"etag", "routineReference", "routineType", "creationTime",
		"lastModifiedTime", "language", "remoteFunctionOptions",
	} {
		if v, ok := full[k]; ok {
			out[k] = v
		}
	}
	return out
}

// rowAccessPolicyMap renders a RowAccessPolicy. creationTime/lastModifiedTime
// are google-datetime strings (not the int64 milliseconds Routine uses), and
// grantees is input-only: real GCP never echoes it, so the response omits it
// while the stored config keeps it (a later update re-sends the full body).
func rowAccessPolicyMap(projectID string, p bqstore.RowAccessPolicy) map[string]any {
	out := map[string]any{}
	if len(p.Config) > 0 {
		_ = json.Unmarshal(p.Config, &out)
	}
	delete(out, "grantees")
	out["rowAccessPolicyReference"] = map[string]any{
		"projectId": projectID, "datasetId": p.DatasetID,
		"tableId": p.TableID, "policyId": p.PolicyID,
	}
	out["etag"] = resourceEtag(p.Config, p.UpdateTime)
	out["creationTime"] = googleDateTime(p.CreateTime)
	out["lastModifiedTime"] = googleDateTime(p.UpdateTime)
	return out
}

// googleDateTime formats a timestamp the way a google-datetime field does.
func googleDateTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// --- Routines ---

func (p *Provider) InsertRoutine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetId is required")
	}
	body := bodyMap(nr)
	routineID := strValue(mapValue(body, "routineReference"), "routineId")
	if err := validateResourceID("routineReference.routineId", routineID, 256); err != nil {
		return nil, err
	}
	if _, err := p.store.GetDataset(ctx, projectOf(nr), datasetID); err != nil {
		return nil, mapErr(err)
	}
	if _, err := p.store.GetRoutine(ctx, projectOf(nr), datasetID, routineID); err == nil {
		return nil, mapErr(bqstore.ErrAlreadyExists)
	}
	now := clock.Now().UTC()
	r := bqstore.Routine{DatasetID: datasetID, RoutineID: routineID, CreateTime: now, UpdateTime: now}
	if data, err := json.Marshal(body); err == nil {
		r.Config = data
	}
	if err := p.store.CreateRoutine(ctx, projectOf(nr), datasetID, r); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(routineMap(projectOf(nr), r)), nil
}

func (p *Provider) GetRoutine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	routineID := strParam(nr, "routineId")
	if datasetID == "" || routineID == "" {
		return nil, invalidArgument("datasetId and routineId are required")
	}
	r, err := p.store.GetRoutine(ctx, projectOf(nr), datasetID, routineID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(routineMap(projectOf(nr), r)), nil
}

func (p *Provider) ListRoutines(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	if datasetID == "" {
		return nil, invalidArgument("datasetId is required")
	}
	if _, err := maxResultsQueryParam(nr); err != nil {
		return nil, err
	}
	if _, err := p.store.GetDataset(ctx, projectOf(nr), datasetID); err != nil {
		return nil, mapErr(err)
	}
	routines, err := p.store.ListRoutines(ctx, projectOf(nr), datasetID)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(routines, func(r bqstore.Routine) string { return r.RoutineID }, pagingParams(nr.Params))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, r := range page {
		items = append(items, routineSummaryMap(projectOf(nr), r))
	}
	resp := map[string]any{"routines": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateRoutine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	routineID := strParam(nr, "routineId")
	if datasetID == "" || routineID == "" {
		return nil, invalidArgument("datasetId and routineId are required")
	}
	body := bodyMap(nr)
	if body == nil {
		return nil, invalidArgument("a Routine body is required")
	}
	mask := fieldMask(nr)
	r, err := p.store.UpdateRoutineAtomic(ctx, projectOf(nr), datasetID, routineID,
		func(current bqstore.Routine) (bqstore.Routine, error) {
			if err := checkEtag(current.Config, current.UpdateTime, body); err != nil {
				return current, err
			}
			next, err := mergeUpdate(current.Config, body, mask)
			if err != nil {
				return current, err
			}
			current.Config = next
			current.UpdateTime = clock.Now().UTC()
			return current, nil
		})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(routineMap(projectOf(nr), r)), nil
}

func (p *Provider) DeleteRoutine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	routineID := strParam(nr, "routineId")
	if datasetID == "" || routineID == "" {
		return nil, invalidArgument("datasetId and routineId are required")
	}
	if err := p.store.DeleteRoutine(ctx, projectOf(nr), datasetID, routineID); err != nil {
		return nil, mapErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: http.StatusNoContent, Data: map[string]any{}}, nil
}

// --- Row access policies ---

func (p *Provider) InsertRowAccessPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	body := bodyMap(nr)
	policyID := strValue(mapValue(body, "rowAccessPolicyReference"), "policyId")
	if err := validateResourceID("rowAccessPolicyReference.policyId", policyID, 256); err != nil {
		return nil, err
	}
	if _, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID); err != nil {
		return nil, mapErr(err)
	}
	if _, err := p.store.GetRowAccessPolicy(ctx, projectOf(nr), datasetID, tableID, policyID); err == nil {
		return nil, mapErr(bqstore.ErrAlreadyExists)
	}
	now := clock.Now().UTC()
	pol := bqstore.RowAccessPolicy{DatasetID: datasetID, TableID: tableID, PolicyID: policyID, CreateTime: now, UpdateTime: now}
	if data, err := json.Marshal(body); err == nil {
		pol.Config = data
	}
	if err := p.store.CreateRowAccessPolicy(ctx, projectOf(nr), datasetID, tableID, pol); err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(rowAccessPolicyMap(projectOf(nr), pol)), nil
}

func (p *Provider) GetRowAccessPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	policyID := strParam(nr, "policyId")
	if datasetID == "" || tableID == "" || policyID == "" {
		return nil, invalidArgument("datasetId, tableId and policyId are required")
	}
	pol, err := p.store.GetRowAccessPolicy(ctx, projectOf(nr), datasetID, tableID, policyID)
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(rowAccessPolicyMap(projectOf(nr), pol)), nil
}

func (p *Provider) ListRowAccessPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	if _, err := p.store.GetTable(ctx, projectOf(nr), datasetID, tableID); err != nil {
		return nil, mapErr(err)
	}
	policies, err := p.store.ListRowAccessPolicies(ctx, projectOf(nr), datasetID, tableID)
	if err != nil {
		return nil, err
	}
	page, next, err := paging.Page(policies, func(pol bqstore.RowAccessPolicy) string { return pol.PolicyID }, nr.Params)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, pol := range page {
		items = append(items, rowAccessPolicyMap(projectOf(nr), pol))
	}
	resp := map[string]any{"rowAccessPolicies": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) UpdateRowAccessPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	policyID := strParam(nr, "policyId")
	if datasetID == "" || tableID == "" || policyID == "" {
		return nil, invalidArgument("datasetId, tableId and policyId are required")
	}
	body := bodyMap(nr)
	if body == nil {
		return nil, invalidArgument("a RowAccessPolicy body is required")
	}
	mask := fieldMask(nr)
	pol, err := p.store.UpdateRowAccessPolicyAtomic(ctx, projectOf(nr), datasetID, tableID, policyID,
		func(current bqstore.RowAccessPolicy) (bqstore.RowAccessPolicy, error) {
			if err := checkEtag(current.Config, current.UpdateTime, body); err != nil {
				return current, err
			}
			next, err := mergeUpdate(current.Config, body, mask)
			if err != nil {
				return current, err
			}
			current.Config = next
			current.UpdateTime = clock.Now().UTC()
			return current, nil
		})
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(rowAccessPolicyMap(projectOf(nr), pol)), nil
}

func (p *Provider) DeleteRowAccessPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	policyID := strParam(nr, "policyId")
	if datasetID == "" || tableID == "" || policyID == "" {
		return nil, invalidArgument("datasetId, tableId and policyId are required")
	}
	if err := p.store.DeleteRowAccessPolicy(ctx, projectOf(nr), datasetID, tableID, policyID); err != nil {
		return nil, mapErr(err)
	}
	return &model.ProviderResponse{HTTPStatus: http.StatusNoContent, Data: map[string]any{}}, nil
}

// BatchDeleteRowAccessPolicies implements
// tables.rowAccessPolicies:batchDelete. It is all-or-nothing: the whole batch
// is validated before anything is deleted, so a missing policy leaves the table
// untouched.
func (p *Provider) BatchDeleteRowAccessPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	datasetID := strParam(nr, "datasetId")
	tableID := strParam(nr, "tableId")
	if datasetID == "" || tableID == "" {
		return nil, invalidArgument("datasetId and tableId are required")
	}
	raw, _ := bodyMap(nr)["policyIds"].([]any)
	if len(raw) == 0 {
		return nil, invalidArgument("policyIds is required")
	}
	ids := make([]string, 0, len(raw))
	for _, v := range raw {
		id, _ := v.(string)
		if id == "" {
			return nil, invalidArgument("policyIds must contain non-empty strings")
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		if _, err := p.store.GetRowAccessPolicy(ctx, projectOf(nr), datasetID, tableID, id); err != nil {
			return nil, mapErr(err)
		}
	}
	for _, id := range ids {
		if err := p.store.DeleteRowAccessPolicy(ctx, projectOf(nr), datasetID, tableID, id); err != nil {
			return nil, mapErr(err)
		}
	}
	return &model.ProviderResponse{HTTPStatus: http.StatusNoContent, Data: map[string]any{}}, nil
}

// --- Update helpers ---

// fieldMask returns the updateMask field-mask paths of a request, or nil when
// the parameter is absent or empty (which on these resources means "replace the
// resource"). The Discovery document pins an apiary revision that does not
// re-declare updateMask on routines.update/rowAccessPolicies.update, but both
// methods accept it in the live API and the official clients send it; the
// emulator therefore honors it when present.
func fieldMask(nr *model.NormalizedRequest) []string {
	raw := strParam(nr, "updateMask")
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	paths := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			// "*" is the FieldMask wildcard: every field is replaced, which is
			// the same thing an absent mask means on these resources.
			if p == "*" {
				return nil
			}
			paths = append(paths, p)
		}
	}
	return paths
}

// checkEtag enforces the optimistic-concurrency precondition: a body carrying a
// non-empty etag must match the stored resource's etag (derived from its
// last-modified time, like every other BigQuery resource in the emulator). The
// mismatch is the real API's 412 conditionNotMet.
func checkEtag(config json.RawMessage, updated time.Time, body map[string]any) error {
	etag := strValue(body, "etag")
	current := resourceEtag(config, updated)
	if etag == "" || etag == current {
		return nil
	}
	return model.NewProviderError("PreconditionFailed",
		"the etag "+etag+" does not match the current etag "+current, http.StatusPreconditionFailed)
}

// mergeUpdate builds the config to persist for an update request: the body when
// no field mask is given (PUT replaces the resource), otherwise the stored
// config with only the masked paths taken from the body. A masked path absent
// from the body clears the stored value, per the FieldMask contract.
func mergeUpdate(stored json.RawMessage, body map[string]any, mask []string) (json.RawMessage, error) {
	if body == nil {
		body = map[string]any{}
	}
	next := map[string]any{}
	if len(mask) == 0 {
		for k, v := range body {
			next[k] = v
		}
	} else {
		if len(stored) > 0 {
			_ = json.Unmarshal(stored, &next)
		}
		for _, path := range mask {
			maskApply(next, body, strings.Split(path, "."))
		}
	}
	// The computed members are always re-derived on render, so they are not
	// persisted (a masked update must not freeze an old lastModifiedTime).
	delete(next, "kind")
	delete(next, "creationTime")
	delete(next, "lastModifiedTime")
	delete(next, "etag")
	data, err := json.Marshal(next)
	if err != nil {
		return nil, invalidArgument("malformed update body")
	}
	return data, nil
}

// maskApply copies the value at path from src into dst, deleting it from dst
// when src has no such value.
func maskApply(dst, src map[string]any, path []string) {
	key := path[0]
	if len(path) == 1 {
		if v, ok := src[key]; ok {
			dst[key] = v
		} else {
			delete(dst, key)
		}
		return
	}
	child, _ := dst[key].(map[string]any)
	if child == nil {
		child = map[string]any{}
	}
	srcChild, _ := src[key].(map[string]any)
	maskApply(child, srcChild, path[1:])
	if len(child) == 0 {
		delete(dst, key)
		return
	}
	dst[key] = child
}
