//go:build gcp_parity

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runFieldCoverage drives one pilot resource over one transport. Every generated
// field must be placed by the resource driver; a field nobody places is a gate
// failure, which is what makes the catalog and the scenario inseparable.
//
// A resource may declare more than one create phase (Cloud DNS accepts a
// forwarding configuration or a peering configuration but not both in one
// zone). The union of the phases must place every generated field.
func runFieldCoverage(ctx context.Context, t *testing.T, e *Env, r fieldResource, transport string, refs map[string]string) []fieldFinding {
	t.Helper()
	service, resource := splitResourceID(r.resourceID())
	placed := map[string]bool{}
	var findings []fieldFinding

	phases := r.phases()
	for i, phase := range phases {
		// The id carries the transport: a resource the API cannot delete (a
		// KMS crypto key) must not collide with the other transport's twin.
		id := e.Resource(fmt.Sprintf("fc-%s-%s", transport, slug(resource)))
		if i > 0 {
			id = e.Resource(fmt.Sprintf("fc%d-%s-%s", i, transport, slug(resource)))
		}
		fs, used := runFieldPhase(ctx, t, e, r, transport, refs, phase, id, i == 0)
		findings = append(findings, fs...)
		for _, f := range used {
			placed[f] = true
		}
	}

	// A message row is covered when itself or a descendant was placed in any
	// phase.
	fields := generatedFieldsFor(service, resource, transport)
	for _, f := range fields {
		if placed[f.Field] {
			continue
		}
		if f.Kind == "message" && descendantPlaced(fields, placed, f.Path) {
			continue
		}
		findings = append(findings, fieldFinding{
			resource: r.resourceID(), transport: transport, band: bandNotDrivable, field: f.Field,
			detail: "generated roundtrip field has no wire adaptation: add it to the resource driver's fill()",
		})
	}
	return findings
}

func descendantPlaced(fields []generatedField, placed map[string]bool, path string) bool {
	for _, f := range fields {
		if !placed[f.Field] {
			continue
		}
		if strings.HasPrefix(f.Path, path+".") || strings.HasPrefix(f.Path, path+"[") {
			return true
		}
	}
	return false
}

// runFieldPhase creates, reads, lists, updates and deletes one create phase. The
// primary phase (primary=true) also drives the negative writes and the absence
// create.
func runFieldPhase(ctx context.Context, t *testing.T, e *Env, r fieldResource, transport string, refs map[string]string, phase, id string, primary bool) ([]fieldFinding, []string) {
	service, resource := splitResourceID(r.resourceID())
	var findings []fieldFinding

	b := newFieldBody(service, resource, transport, refs)
	b.body = r.baseBody(e, id)
	r.fill(b, phase)
	if b.placeErr != nil {
		findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandNotDrivable, field: "", detail: "placing generated fields: " + b.placeErr.Error()})
	}
	findings = append(findings, b.skippedFindings()...)
	used := []string{}
	for field := range b.used {
		used = append(used, field)
	}
	if len(b.body) == 0 {
		return findings, used
	}

	if err := r.create(ctx, e, transport, id, cloneMap(b.body)); err != nil {
		findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandLifecycle, field: "", detail: "create: " + err.Error()})
		return findings, used
	}
	deleted := !r.deletable()
	defer func() {
		if deleted {
			return
		}
		if err := r.delete(ctx, e, transport, id); err != nil {
			t.Logf("fieldcoverage: cleanup delete %s/%s: %v", r.resourceID(), id, err)
		}
	}()

	// get: every placed field is served back with the value that was sent.
	served, err := r.read(ctx, e, transport, id)
	if err != nil {
		findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandLifecycle, field: "", detail: "get after create: " + err.Error()})
	} else {
		findings = append(findings, assertServed(r.resourceID(), transport, b, served)...)
		findings = append(findings, checkCrossTransport(ctx, e, r, transport, b, id)...)
	}
	if ok, err := r.listContains(ctx, e, transport, id); err != nil {
		findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandLifecycle, field: "", detail: "list: " + err.Error()})
	} else if !ok {
		findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandLifecycle, field: "", detail: "the created resource does not appear in its list"})
	}

	// update: one masked step per generated field that has a second value.
	for _, f := range b.fields {
		if !f.Update || !b.used[f.Field] {
			continue
		}
		var value any
		if err := json.Unmarshal([]byte(f.UpdateValue), &value); err != nil {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandUpdate, field: f.Field, detail: "generated update value is not JSON: " + err.Error()})
			continue
		}
		current, rerr := r.read(ctx, e, transport, id)
		if rerr != nil {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandUpdate, field: f.Field, detail: "get before update: " + rerr.Error()})
			continue
		}
		mask := strings.ReplaceAll(f.Path, "[]", "")
		if err := r.update(ctx, e, transport, id, mask, updateBody(current, f.Path, value)); err != nil {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandUpdate, field: f.Field, detail: "masked update rejected: " + err.Error()})
			continue
		}
		got, err := r.read(ctx, e, transport, id)
		if err != nil {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandUpdate, field: f.Field, detail: "get after update: " + err.Error()})
			continue
		}
		actual, present := lookupPath(got, f.Path)
		if !fieldMatches(value, actual, present) {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandUpdate, field: f.Field,
				detail: fmt.Sprintf("masked update was not served back: sent %s, got %s", f.UpdateValue, jsonValue(actual, present))})
		}
	}

	if primary {
		findings = append(findings, runNegativeWrites(ctx, e, r, transport, refs, id)...)
		findings = append(findings, runAbsenceCreate(ctx, e, r, transport, refs)...)
	}

	if !r.deletable() {
		return findings, used
	}
	// delete, then a read that must answer NOT_FOUND.
	if err := r.delete(ctx, e, transport, id); err != nil {
		findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandLifecycle, field: "", detail: "delete: " + err.Error()})
		deleted = true // avoid a duplicate cleanup attempt
		return findings, used
	}
	deleted = true
	if _, err := r.read(ctx, e, transport, id); err == nil {
		findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandLifecycle, field: "", detail: "get after delete answered a resource instead of NOT_FOUND"})
	} else if !statusIs(err, codes.NotFound) {
		findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandLifecycle, field: "", detail: "get after delete did not answer NOT_FOUND: " + err.Error()})
	}
	return findings, used
}

// assertServed checks every placed field against the get response.
func assertServed(resourceID, transport string, b *fieldBody, served map[string]any) []fieldFinding {
	var findings []fieldFinding
	for _, f := range b.fields {
		if !b.used[f.Field] || f.Kind == "message" {
			continue
		}
		want := b.wantValue(f)
		got, present := lookupPath(served, f.Path)
		if !fieldMatches(want, got, present) {
			findings = append(findings, fieldFinding{resource: resourceID, transport: transport, band: bandServe, field: f.Field,
				detail: fmt.Sprintf("sent %s, served %s", b.wantJSON(f), jsonValue(got, present))})
		}
	}
	return findings
}

// checkCrossTransport reads the same resource over the other transport and
// asserts every field both transports classify as roundtrip. This is the TGM1-3
// class — a field accepted on one transport and absent on the other.
func checkCrossTransport(ctx context.Context, e *Env, r fieldResource, transport string, b *fieldBody, id string) []fieldFinding {
	other := otherTransport(r.transports(), transport)
	if other == "" {
		return nil
	}
	otherBody, err := r.read(ctx, e, other, id)
	if err != nil {
		return []fieldFinding{{resource: r.resourceID(), transport: other, band: bandCrossTransport, field: "",
			detail: fmt.Sprintf("read the resource created over %s: %v", transport, err)}}
	}
	service, resource := splitResourceID(r.resourceID())
	otherFields := generatedFieldsFor(service, resource, other)
	byNorm := map[string]generatedField{}
	for _, f := range otherFields {
		if f.Kind == "message" {
			continue
		}
		byNorm[normalizeFieldPath(f.Path)] = f
	}
	var findings []fieldFinding
	for _, f := range b.fields {
		if !b.used[f.Field] || f.Kind == "message" {
			continue
		}
		peer, ok := byNorm[normalizeFieldPath(f.Path)]
		if !ok {
			continue // the catalog already records the transport gap
		}
		want := b.wantValue(f)
		got, present := lookupPath(otherBody, peer.Path)
		if !fieldMatches(want, got, present) {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: other, band: bandCrossTransport, field: peer.Field,
				detail: fmt.Sprintf("value accepted over %s is not served over %s: sent %s, %s served %s", transport, other, b.wantJSON(f), other, jsonValue(got, present))})
		}
	}
	return findings
}

// runNegativeWrites drives the generated output-only (attempted at create) and
// immutable (attempted at update) writes. The contract is "ignored or
// INVALID_ARGUMENT": a server-owned field must never accept the caller's value.
func runNegativeWrites(ctx context.Context, e *Env, r fieldResource, transport string, refs map[string]string, mainID string) []fieldFinding {
	var findings []fieldFinding
	service, resource := splitResourceID(r.resourceID())
	createAttempts := generatedAttemptsFiltered(service, resource, transport, "create")
	if len(createAttempts) > 0 {
		tag := e.Resource("fc-oo-" + transport + "-" + slug(resource))
		b := newFieldBody(service, resource, transport, refs)
		b.body = r.baseBody(e, tag)
		r.fill(b, "")
		attempted := map[string]generatedWriteAttempt{}
		for _, a := range createAttempts {
			if a.Kind == "message" {
				continue
			}
			var value any
			if err := json.Unmarshal([]byte(a.Value), &value); err != nil {
				continue
			}
			if err := insertPath(b.body, a.Path, value, 0); err != nil {
				continue
			}
			attempted[a.Field] = a
		}
		if len(attempted) > 0 {
			err := r.create(ctx, e, transport, tag, b.body)
			switch {
			case err != nil && statusIs(err, codes.InvalidArgument):
				// Documented rejection: accepted.
			case err != nil:
				findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandNegativeWrite, field: "",
					detail: "create carrying server-owned fields failed for an unexpected reason: " + err.Error()})
			default:
				got, rerr := r.read(ctx, e, transport, tag)
				if r.deletable() {
					_ = r.delete(ctx, e, transport, tag)
				}
				if rerr != nil {
					findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandNegativeWrite, field: "", detail: "get after server-owned create: " + rerr.Error()})
					break
				}
				for field, a := range attempted {
					var value any
					_ = json.Unmarshal([]byte(a.Value), &value)
					actual, present := lookupPath(got, a.Path)
					if present && equalField(value, actual) {
						findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandNegativeWrite, field: field,
							detail: fmt.Sprintf("output-only field accepted the caller's value %s", a.Value)})
					}
				}
			}
		}
	}

	// Immutable: a masked update carrying a fresh value must be ignored or
	// rejected.
	for _, a := range generatedAttemptsFiltered(service, resource, transport, "update") {
		if a.Kind == "message" {
			continue
		}
		var value any
		if err := json.Unmarshal([]byte(a.Value), &value); err != nil {
			continue
		}
		mask := strings.ReplaceAll(a.Path, "[]", "")
		current, cerr := r.read(ctx, e, transport, mainID)
		var body map[string]any
		if cerr == nil {
			body = updateBody(current, a.Path, value)
		} else {
			body = updateBody(map[string]any{}, a.Path, value)
		}
		err := r.update(ctx, e, transport, mainID, mask, body)
		if err != nil {
			if statusIs(err, codes.InvalidArgument) {
				continue
			}
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandNegativeWrite, field: a.Field,
				detail: "immutable update failed for an unexpected reason: " + err.Error()})
			continue
		}
		got, rerr := r.read(ctx, e, transport, mainID)
		if rerr != nil {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandNegativeWrite, field: a.Field, detail: "get after immutable update: " + rerr.Error()})
			continue
		}
		actual, present := lookupPath(got, a.Path)
		if present && equalField(value, actual) {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandNegativeWrite, field: a.Field,
				detail: fmt.Sprintf("immutable field accepted the update value %s", a.Value)})
		}
	}
	return findings
}

// runAbsenceCreate creates the resource with none of its optional blocks and
// checks that no block is synthesized.
func runAbsenceCreate(ctx context.Context, e *Env, r fieldResource, transport string, refs map[string]string) []fieldFinding {
	tag := e.Resource("fc-ab-" + transport + "-" + slug(resourceOf(r.resourceID())))
	service, resource := splitResourceID(r.resourceID())
	body := r.absenceBody(e, tag)
	b := newFieldBody(service, resource, transport, refs)
	b.body = body
	if err := r.create(ctx, e, transport, tag, body); err != nil {
		return []fieldFinding{{resource: r.resourceID(), transport: transport, band: bandAbsence, field: "",
			detail: "absence create failed: " + err.Error()}}
	}
	if r.deletable() {
		defer func() { _ = r.delete(ctx, e, transport, tag) }()
	}
	got, err := r.read(ctx, e, transport, tag)
	if err != nil {
		return []fieldFinding{{resource: r.resourceID(), transport: transport, band: bandAbsence, field: "", detail: "get after absence create: " + err.Error()}}
	}
	var findings []fieldFinding
	sent := map[string]bool{}
	for k := range body {
		sent[normalizeFieldPath(k)] = true
	}
	for _, f := range b.fields {
		if f.Kind != "message" || strings.Contains(f.Path, ".") || strings.Contains(f.Path, "[]") {
			continue
		}
		if sent[normalizeFieldPath(f.Path)] {
			continue // the minimal absence body itself sends this block
		}
		if _, present := lookupPath(got, f.Path); present {
			findings = append(findings, fieldFinding{resource: r.resourceID(), transport: transport, band: bandAbsence, field: f.Field,
				detail: "optional block is synthesized by a create that did not send it"})
		}
	}
	return findings
}

// fieldMatches reports whether a served value satisfies the sent one. An absent
// member matches only a zero value: both transports render an API-defined
// default (false/0/""/[]) by omitting the member, so a mask that sets a field
// back to its default is indistinguishable from one that clears it.
func fieldMatches(want, got any, present bool) bool {
	if !present {
		return isZeroJSONValue(want)
	}
	return equalField(want, got)
}

func isZeroJSONValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case bool:
		return !t
	case float64:
		return t == 0
	case string:
		return t == ""
	case []any:
		return len(t) == 0
	case map[string]any:
		return len(t) == 0
	}
	return false
}

// updateBody builds the request body for one masked leaf update from the
// resource's current state. A leaf inside a message block sends the whole
// block with the one changed leaf (the PATCH/replace contract Cloud DNS and
// Cloud Storage have), and a leaf inside a repeated block updates every element
// so the mask's array semantics are honoured.
func updateBody(current map[string]any, path string, value any) map[string]any {
	segs := strings.Split(path, ".")
	if len(segs) == 1 {
		out := map[string]any{}
		out[strings.TrimSuffix(segs[0], "[]")] = value
		return out
	}
	repeated := strings.HasSuffix(segs[0], "[]")
	name := strings.TrimSuffix(segs[0], "[]")
	out := map[string]any{}
	if repeated {
		// A repeated top-level block (AlertPolicy.conditions) keeps its array
		// shape: the mask applies to every element, so each one is merged.
		var elems []any
		if arr, ok := current[name].([]any); ok {
			elems = arr
		}
		if len(elems) == 0 {
			elems = []any{nil}
		}
		merged := make([]any, 0, len(elems))
		for _, el := range elems {
			merged = append(merged, updateBlock(deepCopyValue(el), segs[1:], value))
		}
		out[name] = merged
		return out
	}
	block, _ := current[name].(map[string]any)
	out[name] = updateBlock(deepCopyValue(block), segs[1:], value)
	return out
}

// updateBlock applies value at the remaining path inside block.
func updateBlock(block any, segs []string, value any) any {
	seg := segs[0]
	repeated := strings.HasSuffix(seg, "[]")
	name := strings.TrimSuffix(seg, "[]")
	last := len(segs) == 1
	if repeated {
		m, _ := block.(map[string]any)
		if m == nil {
			m = map[string]any{}
		}
		var elem any
		if existing, ok := m[name].([]any); ok && len(existing) > 0 {
			elem = existing[0]
		}
		if last {
			return []any{value}
		}
		m[name] = []any{updateBlock(elem, segs[1:], value)}
		return m
	}
	m, _ := block.(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	if last {
		m[name] = value
		return m
	}
	m[name] = updateBlock(m[name], segs[1:], value)
	return m
}

func deepCopyValue(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return v
	}
	return out
}

// helpers ----------------------------------------------------------------

func splitResourceID(id string) (service, resource string) {
	parts := strings.SplitN(id, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return id, ""
}

func resourceOf(id string) string {
	_, r := splitResourceID(id)
	return r
}

func otherTransport(all []string, transport string) string {
	for _, t := range all {
		if t != transport {
			return t
		}
	}
	return ""
}

func slug(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

// normalizeFieldPath folds the two transports' field spellings so a REST row and
// its gRPC counterpart are recognised as the same logical field.
func normalizeFieldPath(path string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(path) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func jsonValue(v any, present bool) string {
	if !present {
		return "<absent>"
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(raw)
}

// generatedAttemptsFiltered returns the negative writes for one resource,
// transport and phase (create for output-only, update for immutable).
func generatedAttemptsFiltered(service, resource, transport, at string) []generatedWriteAttempt {
	var out []generatedWriteAttempt
	for _, a := range generatedAttemptsFor(service, resource, transport) {
		if a.At == at {
			out = append(out, a)
		}
	}
	return out
}

// statusIs reports whether err carries a gRPC status (or the REST rendering of
// one) with the given code.
func statusIs(err error, code codes.Code) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == code {
		return true
	}
	msg := strings.ToUpper(err.Error())
	switch code {
	case codes.NotFound:
		return strings.Contains(msg, "NOT_FOUND") || strings.Contains(msg, "HTTP 404")
	case codes.InvalidArgument:
		return strings.Contains(msg, "INVALID_ARGUMENT") || strings.Contains(msg, "HTTP 400")
	case codes.AlreadyExists:
		return strings.Contains(msg, "ALREADY_EXISTS") || strings.Contains(msg, "HTTP 409")
	}
	return false
}
