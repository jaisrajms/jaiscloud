package workflowexecutions

import (
	"net/http"
	"strconv"
	"time"

	core "jaiscloud/internal/gcp/service/workflowexecutions"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
)

// ─── request helpers ──────────────────────────────────────────────────────────

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func strFrom(v any) string {
	s, _ := v.(string)
	return s
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		n, _ := strconv.Atoi(x)
		return n
	default:
		return 0
	}
}

func stringMapFrom(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// queryToParams copies single-valued query parameters into params as strings.
// Repeated parameters keep their slice so list params are not silently dropped.
func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) == 0 {
			continue
		}
		if len(vs) == 1 {
			params[k] = vs[0]
			continue
		}
		vals := make([]any, 0, len(vs))
		for _, v := range vs {
			vals = append(vals, v)
		}
		params[k] = vals
	}
}

// ─── wire encoding ────────────────────────────────────────────────────────────

// viewFromParam maps a REST view enum name to the core view, falling back to
// def for the unset/unspecified value (FULL for get, BASIC for list, matching
// the Discovery document).
func viewFromParam(v string, def core.View) core.View {
	switch v {
	case "BASIC":
		return core.ViewBasic
	case "FULL":
		return core.ViewFull
	default:
		return def
	}
}

// executionToJSON renders an execution as the Discovery Execution shape. Fields
// already projected away by the core view (empty argument/result/error/etc.) are
// omitted, matching protojson.
func executionToJSON(e workflowsstore.Execution, project string) map[string]any {
	out := map[string]any{
		"name":               core.ExecutionName(project, e.Location, e.WorkflowID, e.ID),
		"state":              e.State,
		"workflowRevisionId": e.WorkflowRevisionID,
	}
	if !e.StartTime.IsZero() {
		out["startTime"] = e.StartTime.Format(time.RFC3339Nano)
	}
	if !e.EndTime.IsZero() {
		out["endTime"] = e.EndTime.Format(time.RFC3339Nano)
	}
	if e.Duration != "" {
		out["duration"] = e.Duration
	}
	if e.Argument != "" {
		out["argument"] = e.Argument
	}
	if e.Result != "" {
		out["result"] = e.Result
	}
	if e.Error != nil {
		out["error"] = executionErrorToJSON(e.Error)
	}
	if e.CallLogLevel != "" {
		out["callLogLevel"] = e.CallLogLevel
	}
	if e.Labels != nil {
		out["labels"] = e.Labels
	}
	if len(e.CurrentSteps) > 0 {
		steps := make([]any, 0, len(e.CurrentSteps))
		for _, s := range e.CurrentSteps {
			steps = append(steps, map[string]any{"routine": s.Routine, "step": s.Step})
		}
		out["status"] = map[string]any{"currentSteps": steps}
	}
	return out
}

func executionErrorToJSON(err *workflowsstore.ExecutionError) map[string]any {
	out := map[string]any{"payload": err.Payload}
	if err.Context != "" {
		out["context"] = err.Context
	}
	if err.StackTrace != nil {
		elements := make([]any, 0, len(err.StackTrace.Elements))
		for _, el := range err.StackTrace.Elements {
			em := map[string]any{"step": el.Step, "routine": el.Routine}
			if el.Position != nil {
				// int64 proto fields are emitted as JSON strings by protojson.
				em["position"] = map[string]any{
					"line":   strconv.FormatInt(el.Position.Line, 10),
					"column": strconv.FormatInt(el.Position.Column, 10),
					"length": strconv.FormatInt(el.Position.Length, 10),
				}
			}
			elements = append(elements, em)
		}
		out["stackTrace"] = map[string]any{"elements": elements}
	}
	return out
}
