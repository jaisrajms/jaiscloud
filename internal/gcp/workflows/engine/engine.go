// Package engine interprets Google Workflows YAML source. It is a self-contained
// GCP-native interpreter: it parses the Workflows YAML DSL (main.params,
// main.steps, and the assign/return/call/switch/for/try/retry/except/raise/next
// step types) and executes it directly — never translated to AWS ASL.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/workflows/expr"

	"gopkg.in/yaml.v3"
)

// ValidationError marks an invalid workflow definition (surfaces as HTTP 400
// INVALID_ARGUMENT on the CreateExecution path).
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

// Error is a workflow execution error (surfaces as execution state FAILED with
// an error.payload naming the failing construct).
type Error struct {
	Message string
	Step    string
}

func (e *Error) Error() string { return e.Message }

// Result is the outcome of an execution.
type Result struct {
	Value any    // return value (nil when Err is set)
	Step  string // last attempted step name (for status.currentSteps)
	Err   error  // nil on success; *ValidationError or *Error otherwise
}

// Engine executes workflows.
type Engine struct {
	client *http.Client
	env    expr.Env
	sleep  func(ctx context.Context, d time.Duration) error
}

// Option customizes an Engine.
type Option func(*Engine)

// WithHTTPClient overrides the outbound HTTP client (used by http.* calls).
func WithHTTPClient(c *http.Client) Option { return func(e *Engine) { e.client = c } }

// WithEnv overrides the expression environment (env vars + clock).
func WithEnv(env expr.Env) Option { return func(e *Engine) { e.env = env } }

// WithSleep overrides the sleep function used by sys.sleep and retry backoff.
func WithSleep(f func(ctx context.Context, d time.Duration) error) Option {
	return func(e *Engine) { e.sleep = f }
}

// New returns an Engine with a real HTTP client, the process environment, and
// the global clock.
func New(opts ...Option) *Engine {
	e := &Engine{
		client: &http.Client{},
		env:    expr.Env{Getenv: os.LookupEnv, Now: clock.Now},
		sleep:  defaultSleep,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// control is a non-error control-flow signal (return/next/break/continue).
type control struct {
	kind   string // "", "return", "next", "break", "continue"
	value  any
	target string
}

// WorkflowContext carries the per-execution metadata the emulator knows about
// the running workflow. It is used to resolve the GCP built-in environment
// variables (GOOGLE_CLOUD_*) exposed through sys.get_env.
type WorkflowContext struct {
	ProjectID      string
	ProjectNumber  string
	Location       string
	WorkflowID     string
	RevisionID     string
	ServiceAccount string
	ExecutionID    string
}

// builtins renders the GCP built-in environment variables for the execution.
func (wc WorkflowContext) builtins() map[string]string {
	m := map[string]string{}
	if wc.ProjectID != "" {
		m["GOOGLE_CLOUD_PROJECT_ID"] = wc.ProjectID
	}
	if wc.ProjectNumber != "" {
		m["GOOGLE_CLOUD_PROJECT_NUMBER"] = wc.ProjectNumber
	}
	if wc.Location != "" {
		m["GOOGLE_CLOUD_LOCATION"] = wc.Location
	}
	if wc.WorkflowID != "" {
		m["GOOGLE_CLOUD_WORKFLOW_ID"] = wc.WorkflowID
	}
	if wc.RevisionID != "" {
		m["GOOGLE_CLOUD_WORKFLOW_REVISION_ID"] = wc.RevisionID
	}
	if wc.ServiceAccount != "" {
		m["GOOGLE_CLOUD_SERVICE_ACCOUNT_NAME"] = wc.ServiceAccount
	}
	if wc.ExecutionID != "" {
		m["GOOGLE_CLOUD_WORKFLOW_EXECUTION_ID"] = wc.ExecutionID
	}
	return m
}

// step is a named step: the YAML element "name: body".
type step struct {
	name string
	body map[string]any
}

// Execute parses source and runs the main routine to completion with an empty
// workflow context (no GOOGLE_CLOUD_* built-ins).
func (e *Engine) Execute(ctx context.Context, source string, argument any) Result {
	return e.ExecuteWithContext(ctx, source, argument, WorkflowContext{})
}

// ExecuteWithContext parses source and runs the main routine to completion,
// resolving GCP built-in environment variables from wc.
func (e *Engine) ExecuteWithContext(ctx context.Context, source string, argument any, wc WorkflowContext) Result {
	wf, err := parseWorkflow(source)
	if err != nil {
		return Result{Err: err}
	}
	vars, err := bindArgs(normalize(argument), wf.params)
	if err != nil {
		return Result{Err: err}
	}
	env := e.env
	env.Builtins = wc.builtins()
	last := ""
	c, err := e.runSteps(ctx, wf.steps, vars, env, &last)
	if err != nil {
		return Result{Step: last, Err: err}
	}
	if c.kind == "return" {
		return Result{Value: c.value, Step: last}
	}
	if c.kind == "break" || c.kind == "continue" {
		return Result{Step: last, Err: &Error{Message: "step \"" + last + "\": " + c.kind + " used outside a loop", Step: last}}
	}
	// A workflow that never returns produces a null result (matches GCP, where
	// a step sequence with no return yields null).
	return Result{Value: nil, Step: last}
}

type workflow struct {
	params []string
	steps  []step
}

// parseWorkflow unmarshals the YAML and validates the top-level structure.
func parseWorkflow(source string) (*workflow, error) {
	var root any
	if err := yaml.Unmarshal([]byte(source), &root); err != nil {
		return nil, &ValidationError{Message: "invalid workflow YAML: " + err.Error()}
	}
	root = normalize(root)
	m, ok := root.(map[string]any)
	if !ok {
		return nil, &ValidationError{Message: "workflow source must be a YAML mapping"}
	}
	main, ok := m["main"].(map[string]any)
	if !ok {
		return nil, &ValidationError{Message: "workflow must define a 'main' routine"}
	}
	var params []string
	if pv, present := main["params"]; present && pv != nil {
		pl, ok := pv.([]any)
		if !ok {
			return nil, &ValidationError{Message: "main.params must be a list of parameter names"}
		}
		for _, p := range pl {
			s, ok := p.(string)
			if !ok {
				return nil, &ValidationError{Message: "main.params must be a list of strings"}
			}
			params = append(params, s)
		}
	}
	stepsRaw, ok := main["steps"]
	if !ok {
		return nil, &ValidationError{Message: "main must define 'steps'"}
	}
	steps, err := parseSteps(stepsRaw)
	if err != nil {
		return nil, &ValidationError{Message: err.Error()}
	}
	return &workflow{params: params, steps: steps}, nil
}

// parseSteps converts a YAML steps value (a list of single-key name→body maps)
// into a []step. Structural problems are validation errors (fail loud).
func parseSteps(raw any) ([]step, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("steps must be a list")
	}
	out := make([]step, 0, len(list))
	for i, el := range list {
		em, ok := el.(map[string]any)
		if !ok || len(em) != 1 {
			return nil, fmt.Errorf("step %d must be a single-key map (name: body)", i)
		}
		for name, bodyVal := range em {
			body, ok := bodyVal.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("step %q body must be a map", name)
			}
			out = append(out, step{name: name, body: body})
		}
	}
	return out, nil
}

// bindArgs binds the execution argument to the declared params. The argument
// must be a JSON object (map); declared params with no supplied value default
// to null.
func bindArgs(argument any, params []string) (map[string]any, error) {
	vars := map[string]any{}
	switch a := argument.(type) {
	case nil:
		// no argument supplied
	case map[string]any:
		vars = a
	default:
		return nil, &ValidationError{Message: "argument must be a JSON object"}
	}
	for _, p := range params {
		if _, ok := vars[p]; !ok {
			vars[p] = nil
		}
	}
	return vars, nil
}

// runSteps executes a list of named steps. last records the most recently
// attempted step name for status reporting. Returns a control signal for
// return/next/break/continue, or a *Error for workflow failure.
func (e *Engine) runSteps(ctx context.Context, steps []step, vars map[string]any, env expr.Env, last *string) (control, error) {
	index := make(map[string]int, len(steps))
	for i, st := range steps {
		index[st.name] = i
	}
	i := 0
	for i < len(steps) {
		st := steps[i]
		*last = st.name
		c, err := e.execStep(ctx, st, vars, env, last)
		if err != nil {
			return control{}, err
		}
		switch c.kind {
		case "return":
			return c, nil
		case "next":
			if j, ok := index[c.target]; ok {
				i = j
				continue
			}
			return c, nil // propagate to the enclosing scope
		case "break", "continue":
			return c, nil // propagate to the enclosing for loop
		}
		i++
	}
	return control{}, nil
}

// execStep dispatches one step to its operation type.
func (e *Engine) execStep(ctx context.Context, st step, vars map[string]any, env expr.Env, last *string) (control, error) {
	body := st.body
	switch {
	case has(body, "return"):
		v, err := e.evalValue(body["return"], vars, env)
		if err != nil {
			return control{}, wrapStep(err, st.name)
		}
		return control{kind: "return", value: v}, nil

	case has(body, "assign"):
		assigns, ok := body["assign"].([]any)
		if !ok {
			return control{}, stepError(st.name, "assign must be a list of var: expr entries")
		}
		for _, a := range assigns {
			am, ok := a.(map[string]any)
			if !ok {
				return control{}, stepError(st.name, "assign entry must be a map (var: expr)")
			}
			for varName, exprVal := range am {
				v, err := e.evalValue(exprVal, vars, env)
				if err != nil {
					return control{}, wrapStep(err, st.name)
				}
				vars[varName] = v
			}
		}
		return control{}, nil

	case has(body, "call"):
		return e.execCall(ctx, st, vars, env)

	case has(body, "switch"):
		return e.execSwitch(ctx, st, vars, env, last)

	case has(body, "for"):
		return e.execFor(ctx, st, vars, env, last)

	case has(body, "try"):
		return e.execTry(ctx, st, vars, env, last)

	case has(body, "raise"):
		v, err := e.evalValue(body["raise"], vars, env)
		if err != nil {
			return control{}, wrapStep(err, st.name)
		}
		return control{}, &Error{Message: stringify(v), Step: st.name}

	case has(body, "next"):
		target, ok := body["next"].(string)
		if !ok {
			return control{}, stepError(st.name, "next must name a target step (string)")
		}
		return nextControl(target), nil

	case has(body, "steps"):
		nested, err := parseSteps(body["steps"])
		if err != nil {
			return control{}, stepError(st.name, err.Error())
		}
		return e.runSteps(ctx, nested, vars, env, last)

	default:
		return control{}, stepError(st.name, "unsupported step: no recognized operation key in "+stepKeys(body))
	}
}

// execSwitch runs the first matching branch (or the trailing default).
func (e *Engine) execSwitch(ctx context.Context, st step, vars map[string]any, env expr.Env, last *string) (control, error) {
	branches, ok := st.body["switch"].([]any)
	if !ok {
		return control{}, stepError(st.name, "switch must be a list of branches")
	}
	for _, b := range branches {
		bm, ok := b.(map[string]any)
		if !ok {
			return control{}, stepError(st.name, "switch branch must be a map")
		}
		condRaw, hasCond := bm["condition"]
		if hasCond {
			cv, err := e.evalValue(condRaw, vars, env)
			if err != nil {
				return control{}, wrapStep(err, st.name)
			}
			if !truthy(cv) {
				continue
			}
		}
		// matched (or default): run steps or jump next.
		return e.runBranch(ctx, st.name, bm, vars, env, last)
	}
	// GCP no-ops (continues with the following step) when no branch matches and
	// no default is present; the emulator deliberately fails loud here
	// (documented limitation).
	return control{}, stepError(st.name, "no switch branch matched and no default branch present")
}

// runBranch executes a switch/for branch's steps or next.
func (e *Engine) runBranch(ctx context.Context, stepName string, bm map[string]any, vars map[string]any, env expr.Env, last *string) (control, error) {
	if stepsRaw, ok := bm["steps"]; ok {
		nested, err := parseSteps(stepsRaw)
		if err != nil {
			return control{}, stepError(stepName, err.Error())
		}
		return e.runSteps(ctx, nested, vars, env, last)
	}
	if nextRaw, ok := bm["next"]; ok {
		target, ok := nextRaw.(string)
		if !ok {
			return control{}, stepError(stepName, "branch next must name a target step (string)")
		}
		return nextControl(target), nil
	}
	return control{}, stepError(stepName, "branch must define 'steps' or 'next'")
}

// nextControl maps a next target to a control signal, translating the special
// loop-control targets break/continue into their own kinds so execFor can
// intercept them.
func nextControl(target string) control {
	switch target {
	case "break":
		return control{kind: "break"}
	case "continue":
		return control{kind: "continue"}
	default:
		return control{kind: "next", target: target}
	}
}

// execFor iterates a list (in) or an integer range, binding value/index vars.
func (e *Engine) execFor(ctx context.Context, st step, vars map[string]any, env expr.Env, last *string) (control, error) {
	fm, ok := st.body["for"].(map[string]any)
	if !ok {
		return control{}, stepError(st.name, "for must be a map")
	}
	stepsRaw, ok := fm["steps"]
	if !ok {
		return control{}, stepError(st.name, "for must define 'steps'")
	}
	nested, err := parseSteps(stepsRaw)
	if err != nil {
		return control{}, stepError(st.name, err.Error())
	}
	valueVar, _ := fm["value"].(string)
	indexVar, _ := fm["index"].(string)

	var items []any
	if inRaw, ok := fm["in"]; ok {
		iv, err := e.evalValue(inRaw, vars, env)
		if err != nil {
			return control{}, wrapStep(err, st.name)
		}
		switch c := iv.(type) {
		case []any:
			items = c
		case map[string]any:
			// iterate map keys (GCP iterates a map's keys in sorted order).
			keys := make([]string, 0, len(c))
			for k := range c {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				items = append(items, k)
			}
		default:
			return control{}, stepError(st.name, "for 'in' must iterate a list or map")
		}
	} else if rangeRaw, ok := fm["range"]; ok {
		rv, err := e.evalValue(rangeRaw, vars, env)
		if err != nil {
			return control{}, wrapStep(err, st.name)
		}
		items, err = rangeItems(rv)
		if err != nil {
			return control{}, stepError(st.name, err.Error())
		}
	} else {
		return control{}, stepError(st.name, "for must define 'in' or 'range'")
	}

	for i, item := range items {
		if valueVar != "" {
			vars[valueVar] = item
		}
		if indexVar != "" {
			vars[indexVar] = int64(i)
		}
		c, err := e.runSteps(ctx, nested, vars, env, last)
		if err != nil {
			return control{}, err
		}
		switch c.kind {
		case "break":
			// exit the loop; continue with the step after the for
			return control{}, nil
		case "continue":
			// jump to the next iteration
			continue
		case "return", "next":
			// propagate return and named next (target outside this loop)
			return c, nil
		}
	}
	return control{}, nil
}

// rangeItems expands [start, stop] or [start, stop, step] into integer items.
func rangeItems(rv any) ([]any, error) {
	list, ok := rv.([]any)
	if !ok || len(list) < 2 || len(list) > 3 {
		return nil, fmt.Errorf("for 'range' must be [start, stop] or [start, stop, step]")
	}
	vals := make([]int64, len(list))
	for i, v := range list {
		switch t := v.(type) {
		case int64:
			vals[i] = t
		case float64:
			vals[i] = int64(t)
		default:
			return nil, fmt.Errorf("for 'range' bounds must be integers")
		}
	}
	start, stop := vals[0], vals[1]
	step := int64(1)
	if len(list) == 3 {
		step = vals[2]
		if step == 0 {
			return nil, fmt.Errorf("for 'range' step must be non-zero")
		}
	}
	var out []any
	if step > 0 {
		for i := start; i < stop; i += step {
			out = append(out, i)
		}
	} else {
		for i := start; i > stop; i += step {
			out = append(out, i)
		}
	}
	return out, nil
}

// execTry runs try steps with optional retry and except handling.
func (e *Engine) execTry(ctx context.Context, st step, vars map[string]any, env expr.Env, last *string) (control, error) {
	tryRaw, ok := st.body["try"].(map[string]any)
	if !ok {
		return control{}, stepError(st.name, "try must be a map with 'steps'")
	}
	trySteps, err := parseSteps(tryRaw["steps"])
	if err != nil {
		return control{}, stepError(st.name, err.Error())
	}

	maxRetries := 0
	backoff := backoffSpec{initial: time.Second, max: 60 * time.Second, multiplier: 2}
	if retryRaw, present := st.body["retry"]; present && retryRaw != nil {
		rm, ok := retryRaw.(map[string]any)
		if !ok {
			return control{}, stepError(st.name, "retry must be a map")
		}
		if _, hasPred := rm["predicate"]; hasPred {
			return control{}, stepError(st.name, "retry 'predicate' is not supported")
		}
		if mr, ok := rm["max_retries"].(int64); ok {
			maxRetries = int(mr)
		} else if mr, ok := rm["max_retries"].(float64); ok {
			maxRetries = int(mr)
		}
		if bk, ok := rm["backoff"].(map[string]any); ok {
			backoff = backoff.fromMap(bk)
		}
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			if err := e.sleep(ctx, backoff.delay(attempt-1)); err != nil {
				return control{}, &Error{Message: "execution interrupted: " + err.Error(), Step: st.name}
			}
		}
		c, err := e.runSteps(ctx, trySteps, vars, env, last)
		if err == nil {
			return c, nil // success (return/next/break/continue/ok)
		}
		lastErr = err
	}
	// Exhausted retries.
	if exceptRaw, present := st.body["except"]; present && exceptRaw != nil {
		em, ok := exceptRaw.(map[string]any)
		if !ok {
			return control{}, stepError(st.name, "except must be a map")
		}
		if asVar, ok := em["as"].(string); ok && asVar != "" {
			vars[asVar] = errorValue(lastErr)
		}
		exceptSteps, err := parseSteps(em["steps"])
		if err != nil {
			return control{}, stepError(st.name, err.Error())
		}
		return e.runSteps(ctx, exceptSteps, vars, env, last)
	}
	if lastErr != nil {
		return control{}, lastErr
	}
	return control{}, stepError(st.name, "try failed with no error")
}

type backoffSpec struct {
	initial    time.Duration
	max        time.Duration
	multiplier float64
}

func (b backoffSpec) fromMap(m map[string]any) backoffSpec {
	if v, ok := m["initial_delay"].(int64); ok {
		b.initial = time.Duration(v) * time.Second
	} else if v, ok := m["initial_delay"].(float64); ok {
		b.initial = time.Duration(v * float64(time.Second))
	}
	if v, ok := m["max_delay"].(int64); ok {
		b.max = time.Duration(v) * time.Second
	} else if v, ok := m["max_delay"].(float64); ok {
		b.max = time.Duration(v * float64(time.Second))
	}
	if v, ok := m["multiplier"].(float64); ok {
		b.multiplier = v
	}
	return b
}

func (b backoffSpec) delay(attempt int) time.Duration {
	d := b.initial
	for i := 0; i < attempt; i++ {
		d = time.Duration(float64(d) * b.multiplier)
		if d > b.max {
			d = b.max
		}
	}
	if d > b.max {
		d = b.max
	}
	return d
}

// errorValue renders an error as the value bound to an except 'as' variable.
func errorValue(err error) map[string]any {
	m := map[string]any{"message": err.Error()}
	if we, ok := err.(*Error); ok && we.Step != "" {
		m["tags"] = []any{we.Step}
	}
	return m
}

// execCall invokes a callable (http.* or sys.*) and binds the result.
func (e *Engine) execCall(ctx context.Context, st step, vars map[string]any, env expr.Env) (control, error) {
	fn, ok := st.body["call"].(string)
	if !ok {
		return control{}, stepError(st.name, "call must name a function (string)")
	}
	var args map[string]any
	if a, present := st.body["args"]; present && a != nil {
		am, ok := a.(map[string]any)
		if !ok {
			return control{}, stepError(st.name, "call args must be a map")
		}
		args = am
	}
	// Evaluate args (each value is an expression).
	evaluated := make(map[string]any, len(args))
	for k, v := range args {
		ev, err := e.evalValue(v, vars, env)
		if err != nil {
			return control{}, wrapStep(err, st.name)
		}
		evaluated[k] = ev
	}

	var result any
	switch fn {
	case "http.get", "http.post", "http.request":
		resp, err := e.doHTTP(ctx, fn, evaluated)
		if err != nil {
			return control{}, &Error{Message: err.Error(), Step: st.name}
		}
		result = resp
	case "sys.log":
		e.log(evaluated)
		result = nil
	case "sys.sleep":
		if err := e.doSleep(ctx, evaluated); err != nil {
			return control{}, &Error{Message: "sys.sleep failed: " + err.Error(), Step: st.name}
		}
		result = nil
	default:
		return control{}, stepError(st.name, "unsupported call function "+strconv.Quote(fn))
	}

	if resultVar, ok := st.body["result"].(string); ok && resultVar != "" {
		vars[resultVar] = result
	}
	return control{}, nil
}

// doHTTP performs an http.get/post/request call and returns {body, code, headers}.
func (e *Engine) doHTTP(ctx context.Context, fn string, args map[string]any) (map[string]any, error) {
	rawURL, _ := args["url"].(string)
	if rawURL == "" {
		return nil, fmt.Errorf("%s requires a 'url' argument", fn)
	}
	method := ""
	switch fn {
	case "http.get":
		method = http.MethodGet
	case "http.post":
		method = http.MethodPost
	case "http.request":
		method, _ = args["method"].(string)
		if method == "" {
			return nil, fmt.Errorf("http.request requires a 'method' argument")
		}
	}
	method = strings.ToUpper(method)

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %v", err)
	}
	if q, ok := args["query"].(map[string]any); ok {
		qv := u.Query()
		for k, v := range q {
			qv.Set(k, stringify(v))
		}
		u.RawQuery = qv.Encode()
	}

	var body io.Reader
	contentType := ""
	if b, present := args["body"]; present && b != nil {
		body, contentType = encodeBody(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if h, ok := args["headers"].(map[string]any); ok {
		for k, v := range h {
			req.Header.Set(k, stringify(v))
		}
	}

	client := e.client
	if t, ok := args["timeout"].(int64); ok && t > 0 {
		c := *e.client
		c.Timeout = time.Duration(t) * time.Second
		client = &c
	} else if t, ok := args["timeout"].(float64); ok && t > 0 {
		c := *e.client
		c.Timeout = time.Duration(t * float64(time.Second))
		client = &c
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s failed: %v", fn, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	headers := map[string]any{}
	for k, vs := range resp.Header {
		if len(vs) > 0 {
			headers[k] = vs[0]
		}
	}

	code := int64(resp.StatusCode)
	if code < 200 || code >= 300 {
		return nil, fmt.Errorf("%s returned status %d", fn, code)
	}

	parsed := parseHTTPBody(respBody, resp.Header.Get("Content-Type"))
	return map[string]any{
		"body":    parsed,
		"code":    code,
		"headers": headers,
	}, nil
}

// encodeBody serializes a structured body to JSON; strings are sent raw.
func encodeBody(b any) (io.Reader, string) {
	switch t := b.(type) {
	case string:
		return strings.NewReader(t), "text/plain"
	case map[string]any, []any:
		data, _ := json.Marshal(b)
		return bytes.NewReader(data), "application/json"
	default:
		return strings.NewReader(stringify(t)), "text/plain"
	}
}

// parseHTTPBody decodes a JSON response body when the content type is JSON,
// else returns the raw string.
func parseHTTPBody(body []byte, contentType string) any {
	if strings.Contains(contentType, "json") {
		var v any
		if err := json.Unmarshal(body, &v); err == nil {
			return normalize(v)
		}
	}
	return string(body)
}

// doSleep implements sys.sleep (seconds argument).
func (e *Engine) doSleep(ctx context.Context, args map[string]any) error {
	var seconds float64
	switch t := args["seconds"].(type) {
	case int64:
		seconds = float64(t)
	case float64:
		seconds = t
	default:
		return fmt.Errorf("sys.sleep requires a numeric 'seconds' argument")
	}
	return e.sleep(ctx, time.Duration(seconds*float64(time.Second)))
}

// log implements sys.log (text + optional severity). Output goes to stderr
// (no structured sink in the emulator).
func (e *Engine) log(args map[string]any) {
	text := stringify(args["text"])
	severity := "INFO"
	if s, ok := args["severity"].(string); ok && s != "" {
		severity = s
	}
	fmt.Fprintf(os.Stderr, "[workflows] %s: %s\n", severity, text)
}

// evalValue evaluates a step value: strings are interpolated (${...}), maps and
// lists are recursed, and scalar literals pass through.
func (e *Engine) evalValue(v any, vars map[string]any, env expr.Env) (any, error) {
	switch t := v.(type) {
	case string:
		return expr.Interpolate(t, vars, env)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			rv, err := e.evalValue(vv, vars, env)
			if err != nil {
				return nil, err
			}
			out[k] = rv
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			rv, err := e.evalValue(vv, vars, env)
			if err != nil {
				return nil, err
			}
			out[i] = rv
		}
		return out, nil
	default:
		return t, nil
	}
}

func has(m map[string]any, k string) bool { _, ok := m[k]; return ok }

func stepKeys(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}

func stepError(step, msg string) error {
	return &Error{Message: fmt.Sprintf("step %q: %s", step, msg), Step: step}
}

func wrapStep(err error, step string) error {
	return &Error{Message: fmt.Sprintf("step %q: %s", step, err.Error()), Step: step}
}

func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case int64:
		return t != 0
	case float64:
		return t != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

func stringify(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// normalize converts YAML-decoded values (int, []interface{},
// map[string]interface{}, float32) into the JSON-compatible types the
// expression engine operates on.
func normalize(v any) any {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case uint:
		return int64(t)
	case uint64:
		return int64(t)
	case float32:
		return float64(t)
	case float64:
		return t
	case []interface{}:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = normalize(e)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = normalize(e)
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[fmt.Sprintf("%v", k)] = normalize(e)
		}
		return out
	default:
		return v
	}
}
