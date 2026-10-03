package engine

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func noopSleep() Option {
	return WithSleep(func(ctx context.Context, d time.Duration) error { return nil })
}

func run(t *testing.T, source string, arg any, opts ...Option) Result {
	t.Helper()
	e := New(append([]Option{noopSleep()}, opts...)...)
	return e.Execute(context.Background(), source, arg)
}

func TestAssignAndReturn(t *testing.T) {
	res := run(t, `
main:
  steps:
    - init:
        assign:
          - x: 5
          - y: ${x + 3}
    - done:
        return: ${y}
`, nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != int64(8) {
		t.Fatalf("expected 8, got %v (%T)", res.Value, res.Value)
	}
}

func TestParams(t *testing.T) {
	res := run(t, `
main:
  params: [name]
  steps:
    - greet:
        return: ${"Hello " + name}
`, map[string]any{"name": "world"})
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != "Hello world" {
		t.Fatalf("expected greeting, got %v", res.Value)
	}
}

func TestReturnLiteral(t *testing.T) {
	res := run(t, `
main:
  steps:
    - r:
        return: 42
`, nil)
	if res.Value != int64(42) {
		t.Fatalf("expected 42, got %v", res.Value)
	}
}

func TestSwitchNextAndDefault(t *testing.T) {
	source := `
main:
  params: [n]
  steps:
    - check:
        switch:
          - condition: ${n < 0}
            next: neg
          - condition: ${n == 0}
            steps:
              - z:
                  return: "zero"
          - next: pos
    - unreachable:
        return: "unreachable"
    - pos:
        return: "positive"
    - neg:
        return: "negative"
`
	if res := run(t, source, map[string]any{"n": -5}); res.Value != "negative" {
		t.Fatalf("neg case: %v", res.Value)
	}
	if res := run(t, source, map[string]any{"n": 0}); res.Value != "zero" {
		t.Fatalf("zero case: %v", res.Value)
	}
	if res := run(t, source, map[string]any{"n": 5}); res.Value != "positive" {
		t.Fatalf("default case: %v", res.Value)
	}
}

func TestForInList(t *testing.T) {
	res := run(t, `
main:
  steps:
    - init:
        assign:
          - total: 0
    - loop:
        for:
          value: v
          in: [1, 2, 3, 4]
          steps:
            - add:
                assign:
                  - total: ${total + v}
    - done:
        return: ${total}
`, nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != int64(10) {
		t.Fatalf("expected 10, got %v", res.Value)
	}
}

func TestForRange(t *testing.T) {
	res := run(t, `
main:
  steps:
    - init:
        assign:
          - total: 0
    - loop:
        for:
          value: i
          range: [1, 5, 2]
          steps:
            - add:
                assign:
                  - total: ${total + i}
    - done:
        return: ${total}
`, nil)
	// 1 + 3 = 4
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != int64(4) {
		t.Fatalf("expected 4, got %v", res.Value)
	}
}

func TestForInMapSortedKeys(t *testing.T) {
	// for … in <map> iterates keys in sorted order (deterministic, matches GCP).
	res := run(t, `
main:
  steps:
    - init:
        assign:
          - keys: ""
    - loop:
        for:
          value: k
          in: {"z": 1, "a": 2, "m": 3}
          steps:
            - add:
                assign:
                  - keys: ${keys + k}
    - done:
        return: ${keys}
`, nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != "amz" {
		t.Fatalf("expected sorted keys 'amz', got %v", res.Value)
	}
}

func TestForBreak(t *testing.T) {
	// break exits the loop and continues with the step after the for.
	res := run(t, `
main:
  steps:
    - init:
        assign:
          - total: 0
    - loop:
        for:
          value: v
          in: [1, 2, 3, 4, 5]
          steps:
            - check:
                switch:
                  - condition: ${v == 3}
                    next: break
                  - next: add
            - add:
                assign:
                  - total: ${total + v}
    - done:
        return: ${total}
`, nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	// 1 + 2 then break on 3
	if res.Value != int64(3) {
		t.Fatalf("expected 3, got %v", res.Value)
	}
}

func TestForContinue(t *testing.T) {
	// continue jumps to the next iteration, skipping the rest of the body.
	res := run(t, `
main:
  steps:
    - init:
        assign:
          - total: 0
    - loop:
        for:
          value: v
          in: [1, 2, 3, 4, 5]
          steps:
            - skipEvens:
                switch:
                  - condition: ${v % 2 == 0}
                    next: continue
                  - next: add
            - add:
                assign:
                  - total: ${total + v}
    - done:
        return: ${total}
`, nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	// 1 + 3 + 5 = 9 (evens skipped)
	if res.Value != int64(9) {
		t.Fatalf("expected 9, got %v", res.Value)
	}
}

func TestBuiltinEnvVars(t *testing.T) {
	e := New(noopSleep())
	res := e.ExecuteWithContext(context.Background(), `
main:
  steps:
    - done:
        return: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID") + ":" + sys.get_env("GOOGLE_CLOUD_LOCATION") + ":" + sys.get_env("GOOGLE_CLOUD_WORKFLOW_ID") + ":" + sys.get_env("GOOGLE_CLOUD_WORKFLOW_REVISION_ID")}
`, nil, WorkflowContext{
		ProjectID:  "proj-123",
		Location:   "us-central1",
		WorkflowID: "wf1",
		RevisionID: "000001-a4d",
	})
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != "proj-123:us-central1:wf1:000001-a4d" {
		t.Fatalf("unexpected built-in resolution: %v", res.Value)
	}
}

func TestTryExceptRaise(t *testing.T) {
	res := run(t, `
main:
  steps:
    - attempt:
        try:
          steps:
            - boom:
                raise: "something broke"
        except:
          as: e
          steps:
            - recover:
                return: ${e.message}
`, nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != "something broke" {
		t.Fatalf("expected recovered message, got %v", res.Value)
	}
}

func TestTryRetryExhaustsAndRaises(t *testing.T) {
	// No except → the error propagates after retries are exhausted.
	res := run(t, `
main:
  steps:
    - attempt:
        try:
          steps:
            - boom:
                raise: "always fails"
        retry:
          max_retries: 2
          backoff:
            initial_delay: 1
            max_delay: 10
            multiplier: 2
`, nil)
	if res.Err == nil {
		t.Fatal("expected error after retries exhausted")
	}
	if _, ok := res.Err.(*Error); !ok {
		t.Fatalf("expected *Error, got %T", res.Err)
	}
}

func TestTryRetryThenExcept(t *testing.T) {
	res := run(t, `
main:
  steps:
    - attempt:
        try:
          steps:
            - boom:
                raise: "persistent"
        retry:
          max_retries: 1
        except:
          as: e
          steps:
            - ok:
                return: ${"recovered:" + e.message}
`, nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != "recovered:persistent" {
		t.Fatalf("unexpected value: %v", res.Value)
	}
}

func TestHTTPGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok": true}`))
	}))
	defer srv.Close()

	res := run(t, `
main:
  steps:
    - callIt:
        call: http.get
        args:
          url: ${u}
        result: resp
    - done:
        return: ${resp.body.ok}
`, map[string]any{"u": srv.URL}, WithHTTPClient(srv.Client()))
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != true {
		t.Fatalf("expected parsed JSON body true, got %v", res.Value)
	}
}

func TestHTTPPostJSON(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code": 201}`))
	}))
	defer srv.Close()

	res := run(t, `
main:
  steps:
    - post:
        call: http.post
        args:
          url: ${u}
          body:
            hello: "world"
        result: resp
    - done:
        return: ${resp.code}
`, map[string]any{"u": srv.URL}, WithHTTPClient(srv.Client()))
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != int64(200) {
		t.Fatalf("expected 200, got %v", res.Value)
	}
	if gotBody != `{"hello":"world"}` {
		t.Fatalf("unexpected POST body: %q", gotBody)
	}
}

func TestHTTPNon2xxRaises(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	res := run(t, `
main:
  steps:
    - callIt:
        call: http.get
        args:
          url: ${u}
`, map[string]any{"u": srv.URL}, WithHTTPClient(srv.Client()))
	if res.Err == nil {
		t.Fatal("expected error for non-2xx response")
	}
	if _, ok := res.Err.(*Error); !ok {
		t.Fatalf("expected *Error, got %T", res.Err)
	}
}

func TestUnsupportedConstructFailsLoud(t *testing.T) {
	cases := []struct {
		name   string
		source string
	}{
		{"unknown step type", "main:\n  steps:\n    - x:\n        frobnicate: 1\n"},
		{"unknown call function", "main:\n  steps:\n    - x:\n        call: http.delete\n        args:\n          url: \"http://x\"\n"},
		{"unknown sys function in expr", "main:\n  steps:\n    - x:\n        assign:\n          - y: ${sys.nonexistent()}\n"},
		{"unsupported operator", "main:\n  steps:\n    - x:\n        return: ${1 ^ 2}\n"},
		{"retry predicate unsupported", "main:\n  steps:\n    - x:\n        try:\n          steps:\n            - a:\n                raise: \"e\"\n        retry:\n          predicate: ${true}\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := run(t, tc.source, nil)
			if res.Err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if _, ok := res.Err.(*Error); !ok {
				t.Fatalf("expected *Error (FAILED) for %s, got %T", tc.name, res.Err)
			}
		})
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name   string
		source string
	}{
		{"malformed yaml", "main:\n  steps: [unclosed"},
		{"missing main", "steps:\n  - x:\n      return: 1\n"},
		{"missing steps", "main:\n  params: [a]\n"},
		{"steps not a list", "main:\n  steps: not-a-list\n"},
		{"step not single-key", "main:\n  steps:\n    - a: 1\n      b: 2\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := run(t, tc.source, nil)
			if res.Err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			}
			var ve *ValidationError
			if !errors.As(res.Err, &ve) {
				t.Fatalf("expected *ValidationError for %s, got %T", tc.name, res.Err)
			}
		})
	}
}

func TestSysSleepAndLogSteps(t *testing.T) {
	slept := false
	e := New(WithSleep(func(ctx context.Context, d time.Duration) error {
		slept = true
		if d != 3*time.Second {
			t.Fatalf("expected 3s sleep, got %v", d)
		}
		return nil
	}))
	res := e.Execute(context.Background(), `
main:
  steps:
    - nap:
        call: sys.sleep
        args:
          seconds: 3
    - logit:
        call: sys.log
        args:
          text: "hello"
    - done:
        return: "ok"
`, nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if !slept {
		t.Fatal("sys.sleep did not sleep")
	}
	if res.Value != "ok" {
		t.Fatalf("expected ok, got %v", res.Value)
	}
}

func TestNullResult(t *testing.T) {
	// A workflow with no return step yields a null result.
	res := run(t, "main:\n  steps:\n    - x:\n        assign:\n          - y: 1\n", nil)
	if res.Err != nil {
		t.Fatalf("unexpected error: %v", res.Err)
	}
	if res.Value != nil {
		t.Fatalf("expected nil result, got %v", res.Value)
	}
}
