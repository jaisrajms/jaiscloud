package expr

import (
	"testing"
	"time"
)

func testEnv() Env {
	return Env{
		Getenv: func(k string) (string, bool) { return map[string]string{"FOO": "bar"}[k], k == "FOO" },
		Now:    func() time.Time { return time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC) },
	}
}

func evalStr(t *testing.T, s string) any {
	t.Helper()
	v, err := EvalString(s, map[string]any{}, testEnv())
	if err != nil {
		t.Fatalf("eval %q: %v", s, err)
	}
	return v
}

func TestLiterals(t *testing.T) {
	if v := evalStr(t, "42"); v != int64(42) {
		t.Errorf("int literal: %v (%T)", v, v)
	}
	if v := evalStr(t, "3.5"); v != 3.5 {
		t.Errorf("float literal: %v", v)
	}
	if v := evalStr(t, `"hello"`); v != "hello" {
		t.Errorf("string literal: %v", v)
	}
	if v := evalStr(t, "'single'"); v != "single" {
		t.Errorf("single-quote string: %v", v)
	}
	if v := evalStr(t, "true"); v != true {
		t.Errorf("bool literal: %v", v)
	}
	if v := evalStr(t, "null"); v != nil {
		t.Errorf("null literal: %v", v)
	}
}

func TestArithmetic(t *testing.T) {
	if v := evalStr(t, "2 + 3 * 4"); v != int64(14) {
		t.Errorf("precedence: %v", v)
	}
	if v := evalStr(t, "7 - 2"); v != int64(5) {
		t.Errorf("subtract: %v", v)
	}
	if v := evalStr(t, "10 / 4"); v != 2.5 {
		t.Errorf("divide: %v", v)
	}
	if v := evalStr(t, "10 % 4"); v != int64(2) {
		t.Errorf("modulo: %v", v)
	}
	if v := evalStr(t, "7 // 2"); v != int64(3) {
		t.Errorf("floor division: %v", v)
	}
	if v := evalStr(t, "7.5 // 2"); v != 3.0 {
		t.Errorf("float floor division: %v", v)
	}
	if v := evalStr(t, "10.5 % 3"); v != 1.5 {
		t.Errorf("float modulo: %v", v)
	}
	if v := evalStr(t, "-5"); v != int64(-5) {
		t.Errorf("unary minus: %v", v)
	}
	if v := evalStr(t, `"foo" + "bar"`); v != "foobar" {
		t.Errorf("string concat: %v", v)
	}
	if v := evalStr(t, `"n=" + 5`); v != "n=5" {
		t.Errorf("string+int concat: %v", v)
	}
}

func TestComparison(t *testing.T) {
	if v := evalStr(t, "1 < 2"); v != true {
		t.Errorf("<: %v", v)
	}
	if v := evalStr(t, "2 == 2.0"); v != true {
		t.Errorf("== int/float: %v", v)
	}
	if v := evalStr(t, "1 != 2"); v != true {
		t.Errorf("!=: %v", v)
	}
	if v := evalStr(t, "2 >= 3"); v != false {
		t.Errorf(">=: %v", v)
	}
	if v := evalStr(t, "true and false"); v != false {
		t.Errorf("and: %v", v)
	}
	if v := evalStr(t, "true or false"); v != true {
		t.Errorf("or: %v", v)
	}
	if v := evalStr(t, "not true"); v != false {
		t.Errorf("not: %v", v)
	}
	if v := evalStr(t, "2 in [1, 2, 3]"); v != true {
		t.Errorf("in list: %v", v)
	}
	if v := evalStr(t, `"a" in {"a": 1}`); v != true {
		t.Errorf("in map: %v", v)
	}
}

func TestNotPrecedence(t *testing.T) {
	// Per the Workflows expressions reference, `not` binds tighter than
	// comparison: `not 1 == 0` parses as `(not 1) == 0` (false), not
	// `not (1 == 0)` (which would be true).
	if v := evalStr(t, "not 1 == 0"); v != false {
		t.Errorf("not 1 == 0 should be (not 1) == 0 => false, got %v", v)
	}
	if v := evalStr(t, "not (1 == 0)"); v != true {
		t.Errorf("not (1 == 0) should be true, got %v", v)
	}
	// not not true == true
	if v := evalStr(t, "not not true"); v != true {
		t.Errorf("not not true: %v", v)
	}
	// not binds tighter than and: not true and false => (not true) and false => false
	if v := evalStr(t, "not true and false"); v != false {
		t.Errorf("not true and false: %v", v)
	}
}

func TestCollections(t *testing.T) {
	if v := evalStr(t, "[1, 2, 3]"); len(v.([]any)) != 3 {
		t.Errorf("list: %v", v)
	}
	m := evalStr(t, `{"a": 1, "b": 2}`).(map[string]any)
	if m["a"] != int64(1) {
		t.Errorf("map: %v", m)
	}
	// index and member access
	if v := evalStr(t, "[10, 20][1]"); v != int64(20) {
		t.Errorf("index: %v", v)
	}
	if v := evalStr(t, `{"a": 5}["a"]`); v != int64(5) {
		t.Errorf("map index: %v", v)
	}
}

func TestStdlib(t *testing.T) {
	if v := evalStr(t, `sys.get_env("FOO")`); v != "bar" {
		t.Errorf("get_env: %v", v)
	}
	if v := evalStr(t, `sys.get_env("MISSING", "dflt")`); v != "dflt" {
		t.Errorf("get_env default: %v", v)
	}
	// An unset variable with no default yields null, not an error.
	if v, err := EvalString(`sys.get_env("MISSING")`, map[string]any{}, testEnv()); err != nil || v != nil {
		t.Errorf("expected null for unset env with no default, got %v (err %v)", v, err)
	}
	if v := evalStr(t, "sys.now()"); v != "2024-01-02T03:04:05Z" {
		t.Errorf("now: %v", v)
	}
}

func TestGetEnvTypeErrors(t *testing.T) {
	// Non-string name or default must error (a TypeError in GCP).
	if _, err := EvalString(`sys.get_env(5)`, map[string]any{}, testEnv()); err == nil {
		t.Error("expected error for non-string get_env name")
	}
	if _, err := EvalString(`sys.get_env("FOO", 5)`, map[string]any{}, testEnv()); err == nil {
		t.Error("expected error for non-string get_env default")
	}
}

func TestGetEnvBuiltins(t *testing.T) {
	env := testEnv()
	env.Builtins = map[string]string{
		"GOOGLE_CLOUD_PROJECT_ID": "my-project",
		"GOOGLE_CLOUD_LOCATION":   "us-central1",
	}
	if v, err := EvalString(`sys.get_env("GOOGLE_CLOUD_PROJECT_ID")`, map[string]any{}, env); err != nil || v != "my-project" {
		t.Errorf("builtin project id: %v (err %v)", v, err)
	}
	// Built-ins shadow the process environment.
	if v, err := EvalString(`sys.get_env("GOOGLE_CLOUD_LOCATION")`, map[string]any{}, env); err != nil || v != "us-central1" {
		t.Errorf("builtin location: %v (err %v)", v, err)
	}
	// A built-in name that isn't populated still falls back to null.
	if v, err := EvalString(`sys.get_env("GOOGLE_CLOUD_WORKFLOW_ID")`, map[string]any{}, env); err != nil || v != nil {
		t.Errorf("unset builtin should be null, got %v (err %v)", v, err)
	}
}

func TestInterpolate(t *testing.T) {
	vars := map[string]any{"name": "world", "n": int64(5)}
	// Single expression returns typed value.
	v, err := Interpolate("${5 + 5}", vars, testEnv())
	if err != nil || v != int64(10) {
		t.Fatalf("single expr: %v %v", v, err)
	}
	// Mixed string interpolates.
	v, err = Interpolate("Hello ${name}!", vars, testEnv())
	if err != nil || v != "Hello world!" {
		t.Fatalf("mixed: %v %v", v, err)
	}
	// List value stringified.
	v, err = Interpolate("n=${n}", vars, testEnv())
	if err != nil || v != "n=5" {
		t.Fatalf("int interp: %v %v", v, err)
	}
	// No dollar → literal.
	v, err = Interpolate("plain", vars, testEnv())
	if err != nil || v != "plain" {
		t.Fatalf("literal: %v %v", v, err)
	}
	// $$ escape.
	v, err = Interpolate("cost $$5", vars, testEnv())
	if err != nil || v != "cost $5" {
		t.Fatalf("escape: %v %v", v, err)
	}
}

func TestUnsupportedOperatorFailsLoud(t *testing.T) {
	// The '^' operator is not part of the DSL; it must fail with a parse error.
	if _, err := EvalString("2 ^ 3", map[string]any{}, testEnv()); err == nil {
		t.Fatal("expected error for unsupported '^' operator")
	}
	if _, err := EvalString(`sys.nonexistent()`, map[string]any{}, testEnv()); err == nil {
		t.Fatal("expected error for unsupported sys function")
	}
	if _, err := EvalString("unknown_var", map[string]any{}, testEnv()); err == nil {
		t.Fatal("expected error for unknown variable")
	}
}
