package queryengine

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func mustExecErr(t *testing.T, q string) error {
	t.Helper()
	_, err := Execute(context.Background(), seedCatalog(), Request{
		Project: "p", Query: q, DefaultProject: "p", DefaultDataset: "ds",
	})
	if err == nil {
		t.Fatalf("Execute(%q) succeeded, want an error", q)
	}
	return err
}

// TestDivisionByZeroErrors pins BQ10's arithmetic half: SQLite's NULL result is
// intercepted and surfaced as BigQuery's invalidQuery division error.
func TestDivisionByZeroErrors(t *testing.T) {
	for _, q := range []string{
		"SELECT 1/0",
		"SELECT 1.0/0.0",
		"SELECT age/0 FROM `p.ds.people`",
		"SELECT age/(age-age) FROM `p.ds.people`",
	} {
		err := mustExecErr(t, q)
		if !errors.Is(err, ErrUnsupported) {
			t.Errorf("%q error = %v, want ErrUnsupported", q, err)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "division by zero") {
			t.Errorf("%q error = %v, want a division-by-zero reason", q, err)
		}
	}

	// NULL is not zero: BigQuery division by NULL yields NULL, not an error.
	res := exec(t, "SELECT 1/NULL AS x")
	if len(res.Rows) != 1 || res.Rows[0][0] != nil {
		t.Fatalf("1/NULL = %v, want NULL", res.Rows)
	}
	// ... and a valid denominator still divides as FLOAT64.
	res = exec(t, "SELECT 3/2 AS x")
	if len(res.Rows) != 1 || res.Rows[0][0] != 1.5 {
		t.Fatalf("3/2 = %v, want 1.5", res.Rows)
	}
}

// TestCrossTypeComparisonErrors pins BQ10's comparison half: SQLite's implicit
// affinity coercion must not turn a type error into a match.
func TestCrossTypeComparisonErrors(t *testing.T) {
	cases := []string{
		"SELECT id FROM `p.ds.people` WHERE id = '1'",
		"SELECT id FROM `p.ds.people` WHERE name = 1",
		"SELECT id FROM `p.ds.people` WHERE active = 1",
		"SELECT 1 = '1'",
		"SELECT 'alice' <> 1",
	}
	for _, q := range cases {
		err := mustExecErr(t, q)
		if !strings.Contains(err.Error(), "No matching signature for operator") {
			t.Errorf("%q error = %v, want a BigQuery signature error", q, err)
		}
	}

	// The same-type and numeric-family comparisons BigQuery permits still run.
	for _, q := range []string{
		"SELECT id FROM `p.ds.people` WHERE id = 1",
		"SELECT id FROM `p.ds.people` WHERE id = 1.0",
		"SELECT id FROM `p.ds.people` WHERE name = 'alice'",
		"SELECT id FROM `p.ds.people` WHERE id = CAST('1' AS INT64)",
		"SELECT id FROM `p.ds.people` WHERE active = TRUE",
		"SELECT id FROM `p.ds.people` WHERE id = NULL",
	} {
		if _, err := Execute(context.Background(), seedCatalog(), Request{
			Project: "p", Query: q, DefaultProject: "p", DefaultDataset: "ds",
		}); err != nil {
			t.Errorf("Execute(%q): %v", q, err)
		}
	}
}

// TestCrossTypeArithmeticErrors covers the arithmetic side of the coercion
// guard: BigQuery arithmetic is numeric-only.
func TestCrossTypeArithmeticErrors(t *testing.T) {
	for _, q := range []string{
		"SELECT id + '1' FROM `p.ds.people`",
		"SELECT name * 2 FROM `p.ds.people`",
		"SELECT 'alice' / 2",
		"SELECT active - 1 FROM `p.ds.people`",
	} {
		err := mustExecErr(t, q)
		if !strings.Contains(err.Error(), "No matching signature for operator") {
			t.Errorf("%q error = %v, want a BigQuery signature error", q, err)
		}
	}
	for _, q := range []string{
		"SELECT id + 1 FROM `p.ds.people`",
		"SELECT score * 2 FROM `p.ds.people`",
		"SELECT 1 + NULL",
	} {
		if _, err := Execute(context.Background(), seedCatalog(), Request{
			Project: "p", Query: q, DefaultProject: "p", DefaultDataset: "ds",
		}); err != nil {
			t.Errorf("Execute(%q): %v", q, err)
		}
	}
}

// TestDivisionGuardDoesNotChangePrecedence checks the injected denominator
// guard preserves left associativity and surrounding arithmetic.
func TestDivisionGuardDoesNotChangePrecedence(t *testing.T) {
	res := exec(t, "SELECT 8/2/2 AS x")
	if res.Rows[0][0] != 2.0 {
		t.Fatalf("8/2/2 = %v, want 2.0", res.Rows[0][0])
	}
	res = exec(t, "SELECT 8/(2/2) AS x")
	if res.Rows[0][0] != 8.0 {
		t.Fatalf("8/(2/2) = %v, want 8.0", res.Rows[0][0])
	}
	res = exec(t, "SELECT 1 + 8/4 * 3 AS x")
	if res.Rows[0][0] != 7.0 {
		t.Fatalf("1 + 8/4 * 3 = %v, want 7.0", res.Rows[0][0])
	}
}

// TestTemporalStringLiteralCoercion keeps GoogleSQL's documented literal
// coercion: a string literal may compare with a temporal column, but a number
// may not and a STRING column is still not implicitly temporal.
func TestTemporalStringLiteralCoercion(t *testing.T) {
	for _, q := range []string{
		"SELECT d FROM `p.ds.typed` WHERE d = '2026-01-02'",
		"SELECT d FROM `p.ds.typed` WHERE '2026-01-02' < d",
	} {
		if _, err := Execute(context.Background(), specialCatalog(), Request{
			Project: "p", Query: q, DefaultDataset: "ds",
		}); err != nil {
			t.Errorf("Execute(%q): %v", q, err)
		}
	}
	if err := func() error {
		_, err := Execute(context.Background(), specialCatalog(), Request{
			Project: "p", Query: "SELECT d FROM `p.ds.typed` WHERE d = 1", DefaultDataset: "ds",
		})
		return err
	}(); err == nil || !strings.Contains(err.Error(), "No matching signature") {
		t.Errorf("DATE = INT64 error = %v, want a BigQuery signature error", err)
	}
}
