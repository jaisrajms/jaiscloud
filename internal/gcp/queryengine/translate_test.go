package queryengine

import (
	"strings"
	"testing"
)

func fixedResolver(t *testing.T) resolveFunc {
	t.Helper()
	known := map[string]string{
		"p.ds.t": "bq_t0",
		"ds.t":   "bq_t1",
		"t":      "bq_t2",
	}
	return func(ref string) (string, error) {
		if name, ok := known[ref]; ok {
			return name, nil
		}
		t.Fatalf("unexpected table ref %q", ref)
		return "", ErrTableNotFound
	}
}

func TestTranslateGolden(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"qualified table", "SELECT a FROM `p.ds.t`", "SELECT a FROM bq_t0"},
		{"dataset table", "SELECT a FROM ds.t", "SELECT a FROM bq_t1"},
		{"unqualified table", "SELECT a FROM t", "SELECT a FROM bq_t2"},
		{"float division", "SELECT 1/2", "SELECT 1 * 1.0 / 2"},
		{"precedence safe", "SELECT a/b*c", "SELECT a * 1.0 / b * c"},
		{"string literal slash", "SELECT 'a/b'", "SELECT 'a/b'"},
		{"string escape quote", `SELECT 'a\'b'`, "SELECT 'a''b'"},
		{"double quoted string", `SELECT "x"`, "SELECT 'x'"},
		{"string escape newline", `SELECT 'a\nb'`, "SELECT 'a" + "\n" + "b'"},
		{"line comment slash", "SELECT a -- / x\n/ b", "SELECT a * 1.0 / b"},
		{"block comment slash", "SELECT a /* / */ / b", "SELECT a * 1.0 / b"},
		{"cast int", "SELECT CAST(a AS INT64)", "SELECT CAST ( a AS INTEGER )"},
		{"cast numeric params", "SELECT CAST(a AS NUMERIC(10,2))", "SELECT CAST ( a AS REAL )"},
		{"join", "SELECT a FROM `p.ds.t` JOIN ds.t ON x = y", "SELECT a FROM bq_t0 JOIN bq_t1 ON x = y"},
		{"from list", "SELECT a FROM t, ds.t", "SELECT a FROM bq_t2 , bq_t1"},
		{"quoted column", "SELECT `my col` FROM t", `SELECT "my col" FROM bq_t2`},
		{"dotted quoted column", "SELECT `t`.`col` FROM t", `SELECT "t" . "col" FROM bq_t2`},
		{"mixed dotted ref", "SELECT a FROM ds.`t`", "SELECT a FROM bq_t1"},
		{"quoted dotted ref", "SELECT a FROM `p`.`ds`.`t`", "SELECT a FROM bq_t0"},
		{"mixed full ref", "SELECT a FROM `p.ds`.`t`", "SELECT a FROM bq_t0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := translate(tc.in, fixedResolver(t))
			if err != nil {
				t.Fatalf("translate(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("translate(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestTranslateRejects(t *testing.T) {
	cases := []string{
		"SELECT * FROM UNNEST([1,2])",
		"SELECT ARRAY<INT64>[1]",
		"SELECT STRUCT(1)",
		"CREATE TABLE x (a INT64)",
		"DELETE FROM t",
		"SELECT * FROM `p.ds.t` QUALIFY ROW_NUMBER() OVER (ORDER BY 1) = 1",
		"SELECT FARM_FINGERPRINT('x')",
		"SELECT DATE '2020-01-01'",
		"SELECT * FROM `p.ds.t` RIGHT JOIN ds.t ON a = b",
		"SELECT 1 EXCEPT SELECT 2",
		"SELECT * FROM `p.ds.t` PIVOT (SUM(a) FOR b IN ('x'))",
		"SELECT * FROM `p.ds.t` WHERE a IN (SELECT 1) OR GEOGRAPHY",
		"SELECT r'raw'",
		"SELECT b'bytes'",
		"SELECT @named_param",
		"SELECT ?",
		"SELECT '''triple'''",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			_, err := translate(q, fixedResolver(t))
			if err == nil {
				t.Fatalf("expected %q to be rejected", q)
			}
			if !strings.Contains(err.Error(), "not supported") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestTranslateCTEIsNotATable(t *testing.T) {
	resolve := func(ref string) (string, error) {
		if ref == "p.ds.t" {
			return "bq_t0", nil
		}
		return "", ErrTableNotFound
	}
	got, err := translate("WITH adults AS (SELECT * FROM `p.ds.t`) SELECT * FROM adults", resolve)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	want := "WITH adults AS ( SELECT * FROM bq_t0 ) SELECT * FROM adults"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestDecodeEscape(t *testing.T) {
	cases := []struct {
		in   string
		want string
		adv  int
		bad  bool
	}{
		{`\a`, "\a", 2, false},
		{`\b`, "\b", 2, false},
		{`\f`, "\f", 2, false},
		{`\n`, "\n", 2, false},
		{`\r`, "\r", 2, false},
		{`\t`, "\t", 2, false},
		{`\v`, "\v", 2, false},
		{`\\`, "\\", 2, false},
		{`\?`, "?", 2, false},
		{`\"`, `"`, 2, false},
		{`\'`, "'", 2, false},
		{"\\`", "`", 2, false},
		{"\\\n", "", 2, false},
		{`\x41`, "A", 4, false},
		{`\X41`, "A", 4, false},
		{`\101`, "A", 4, false},
		{`\u0041`, "A", 6, false},
		{`\U00000041`, "A", 10, false},
		{`\`, "", 0, true},
		{`\q`, "", 0, true},
		{`\x`, "", 0, true},
		{`\u12`, "", 0, true},
	}
	for _, c := range cases {
		got, adv, err := decodeEscape(c.in)
		if c.bad {
			if err == nil {
				t.Errorf("decodeEscape(%q): expected error", c.in)
			}
			continue
		}
		if err != nil || got != c.want || adv != c.adv {
			t.Errorf("decodeEscape(%q) = %q, %d, %v; want %q, %d", c.in, got, adv, err, c.want, c.adv)
		}
	}
}

func TestTokenizeRejectsUnterminated(t *testing.T) {
	for _, q := range []string{"SELECT 'unterminated", "SELECT `unterminated", "SELECT /* unterminated"} {
		if _, err := tokenize(q); err == nil {
			t.Fatalf("expected tokenize(%q) to fail", q)
		}
	}
}
