package pubsubfilter

import "testing"

func TestCompileAndMatch(t *testing.T) {
	cases := []struct {
		filter string
		attrs  map[string]string
		want   bool
	}{
		{`attributes.event_type = "ocr-invoice"`, map[string]string{"event_type": "ocr-invoice"}, true},
		{`attributes.event_type = "ocr-invoice"`, map[string]string{"event_type": "portal.upload"}, false},
		{`attributes.event_type = "ocr-invoice"`, nil, false},
		{`attributes:event_type != "ocr-invoice"`, map[string]string{"event_type": "x"}, true},
		{`attributes:event_type != "ocr-invoice"`, map[string]string{"event_type": "ocr-invoice"}, false},
		{`hasPrefix(attributes.event_type, "portal.")`, map[string]string{"event_type": "portal.upload"}, true},
		{`hasPrefix(attributes.event_type, "portal.")`, map[string]string{"event_type": "ocr-invoice"}, false},
		{`attributes.a = "1" AND attributes.b = "2"`, map[string]string{"a": "1", "b": "2"}, true},
		{`attributes.a = "1" AND attributes.b = "2"`, map[string]string{"a": "1"}, false},
		{`attributes.a = "1" OR attributes.b = "2"`, map[string]string{"b": "2"}, true},
		{`NOT attributes.a = "1"`, map[string]string{"a": "2"}, true},
		{`NOT attributes.a = "1"`, map[string]string{"a": "1"}, false},
		{`(attributes.a = "1" OR attributes.b = "2") AND attributes.c = "3"`, map[string]string{"a": "1", "c": "3"}, true},
		{`attributes.a = "1" AND (attributes.b = "2" OR attributes.c = "3")`, map[string]string{"a": "1", "c": "3"}, true},
	}
	for _, c := range cases {
		f, err := Compile(c.filter)
		if err != nil {
			t.Fatalf("Compile(%q): %v", c.filter, err)
		}
		if got := f.Match(c.attrs); got != c.want {
			t.Errorf("Compile(%q).Match(%v) = %v, want %v", c.filter, c.attrs, got, c.want)
		}
	}
}

func TestCompileEmptyMatchesAll(t *testing.T) {
	f, err := Compile("")
	if err != nil {
		t.Fatalf("Compile empty: %v", err)
	}
	if f != nil {
		t.Fatalf("expected nil filter for empty expression")
	}
	if !f.Match(map[string]string{"a": "b"}) {
		t.Error("nil filter should match everything")
	}
}

func TestCompileInvalid(t *testing.T) {
	for _, expr := range []string{
		"this is not a filter (((",
		`attributes.event_type`,
		`attributes.event_type = unquoted`,
		`hasPrefix(attributes.event_type)`,
		`hasPrefix(attributes.event_type, "x"`,
		`attributes. = "x"`,
		`attributes.a == "x"`,
	} {
		if _, err := Compile(expr); err == nil {
			t.Errorf("Compile(%q) succeeded, want error", expr)
		}
	}
}
