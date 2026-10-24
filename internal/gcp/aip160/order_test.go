package aip160

import (
	"testing"
	"time"
)

type orderItem struct {
	id    string
	state string
	t     time.Time
}

func testOrderSpec() OrderSpec[orderItem] {
	return NewOrderSpec(func(i orderItem) string { return i.id },
		OrderField[orderItem]{Name: "name", Value: func(i orderItem) string { return i.id }},
		OrderField[orderItem]{Name: "state", Value: func(i orderItem) string { return i.state }},
		OrderField[orderItem]{Name: "createTime", Value: func(i orderItem) string { return i.t.UTC().Format(TimeLayout) }},
	)
}

func TestOrderSpecParse(t *testing.T) {
	spec := testOrderSpec()

	// Empty orderBy is the default (tie ascending) order with no keys.
	o, err := spec.Parse("")
	if err != nil {
		t.Fatalf("empty orderBy: %v", err)
	}
	if len(o.keys) != 0 {
		t.Fatalf("empty orderBy produced %d keys", len(o.keys))
	}

	// A field with an explicit direction, and the proto snake_case spelling.
	o, err = spec.Parse("create_time desc, name")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(o.keys) != 2 || !o.keys[0].desc || o.keys[1].desc {
		t.Fatalf("parsed keys = %+v, want desc createTime then asc name", o.keys)
	}

	// An unknown field fails loud rather than silently returning default order.
	if _, err := spec.Parse("uid"); err == nil {
		t.Fatal("expected error for unsupported orderBy field")
	}
	// A bad direction fails loud.
	if _, err := spec.Parse("name sideways"); err == nil {
		t.Fatal("expected error for unsupported orderBy direction")
	}
	// An empty field between commas fails loud.
	if _, err := spec.Parse("name, ,state"); err == nil {
		t.Fatal("expected error for empty orderBy field")
	}
}

func TestOrderSortAndTiebreak(t *testing.T) {
	spec := testOrderSpec()
	base := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

	// Two items sharing createTime and one newer: desc must place the newer
	// first and break the tie by id ascending.
	items := []orderItem{
		{id: "b", state: "FAILED", t: base},
		{id: "a", state: "FAILED", t: base},
		{id: "c", state: "FAILED", t: base.Add(time.Hour)},
	}
	o, err := spec.Parse("createTime desc")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	o.Sort(items)
	if items[0].id != "c" || items[1].id != "a" || items[2].id != "b" {
		t.Fatalf("desc sort = %v", []string{items[0].id, items[1].id, items[2].id})
	}

	// Default order is by tie (id) ascending.
	o, _ = spec.Parse("")
	o.Sort(items)
	if items[0].id != "a" || items[1].id != "b" || items[2].id != "c" {
		t.Fatalf("default sort = %v", []string{items[0].id, items[1].id, items[2].id})
	}
}

func TestOrderCursorRoundTrip(t *testing.T) {
	spec := testOrderSpec()
	base := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	items := []orderItem{
		{id: "a", t: base.Add(time.Hour)},
		{id: "b", t: base},
		{id: "c", t: base.Add(2 * time.Hour)},
	}
	o, err := spec.Parse("createTime desc")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	o.Sort(items) // c, a, b

	cur := o.Cursor(items[0])
	if o.AfterCursor(items[0], cur) {
		t.Fatal("item must not sort after its own cursor")
	}
	if !o.AfterCursor(items[1], cur) {
		t.Fatal("later item must sort after the cursor")
	}
	if !o.AfterCursor(items[2], cur) {
		t.Fatal("last item must sort after the cursor")
	}

	// An undecodable / wrong-shape token is treated as "after" for everything,
	// so the scan restarts at the first item rather than resuming blindly.
	if !o.AfterCursor(items[0], "not-a-composite-token") {
		t.Fatal("uninterpretable token must restart the scan")
	}

	// The default order emits the bare tie key, so it stays compatible with
	// the id-based token every other GCP list produces.
	d, _ := spec.Parse("")
	if got := d.Cursor(items[0]); got != "c" {
		t.Fatalf("default cursor = %q, want the tie key", got)
	}
	if !d.AfterCursor(items[0], "b") || d.AfterCursor(items[1], "b") {
		t.Fatal("default AfterCursor must compare by tie key")
	}
}

func TestTimeLayoutIsFixedWidth(t *testing.T) {
	whole := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Format(TimeLayout)
	frac := time.Date(2026, 1, 2, 3, 4, 5, 100_000_000, time.UTC).Format(TimeLayout)
	if len(whole) != len(frac) {
		t.Fatalf("TimeLayout is not fixed width: %q vs %q", whole, frac)
	}
	if !(whole < frac) {
		t.Fatalf("textual order must be chronological: %q !< %q", whole, frac)
	}
}

func TestParseTime(t *testing.T) {
	cases := []struct {
		in   string
		want time.Time
	}{
		{"2026-01-02T03:04:05Z", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
		{"2026-01-02T03:04:05.100Z", time.Date(2026, 1, 2, 3, 4, 5, 100_000_000, time.UTC)},
		{"2026-01-02T03:04:05+01:00", time.Date(2026, 1, 2, 2, 4, 5, 0, time.UTC)},
		{"2026-01-02", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		got, err := ParseTime(tc.in)
		if err != nil {
			t.Errorf("ParseTime(%q): %v", tc.in, err)
			continue
		}
		if !got.Equal(tc.want) {
			t.Errorf("ParseTime(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	for _, bad := range []string{"", "yesterday", "2026-13-01", "03:04:05"} {
		if _, err := ParseTime(bad); err == nil {
			t.Errorf("ParseTime(%q) = nil error, want an error", bad)
		}
	}
}
