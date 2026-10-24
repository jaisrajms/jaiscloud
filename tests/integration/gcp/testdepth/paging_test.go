package testdepth

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// page builds one page of a string-named list. An empty names list with a token
// is an empty non-terminal page.
func page(token string, names ...string) Page[string] {
	return Page[string]{Items: names, NextPageToken: token}
}

// strCfg is the canonical contract used by the paging tests: names are the
// resources themselves.
func strCfg(pageSize int64, order Ordering, expect ...string) Paging[string] {
	return Paging[string]{
		Name:     func(s string) string { return s },
		PageSize: pageSize,
		Order:    order,
		Expect:   expect,
	}
}

func TestCheckPagesClean(t *testing.T) {
	pages := []Page[string]{
		page("t2", "a", "b"),
		page("t3", "c", "d"),
		page("", "e"),
	}
	if got := CheckPages(pages, strCfg(2, OrderAscendingName, "a", "b", "c", "d", "e")); len(got) != 0 {
		t.Fatalf("a clean walk reported violations: %v", got)
	}
	// The same walk with no expected set and no documented order is clean too.
	if got := CheckPages(pages, strCfg(2, OrderUnspecified)); len(got) != 0 {
		t.Fatalf("an order-unspecified clean walk reported violations: %v", got)
	}
}

func TestCheckPagesSeededNegatives(t *testing.T) {
	cases := []struct {
		name   string
		pages  []Page[string]
		cfg    Paging[string]
		expect string
	}{
		{
			name:   "dropped resource across a page",
			pages:  []Page[string]{page("t2", "a", "b"), page("", "c")},
			cfg:    strCfg(2, OrderAscendingName, "a", "b", "c", "d"),
			expect: `expected resource "d" is missing from the walk`,
		},
		{
			name:   "duplicate resource across pages",
			pages:  []Page[string]{page("t2", "a", "b"), page("", "a", "c")},
			cfg:    strCfg(2, OrderAscendingName),
			expect: `resource "a" appears on more than one page (pages 1 and 2)`,
		},
		{
			name:   "mis-ordered page",
			pages:  []Page[string]{page("", "b", "a")},
			cfg:    strCfg(2, OrderAscendingName),
			expect: `page 1 is out of order: "a" comes after "b"`,
		},
		{
			// The DC17 regression: the misorder is between the last item of page 1
			// and the first item of page 2, so no within-page comparison can see it.
			name:   "order violation across a page boundary",
			pages:  []Page[string]{page("t2", "a", "c"), page("", "b", "d")},
			cfg:    strCfg(2, OrderAscendingName),
			expect: `page 2 is out of order across the page boundary: "b" comes after page 1's last item "c"`,
		},
		{
			name:   "order violation across a page boundary onto an empty page's successor",
			pages:  []Page[string]{page("t2", "a"), page("t3"), page("", "0")},
			cfg:    strCfg(1, OrderAscendingName),
			expect: `page 3 is out of order across the page boundary: "0" comes after page 1's last item "a"`,
		},
		{
			// A globally consistent mis-order (every walk descending) is a real
			// contract violation even though a second walk repeats it exactly, so
			// CheckPages — not CheckStable — must be the thing that bites.
			name:   "consistently descending walk",
			pages:  []Page[string]{page("t2", "c", "b"), page("", "a")},
			cfg:    strCfg(2, OrderAscendingName, "a", "b", "c"),
			expect: `page 2 is out of order across the page boundary: "a" comes after page 1's last item "b"`,
		},
		{
			name:   "page larger than the requested pageSize",
			pages:  []Page[string]{page("", "a", "b", "c")},
			cfg:    strCfg(2, OrderAscendingName),
			expect: `page 1 returned 3 items, more than the requested pageSize 2`,
		},
		{
			name:  "empty intermediate page when the service forbids one",
			pages: []Page[string]{page("t2", "a"), page("t3"), page("", "b")},
			cfg: func() Paging[string] {
				cfg := strCfg(2, OrderAscendingName)
				cfg.RequireNonEmptyPages = true
				return cfg
			}(),
			expect: `page 2 is empty but carries a non-terminal next page token "t3"`,
		},
		{
			name:   "final page still carries a token",
			pages:  []Page[string]{page("t2", "a"), page("t3", "b")},
			cfg:    strCfg(2, OrderAscendingName),
			expect: `the final page still carries next page token "t3"`,
		},
		{
			name:  "item outside the filtered set",
			pages: []Page[string]{page("", "a", "z")},
			cfg: Paging[string]{
				Name:     func(s string) string { return s },
				PageSize: 2,
				Filter:   "name:a",
				Include:  func(s string) bool { return s == "a" },
			},
			expect: `page 1 item "z" is outside the expected set for filter "name:a"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := strings.Join(CheckPages(tc.pages, tc.cfg), "\n")
			if !strings.Contains(got, tc.expect) {
				t.Fatalf("CheckPages reported %q, want it to contain %q", got, tc.expect)
			}
		})
	}
}

// TestCheckPagesAllowsAnEmptyIntermediatePage pins the AIP-158 rule the kit
// follows by default: a list method MAY return zero results on a page that is not
// the last, so the default contract must not flag it (only
// Paging.RequireNonEmptyPages, a service-specific opt-in, does).
func TestCheckPagesAllowsAnEmptyIntermediatePage(t *testing.T) {
	pages := []Page[string]{page("t2", "a"), page("t3"), page("", "b")}
	if got := CheckPages(pages, strCfg(2, OrderAscendingName)); len(got) != 0 {
		t.Fatalf("CheckPages reported %v, want AIP-158's zero-result mid-collection page to be allowed", got)
	}
	cfg := strCfg(2, OrderAscendingName)
	cfg.RequireNonEmptyPages = true
	if got := strings.Join(CheckPages(pages, cfg), "\n"); !strings.Contains(got, "is empty but carries a non-terminal next page token") {
		t.Fatalf("CheckPages reported %q, want the opt-in empty-page violation", got)
	}
}

func TestCheckPagesRequiresName(t *testing.T) {
	got := CheckPages([]Page[string]{page("", "a")}, Paging[string]{})
	if len(got) != 1 || !strings.Contains(got[0], "Paging.Name is required") {
		t.Fatalf("CheckPages = %v, want a single Name-required violation", got)
	}
}

func TestWalkTerminatesOnALoopingCursor(t *testing.T) {
	lister := func(ListRequest) (Page[string], error) {
		return page("always-the-same"), nil
	}
	_, err := Walk(lister, Paging[string]{Name: func(s string) string { return s }, MaxPages: 3})
	if err == nil || !strings.Contains(err.Error(), "did not terminate within 3 pages (last token \"always-the-same\")") {
		t.Fatalf("Walk error = %v, want a did-not-terminate error naming the cursor", err)
	}
}

func TestWalkThreadsTheRequest(t *testing.T) {
	var got []ListRequest
	names := []string{"a", "b", "c"}
	lister := func(req ListRequest) (Page[string], error) {
		got = append(got, req)
		i := 0
		if req.PageToken != "" {
			fmt.Sscanf(req.PageToken, "%d", &i)
		}
		end := i + int(req.PageSize)
		if end > len(names) || req.PageSize == 0 {
			end = len(names)
		}
		token := ""
		if end < len(names) {
			token = fmt.Sprint(end)
		}
		return page(token, names[i:end]...), nil
	}
	cfg := Paging[string]{
		Name:     func(s string) string { return s },
		PageSize: 1,
		Filter:   "name:depth-*",
		OrderBy:  "name",
		Expect:   names,
	}
	pages, err := Walk(lister, cfg)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(pages) != 3 || len(got) != 3 {
		t.Fatalf("walked %d pages in %d calls, want 3/3", len(pages), len(got))
	}
	for i, req := range got {
		if req.PageSize != cfg.PageSize || req.Filter != cfg.Filter || req.OrderBy != cfg.OrderBy {
			t.Errorf("request %d = %+v, want pageSize/filter/orderBy forwarded from the config", i, req)
		}
	}
	if got[0].PageToken != "" || got[1].PageToken != "1" || got[2].PageToken != "2" {
		t.Errorf("page tokens = %q/%q/%q, want the server token threaded through", got[0].PageToken, got[1].PageToken, got[2].PageToken)
	}
	if violations := CheckPages(pages, cfg); len(violations) != 0 {
		t.Errorf("CheckPages = %v, want none", violations)
	}
}

func TestWalkReportsTheFailingPage(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	lister := func(req ListRequest) (Page[string], error) {
		calls++
		if calls == 2 {
			return Page[string]{}, boom
		}
		return page("t2", "a"), nil
	}
	_, err := Walk(lister, Paging[string]{Name: func(s string) string { return s }})
	if err == nil || !strings.Contains(err.Error(), `page 2 (token "t2"): boom`) {
		t.Fatalf("Walk error = %v, want it to name page 2 and wrap the lister error", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Walk error %v does not wrap the lister error", err)
	}
}

func TestCheckStable(t *testing.T) {
	if got := CheckStable([]string{"a", "b"}, []string{"a", "b"}, true); len(got) != 0 {
		t.Fatalf("identical walks reported %v", got)
	}
	if got := strings.Join(CheckStable([]string{"a", "b"}, []string{"b", "a"}, true), "\n"); !strings.Contains(got, `diverged at index 0: "a" then "b"`) {
		t.Fatalf("reordered walk = %q, want a divergence at index 0", got)
	}
	if got := strings.Join(CheckStable([]string{"a", "b"}, []string{"a"}, true), "\n"); !strings.Contains(got, "returned 1 resources, the first returned 2") {
		t.Fatalf("short walk = %q, want a length violation", got)
	}
	// An unordered list method is only held to the same *set* across walks.
	if got := CheckStable([]string{"a", "b"}, []string{"b", "a"}, false); len(got) != 0 {
		t.Fatalf("a reordered second walk of an unordered list reported %v, want the set comparison", got)
	}
	if got := strings.Join(CheckStable([]string{"a", "b"}, []string{"a", "c"}, false), "\n"); !strings.Contains(got, `diverged at index 1: "b" then "c"`) {
		t.Fatalf("a different set reported %q", got)
	}
}

func TestAssertPagingWalksTwiceAndFiresOnASeededNegative(t *testing.T) {
	names := []string{"a", "b", "c"}
	factory := func() Lister[string] {
		return func(req ListRequest) (Page[string], error) {
			i := 0
			if req.PageToken != "" {
				fmt.Sscanf(req.PageToken, "%d", &i)
			}
			end := i + int(req.PageSize)
			if end > len(names) {
				end = len(names)
			}
			token := ""
			if end < len(names) {
				token = fmt.Sprint(end)
			}
			return page(token, names[i:end]...), nil
		}
	}
	cfg := strCfg(2, OrderAscendingName, "a", "b", "c")
	if got := AssertPaging(t, factory, cfg); strings.Join(got, "") != "abc" {
		t.Fatalf("AssertPaging returned %v, want the resources in walk order", got)
	}

	// The same walk, but the list method drops "b" on the second page: the
	// assertion must fire (this is the seeded negative that proves the kit bites).
	dropping := func() Lister[string] {
		return func(req ListRequest) (Page[string], error) {
			p, err := factory()(req)
			if err != nil {
				return p, err
			}
			kept := p.Items[:0]
			for _, n := range p.Items {
				if n != "b" {
					kept = append(kept, n)
				}
			}
			p.Items = kept
			return p, nil
		}
	}
	rec := wantReported(func(tb TB) { AssertPaging(tb, dropping, cfg) })
	if !strings.Contains(rec.reported(), `expected resource "b" is missing from the walk`) {
		t.Fatalf("AssertPaging on a dropping lister reported %q, want the missing-resource violation", rec.reported())
	}

	// A config without Name is a programming error: it must stop the test.
	rec = wantReported(func(tb TB) { AssertPaging[string](tb, factory, Paging[string]{PageSize: 2}) })
	if !strings.Contains(rec.reported(), "Paging.Name is required") {
		t.Fatalf("AssertPaging with no Name reported %q, want the Name-required fatal", rec.reported())
	}
}

// TestAssertPagingCatchesAPageOrderedInternallyButOutOfOrderAcrossTheBoundary is
// the DC17 end-to-end proof: the first walk is ascending *within* each page and a
// second walk repeats it exactly, so only a cross-page order check can fire.
func TestAssertPagingCatchesAPageOrderedInternallyButOutOfOrderAcrossTheBoundary(t *testing.T) {
	pages := []Page[string]{page("t2", "b", "d"), page("", "a", "c")}
	factory := func() Lister[string] {
		i := 0
		return func(ListRequest) (Page[string], error) {
			p := pages[i]
			if i < len(pages)-1 {
				i++
			}
			return p, nil
		}
	}
	cfg := strCfg(2, OrderAscendingName, "a", "b", "c", "d")
	rec := wantReported(func(tb TB) { AssertPaging(tb, factory, cfg) })
	if !strings.Contains(rec.reported(), `page 2 is out of order across the page boundary: "a" comes after page 1's last item "d"`) {
		t.Fatalf("AssertPaging on a cross-page misorder reported %q, want the boundary violation", rec.reported())
	}
}

func TestAssertInvalidPageSize(t *testing.T) {
	reject := func(ListRequest) (Page[string], error) {
		return Page[string]{}, httpStatusError(400, "INVALID_ARGUMENT", "pageSize must be positive")
	}
	if rec := wantReported(func(tb TB) { AssertInvalidPageSize[string](tb, reject, -1, SpecInvalidArgument()) }); rec.failed() {
		t.Fatalf("a documented rejection reported %q, want silence", rec.reported())
	}
	accept := func(ListRequest) (Page[string], error) {
		return Page[string]{Items: []string{"a"}}, nil
	}
	rec := wantReported(func(tb TB) { AssertInvalidPageSize[string](tb, accept, -1, SpecInvalidArgument()) })
	if !strings.Contains(rec.reported(), "the client returned none") {
		t.Fatalf("a negative pageSize that was accepted reported %q, want a missing-error violation", rec.reported())
	}
}
