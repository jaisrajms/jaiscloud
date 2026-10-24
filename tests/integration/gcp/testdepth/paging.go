package testdepth

import (
	"fmt"
	"sort"
)

// ListRequest is one list call: the paging cursor plus the documented
// filter/orderBy composition the caller wants exercised.
type ListRequest struct {
	PageSize  int64
	PageToken string
	Filter    string
	OrderBy   string
}

// Page is one returned page: the resources and the server's continuation token
// ("" on the last page).
type Page[T any] struct {
	Items         []T
	NextPageToken string
}

// Lister performs one list call. It is a closure over the caller's client, and
// must be produced fresh for every walk — a gRPC iterator is stateful — which
// is why AssertPaging takes a factory.
type Lister[T any] func(ListRequest) (Page[T], error)

// Ordering is the order a list method documents for its resources.
type Ordering int

const (
	// OrderUnspecified asserts the page mechanics and uniqueness only: real GCP
	// does not document a stable order for every List method.
	OrderUnspecified Ordering = iota
	// OrderAscendingName asserts the resources arrive in ascending resource
	// name order, including across a page boundary.
	OrderAscendingName
)

// defaultMaxPages bounds a walk when Paging.MaxPages is unset, so a server that
// hands back a looping cursor fails loudly instead of hanging the suite.
const defaultMaxPages = 1000

// Paging configures the list/paging contract for one method.
type Paging[T any] struct {
	// Name returns a resource's full name. Required: uniqueness, ordering and
	// the expected-set assertion are all name-based.
	Name func(T) string
	// PageSize is requested on every page (0 = the server default, which also
	// disables the page-size assertion).
	PageSize int64
	// Filter and OrderBy are passed through on every page when non-empty.
	Filter, OrderBy string
	// Order is the order the API documents; OrderUnspecified skips the order
	// assertion.
	Order Ordering
	// MaxPages bounds the walk (0 = 1000).
	MaxPages int
	// RequireNonEmptyPages asserts that no page before the last is empty. Real
	// GCP does NOT promise this — AIP-158 lets a list method return zero results
	// on a page in the middle of a collection — so it is opt-in, and belongs to
	// a service whose documented contract (or the emulator's own page filling) is
	// "every non-terminal page carries items".
	RequireNonEmptyPages bool
	// Expect names every resource the walk must return. A name that never comes
	// back is a violation; a name that comes back twice is one too.
	Expect []string
	// Include, when set, is applied to every returned resource: an item outside
	// the expected set (typically the complement of the filter) is a violation.
	Include func(T) bool
}

func (p Paging[T]) maxPages() int {
	if p.MaxPages > 0 {
		return p.MaxPages
	}
	return defaultMaxPages
}

// Walk follows the server's page token to the end and returns every page.
func Walk[T any](lister Lister[T], cfg Paging[T]) ([]Page[T], error) {
	var pages []Page[T]
	token := ""
	for i := 0; i < cfg.maxPages(); i++ {
		page, err := lister(ListRequest{
			PageSize:  cfg.PageSize,
			PageToken: token,
			Filter:    cfg.Filter,
			OrderBy:   cfg.OrderBy,
		})
		if err != nil {
			return pages, fmt.Errorf("page %d (token %q): %w", i+1, token, err)
		}
		pages = append(pages, page)
		token = page.NextPageToken
		if token == "" {
			return pages, nil
		}
	}
	return pages, fmt.Errorf("pagination did not terminate within %d pages (last token %q)", cfg.maxPages(), token)
}

// CheckPages returns one message per paging-contract violation across the pages
// a Walk returned. It is pure, so the kit's own tests can drive seeded negatives
// (a dropped resource, a duplicate, a mis-ordered resource — within a page or
// across a page boundary) through it.
func CheckPages[T any](pages []Page[T], cfg Paging[T]) []string {
	if cfg.Name == nil {
		return []string{"Paging.Name is required (uniqueness and ordering are name-based)"}
	}
	var out []string
	firstPage := map[string]int{}
	// prev/prevPage carry the last name seen in the walk, not just the last name
	// of the current page, so the order guard spans page boundaries too.
	var prev string
	prevPage := -1
	for i, page := range pages {
		if cfg.PageSize > 0 && int64(len(page.Items)) > cfg.PageSize {
			out = append(out, fmt.Sprintf("page %d returned %d items, more than the requested pageSize %d",
				i+1, len(page.Items), cfg.PageSize))
		}
		if cfg.RequireNonEmptyPages && len(page.Items) == 0 && page.NextPageToken != "" {
			out = append(out, fmt.Sprintf("page %d is empty but carries a non-terminal next page token %q",
				i+1, page.NextPageToken))
		}
		if i == len(pages)-1 && page.NextPageToken != "" {
			out = append(out, fmt.Sprintf("the final page still carries next page token %q", page.NextPageToken))
		}
		for _, item := range page.Items {
			name := cfg.Name(item)
			if seenPage, seen := firstPage[name]; seen {
				out = append(out, fmt.Sprintf("resource %q appears on more than one page (pages %d and %d)",
					name, seenPage+1, i+1))
			} else {
				firstPage[name] = i
			}
			if cfg.Order == OrderAscendingName {
				switch {
				case prevPage < 0 || prev <= name:
				case prevPage == i:
					out = append(out, fmt.Sprintf("page %d is out of order: %q comes after %q", i+1, name, prev))
				default:
					out = append(out, fmt.Sprintf("page %d is out of order across the page boundary: %q comes after page %d's last item %q",
						i+1, name, prevPage+1, prev))
				}
			}
			if cfg.Include != nil && !cfg.Include(item) {
				out = append(out, fmt.Sprintf("page %d item %q is outside the expected set for filter %q",
					i+1, name, cfg.Filter))
			}
			prev, prevPage = name, i
		}
	}
	for _, want := range cfg.Expect {
		if _, ok := firstPage[want]; !ok {
			out = append(out, fmt.Sprintf("expected resource %q is missing from the walk", want))
		}
	}
	return out
}

// CheckStable returns a violation when two walks of the same list method did not
// return the same resources. With ordered (the API documents a resource order)
// the comparison is sequence-sensitive; otherwise it compares the resources as a
// set, because a List method that documents no order is not obliged to return
// the same order twice. It asserts *stability*, not validity: a walk that is
// consistently mis-ordered repeats itself exactly, so the documented order is
// CheckPages' assertion, not this one's.
func CheckStable(first, second []string, ordered bool) []string {
	if len(first) != len(second) {
		return []string{fmt.Sprintf("a second walk returned %d resources, the first returned %d", len(second), len(first))}
	}
	if !ordered {
		return CheckStable(sortedNames(first), sortedNames(second), true)
	}
	for i := range first {
		if first[i] != second[i] {
			return []string{fmt.Sprintf("a second walk diverged at index %d: %q then %q", i, first[i], second[i])}
		}
	}
	return nil
}

func sortedNames(names []string) []string {
	out := append([]string(nil), names...)
	sort.Strings(out)
	return out
}

// Names returns cfg.Name for every item, in walk order.
func Names[T any](items []T, cfg Paging[T]) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, cfg.Name(item))
	}
	return out
}

// AssertPaging walks a list method to the end, asserts the full paging contract
// (uniqueness, documented order across page boundaries, page-size honesty, an
// empty final token, the expected set) and that a second walk is identical, and
// returns the resources in walk order. It fails t on the first transport error.
func AssertPaging[T any](t TB, newLister func() Lister[T], cfg Paging[T]) []T {
	t.Helper()
	if cfg.Name == nil {
		t.Fatal("testdepth.AssertPaging: Paging.Name is required")
	}
	pages, err := Walk(newLister(), cfg)
	if err != nil {
		t.Fatalf("paging walk failed: %v", err)
	}
	for _, violation := range CheckPages(pages, cfg) {
		t.Errorf("paging contract: %s", violation)
	}
	first := flatten(pages)

	names := Names(first, cfg)
	pages2, err := Walk(newLister(), cfg)
	if err != nil {
		t.Fatalf("second paging walk failed: %v", err)
	}
	for _, violation := range CheckStable(names, Names(flatten(pages2), cfg), cfg.Order == OrderAscendingName) {
		t.Errorf("paging contract: %s", violation)
	}
	return first
}

func flatten[T any](pages []Page[T]) []T {
	var out []T
	for _, page := range pages {
		out = append(out, page.Items...)
	}
	return out
}

// AssertInvalidPageSize calls the list method with the given pageSize and
// asserts the documented rejection. The rule is per Discovery, not universal:
// AIP-158 requires INVALID_ARGUMENT for a negative pageSize and an over-max value
// to be coerced down (never rejected), but a service may document that a
// non-positive value is ignored (Cloud Logging) or clamped — so pass the value
// the service documents as invalid, and its documented envelope.
func AssertInvalidPageSize[T any](t TB, lister Lister[T], pageSize int64, want ErrorSpec) {
	t.Helper()
	_, err := lister(ListRequest{PageSize: pageSize})
	AssertError(t, err, want)
}
