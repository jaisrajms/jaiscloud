package eventarc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/store"
)

// seededTrigger is one trigger seeded directly into the store so the tests
// control its create/update time, destination config and labels.
type seededTrigger struct {
	id         string
	dest       string // the destination JSON body
	labels     map[string]string
	createTime time.Time
	updateTime time.Time // defaults to createTime
}

// newListService builds a Service over a store seeded with ts. Triggers have no
// references to validate here (they are written straight to the store), so the
// workflow store is empty.
func newListService(t *testing.T, ts ...seededTrigger) (*Service, eventarcstore.Store) {
	t.Helper()
	st := eventarcstore.NewMemoryStore()
	ctx := context.Background()
	for _, s := range ts {
		body := map[string]any{"destination": json.RawMessage(s.dest)}
		cfg, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal config: %v", err)
		}
		updated := s.updateTime
		if updated.IsZero() {
			updated = s.createTime
		}
		trig := eventarcstore.Trigger{
			Location:   "us-central1",
			Name:       s.id,
			Config:     cfg,
			Labels:     s.labels,
			UID:        "uid-" + s.id,
			Etag:       "etag-" + s.id,
			CreateTime: s.createTime,
			UpdateTime: updated,
		}
		if err := st.CreateTrigger(ctx, "proj", "us-central1", trig); err != nil {
			t.Fatalf("seed trigger %s: %v", s.id, err)
		}
	}
	return NewService(st, store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore()), st
}

// listIDs lists the trigger ids for the given filter/orderBy.
func listIDs(t *testing.T, svc *Service, filter, orderBy string, pageSize int, pageToken string) []string {
	t.Helper()
	page, _, err := svc.ListTriggers(context.Background(), "proj", "us-central1", filter, orderBy, pageSize, pageToken)
	if err != nil {
		t.Fatalf("ListTriggers(%q, %q): %v", filter, orderBy, err)
	}
	ids := make([]string, 0, len(page))
	for _, tr := range page {
		ids = append(ids, tr.Name)
	}
	return ids
}

func assertIDs(t *testing.T, what string, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func listTriggers(t *testing.T, svc *Service, filter, orderBy string, pageSize int, pageToken string) ([]string, string, error) {
	t.Helper()
	page, next, err := svc.ListTriggers(context.Background(), "proj", "us-central1", filter, orderBy, pageSize, pageToken)
	ids := make([]string, 0, len(page))
	for _, tr := range page {
		ids = append(ids, tr.Name)
	}
	return ids, next, err
}

func TestTriggerListFilter(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	svc, _ := newListService(t,
		seededTrigger{id: "tA", dest: `{"gke":{"cluster":"c"}}`, labels: map[string]string{"env": "test"}, createTime: base},
		seededTrigger{id: "tB", dest: `{"cloudRun":{"service":"svc","region":"us-central1"}}`, labels: map[string]string{"env": "prod"}, createTime: base.Add(time.Minute)},
		seededTrigger{id: "tC", dest: `{"httpEndpoint":{"uri":"http://x/"}}`, createTime: base.Add(2 * time.Minute)},
		seededTrigger{id: "tD", dest: `{"networkConfig":{"network":"n"}}`, createTime: base.Add(3 * time.Minute)},
	)

	cases := []struct {
		filter string
		want   []string
	}{
		{"", []string{"tA", "tB", "tC", "tD"}},
		{"destination:gke", []string{"tA"}},
		{"destination:cloudRun", []string{"tB"}},
		{"destination:cloudrun", []string{"tB"}},            // case-folded spelling
		{"destination:http_endpoint", []string{"tC"}},       // proto snake_case spelling
		{"destination:networkConfig", []string{"tD"}},       // the whole documented oneof
		{"destination:*", []string{"tA", "tB", "tC", "tD"}}, // any destination set
		{"labels.env=test", []string{"tA"}},                 //
		{"labels.env:*", []string{"tA", "tB"}},              // key present
		{"labels.env=*", []string{"tA", "tB"}},              // * is the same as :
		{"labels.env != *", []string{"tC", "tD"}},           // key absent
		{"labels:*", []string{"tA", "tB"}},                  // any label present
		{"labels.env != test", []string{"tB", "tC", "tD"}},  // absent label is not equal
		{`name="projects/proj/locations/us-central1/triggers/tA"`, []string{"tA"}},
		{`name:"triggers/tB"`, []string{"tB"}}, // ":" is AIP-160 contains; a value with "/" must be quoted
		{"uid=uid-tC", []string{"tC"}},         //
		{"destination:gke AND labels.env=test", []string{"tA"}},
		{"labels.env=prod destination:cloudRun", []string{"tB"}}, // implicit AND
		{"labels.env=prod destination:gke", nil},                 // no trigger satisfies both
	}
	for _, tc := range cases {
		ids := listIDs(t, svc, tc.filter, "", 0, "")
		if tc.want == nil {
			if len(ids) != 0 {
				t.Errorf("filter %q = %v, want none", tc.filter, ids)
			}
			continue
		}
		assertIDs(t, "filter "+tc.filter, ids, tc.want)
	}

	// A trigger has no displayName field (Discovery Trigger schema), so naming
	// one is an unknown field like any other.
	for _, f := range []string{"displayName=hello", "display_name:hello"} {
		if _, _, err := listTriggers(t, svc, f, "", 0, ""); maskStatus(err) != 400 {
			t.Errorf("filter %q err = %v, want 400", f, err)
		}
	}

	// Anything outside the grammar fails with InvalidArgument (400) rather than
	// silently returning the unfiltered list.
	bad := []string{
		"destination=gke",     // ":" is the only documented operator on destination
		"destination:zzz",     // unknown destination
		"name=*",              // name takes no wildcard
		"bogus=1",             // unknown field
		"labels.env>1",        // comparison operators are unsupported
		"labels.env=",         // missing value
		"name=\"unterminated", // tokenizer error
		"(name=x)",            // parenthesised groups are unsupported
		"name=x OR name=y",    // OR is unsupported
		"labels.env=test AND", // dangling AND
		"destination:",        // missing value
		"name=projects/proj/locations/us-central1/triggers/tA", // an unquoted value with "/" is invalid
	}
	for _, f := range bad {
		if _, _, err := listTriggers(t, svc, f, "", 0, ""); maskStatus(err) != 400 {
			t.Errorf("filter %q err = %v, want 400", f, err)
		}
	}
}

func TestTriggerListOrder(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	svc, _ := newListService(t,
		seededTrigger{id: "tb", dest: `{"gke":{}}`, createTime: base.Add(2*time.Minute + 100*time.Millisecond)},
		seededTrigger{id: "ta", dest: `{"gke":{}}`, createTime: base},
		seededTrigger{id: "tc", dest: `{"gke":{}}`, createTime: base.Add(time.Minute)},
		// td shares ta's second but has a non-zero fraction: RFC3339Nano would
		// render "…05Z" vs "…05.1Z" and sort the later time first.
		seededTrigger{id: "td", dest: `{"gke":{}}`, createTime: base.Add(100 * time.Millisecond),
			updateTime: base.Add(5 * time.Minute)},
	)

	cases := []struct {
		orderBy string
		want    []string
	}{
		{"", []string{"ta", "tb", "tc", "td"}}, // default: ascending name
		{"name", []string{"ta", "tb", "tc", "td"}},
		{"name desc", []string{"td", "tc", "tb", "ta"}},
		{"trigger_id", []string{"ta", "tb", "tc", "td"}},     // the Discovery example's id field
		{"triggerId desc", []string{"td", "tc", "tb", "ta"}}, //
		{"createTime", []string{"ta", "td", "tc", "tb"}},
		{"createTime desc", []string{"tb", "tc", "td", "ta"}},
		{"createTime asc, name desc", []string{"ta", "td", "tc", "tb"}},
		{"updateTime desc", []string{"td", "tb", "tc", "ta"}}, // only td moved
	}
	for _, tc := range cases {
		assertIDs(t, "orderBy "+tc.orderBy, listIDs(t, svc, "", tc.orderBy, 0, ""), tc.want)
	}

	// A Trigger has no displayName, so it is not orderable either.
	for _, bad := range []string{"bogus", "name sideways", "createTime desc,", "createTime DESC blah", "displayName"} {
		if _, _, err := listTriggers(t, svc, "", bad, 0, ""); maskStatus(err) != 400 {
			t.Errorf("orderBy %q err = %v, want 400", bad, err)
		}
	}
}

// TestTriggerListPaging walks every page of an ordered, filtered list and then
// resumes from a page token whose origin item was deleted in between: the
// remaining items are still returned exactly once.
func TestTriggerListPaging(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	svc, st := newListService(t,
		seededTrigger{id: "t1", dest: `{"gke":{}}`, labels: map[string]string{"env": "test"}, createTime: base},
		seededTrigger{id: "t2", dest: `{"gke":{}}`, labels: map[string]string{"env": "test"}, createTime: base.Add(time.Minute)},
		seededTrigger{id: "t3", dest: `{"cloudRun":{"service":"s","region":"r"}}`, labels: map[string]string{"env": "test"}, createTime: base.Add(2 * time.Minute)},
		seededTrigger{id: "t4", dest: `{"gke":{}}`, labels: map[string]string{"env": "prod"}, createTime: base.Add(3 * time.Minute)},
		seededTrigger{id: "t5", dest: `{"gke":{}}`, labels: map[string]string{"env": "prod"}, createTime: base.Add(4 * time.Minute)},
	)

	// Unfiltered, createTime desc, pageSize 2: every trigger exactly once.
	var seen []string
	token := ""
	for {
		ids, next, err := listTriggers(t, svc, "", "createTime desc", 2, token)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		seen = append(seen, ids...)
		if next == "" {
			break
		}
		token = next
	}
	assertIDs(t, "paged createTime desc", seen, []string{"t5", "t4", "t3", "t2", "t1"})

	// A filter composes with the order and the cursor: only the gke triggers,
	// newest first.
	ids, next, err := listTriggers(t, svc, "destination:gke", "createTime desc", 2, "")
	if err != nil || next == "" {
		t.Fatalf("filtered page 1 = %v, next=%q, err=%v", ids, next, err)
	}
	assertIDs(t, "filtered page 1", ids, []string{"t5", "t4"})
	ids, next, err = listTriggers(t, svc, "destination:gke", "createTime desc", 2, next)
	if err != nil {
		t.Fatalf("filtered page 2: %v", err)
	}
	assertIDs(t, "filtered page 2", ids, []string{"t2", "t1"})
	if next != "" {
		t.Fatalf("filtered page 2 next = %q, want empty", next)
	}

	// Deleting the item a token was minted from must not re-serve or skip the
	// items after it.
	ids, next, err = listTriggers(t, svc, "", "createTime desc", 2, "")
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	assertIDs(t, "paging origin page", ids, []string{"t5", "t4"})
	if err := st.DeleteTrigger(context.Background(), "proj", "us-central1", "t4"); err != nil {
		t.Fatalf("delete the token origin: %v", err)
	}
	ids, _, err = listTriggers(t, svc, "", "createTime desc", 2, next)
	if err != nil {
		t.Fatalf("resume after delete: %v", err)
	}
	assertIDs(t, "resume after token origin deleted", ids, []string{"t3", "t2"})

	// A garbage or undecodable token restarts at the first page (the emulator's
	// pre-existing paging contract) instead of failing the request.
	ids, _, err = listTriggers(t, svc, "", "createTime desc", 2, "!!!not-base64!!!")
	if err != nil {
		t.Fatalf("garbage token: %v", err)
	}
	assertIDs(t, "garbage token restarts", ids, []string{"t5", "t3"})

	// A token minted under a differently-shaped order does not mis-resume: the
	// composite key count does not match the requested order, so the scan
	// restarts rather than skipping or repeating silently.
	_, token2, err := listTriggers(t, svc, "", "name, uid", 2, "")
	if err != nil {
		t.Fatalf("two-key order page: %v", err)
	}
	ids, _, err = listTriggers(t, svc, "", "createTime desc", 2, token2)
	if err != nil {
		t.Fatalf("mismatched token: %v", err)
	}
	assertIDs(t, "mismatched order token restarts", ids, []string{"t5", "t3"})
}

// TestTriggerListDefaultCursorIsName pins the token encoding for the default
// order to the bare trigger name, which is what the previous paging.Page-based
// implementation emitted.
func TestTriggerListDefaultCursorIsName(t *testing.T) {
	trig := eventarcstore.Trigger{Name: "t1"}
	if got := (triggerOrder{}).cursor(trig); got != "t1" {
		t.Fatalf("default cursor = %q, want the bare name", got)
	}
	if (triggerOrder{}).afterCursor(trig, "t1") || !(triggerOrder{}).afterCursor(trig, "t0") {
		t.Fatal("default afterCursor comparison is wrong")
	}
	order, err := compileTriggerOrder("name desc")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	cur := order.cursor(trig)
	if cur == "t1" || !strings.Contains(cur, ".") {
		t.Fatalf("custom-order cursor %q must be the composite form", cur)
	}
	if order.afterCursor(trig, cur) {
		t.Fatal("an item is never after its own cursor")
	}
}
