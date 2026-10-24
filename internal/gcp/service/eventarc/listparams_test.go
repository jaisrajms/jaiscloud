package eventarc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	"jaiscloud/internal/store"
)

// This file exercises the documented filter/orderBy parameters of the Eventarc
// list methods that used to drop them (TD17): the advanced surface
// (messageBuses/enrollments/pipelines/googleApiSources), channels and providers.
// It asserts the params actually restrict/order the page (rather than a silently
// unfiltered list) and that anything outside the bounded grammar fails with
// INVALID_ARGUMENT.

var listBase = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// seedAdvancedRecord writes a record straight into the shared ResourceStore so
// the test controls its create/update time, uid, labels and config.
func seedAdvancedRecord(t *testing.T, s *Service, project string, k Kind, r AdvancedRecord) {
	t.Helper()
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal %s: %v", r.ID, err)
	}
	if err := s.resources.Create(context.Background(), project, store.GlobalRegion, store.ResourceEntry{
		Type: k.StoreType, ID: k.key(r.Location, r.ID), Data: data,
	}); err != nil {
		t.Fatalf("seed %s: %v", r.ID, err)
	}
}

// advancedIDs lists a kind's ids for the given filter/orderBy.
func advancedIDs(t *testing.T, s *Service, k Kind, filter, orderBy string, pageSize int, pageToken string) ([]string, string, error) {
	t.Helper()
	page, next, err := s.ListAdvanced(context.Background(), "proj", k, "us-central1", filter, orderBy, pageSize, pageToken)
	ids := make([]string, 0, len(page))
	for _, r := range page {
		ids = append(ids, r.ID)
	}
	return ids, next, err
}

func TestAdvancedListFilter(t *testing.T) {
	s := newAdvancedService()
	seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
		Location: "us-central1", ID: "mb-a", UID: "uid-mb-a", Labels: map[string]string{"env": "test"},
		Config: json.RawMessage(`{"displayName":"alpha"}`), CreateTime: listBase, UpdateTime: listBase,
	})
	seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
		Location: "us-central1", ID: "mb-b", UID: "uid-mb-b", Labels: map[string]string{"env": "prod"},
		Config: json.RawMessage(`{"displayName":"beta"}`), CreateTime: listBase.Add(time.Minute), UpdateTime: listBase.Add(2 * time.Minute),
	})
	seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
		Location: "us-central1", ID: "mb-c", UID: "uid-mb-c",
		CreateTime: listBase.Add(2 * time.Minute), UpdateTime: listBase.Add(time.Minute),
	})

	cases := []struct {
		filter string
		want   []string
	}{
		{"", []string{"mb-a", "mb-b", "mb-c"}},
		{"labels.env=prod", []string{"mb-b"}},
		{"labels.env:*", []string{"mb-a", "mb-b"}},
		{"labels.env != *", []string{"mb-c"}},
		{"labels:*", []string{"mb-a", "mb-b"}},
		{`displayName="alpha"`, []string{"mb-a"}},
		{`displayName:alpha`, []string{"mb-a"}},
		{`display_name=alpha`, []string{"mb-a"}}, // AIP-160 snake_case spelling
		{`name:"messageBuses/mb-b"`, []string{"mb-b"}},
		{"uid=uid-mb-c", []string{"mb-c"}},
		{`createTime>="2026-01-02T03:05:05Z"`, []string{"mb-b", "mb-c"}},
		{`create_time>="2026-01-02T03:05:05Z"`, []string{"mb-b", "mb-c"}}, // snake_case
		{`updateTime<"2026-01-02T03:05:05Z"`, []string{"mb-a"}},
		{"labels.env=prod displayName:beta", []string{"mb-b"}}, // implicit AND
		{"labels.env=test displayName:beta", nil},              // no bus satisfies both
	}
	for _, tc := range cases {
		ids, _, err := advancedIDs(t, s, MessageBusKind, tc.filter, "", 0, "")
		if err != nil {
			t.Fatalf("filter %q: %v", tc.filter, err)
		}
		if tc.want == nil {
			if len(ids) != 0 {
				t.Errorf("filter %q = %v, want none", tc.filter, ids)
			}
			continue
		}
		assertIDs(t, "filter "+tc.filter, ids, tc.want)
	}
}

func TestAdvancedListOrder(t *testing.T) {
	s := newAdvancedService()
	seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
		Location: "us-central1", ID: "mb-a", UID: "uid-a", Config: json.RawMessage(`{"displayName":"alpha"}`),
		CreateTime: listBase, UpdateTime: listBase,
	})
	seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
		Location: "us-central1", ID: "mb-b", UID: "uid-b", Config: json.RawMessage(`{"displayName":"beta"}`),
		CreateTime: listBase.Add(time.Minute), UpdateTime: listBase.Add(2 * time.Minute),
	})
	seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
		Location: "us-central1", ID: "mb-c", UID: "uid-c",
		CreateTime: listBase.Add(2 * time.Minute), UpdateTime: listBase.Add(time.Minute),
	})

	cases := []struct {
		orderBy string
		want    []string
	}{
		{"", []string{"mb-a", "mb-b", "mb-c"}},
		{"name", []string{"mb-a", "mb-b", "mb-c"}},
		{"name desc", []string{"mb-c", "mb-b", "mb-a"}},
		{"messageBusId desc", []string{"mb-c", "mb-b", "mb-a"}}, // the Discovery example's id field
		{"updateTime", []string{"mb-a", "mb-c", "mb-b"}},
		{"updateTime desc", []string{"mb-b", "mb-c", "mb-a"}},
		{"displayName", []string{"mb-c", "mb-a", "mb-b"}}, // "" sorts before "alpha"
		{"createTime asc, name desc", []string{"mb-a", "mb-b", "mb-c"}},
	}
	for _, tc := range cases {
		ids, _, err := advancedIDs(t, s, MessageBusKind, "", tc.orderBy, 0, "")
		if err != nil {
			t.Fatalf("orderBy %q: %v", tc.orderBy, err)
		}
		assertIDs(t, "orderBy "+tc.orderBy, ids, tc.want)
	}
}

// TestAdvancedListFilterOrderPaging composes a filter, a non-name order and the
// page cursor: every matching record is returned exactly once, in order.
func TestAdvancedListFilterOrderPaging(t *testing.T) {
	s := newAdvancedService()
	for i, id := range []string{"mb-a", "mb-b", "mb-c"} {
		seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
			Location: "us-central1", ID: id, UID: "uid-" + id, Labels: map[string]string{"env": "test"},
			CreateTime: listBase.Add(time.Duration(i) * time.Minute),
		})
	}
	seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
		Location: "us-central1", ID: "mb-d", UID: "uid-d", Labels: map[string]string{"env": "prod"},
		CreateTime: listBase.Add(3 * time.Minute),
	})

	var seen []string
	token := ""
	for {
		ids, next, err := advancedIDs(t, s, MessageBusKind, "labels.env=test", "createTime desc", 1, token)
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		seen = append(seen, ids...)
		if next == "" {
			break
		}
		token = next
	}
	assertIDs(t, "paged filter+order", seen, []string{"mb-c", "mb-b", "mb-a"})
}

func TestAdvancedListRejectsBadParams(t *testing.T) {
	s := newAdvancedService()
	seedAdvancedRecord(t, s, "proj", MessageBusKind, AdvancedRecord{
		Location: "us-central1", ID: "mb-a", UID: "uid-a", CreateTime: listBase, UpdateTime: listBase,
	})

	badFilters := []string{
		"bogus=1",
		"name=x OR name=y",
		"(name=x)",
		"labels.env>1",
		"name=*",
		`name="unterminated`,
	}
	for _, f := range badFilters {
		if _, _, err := advancedIDs(t, s, MessageBusKind, f, "", 0, ""); maskStatus(err) != 400 {
			t.Errorf("filter %q err = %v, want 400", f, err)
		}
	}
	for _, o := range []string{"bogus", "name sideways", "createTime desc,", "createTime DESC blah"} {
		if _, _, err := advancedIDs(t, s, MessageBusKind, "", o, 0, ""); maskStatus(err) != 400 {
			t.Errorf("orderBy %q err = %v, want 400", o, err)
		}
	}

	// A ChannelConnection has no displayName (Discovery schema), so neither
	// filtering nor ordering on it is accepted.
	if _, _, err := advancedIDs(t, s, ChannelConnectionKind, "displayName=x", "", 0, ""); maskStatus(err) != 400 {
		t.Errorf("channel connection displayName filter err = %v, want 400", err)
	}
	if _, _, err := advancedIDs(t, s, ChannelConnectionKind, "", "displayName", 0, ""); maskStatus(err) != 400 {
		t.Errorf("channel connection displayName orderBy err = %v, want 400", err)
	}
}

func TestChannelListOrder(t *testing.T) {
	s := newAdvancedService()
	seed := func(id string, create time.Time) {
		t.Helper()
		if err := s.store.CreateChannel(context.Background(), "proj", "us-central1", eventarcstore.Channel{
			Location: "us-central1", Name: id, UID: "uid-" + id, CreateTime: create, UpdateTime: create,
		}); err != nil {
			t.Fatalf("seed channel %s: %v", id, err)
		}
	}
	seed("ch-a", listBase.Add(2*time.Minute))
	seed("ch-b", listBase)
	seed("ch-c", listBase.Add(time.Minute))

	channelIDs := func(orderBy string) ([]string, error) {
		page, _, err := s.ListChannels(context.Background(), "proj", "us-central1", orderBy, 0, "")
		ids := make([]string, 0, len(page))
		for _, c := range page {
			ids = append(ids, c.Name)
		}
		return ids, err
	}

	cases := []struct {
		orderBy string
		want    []string
	}{
		{"", []string{"ch-a", "ch-b", "ch-c"}},
		{"name desc", []string{"ch-c", "ch-b", "ch-a"}},
		{"channelId desc", []string{"ch-c", "ch-b", "ch-a"}}, // the Discovery example's id field
		{"createTime", []string{"ch-b", "ch-c", "ch-a"}},
		{"uid", []string{"ch-a", "ch-b", "ch-c"}},
	}
	for _, tc := range cases {
		ids, err := channelIDs(tc.orderBy)
		if err != nil {
			t.Fatalf("orderBy %q: %v", tc.orderBy, err)
		}
		assertIDs(t, "channel orderBy "+tc.orderBy, ids, tc.want)
	}

	if _, err := channelIDs("bogus"); maskStatus(err) != 400 {
		t.Errorf("channel orderBy bogus err = %v, want 400", err)
	}
	// channels.list documents no filter, so there is no filter parameter to
	// reject here.
}

func TestProviderListFilterAndOrder(t *testing.T) {
	s := newAdvancedService()
	providerIDs := func(filter, orderBy string) ([]string, error) {
		page, _, err := s.ListProviders(context.Background(), "proj", "us-central1", filter, orderBy, 0, "")
		ids := make([]string, 0, len(page))
		for _, d := range page {
			ids = append(ids, d.ID)
		}
		return ids, err
	}

	ids, err := providerIDs("", "")
	if err != nil {
		t.Fatalf("list providers: %v", err)
	}
	if len(ids) != len(Catalog()) {
		t.Fatalf("unfiltered providers = %v, want all %d", ids, len(Catalog()))
	}

	if ids, err = providerIDs(`displayName:"Storage"`, ""); err != nil {
		t.Fatalf("filter displayName: %v", err)
	} else {
		assertIDs(t, "provider filter displayName", ids, []string{"storage.googleapis.com"})
	}
	if ids, err = providerIDs(`name:"pubsub"`, ""); err != nil {
		t.Fatalf("filter name: %v", err)
	} else {
		assertIDs(t, "provider filter name", ids, []string{"pubsub.googleapis.com"})
	}
	if ids, err = providerIDs("", "displayName desc"); err != nil {
		t.Fatalf("order displayName: %v", err)
	} else {
		assertIDs(t, "provider order displayName desc", ids, []string{"storage.googleapis.com", "pubsub.googleapis.com"})
	}
	// The Discovery parameter's own example is "name desc, _id", and a
	// snake_case filter field is accepted (AIP-160).
	if ids, err = providerIDs("", "name desc, _id"); err != nil {
		t.Fatalf("order _id: %v", err)
	} else {
		assertIDs(t, "provider order name desc, _id", ids, []string{"storage.googleapis.com", "pubsub.googleapis.com"})
	}
	if ids, err = providerIDs(`display_name:"Storage"`, ""); err != nil {
		t.Fatalf("filter display_name: %v", err)
	} else {
		assertIDs(t, "provider filter display_name", ids, []string{"storage.googleapis.com"})
	}

	// The Provider schema has no uid, labels or timestamps, so those are
	// unknown fields rather than silently ignored.
	for _, f := range []string{"uid=x", "labels.a=b", `createTime>"2026-01-02T03:04:05Z"`} {
		if _, err := providerIDs(f, ""); maskStatus(err) != 400 {
			t.Errorf("provider filter %q err = %v, want 400", f, err)
		}
	}
	for _, o := range []string{"uid", "createTime"} {
		if _, err := providerIDs("", o); maskStatus(err) != 400 {
			t.Errorf("provider orderBy %q err = %v, want 400", o, err)
		}
	}
}
