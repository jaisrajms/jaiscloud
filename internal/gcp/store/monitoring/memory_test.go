package monitoring

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"
)

func double(v float64) *float64 { return &v }

func testPoint(v float64, ts time.Time) Point {
	return Point{EndTime: ts, Value: TypedValue{DoubleValue: double(v)}}
}

func TestMemoryStoreMetricDescriptorCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if got, err := s.ListMetricDescriptors(ctx, "p1"); err != nil || len(got) != 0 {
		t.Fatalf("initial list = %v, %v", got, err)
	}

	d := MetricDescriptor{Type: "custom.googleapis.com/test", ValueType: 3, MetricKind: 1, Unit: "1", Labels: []LabelDescriptor{{Key: "old", ValueType: 1}}}
	if _, err := s.CreateMetricDescriptor(ctx, "p1", d); err != nil {
		t.Fatal(err)
	}

	// Re-creating the same type upserts: fields are overwritten and existing
	// labels are unioned (never removed).
	dup := MetricDescriptor{Type: d.Type, ValueType: 4, MetricKind: 2, Unit: "2", Description: "updated", Labels: []LabelDescriptor{{Key: "new", ValueType: 2}}}
	stored, err := s.CreateMetricDescriptor(ctx, "p1", dup)
	if err != nil {
		t.Fatalf("upsert err = %v", err)
	}
	if stored.Description != "updated" || stored.ValueType != 4 {
		t.Fatalf("upsert fields not overwritten = %+v", stored)
	}
	if len(stored.Labels) != 2 {
		t.Fatalf("upsert labels = %+v, want 2 (old retained)", stored.Labels)
	}

	got, err := s.GetMetricDescriptor(ctx, "p1", d.Type)
	if err != nil || got.Type != d.Type || got.Unit != "2" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if _, err := s.GetMetricDescriptor(ctx, "p2", d.Type); err != ErrMetricDescriptorNotFound {
		t.Fatalf("get other project err = %v, want ErrMetricDescriptorNotFound", err)
	}

	list, err := s.ListMetricDescriptors(ctx, "p1")
	if err != nil || len(list) != 1 || list[0].Type != d.Type {
		t.Fatalf("list = %+v, %v", list, err)
	}

	if err := s.DeleteMetricDescriptor(ctx, "p1", d.Type); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMetricDescriptor(ctx, "p1", d.Type); err != ErrMetricDescriptorNotFound {
		t.Fatalf("delete missing err = %v, want ErrMetricDescriptorNotFound", err)
	}
}

func TestMemoryStoreTimeSeriesAppend(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	base := time.Now().UTC()

	ts := TimeSeries{
		MetricType:   "custom.googleapis.com/m",
		MetricLabels: map[string]string{"k": "v"},
		ResourceType: "global",
		Points:       []Point{testPoint(1, base)},
	}
	if err := s.CreateTimeSeries(ctx, "p1", ts); err != nil {
		t.Fatal(err)
	}

	// A second write with the same identity appends points rather than creating
	// a new series.
	ts.Points = []Point{testPoint(2, base.Add(time.Second))}
	if err := s.CreateTimeSeries(ctx, "p1", ts); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListTimeSeries(ctx, "p1")
	if err != nil || len(got) != 1 {
		t.Fatalf("list = %+v, %v", got, err)
	}
	if len(got[0].Points) != 2 {
		t.Fatalf("points = %d, want 2", len(got[0].Points))
	}

	// A different label set is a different series.
	ts.MetricLabels = map[string]string{"k": "other"}
	if err := s.CreateTimeSeries(ctx, "p1", ts); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListTimeSeries(ctx, "p1")
	if len(got) != 2 {
		t.Fatalf("list after distinct series = %d, want 2", len(got))
	}
}

func TestMemoryStoreAlertPolicyCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	enabled := true
	p := AlertPolicy{ID: "ap-1", DisplayName: "High CPU", Combiner: 1, Enabled: &enabled}
	if err := s.CreateAlertPolicy(ctx, "p1", p); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAlertPolicy(ctx, "p1", p); err != ErrAlertPolicyExists {
		t.Fatalf("duplicate policy err = %v, want ErrAlertPolicyExists", err)
	}

	got, err := s.GetAlertPolicy(ctx, "p1", "ap-1")
	if err != nil || got.DisplayName != "High CPU" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if _, err := s.GetAlertPolicy(ctx, "p1", "missing"); err != ErrAlertPolicyNotFound {
		t.Fatalf("get missing err = %v, want ErrAlertPolicyNotFound", err)
	}

	list, err := s.ListAlertPolicies(ctx, "p1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}

	p.DisplayName = "Updated"
	if err := s.UpdateAlertPolicy(ctx, "p1", p); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetAlertPolicy(ctx, "p1", "ap-1")
	if got.DisplayName != "Updated" {
		t.Fatalf("after update = %+v", got)
	}

	if err := s.DeleteAlertPolicy(ctx, "p1", "ap-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAlertPolicy(ctx, "p1", "ap-1"); err != ErrAlertPolicyNotFound {
		t.Fatalf("delete missing err = %v, want ErrAlertPolicyNotFound", err)
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_, _ = s.CreateMetricDescriptor(ctx, "p1", MetricDescriptor{Type: "t"})
	_ = s.CreateTimeSeries(ctx, "p1", TimeSeries{MetricType: "m", ResourceType: "r", Points: []Point{testPoint(1, time.Now())}})
	_ = s.CreateAlertPolicy(ctx, "p1", AlertPolicy{ID: "a", DisplayName: "x"})

	s.Reset(ctx)

	if got, _ := s.ListMetricDescriptors(ctx, "p1"); len(got) != 0 {
		t.Fatalf("descriptors after reset = %+v", got)
	}
	if got, _ := s.ListTimeSeries(ctx, "p1"); len(got) != 0 {
		t.Fatalf("series after reset = %+v", got)
	}
	if got, _ := s.ListAlertPolicies(ctx, "p1"); len(got) != 0 {
		t.Fatalf("policies after reset = %+v", got)
	}
}

func TestMemoryStoreSnapshot(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_, _ = s.CreateMetricDescriptor(ctx, "p1", MetricDescriptor{Type: "t", ValueType: 3})
	_ = s.CreateTimeSeries(ctx, "p1", TimeSeries{MetricType: "m", ResourceType: "r", Points: []Point{testPoint(1, time.Now().UTC())}})
	_ = s.CreateAlertPolicy(ctx, "p1", AlertPolicy{ID: "a", DisplayName: "x", Combiner: 1})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatal(err)
	}

	if got, _ := s2.ListMetricDescriptors(ctx, "p1"); len(got) != 1 || got[0].Type != "t" {
		t.Fatalf("restored descriptors = %+v", got)
	}
	if got, _ := s2.ListTimeSeries(ctx, "p1"); len(got) != 1 || len(got[0].Points) != 1 {
		t.Fatalf("restored series = %+v", got)
	}
	if got, _ := s2.ListAlertPolicies(ctx, "p1"); len(got) != 1 || got[0].DisplayName != "x" {
		t.Fatalf("restored policies = %+v", got)
	}
}

func TestMemoryStoreNotificationChannelCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	enabled := true
	c := NotificationChannel{ID: "nc-1", Type: "pubsub", DisplayName: "ops", Labels: map[string]string{"topic": "projects/p1/topics/t"}, Enabled: &enabled}
	if err := s.CreateNotificationChannel(ctx, "p1", c); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateNotificationChannel(ctx, "p1", c); err != ErrNotificationChannelExists {
		t.Fatalf("duplicate channel err = %v, want ErrNotificationChannelExists", err)
	}
	if _, err := s.GetNotificationChannel(ctx, "p1", "missing"); err != ErrNotificationChannelNotFound {
		t.Fatalf("get missing err = %v, want ErrNotificationChannelNotFound", err)
	}
	list, err := s.ListNotificationChannels(ctx, "p1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %+v, %v", list, err)
	}
	updated, err := s.UpdateNotificationChannelAtomic(ctx, "p1", "nc-1", func(cur NotificationChannel) (NotificationChannel, error) {
		cur.DisplayName = "ops-renamed"
		return cur, nil
	})
	if err != nil || updated.DisplayName != "ops-renamed" {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	if err := s.DeleteNotificationChannel(ctx, "p1", "nc-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteNotificationChannel(ctx, "p1", "nc-1"); err != ErrNotificationChannelNotFound {
		t.Fatalf("delete missing err = %v, want ErrNotificationChannelNotFound", err)
	}
}

func TestMemoryStoreIncidents(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	inc := Incident{ID: "inc-1", ProjectID: "p1", PolicyID: "ap-1", State: IncidentOpen, StartedAt: time.Now().UTC()}
	if err := s.CreateIncident(ctx, inc); err != nil {
		t.Fatal(err)
	}
	// A second OPEN incident for the same policy is rejected.
	if err := s.CreateIncident(ctx, Incident{ID: "inc-2", ProjectID: "p1", PolicyID: "ap-1", State: IncidentOpen}); err != ErrIncidentExists {
		t.Fatalf("second open incident err = %v, want ErrIncidentExists", err)
	}
	found, err := s.FindOpenIncident(ctx, "p1", "ap-1")
	if err != nil || found.ID != "inc-1" {
		t.Fatalf("find open = %+v, %v", found, err)
	}
	if _, err := s.FindOpenIncident(ctx, "p1", "other"); err != ErrIncidentNotFound {
		t.Fatalf("find other err = %v, want ErrIncidentNotFound", err)
	}
	closed, err := s.UpdateIncidentAtomic(ctx, "p1", "inc-1", func(cur Incident) (Incident, error) {
		cur.State = IncidentClosed
		cur.EndedAt = time.Now().UTC()
		return cur, nil
	})
	if err != nil || closed.State != IncidentClosed {
		t.Fatalf("close = %+v, %v", closed, err)
	}
	if _, err := s.FindOpenIncident(ctx, "p1", "ap-1"); err != ErrIncidentNotFound {
		t.Fatalf("find after close err = %v, want ErrIncidentNotFound", err)
	}
	if got, err := s.ListIncidents(ctx, "p1"); err != nil || len(got) != 1 {
		t.Fatalf("list = %+v, %v", got, err)
	}
	if got, _ := s.ListProjects(ctx); len(got) != 1 || got[0] != "p1" {
		t.Fatalf("list projects = %+v", got)
	}
}

func TestMemoryStoreSnapshotIncludesChannelsAndIncidents(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	enabled := true
	_ = s.CreateNotificationChannel(ctx, "p1", NotificationChannel{ID: "nc-1", Type: "email", Labels: map[string]string{"email_address": "a@b.c"}, Enabled: &enabled})
	_ = s.CreateIncident(ctx, Incident{ID: "inc-1", ProjectID: "p1", PolicyID: "ap-1", State: IncidentClosed, StartedAt: time.Now().UTC()})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	if got, _ := s2.ListNotificationChannels(ctx, "p1"); len(got) != 1 || got[0].Labels["email_address"] != "a@b.c" {
		t.Fatalf("restored channels = %+v", got)
	}
	if got, _ := s2.ListIncidents(ctx, "p1"); len(got) != 1 || got[0].State != IncidentClosed {
		t.Fatalf("restored incidents = %+v", got)
	}
}

func TestSeriesKeyDeterministic(t *testing.T) {
	a := TimeSeries{MetricType: "m", MetricLabels: map[string]string{"x": "1", "y": "2"}, ResourceType: "r", ResourceLabels: map[string]string{"z": "3"}}
	b := TimeSeries{MetricType: "m", MetricLabels: map[string]string{"y": "2", "x": "1"}, ResourceType: "r", ResourceLabels: map[string]string{"z": "3"}}
	if seriesKey(a) != seriesKey(b) {
		t.Fatalf("seriesKey not label-order independent: %q vs %q", seriesKey(a), seriesKey(b))
	}
}

func TestMemoryStoreDistributionRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	base := time.Now().UTC().Truncate(time.Second)

	linear := &Distribution{
		Count:                 6,
		Mean:                  3.5,
		SumOfSquaredDeviation: 17.5,
		Range:                 &DistributionRange{Min: 0, Max: 10},
		BucketOptions: &BucketOptions{
			Linear: &LinearBuckets{NumFiniteBuckets: 3, Width: 2, Offset: 1},
		},
		BucketCounts: []int64{1, 2, 2, 1, 0},
	}
	explicit := &Distribution{
		Count:                 6,
		Mean:                  4.25,
		SumOfSquaredDeviation: 9.25,
		BucketOptions: &BucketOptions{
			Explicit: &ExplicitBuckets{Bounds: []float64{1, 2, 5, 10}},
		},
		BucketCounts: []int64{0, 1, 2, 2, 1},
	}
	for _, tc := range []struct {
		metric string
		dist   *Distribution
	}{
		{"linear", linear},
		{"explicit", explicit},
	} {
		if err := s.CreateTimeSeries(ctx, "p1", TimeSeries{
			MetricType:   tc.metric,
			ResourceType: "global",
			Points:       []Point{{EndTime: base, Value: TypedValue{DistributionValue: tc.dist}}},
		}); err != nil {
			t.Fatalf("create %s: %v", tc.metric, err)
		}
	}

	check := func(t *testing.T, store *MemoryStore) {
		t.Helper()
		got, err := store.ListTimeSeries(ctx, "p1")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		byMetric := map[string]*Distribution{}
		for _, ts := range got {
			if len(ts.Points) == 1 {
				byMetric[ts.MetricType] = ts.Points[0].Value.DistributionValue
			}
		}
		if !reflect.DeepEqual(byMetric["linear"], linear) {
			t.Fatalf("linear = %+v, want %+v", byMetric["linear"], linear)
		}
		if !reflect.DeepEqual(byMetric["explicit"], explicit) {
			t.Fatalf("explicit = %+v, want %+v", byMetric["explicit"], explicit)
		}
	}
	check(t, s)

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	check(t, s2)
}
