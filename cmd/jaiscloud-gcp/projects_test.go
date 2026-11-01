package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"jaiscloud/internal/admin"
)

func TestRelativeAge(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		at   time.Time
		want string
	}{
		{now.Add(-18 * time.Second), "18s ago"},
		{now.Add(-90 * time.Second), "1m ago"},
		{now.Add(-2 * time.Hour), "2h ago"},
		{now.Add(-26 * time.Hour), "1d ago"},
		{now.Add(time.Minute), "0s ago"},
	}
	for _, tc := range cases {
		if got := relativeAge(tc.at, now); got != tc.want {
			t.Errorf("relativeAge(%v) = %q, want %q", tc.at, got, tc.want)
		}
	}
}

func TestFilterProjectRows(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	rows := []admin.ProjectRow{
		{ID: "jaiscloud-project", Kind: "configured"},
		{ID: "billing-demo", Kind: "registered"},
		{ID: "acme-e2e", Kind: "observed", LastSeen: now.Add(-18 * time.Second).Format(time.RFC3339)},
		{ID: "old-e2e", Kind: "observed", LastSeen: now.Add(-2 * time.Hour).Format(time.RFC3339)},
	}

	ids := func(in []admin.ProjectRow) []string {
		out := make([]string, 0, len(in))
		for _, row := range in {
			out = append(out, row.ID)
		}
		return out
	}

	if got := ids(filterProjectRows(rows, true, false, 0, now)); len(got) != 2 || got[0] != "acme-e2e" {
		t.Errorf("--observed = %v, want the two observed rows", got)
	}
	if got := ids(filterProjectRows(rows, false, true, 0, now)); len(got) != 2 || got[0] != "jaiscloud-project" {
		t.Errorf("--registered = %v, want the configured and registered rows", got)
	}
	if got := ids(filterProjectRows(rows, true, false, 10*time.Minute, now)); len(got) != 1 || got[0] != "acme-e2e" {
		t.Errorf("--observed --since 10m = %v, want only acme-e2e", got)
	}
	if got := ids(filterProjectRows(rows, false, true, 10*time.Minute, now)); len(got) != 0 {
		t.Errorf("--registered --since 10m = %v, want none (no activity timestamps)", got)
	}
}

func TestSortProjectRowsPutsObservedFirstByActivity(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	rows := []admin.ProjectRow{
		{ID: "jaiscloud-project", Kind: "configured"},
		{ID: "billing-demo", Kind: "registered"},
		{ID: "old-e2e", Kind: "observed", LastSeen: now.Add(-2 * time.Hour).Format(time.RFC3339)},
		{ID: "acme-e2e", Kind: "observed", LastSeen: now.Add(-18 * time.Second).Format(time.RFC3339)},
	}
	sortProjectRows(rows)
	want := []string{"acme-e2e", "old-e2e", "jaiscloud-project", "billing-demo"}
	for i, id := range want {
		if rows[i].ID != id {
			t.Fatalf("row %d = %s, want %s (%v)", i, rows[i].ID, id, rows)
		}
	}
}

func TestWriteProjectsTable(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	rows := []admin.ProjectRow{
		{ID: "acme-e2e", Kind: "observed", LastSeen: now.Add(-18 * time.Second).Format(time.RFC3339), Services: []string{"storage", "pubsub", "firestore"}},
		{ID: "jaiscloud-project", Kind: "configured"},
		{ID: "billing-demo", Kind: "registered", LastSeen: now.Add(-2 * time.Hour).Format(time.RFC3339), Services: []string{"bigquery"}},
	}
	var buf bytes.Buffer
	if err := writeProjectsTable(&buf, rows, now); err != nil {
		t.Fatal(err)
	}
	want := "PROJECT            KIND        LAST SEEN  SERVICES\n" +
		"acme-e2e           observed    18s ago    storage, pubsub, firestore\n" +
		"jaiscloud-project  configured  —          —\n" +
		"billing-demo       registered  2h ago     bigquery\n"
	if buf.String() != want {
		t.Errorf("table =\n%q\nwant\n%q", buf.String(), want)
	}
}

func TestWriteProjectsJSONEmptyIsArray(t *testing.T) {
	var buf bytes.Buffer
	if err := writeProjectsJSON(&buf, nil); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "[]\n" {
		t.Errorf("json = %q, want []", buf.String())
	}
}

func TestFetchProjectRows(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_jaiscloud/projects" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"projects":[{"id":"acme-e2e","kind":"observed","lastSeen":"2026-10-31T11:59:42Z","services":["storage"]}]}`))
	}))
	defer srv.Close()

	rows, err := fetchProjectRows(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "acme-e2e" {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestFetchProjectRowsEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"projects":[]}`))
	}))
	defer srv.Close()

	rows, err := fetchProjectRows(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if rows == nil || len(rows) != 0 {
		t.Fatalf("rows = %#v, want an empty non-nil slice", rows)
	}
}

func TestFetchProjectRowsUnreachable(t *testing.T) {
	if _, err := fetchProjectRows("http://127.0.0.1:1"); err == nil {
		t.Fatal("want an error for an unreachable host")
	}
}

func TestFetchProjectRowsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "project discovery not configured", http.StatusNotImplemented)
	}))
	defer srv.Close()

	if _, err := fetchProjectRows(srv.URL); err == nil {
		t.Fatal("want an error for a non-200 response")
	}
}
