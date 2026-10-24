package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestMemoryStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	j := Job{Name: "j1", Schedule: "* * * * *", TimeZone: "UTC", Target: TargetHTTP, HTTP: &HttpTarget{URI: "http://x"}}
	if err := s.CreateJob(ctx, "p", "l", j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if err := s.CreateJob(ctx, "p", "l", j); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate err = %v, want ErrAlreadyExists", err)
	}
	got, err := s.GetJob(ctx, "p", "l", "j1")
	if err != nil || got.Name != "j1" || got.ProjectID != "p" || got.Location != "l" {
		t.Fatalf("GetJob = %+v, %v", got, err)
	}
	got.Description = "d"
	if err := s.UpdateJob(ctx, "p", "l", got); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if g, _ := s.GetJob(ctx, "p", "l", "j1"); g.Description != "d" {
		t.Fatalf("update not persisted: %+v", g)
	}
	if err := s.UpdateJob(ctx, "p", "l", Job{Name: "missing"}); !errors.Is(err, ErrNoSuchJob) {
		t.Fatalf("update missing err = %v, want ErrNoSuchJob", err)
	}
	all, err := s.ListAllJobs(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("ListAllJobs = %d, %v", len(all), err)
	}
	if err := s.DeleteJob(ctx, "p", "l", "j1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := s.GetJob(ctx, "p", "l", "j1"); !errors.Is(err, ErrNoSuchJob) {
		t.Fatalf("after delete err = %v", err)
	}
}

func TestMemoryStoreListJobsByProject(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	for _, tc := range []struct{ project, location, name string }{
		{"p", "us-central1", "b"},
		{"p", "us-central1", "a"},
		{"p", "europe-west1", "c"},
		{"other", "us-central1", "d"},
	} {
		if err := s.CreateJob(ctx, tc.project, tc.location, Job{Name: tc.name}); err != nil {
			t.Fatalf("CreateJob %s: %v", tc.name, err)
		}
	}
	got, err := s.ListJobsByProject(ctx, "p")
	if err != nil {
		t.Fatalf("ListJobsByProject: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	// Sorted by location then name; the other project's job is excluded.
	want := []string{"c", "a", "b"}
	for i, name := range want {
		if got[i].Name != name {
			t.Fatalf("got[%d] = %q, want %q", i, got[i].Name, name)
		}
	}
}

func TestMemoryStoreAtomicUpdateConcurrent(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateJob(ctx, "p", "l", Job{Name: "j1", Schedule: "* * * * *"}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = s.UpdateJobAtomic(ctx, "p", "l", "j1", func(j Job) (Job, error) {
				j.Description = j.Description + "x"
				return j, nil
			})
		}()
	}
	wg.Wait()
	got, _ := s.GetJob(ctx, "p", "l", "j1")
	if len(got.Description) != n {
		t.Fatalf("description length = %d, want %d (lost updates)", len(got.Description), n)
	}
	s.Reset(ctx)
	if all, _ := s.ListAllJobs(ctx); len(all) != 0 {
		t.Fatalf("reset left %d jobs", len(all))
	}
}
