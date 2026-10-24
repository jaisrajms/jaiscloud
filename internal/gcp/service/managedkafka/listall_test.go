package managedkafka

import (
	"context"
	"testing"
)

func TestListAllClusters_AcrossLocationsSorted(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	for _, c := range []struct{ location, name string }{
		{"us-central1", "beta"},
		{"europe-west1", "alpha"},
		{"us-central1", "alpha"},
	} {
		if _, _, err := s.CreateCluster(ctx, "proj", c.location, c.name, clusterIn(nil)); err != nil {
			t.Fatalf("CreateCluster(%s/%s): %v", c.location, c.name, err)
		}
	}
	if _, _, err := s.CreateCluster(ctx, "other", "us-central1", "zzz", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster(other): %v", err)
	}

	got, err := s.ListAllClusters(ctx, "proj")
	if err != nil {
		t.Fatalf("ListAllClusters: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d clusters, want 3", len(got))
	}
	// Sorted by location then name, and scoped to the project.
	want := []string{"europe-west1/alpha", "us-central1/alpha", "us-central1/beta"}
	for i, key := range want {
		if got[i].Location+"/"+got[i].Name != key {
			t.Fatalf("cluster[%d] = %s/%s, want %s", i, got[i].Location, got[i].Name, key)
		}
	}
}

func TestListAllTopics_AcrossClustersSorted(t *testing.T) {
	ctx := context.Background()
	s := newCore()

	for _, loc := range []string{"us-central1", "europe-west1"} {
		if _, _, err := s.CreateCluster(ctx, "proj", loc, "c1", clusterIn(nil)); err != nil {
			t.Fatalf("CreateCluster(%s): %v", loc, err)
		}
	}
	if _, _, err := s.CreateCluster(ctx, "proj", "us-central1", "c2", clusterIn(nil)); err != nil {
		t.Fatalf("CreateCluster(c2): %v", err)
	}

	for _, row := range []struct{ location, cluster, name string }{
		{"us-central1", "c1", "t2"},
		{"us-central1", "c1", "t1"},
		{"europe-west1", "c1", "t1"},
		{"us-central1", "c2", "t1"},
	} {
		if _, err := s.CreateTopic(ctx, "proj", row.location, row.cluster, row.name, topicIn(1, 1)); err != nil {
			t.Fatalf("CreateTopic(%s/%s/%s): %v", row.location, row.cluster, row.name, err)
		}
	}

	got, err := s.ListAllTopics(ctx, "proj")
	if err != nil {
		t.Fatalf("ListAllTopics: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d topics, want 4", len(got))
	}
	want := []string{
		"europe-west1/c1/t1",
		"us-central1/c1/t1",
		"us-central1/c1/t2",
		"us-central1/c2/t1",
	}
	for i, key := range want {
		gotKey := got[i].Location + "/" + got[i].ClusterName + "/" + got[i].Name
		if gotKey != key {
			t.Fatalf("topic[%d] = %s, want %s", i, gotKey, key)
		}
	}
}

func TestListAllClusters_Empty(t *testing.T) {
	s := newCore()
	got, err := s.ListAllClusters(context.Background(), "proj")
	if err != nil {
		t.Fatalf("ListAllClusters: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d clusters, want 0", len(got))
	}
}
