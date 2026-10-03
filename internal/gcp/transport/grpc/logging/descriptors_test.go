package logging

import (
	"context"
	"testing"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
)

func TestListMonitoredResourceDescriptors(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.ListMonitoredResourceDescriptors(ctx, &loggingpb.ListMonitoredResourceDescriptorsRequest{})
	if err != nil {
		t.Fatalf("ListMonitoredResourceDescriptors: %v", err)
	}
	if len(resp.GetResourceDescriptors()) < 10 {
		t.Fatalf("catalog = %d descriptors, want at least 10", len(resp.GetResourceDescriptors()))
	}

	found := false
	for _, d := range resp.GetResourceDescriptors() {
		if d.GetType() != "gce_instance" {
			continue
		}
		found = true
		if d.GetDisplayName() == "" {
			t.Fatalf("gce_instance has no display name")
		}
		if d.GetName() != "" {
			t.Fatalf("gce_instance name = %q, want empty on the Logging surface", d.GetName())
		}
		labels := map[string]bool{}
		for _, l := range d.GetLabels() {
			labels[l.GetKey()] = true
		}
		for _, key := range []string{"project_id", "instance_id", "zone"} {
			if !labels[key] {
				t.Fatalf("gce_instance missing label %q: %+v", key, d.GetLabels())
			}
		}
	}
	if !found {
		t.Fatalf("catalog missing gce_instance: %+v", resp.GetResourceDescriptors())
	}
}

func TestListMonitoredResourceDescriptorsPagination(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		resp, err := client.ListMonitoredResourceDescriptors(ctx, &loggingpb.ListMonitoredResourceDescriptorsRequest{
			PageSize:  3,
			PageToken: token,
		})
		if err != nil {
			t.Fatalf("ListMonitoredResourceDescriptors: %v", err)
		}
		pages++
		if len(resp.GetResourceDescriptors()) > 3 {
			t.Fatalf("page size = %d, want at most 3", len(resp.GetResourceDescriptors()))
		}
		for _, d := range resp.GetResourceDescriptors() {
			if seen[d.GetType()] {
				t.Fatalf("duplicate descriptor across pages: %q", d.GetType())
			}
			seen[d.GetType()] = true
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
		if pages > 20 {
			t.Fatalf("pagination did not terminate")
		}
	}
	if len(seen) != 11 {
		t.Fatalf("paginated catalog = %d types, want 11: %v", len(seen), seen)
	}
}
