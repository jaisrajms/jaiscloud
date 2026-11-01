package resource

import "testing"

// TestComputeLinkFor pins the shared Compute-link resolution both Dataproc and
// GKE use for a caller-supplied link: a short name, a partial resource name and
// a full self-link resolve to one absolute link, and the project the link names
// wins over the fallback (a Shared-VPC host project is preserved).
func TestComputeLinkFor(t *testing.T) {
	const rt = "compute-network"
	for _, tc := range []struct {
		name string
		uri  string
		want string
	}{
		{"short name", "default", ComputeBasePath + "projects/cluster-project/global/networks/default"},
		{"partial resource name", "projects/host-project/global/networks/vpc", ComputeBasePath + "projects/host-project/global/networks/vpc"},
		{"full self-link", "https://www.googleapis.com/compute/v1/projects/host-project/global/networks/vpc", ComputeBasePath + "projects/host-project/global/networks/vpc"},
	} {
		got := ComputeLinkFor(tc.uri, "cluster-project", rt, LinkTail(tc.uri))
		if got != tc.want {
			t.Fatalf("%s: ComputeLinkFor = %q, want %q", tc.name, got, tc.want)
		}
	}
	if got := ComputeLink("cluster-project", rt, "default"); got != ComputeBasePath+"projects/cluster-project/global/networks/default" {
		t.Fatalf("ComputeLink = %q", got)
	}
	if got := ComputePathFor("projects/host/global/networks/vpc", "cluster-project", rt, "vpc"); got != "projects/host/global/networks/vpc" {
		t.Fatalf("ComputePathFor = %q", got)
	}
	if got := PathSegmentAfter("projects/p/regions/europe-west1/subnetworks/sn", "regions"); got != "europe-west1" {
		t.Fatalf("PathSegmentAfter(regions) = %q", got)
	}
	if got := ProjectFromURI("default"); got != "" {
		t.Fatalf("ProjectFromURI(short name) = %q, want empty", got)
	}
}
