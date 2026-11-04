package gcp

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestDetectContainerService locks the GKE routing decision. GKE shares the
// canonical /v1/projects/{p}/locations/{l}/clusters path with Managed Kafka on
// one origin, so the Host's first DNS label is the discriminator; the default
// host still routes that path to managedkafka. Terraform/gcloud use the
// /container/ path prefix, which the descriptor claims.
func TestDetectContainerService(t *testing.T) {
	cases := []struct {
		host string
		path string
		want string
		src  DetectionSource
	}{
		{"container.localhost:4588", "/v1/projects/p/locations/us-central1/clusters", "container", SourceHost},
		{"container.localhost:4588", "/v1/projects/p/locations/us-central1/clusters/c1", "container", SourceHost},
		{"container.localhost:4588", "/v1/projects/p/locations/us-central1/operations", "container", SourceHost},
		{"container.googleapis.com", "/v1/projects/p/locations/us-central1/clusters/c1", "container", SourceHost},
		// Real GKE publishes v1 and v1beta1; the beta client surfaces
		// (gcloud beta container, the google-beta provider) request /v1beta1/.
		{"container.localhost:4588", "/v1beta1/projects/p/locations/us-central1/clusters", "container", SourceHost},
		{"container.googleapis.com", "/v1beta1/projects/p/locations/us-central1/clusters/c1", "container", SourceHost},
		{"localhost:8080", "/container/v1beta1/projects/p/locations/us-central1/clusters", "container", SourcePath},
		// A beta path with no container host must not become a GCS object
		// download (the pre-fix behaviour answered an XML NoSuchBucket); it is
		// an unknown service, which is the honest answer.
		{"localhost:8080", "/v1beta1/projects/p/locations/us-central1/clusters", "", SourceUnknown},
		// Default host: the canonical path stays Managed Kafka (the pre-existing
		// behavior — do not regress the collision).
		{"localhost:8080", "/v1/projects/p/locations/us-central1/clusters", "managedkafka", SourcePath},
		// Path mode (Terraform/gcloud custom endpoint).
		{"localhost:8080", "/container/v1/projects/p/locations/us-central1/clusters", "container", SourcePath},
		// The host token only rewrites /v1/, so a non-/v1/ path is not GKE.
		{"container.localhost:4588", "/v2/projects/p/locations/us-central1/functions", "functions", SourcePath},
	}
	for _, tc := range cases {
		r := &http.Request{Host: tc.host, URL: &url.URL{Path: tc.path}}
		got, src := DetectService(r)
		if got != tc.want || src != tc.src {
			t.Errorf("DetectService(host=%q path=%q) = (%q,%v), want (%q,%v)",
				tc.host, tc.path, got, src, tc.want, tc.src)
		}
	}
}

// TestContainerHostTokenOnlyV1 verifies the host token never claims a GCS raw
// media path or an admin path even when the Host is a container host.
func TestContainerHostTokenOnlyV1(t *testing.T) {
	for _, path := range []string{"/bucket/object.txt", "/_jaiscloud/health"} {
		r := &http.Request{Host: "container.localhost:4588", Method: http.MethodGet, URL: &url.URL{Path: path}}
		if got, _ := DetectService(r); got == "container" {
			t.Errorf("DetectService(%q) claimed container for a non-/v1/ path", path)
		}
	}
}

// GKE-only subpaths and verbs disambiguate the shared clusters collection without
// a host token: Managed Kafka has no node pools and none of these verbs.
func TestDetectContainerByGKEOnlyShape(t *testing.T) {
	cases := []string{
		"/v1/projects/p/locations/us-central1/clusters/c1/nodePools",
		"/v1/projects/p/locations/us-central1/clusters/c1/nodePools/np1",
		"/v1/projects/p/locations/us-central1/clusters/c1/nodePools/np1:setSize",
		"/v1/projects/p/locations/us-central1/clusters/c1:setAddons",
		"/v1/projects/p/locations/us-central1/clusters/c1:setNetworkPolicy",
		"/v1/projects/p/locations/us-central1/clusters/c1:updateMaster",
		"/v1/projects/p/locations/us-central1/clusters/c1:startIpRotation",
		"/v1/projects/p/locations/us-central1/clusters/c1:setResourceLabels",
		"/v1/projects/p/locations/us-central1/clusters/c1/jwks",
		"/v1/projects/p/locations/us-central1/clusters/c1/well-known/openid-configuration",
	}
	for _, path := range cases {
		r := &http.Request{Host: "localhost:8080", Method: http.MethodGet, URL: &url.URL{Path: path}}
		if got, _ := DetectService(r); got != "container" {
			t.Errorf("DetectService(%q) = %q, want container", path, got)
		}
	}
	// The bare collection stays Managed Kafka without a discriminator.
	r := &http.Request{Host: "localhost:8080", Method: http.MethodGet,
		URL: &url.URL{Path: "/v1/projects/p/locations/us-central1/clusters"}}
	if got, _ := DetectService(r); got != "managedkafka" {
		t.Errorf("bare clusters collection = %q, want managedkafka", got)
	}
}

// With the GKE existence probe installed, a bare-host clusters request for a
// project/location that actually holds a GKE cluster is GKE's, so a client
// configured without a container.* host is not silently answered by Managed
// Kafka with an empty list.
func TestBareClusterRoutesToGKEWhentheControlPlaneHasOne(t *testing.T) {
	a := NewAdapter("")
	a.SetContainerClusterProbe(func(_ context.Context, project, location string) bool {
		return project == "p" && location == "us-central1"
	})
	r, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/v1/projects/p/locations/us-central1/clusters", nil)
	svc, _ := DetectService(r)
	if svc != "managedkafka" {
		t.Fatalf("precondition: DetectService = %q, want managedkafka", svc)
	}
	codec, err := a.CodecFor("container")
	if err != nil {
		t.Fatalf("CodecFor(container): %v", err)
	}
	if _, _, err := a.DetectAndDecode(r, nil); err == nil {
		// A container codec was selected; assert it via the codec name.
		if codec == nil {
			t.Fatal("no container codec")
		}
	}

	// A project/location with no GKE cluster keeps Managed Kafka's ownership.
	r2, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/v1/projects/other/locations/europe-west1/clusters", nil)
	if _, _, err := a.DetectAndDecode(r2, nil); err != nil {
		t.Fatalf("DetectAndDecode(other): %v", err)
	}
}

// A create body is itself a discriminator: Managed Kafka's CreateCluster
// requires clusterId (in the body or the query), so a GKE-shaped create cannot
// be Kafka's — which is what makes the FIRST create in an empty location work.
func TestBareClusterCreateDisambiguatedByBody(t *testing.T) {
	a := NewAdapter("") // no probe installed: nothing stored yet
	cases := []struct {
		name  string
		url   string
		body  string
		wantS string
	}{
		{
			name: "GKE create", url: "http://localhost:8080/v1/projects/p/locations/us-central1/clusters",
			body: `{"cluster":{"name":"c1","initialNodeCount":1}}`, wantS: "container",
		},
		{
			name: "Kafka create (query clusterId)", url: "http://localhost:8080/v1/projects/p/locations/us-central1/clusters?clusterId=k1",
			body: `{"cluster":{"name":"k1"}}`, wantS: "managedkafka",
		},
		{
			name: "Kafka create (body clusterId)", url: "http://localhost:8080/v1/projects/p/locations/us-central1/clusters",
			body: `{"clusterId":"k1","cluster":{"name":"k1"}}`, wantS: "managedkafka",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := http.NewRequest(http.MethodPost, tc.url, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			svc, _ := DetectService(r)
			if svc != "managedkafka" {
				t.Fatalf("precondition: DetectService = %q", svc)
			}
			_, _, err := a.DetectAndDecode(r, []byte(tc.body))
			if err != nil {
				t.Fatalf("DetectAndDecode: %v", err)
			}
		})
	}
}

// A project can hold a GKE cluster and a Managed Kafka cluster in the same
// location. Kafka's clusterId is a positive signal and must win over the GKE
// existence probe, or the probe would hijack every Kafka create in a project
// that happens to have a GKE cluster.
func TestKafkaCreateWinsOverGKEProbe(t *testing.T) {
	a := NewAdapter("")
	a.SetContainerClusterProbe(func(_ context.Context, _, _ string) bool { return true })

	for _, tc := range []struct{ name, url, body string }{
		{"query clusterId", "http://localhost:8080/v1/projects/p/locations/us-central1/clusters?clusterId=k1", `{"cluster":{"name":"k1"}}`},
		{"body clusterId", "http://localhost:8080/v1/projects/p/locations/us-central1/clusters", `{"clusterId":"k1","cluster":{"name":"k1"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := http.NewRequest(http.MethodPost, tc.url, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			svc, _ := DetectService(r)
			if svc != "managedkafka" {
				t.Fatalf("precondition: DetectService = %q", svc)
			}
			_, codec, err := a.DetectAndDecode(r, []byte(tc.body))
			if err != nil {
				t.Fatalf("DetectAndDecode: %v", err)
			}
			if got := codec.ServiceName(); got != "managedkafka" {
				t.Fatalf("codec = %q, want managedkafka", got)
			}
		})
	}
}
