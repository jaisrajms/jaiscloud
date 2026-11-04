// Package gcp implements the GCP CloudAdapter.
//
// GCP identifies requests by URL path (e.g. /storage/v1/b/{bucket}/o, or
// /v1/projects/{project}/topics/{topic}) rather than by a SigV4 header scope.
// Identity comes from an OAuth2 bearer token plus the project ID embedded in
// the path — see internal/gcp/identity.
package gcp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/gcp/identity"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/throttle"
	restfunctions "jaiscloud/internal/gcp/transport/rest/functions"
	restrun "jaiscloud/internal/gcp/transport/rest/run"
	"jaiscloud/internal/model"
)

// GCPAdapter routes incoming HTTP requests to the appropriate service codec.
// Implements adapter.CloudAdapter.
type GCPAdapter struct {
	codecs         map[string]adapter.Codec
	serviceAccount string // default SA identity when the token carries none
	// functionTrigger is the codec for a deployed function's HTTPS-trigger URL.
	// It is selected by request host (SourceHost), not by the service map.
	functionTrigger adapter.Codec
	// runInvocation is the codec for a deployed Cloud Run service's synthesized
	// host (*.run.app); also selected by request host (SourceHost).
	runInvocation adapter.Codec
	// throttle is the optional opt-in throttle/quota injector. The adapter
	// always implements gateway.RequestFilter, but FilterRequest returns nil
	// while this is nil, so the default run is unaffected.
	throttle *throttle.Injector
	// accountObserver is the optional sink for served (project, service) pairs,
	// used by the console's observed-project discovery. Nil disables it.
	accountObserver func(ctx context.Context, account, service string)
	// containerClusterProbe / managedKafkaClusterProbe resolve the one path the
	// two services genuinely share on a bare origin
	// (/v1/projects/{p}/locations/{l}/clusters): each reports whether its own
	// store holds a cluster at that project+location. Nil keeps the historical
	// Managed-Kafka ownership.
	containerClusterProbe    func(ctx context.Context, project, location string) bool
	managedKafkaClusterProbe func(ctx context.Context, project, location string) bool
}

// New returns a GCPAdapter with the default codec set and a blank default
// service account (identity falls back to identity.DefaultServiceAccount).
func New() *GCPAdapter {
	return NewAdapter("")
}

// NewAdapter returns a GCPAdapter with the codecs derived from gcpServices and
// the given default service-account identity.
func NewAdapter(serviceAccount string) *GCPAdapter {
	codecs := make(map[string]adapter.Codec, len(gcpServices))
	for _, svc := range gcpServices {
		if svc.Codec != nil {
			codecs[svc.ServiceName] = svc.Codec()
		}
	}
	return &GCPAdapter{
		codecs:          codecs,
		serviceAccount:  serviceAccount,
		functionTrigger: restfunctions.NewTriggerCodec(),
		runInvocation:   restrun.NewInvocationCodec(),
	}
}

// SetContainerClusterProbe installs the GKE existence probe used to resolve the
// bare-origin /v1/projects/{p}/locations/{l}/clusters ambiguity. It must report
// whether the emulated GKE control plane holds a cluster at that project and
// location.
func (a *GCPAdapter) SetContainerClusterProbe(probe func(ctx context.Context, project, location string) bool) {
	a.containerClusterProbe = probe
}

// SetManagedKafkaClusterProbe installs the Managed Kafka existence probe for the
// same path. Both probes together keep the shared collection correct in both
// directions: a location that holds Kafka clusters stays Kafka's even when the
// project also runs GKE elsewhere (or at the same location), and a location that
// holds only GKE clusters becomes GKE's.
func (a *GCPAdapter) SetManagedKafkaClusterProbe(probe func(ctx context.Context, project, location string) bool) {
	a.managedKafkaClusterProbe = probe
}

// Cloud implements adapter.CloudAdapter.
func (a *GCPAdapter) Cloud() model.Cloud { return model.CloudGCP }

// CodecFor returns the codec for the given service name.
func (a *GCPAdapter) CodecFor(service string) (adapter.Codec, error) {
	c, ok := a.codecs[service]
	if !ok {
		return nil, model.NewProviderError("UnknownService",
			fmt.Sprintf("no codec for service %q", service), 404)
	}
	return c, nil
}

// ServiceToProvider implements adapter.CloudAdapter.
// Looks up the provider registry prefix for a GCP wire service name.
// Driven by gcpServices in services.go — no hardcoded cases here.
func (a *GCPAdapter) ServiceToProvider(service string) string {
	if prefix, ok := serviceProviderMap[service]; ok {
		return prefix
	}
	return service
}

// DetectAndDecode implements adapter.CloudAdapter.
// Identifies the service from the URL path, selects the codec, and decodes.
func (a *GCPAdapter) DetectAndDecode(r *http.Request, body []byte) (*model.NormalizedRequest, adapter.Codec, error) {
	applyMethodOverride(r)
	body, err := decodeGzippedBody(r, body)
	if err != nil {
		return nil, nil, err
	}
	service, source := DetectService(r)
	if service == "" {
		return nil, nil, model.NewProviderError("UnknownService", "cannot detect target GCP service", 404)
	}

	// The bare /v1/projects/{p}/locations/{l}/clusters collection is genuinely
	// shared with Managed Kafka, and path alone cannot separate the two. Three
	// rules narrow it, so neither service is silently answered by the other:
	//
	//   - Managed Kafka's CreateCluster REQUIRES a clusterId (in the body or the
	//     query — its own error is "missing location or clusterId"), so a request
	//     carrying one is Kafka's, full stop. This must win over the store probe:
	//     a project can hold a GKE cluster and a Kafka cluster in the same
	//     location.
	//   - A create body carrying a GKE `cluster` object and no clusterId cannot
	//     be Kafka's. This is what makes the FIRST GKE create in an empty
	//     project/location work, before there is any stored cluster to probe for.
	//   - Once the control plane holds a GKE cluster at that project+location,
	//     the location is GKE's for reads, updates and deletes too — but only
	//     while Kafka holds none there. A location that holds Kafka clusters
	//     stays Kafka's, so neither service can hijack the other's data.
	if service == "managedkafka" {
		if project, location, ok := bareClusterScope(r.URL.Path); ok {
			kafkaSignal := jsonHasKey(body, "clusterId") || r.URL.Query().Get("clusterId") != ""
			switch {
			case kafkaSignal:
				// Kafka's own request; leave it.
			case r.Method == http.MethodPost && jsonHasKey(body, "cluster"):
				service, source = "container", SourcePath
			case a.managedKafkaClusterProbe != nil && a.managedKafkaClusterProbe(r.Context(), project, location):
				// Kafka has clusters here; keep them.
			case a.containerClusterProbe != nil && a.containerClusterProbe(r.Context(), project, location):
				service, source = "container", SourcePath
			}
		}
	}

	// A host-detected request is a deployed function's HTTPS trigger, which has
	// its own raw-HTTP codec instead of the control-plane JSON codec. Other
	// host-token services (GKE's "container" token) still use their service
	// codec; only the Functions trigger host is special. Every path-detected
	// request uses the service map as before.
	var codec adapter.Codec
	if service == "functions" && source == SourceHost {
		codec = a.functionTrigger
	} else if service == "run" && source == SourceHost {
		codec = a.runInvocation
	} else {
		c, err := a.CodecFor(service)
		if err != nil {
			return nil, nil, err
		}
		codec = c
	}
	nr, err := codec.Decode(r, body)
	if err != nil {
		return nil, codec, err
	}
	return nr, codec, nil
}

// applyMethodOverride honors the X-HTTP-Method-Override header, which Google's
// HTTP clients send when they tunnel a PATCH through POST (the Java
// google-http-client does this for GCS object/bucket PATCH requests). Real GCP
// documents PATCH as the sole valid value and evaluates the overridden method,
// so rewrite r.Method before service detection and codec dispatch. Without this
// an objects.patch arrives as POST and is treated as an objects.update (strict
// replacement) — silently clearing fields the PATCH body omitted, e.g.
// contentType on a Java Storage.update call.
func applyMethodOverride(r *http.Request) {
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("X-HTTP-Method-Override")), http.MethodPatch) {
		r.Method = http.MethodPatch
	}
}

// decodeGzippedBody transparently decompresses a request whose
// Content-Encoding is gzip. Google's JSON APIs accept gzip request bodies, and
// the official SDKs rely on it: the Java google-http-client enables gzip by
// default for every request body, so GCS bucket/object metadata writes,
// multipart uploads and BigQuery jobs all arrive gzip-encoded. Without this a
// JSON body fails to parse ("malformed JSON body", 400).
//
// GCS media uploads (uploadType=media) are deliberately excluded: there
// Content-Encoding describes the stored object's own encoding (transcoding),
// not the transport, so the bytes must reach the provider untouched.
//
// For non-streaming requests the decoded bytes are returned; for streaming
// requests (e.g. multipart/related uploads) the request body is wrapped so the
// codec reads decompressed bytes while Close still closes the original body.
func decodeGzippedBody(r *http.Request, body []byte) ([]byte, error) {
	if !strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "gzip") {
		return body, nil
	}
	if r.URL.Query().Get("uploadType") == "media" {
		return body, nil
	}
	// A raw XML API object PUT (/ {bucket}/{object}) carries Content-Encoding as
	// the object's own encoding, not the transport's: real GCS stores the gzip
	// bytes as-is and records x-goog-stored-content-encoding. Decompressing here
	// would corrupt the stored object, so leave the body and header untouched
	// (the codec captures the header as Object.contentEncoding).
	if r.Method == http.MethodPut && isRawStorageMediaPath(r) {
		return body, nil
	}
	r.Header.Del("Content-Encoding")

	// Non-streaming: the gateway already buffered the body.
	if body != nil {
		if len(body) == 0 {
			return body, nil
		}
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed gzip request body", 400)
		}
		defer zr.Close()
		out, err := io.ReadAll(zr)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed gzip request body", 400)
		}
		return out, nil
	}

	// Streaming: leave r.Body readable by the codec, decompressed.
	if r.Body != nil {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed gzip request body", 400)
		}
		r.Body = &gzipBody{Reader: zr, underlying: r.Body}
		r.ContentLength = -1
	}
	return body, nil
}

// gzipBody adapts a gzip.Reader over an underlying request body so closing it
// releases both (gzip.Reader.Close does not close its source).
type gzipBody struct {
	*gzip.Reader
	underlying io.ReadCloser
}

func (g *gzipBody) Close() error {
	err := g.Reader.Close()
	if cerr := g.underlying.Close(); err == nil {
		err = cerr
	}
	return err
}

// EnrichRequest implements adapter.CloudAdapter.
// Resolves project ID (path → token → config default) and returns the bearer
// token as the access key. The service-account email is decoded from the token
// when present (exposed to providers via identity, not here).
func (a *GCPAdapter) EnrichRequest(r *http.Request, defaultRegion, defaultAccountID string) (region, accountID, accessKey string) {
	ident := identity.FromRequest(r)

	// The identity package falls back to its own hardcoded default. Prefer the
	// configured project (defaultAccountID) when the request carried no explicit
	// project in the URL path or bearer token.
	project := ident.ProjectID
	if ident.Source == identity.SourceDefault ||
		(ident.Source == identity.SourceBearer && ident.ProjectID == identity.DefaultProjectID) {
		if defaultAccountID != "" {
			project = defaultAccountID
		}
	}

	region = defaultRegion
	if region == "" {
		region = "global"
	}
	return region, project, ident.AccessKey
}

// ResourceIDFor implements adapter.CloudAdapter.
// Returns a GCP resource-name formatter for the given project.
func (a *GCPAdapter) ResourceIDFor(_, accountID string) func(resourceType, name string) string {
	return resource.ResourceID(accountID)
}

// jsonHasKey reports whether body is a JSON object carrying key at its top
// level. It is only used for routing disambiguation, so a malformed or non-JSON
// body simply answers false and the request keeps its path-detected service.
func jsonHasKey(body []byte, key string) bool {
	if len(body) == 0 {
		return false
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return false
	}
	_, ok := m[key]
	return ok
}
