// Package seams is how one GCP service calls another inside jaiscloud-gcp.
//
// Cloud Storage notifications, Secret Manager events, Dataproc events, Cloud
// Scheduler and Monitoring notifications, and Eventarc's transport topics all
// need Pub/Sub. They used to call the Pub/Sub provider's Go methods directly, so
// a plugin that overrode a Pub/Sub key never saw them. Here each such caller is
// an ordinary client of the key: it builds the request a REST client would send
// and dispatches it through the registry (Origin "internal"), so a plugin that
// overrides the key is used, as if the built-in did not exist.
//
// One key still means one contract. The documented differences from a REST
// client are: no HTTP-level validation of the request (there is no HTTP request),
// and a dead-letter forward asks the built-in not to hand the message to the
// event-delivery engine again (provider/pubsub.NoDispatchMeta).
package seams

import (
	"context"
	"encoding/base64"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Set is the registry plus the identity the gateway would inject.
type Set struct {
	reg     *provider.Registry
	ad      adapter.CloudAdapter
	clock   clock.Clock
	port    int
	region  string
	project string // config default project, for callers that carry none
}

// New builds a Set. reg is the registry plugins are installed on; it may still
// be empty when New is called, because nothing dispatches before startup ends.
func New(reg *provider.Registry, ad adapter.CloudAdapter, clk clock.Clock, port int, region, project string) *Set {
	return &Set{reg: reg, ad: ad, clock: clk, port: port, region: region, project: project}
}

// request builds the NormalizedRequest of an internal client call of project.
func (s *Set) request(service, action, project string, params map[string]any, meta map[string]string) *model.NormalizedRequest {
	nr := &model.NormalizedRequest{
		Service: service, Action: action, Params: params,
		Clock: s.clock, Region: s.region, AccountID: project, Port: s.port,
		Cloud: s.ad.Cloud(), Origin: model.OriginInternal,
		ResourceID: s.ad.ResourceIDFor(s.region, project),
	}
	for k, v := range meta {
		nr.SetMeta(k, v)
	}
	return nr
}

// dispatch calls the registry key of service/action as an internal client of
// project, with REST-shaped params (what the REST decoder produces).
func (s *Set) dispatch(ctx context.Context, service, action, project string, params map[string]any, meta map[string]string) (*model.ProviderResponse, error) {
	return s.reg.Dispatch(ctx, s.ad.ServiceToProvider(service)+"."+action, s.request(service, action, project, params, meta))
}

func b64(data []byte) string { return base64.StdEncoding.EncodeToString(data) }
