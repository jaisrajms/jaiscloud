// Package managedkafka is the REST transport for Apache Kafka for BigQuery
// (Managed Kafka) v1 (managedkafka.googleapis.com).
//
// Real GCP serves the proto-defined v1 API over both gRPC and REST
// (grpc-gateway transcoding), and the REST surface here follows the vendored
// Discovery document. The Codec is a NormalizedRequest adapter (HTTP path/body
// ↔ the core's typed API); the Provider holds the routes. Neither owns business
// logic — both delegate to the single core Service shared with the gRPC
// transport (see internal/gcp/service/managedkafka).
//
// Resources live under
// /v1/projects/{project}/locations/{location}/clusters[/{id}][/topics[/{topicId}]|
// /consumerGroups[/{group}]|/acls[/{aclId}]], with the ACL custom methods
// :addAclEntry and :removeAclEntry. Cluster create/update/delete return a
// google.longrunning.Operation inline with done:true; topic and ACL CRUD are
// synchronous.
package managedkafka

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "managedkafka"

// Codec decodes Managed Kafka REST requests into a NormalizedRequest and encodes
// provider responses as the GCP JSON envelope. It satisfies adapter.Codec
// structurally (the adapter package imports this package, so this package must
// not import it).
type Codec struct{}

// NewCodec returns the Managed Kafka REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a v1 Managed Kafka path into a NormalizedRequest.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+1 >= len(seg) {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource path", 404)
	}

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	// Query parameters are decoded FIRST; the authoritative path segments are
	// assigned afterwards so a crafted ?project=/?location= cannot override the
	// resource addressed by the URL.
	queryToParams(r, nr.Params)
	nr.Params["project"] = seg[pi+1]
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	// rest = ["locations", "{location}", "clusters", ...] after projects/{project}.
	rest := seg[pi+2:]
	if len(rest) < 3 || rest[0] != "locations" {
		return nil, model.NewProviderError("InvalidRequest", "expected locations/{location}/clusters in managed kafka path", 404)
	}
	nr.Params["location"] = rest[1]
	tail := rest[2:]

	// Long-running operations: locations/{location}/operations[/{id}]. The path
	// is path-ambiguous with Cloud Workflows' LRO surface on a single host and
	// is routed to Workflows; decoding it here keeps the registered
	// ManagedKafka.GetOperation/ListOperations handlers reachable by direct
	// dispatch (operations are returned inline with done:true, so no client
	// needs to poll them).
	if len(tail) > 0 && tail[0] == "operations" {
		nr.Params["resourceType"] = "operations"
		nr.Params["name"] = strings.Join(rest, "/")
		switch {
		case len(tail) == 1 && r.Method == http.MethodGet:
			nr.Action = "ListOperations"
		case len(tail) == 2 && r.Method == http.MethodGet:
			nr.Params["operationId"] = tail[1]
			nr.Action = "GetOperation"
		}
		if nr.Action == "" {
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
		}
		return nr, nil
	}

	if len(tail) == 0 || tail[0] != "clusters" {
		return nil, model.NewProviderError("InvalidRequest", "expected clusters in managed kafka path", 404)
	}

	switch len(tail) {
	case 1: // ["clusters"]
		return c.decodeClusterCollection(nr, r)
	case 2: // ["clusters", id]
		return c.decodeClusterItem(nr, r, tail[1])
	}

	clusterID := tail[1]
	sub := tail[2]
	switch sub {
	case "topics":
		return c.decodeTopic(nr, r, clusterID, tail[3:])
	case "consumerGroups":
		return c.decodeConsumerGroup(nr, r, clusterID, tail[3:])
	case "acls":
		return c.decodeAcl(nr, r, clusterID, tail[3:])
	}
	return nil, model.NewProviderError("InvalidRequest", "unrecognized managed kafka path", 404)
}

func (c *Codec) decodeClusterCollection(nr *model.NormalizedRequest, r *http.Request) (*model.NormalizedRequest, error) {
	nr.Params["resourceType"] = "clusters"
	nr.Params["name"] = "locations/" + strParam(nr, "location") + "/clusters"
	switch r.Method {
	case http.MethodGet:
		nr.Action = "ListClusters"
	case http.MethodPost:
		nr.Action = "CreateCluster"
	default:
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

func (c *Codec) decodeClusterItem(nr *model.NormalizedRequest, r *http.Request, clusterID string) (*model.NormalizedRequest, error) {
	if clusterID == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cluster id", 404)
	}
	nr.Params["resourceType"] = "clusters"
	nr.Params["clusterId"] = clusterID
	nr.Params["name"] = "locations/" + strParam(nr, "location") + "/clusters/" + clusterID
	switch r.Method {
	case http.MethodGet:
		nr.Action = "GetCluster"
	case http.MethodPatch:
		nr.Action = "UpdateCluster"
	case http.MethodDelete:
		nr.Action = "DeleteCluster"
	default:
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

func (c *Codec) decodeTopic(nr *model.NormalizedRequest, r *http.Request, clusterID string, tail []string) (*model.NormalizedRequest, error) {
	base := "locations/" + strParam(nr, "location") + "/clusters/" + clusterID
	nr.Params["resourceType"] = "topics"
	nr.Params["clusterId"] = clusterID
	switch len(tail) {
	case 0:
		nr.Params["name"] = base + "/topics"
		switch r.Method {
		case http.MethodGet:
			nr.Action = "ListTopics"
		case http.MethodPost:
			nr.Action = "CreateTopic"
		default:
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
		}
	case 1:
		if tail[0] == "" {
			return nil, model.NewProviderError("InvalidRequest", "missing topic id", 404)
		}
		nr.Params["topicId"] = tail[0]
		nr.Params["name"] = base + "/topics/" + tail[0]
		switch r.Method {
		case http.MethodGet:
			nr.Action = "GetTopic"
		case http.MethodPatch:
			nr.Action = "UpdateTopic"
		case http.MethodDelete:
			nr.Action = "DeleteTopic"
		default:
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
		}
	default:
		return nil, model.NewProviderError("InvalidRequest", "unrecognized managed kafka path", 404)
	}
	return nr, nil
}

func (c *Codec) decodeConsumerGroup(nr *model.NormalizedRequest, r *http.Request, clusterID string, tail []string) (*model.NormalizedRequest, error) {
	base := "locations/" + strParam(nr, "location") + "/clusters/" + clusterID
	nr.Params["resourceType"] = "consumerGroups"
	nr.Params["clusterId"] = clusterID
	if len(tail) == 0 {
		nr.Params["name"] = base + "/consumerGroups"
		if r.Method != http.MethodGet {
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
		}
		nr.Action = "ListConsumerGroups"
		return nr, nil
	}
	group := strings.Join(tail, "/")
	if group == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing consumer group id", 404)
	}
	nr.Params["consumerGroupId"] = group
	nr.Params["name"] = base + "/consumerGroups/" + group
	switch r.Method {
	case http.MethodGet:
		nr.Action = "GetConsumerGroup"
	case http.MethodPatch:
		nr.Action = "UpdateConsumerGroup"
	case http.MethodDelete:
		nr.Action = "DeleteConsumerGroup"
	default:
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

func (c *Codec) decodeAcl(nr *model.NormalizedRequest, r *http.Request, clusterID string, tail []string) (*model.NormalizedRequest, error) {
	base := "locations/" + strParam(nr, "location") + "/clusters/" + clusterID
	nr.Params["resourceType"] = "acls"
	nr.Params["clusterId"] = clusterID
	if len(tail) == 0 {
		nr.Params["name"] = base + "/acls"
		switch r.Method {
		case http.MethodGet:
			nr.Action = "ListAcls"
		case http.MethodPost:
			nr.Action = "CreateAcl"
		default:
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
		}
		return nr, nil
	}

	leaf := strings.Join(tail, "/")
	verb := ""
	if i := strings.LastIndexByte(leaf, ':'); i >= 0 {
		verb = leaf[i+1:]
		leaf = leaf[:i]
	}
	if leaf == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing acl id", 404)
	}
	nr.Params["aclId"] = leaf
	nr.Params["name"] = base + "/acls/" + leaf

	if verb != "" {
		if r.Method != http.MethodPost {
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
		}
		switch verb {
		case "addAclEntry":
			nr.Action = "AddAclEntry"
		case "removeAclEntry":
			nr.Action = "RemoveAclEntry"
		default:
			return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
		}
		return nr, nil
	}

	switch r.Method {
	case http.MethodGet:
		nr.Action = "GetAcl"
	case http.MethodPatch:
		nr.Action = "UpdateAcl"
	case http.MethodDelete:
		nr.Action = "DeleteAcl"
	default:
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// Encode serialises a provider response as JSON.
func (c *Codec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	if raw, ok := resp.Data[wire.RawJSONKey].(json.RawMessage); ok {
		return status, headers, raw
	}
	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as a GCP error envelope.
func (c *Codec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	statusStr, _ := gcperr.Resolve(perr)
	env := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": perr.Message,
			"status":  statusStr,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}
