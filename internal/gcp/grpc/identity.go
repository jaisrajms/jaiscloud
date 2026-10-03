package grpc

import (
	"context"
	"net/url"
	"regexp"
	"strings"

	"jaiscloud/internal/gcp/identity"

	"google.golang.org/grpc/metadata"
)

var routingProjectRE = regexp.MustCompile(`projects/([^/]+)`)

// ProjectFromMetadata resolves the project for an RPC from gRPC metadata:
// x-goog-request-params routing metadata, then the bearer token's JWT
// project_id claim, then the configured default. The request messages of the
// individual services usually carry full resource names, so those are
// authoritative; this is the fallback when a request omits the project.
func ProjectFromMetadata(ctx context.Context, defaultProject string) string {
	if p := projectFromMetadata(ctx); p != "" {
		return p
	}
	return defaultProject
}

func projectFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vals := md.Get("x-goog-request-params"); len(vals) > 0 {
		if p := routingProject(vals[0]); p != "" {
			return p
		}
	}
	if vals := md.Get("authorization"); len(vals) > 0 {
		if p := identity.ProjectFromToken(identity.BearerToken(vals[0])); p != "" {
			return p
		}
	}
	return ""
}

func routingProject(params string) string {
	for _, kv := range strings.Split(params, "&") {
		if kv == "" {
			continue
		}
		key, val, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		unesc, err := url.QueryUnescape(val)
		if err != nil {
			unesc = val
		}
		if key == "project_id" {
			return unesc
		}
		if m := routingProjectRE.FindStringSubmatch(unesc); len(m) == 2 {
			return m[1]
		}
	}
	return ""
}
