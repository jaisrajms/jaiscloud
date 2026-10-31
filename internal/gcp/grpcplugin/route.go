package grpcplugin

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// route is what the interceptor knows about one RPC: the registry key its REST
// twin has (so one plugin key covers both transports) and its message types.
type route struct {
	key      string // empty when dynamic
	dynamic  bool   // the key depends on the resource named in the request (the generic IAM RPCs)
	method   protoreflect.MethodDescriptor
	host     string // the service's google.api.default_host, which some REST detectors key on
	verb     string
	template string
	in, out  protoreflect.MessageType
}

// resolver maps a gRPC full method ("/google.pubsub.v1.Publisher/CreateTopic")
// to its route, once per method. The registry key is the one the REST adapter
// derives for the method's google.api.http rule, so the REST and gRPC calls of
// an operation meet at the same key without a hand-kept table.
type resolver struct {
	keyOf func(host, verb, path string) (string, bool) // REST detect+decode → registry key
	cache sync.Map                                     // full method → *route (nil = unmapped)
}

func (rs *resolver) route(fullMethod string) *route {
	if v, ok := rs.cache.Load(fullMethod); ok {
		return v.(*route)
	}
	rt := rs.build(fullMethod)
	rs.cache.Store(fullMethod, rt)
	return rt
}

func (rs *resolver) build(fullMethod string) *route {
	name := strings.ReplaceAll(strings.TrimPrefix(fullMethod, "/"), "/", ".")
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil
	}
	md, ok := d.(protoreflect.MethodDescriptor)
	if !ok {
		return nil
	}
	// google.longrunning.Operations serves the operations of every service; which
	// service's key an operation belongs to follows its name, which this resolver does
	// not interpret, so these RPCs are left alone rather than mapped to one service.
	if md.Parent().FullName() == "google.longrunning.Operations" {
		return nil
	}
	rule, _ := proto.GetExtension(md.Options(), annotations.E_Http).(*annotations.HttpRule)
	in, err := protoregistry.GlobalTypes.FindMessageByName(md.Input().FullName())
	if err != nil {
		return nil
	}
	out, err := protoregistry.GlobalTypes.FindMessageByName(md.Output().FullName())
	if err != nil {
		return nil
	}
	host, _ := proto.GetExtension(md.Parent().Options(), annotations.E_DefaultHost).(string)
	// The primary binding first, then the additional ones: some REST adapters
	// serve only one of a method's URLs (e.g. regions/ rather than locations/).
	bindings := append([]*annotations.HttpRule{rule}, rule.GetAdditionalBindings()...)
	for _, b := range bindings {
		verb, tmpl := ruleOf(b)
		if tmpl == "" {
			continue
		}
		path, ok := render(tmpl, nil)
		if !ok {
			continue
		}
		for _, p := range versions(path) {
			if key, ok := rs.keyOf(host, verb, p); ok && rs.respectsSuffix(host, verb, tmpl, p, key) {
				return &route{key: key, method: md, host: host, verb: verb, template: tmpl, in: in, out: out}
			}
		}
	}
	// google.iam.v1.IAMPolicy serves every resource type with one method; which
	// registry key a call means depends on the resource it names.
	if verb, tmpl := ruleOf(rule); strings.Contains(tmpl, "{resource=**}") {
		return &route{dynamic: true, method: md, host: host, verb: verb, template: tmpl, in: in, out: out}
	}
	return nil
}

// versions lists the path as written and then at the other API versions the
// emulator's REST adapters use: a service can expose its REST twin under /v1
// while the gRPC method is /v3 (Resource Manager).
func versions(path string) []string {
	out := []string{path}
	for _, v := range []string{"/v1/", "/v1beta1/", "/v2/"} {
		if rest, ok := strings.CutPrefix(path, "/v3/"); ok {
			out = append(out, v+rest)
		}
	}
	return out
}

// respectsSuffix rejects a key the REST detector reached by ignoring the end of the
// path. A template that ends in a literal (".../topics/*/subscriptions", ":cancel")
// names an operation of its own; if the same path without that literal resolves to
// the very same key, the REST adapter has no route for the operation and the key
// is a neighbour's (ListTopicSubscriptions would otherwise map to PubSub.TopicGet).
func (rs *resolver) respectsSuffix(host, verb, tmpl, path, key string) bool {
	trimmed, ok := trimTrailingLiteral(tmpl)
	if !ok {
		return true
	}
	short, ok := render(trimmed, nil)
	if !ok {
		return true
	}
	for _, p := range versions(short) {
		if other, ok := rs.keyOf(host, verb, p); ok && other == key {
			return false
		}
	}
	return true
}

// trimTrailingLiteral removes the literal that follows the last {variable} of a
// template ("/v1/{name=...}/subscriptions" -> "/v1/{name=...}"; "...}:getIamPolicy"
// -> "...}"). ok is false when the template does not end in such a literal.
func trimTrailingLiteral(tmpl string) (string, bool) {
	end := strings.LastIndex(tmpl, "}")
	if end < 0 || end == len(tmpl)-1 {
		return "", false
	}
	return tmpl[:end+1], true
}

// ruleOf returns the verb and path template of a rule's primary binding.
func ruleOf(r *annotations.HttpRule) (verb, tmpl string) {
	switch p := r.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		return http.MethodGet, p.Get
	case *annotations.HttpRule_Put:
		return http.MethodPut, p.Put
	case *annotations.HttpRule_Post:
		return http.MethodPost, p.Post
	case *annotations.HttpRule_Delete:
		return http.MethodDelete, p.Delete
	case *annotations.HttpRule_Patch:
		return http.MethodPatch, p.Patch
	case *annotations.HttpRule_Custom:
		return p.Custom.GetKind(), p.Custom.GetPath()
	}
	return "", ""
}

var varRe = regexp.MustCompile(`\{([^{}=]+)(?:=([^{}]+))?\}`)

// render turns a path template into a concrete path. Each {field=pattern}
// takes the string value of that field of msg; when msg is nil, or the value is
// empty, a placeholder that satisfies the pattern is used. ok is false when a
// value was needed but not usable.
func render(tmpl string, msg protoreflect.Message) (string, bool) {
	out := varRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		sub := varRe.FindStringSubmatch(m)
		if msg != nil {
			if v := stringField(msg, sub[1]); v != "" {
				return v
			}
		}
		pat := sub[2]
		if pat == "" {
			return "x"
		}
		// A parent that may be a project, folder or organization is shown as a project.
		if strings.HasPrefix(pat, "*/*") {
			//gcplint:ignore resource-prefix placeholder used to find a registry key, not a name returned to a client
			pat = "projects/*" + strings.TrimPrefix(pat, "*/*")
		}
		return strings.NewReplacer("**", "x", "*", "x").Replace(pat)
	})
	return out, !strings.ContainsAny(out, "{}")
}

// stringField reads a (possibly nested, dotted) string field by proto name.
func stringField(msg protoreflect.Message, path string) string {
	segs := strings.Split(path, ".")
	for i, seg := range segs {
		fd := msg.Descriptor().Fields().ByName(protoreflect.Name(seg))
		if fd == nil || fd.IsList() || fd.IsMap() {
			return ""
		}
		if i == len(segs)-1 {
			if fd.Kind() != protoreflect.StringKind {
				return ""
			}
			return msg.Get(fd).String()
		}
		if fd.Kind() != protoreflect.MessageKind {
			return ""
		}
		msg = msg.Get(fd).Message()
	}
	return ""
}

func (rt *route) String() string { return fmt.Sprintf("%s (%s %s)", rt.key, rt.verb, rt.template) }
