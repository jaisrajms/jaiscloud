package adapter

import (
	"net/http"

	"jaiscloud/internal/model"
)

// PluginCodecs is implemented by an adapter that can encode the services a
// plugin declares. Only Encode and EncodeError of the returned codec are used.
// The error rejects a service name that is built in or unsupported on this
// adapter.
type PluginCodecs interface {
	PluginCodec(service string) (Codec, error)
}

// PluginServiceMatcher is implemented by the plugin host once plugin-declared
// services are installed. Match reports whether a request belongs to one of
// them: ok is false when no declared pattern matches, and an error (a
// *model.ProviderError, encoded with the returned codec) rejects a request whose
// path the plugin owns but whose method or parameters it cannot serve.
type PluginServiceMatcher interface {
	Match(r *http.Request, body []byte) (*model.NormalizedRequest, Codec, bool, error)
}
