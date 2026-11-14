package gcp

import (
	"fmt"

	"jaiscloud/internal/adapter"
)

// builtinServiceError is the rejection shared by every built-in name: adding a
// service the emulator already serves would let a plugin shadow a surface whose
// providers, transports and fixtures the emulator itself owns.
func builtinServiceError(service string) error {
	return fmt.Errorf("service %q is built in; a plugin can only add services the emulator does not have", service)
}

// PluginCodec implements adapter.PluginCodecs. Plugin-declared services get the
// generic GCP JSON codec: its Encode (JSON plus wire.RawJSONKey) and EncodeError
// ({"error": {code, message, status, details}}) are what a Google API returns,
// so a plugin response needs no special casing in the gateway.
//
// The name check lives here because this package owns the authoritative service
// vocabulary: serviceProviderMap holds every built-in REST wire service and
// grpcOnlyServices the names that exist only over gRPC. An empty name is
// rejected for the same reason, since it cannot address a provider key.
func (a *GCPAdapter) PluginCodec(service string) (adapter.Codec, error) {
	if service == "" {
		return nil, builtinServiceError(service)
	}
	if _, ok := serviceProviderMap[service]; ok {
		return nil, builtinServiceError(service)
	}
	for _, name := range grpcOnlyServices {
		if name == service {
			return nil, builtinServiceError(service)
		}
	}
	return &JSONCodec{Service: service}, nil
}
