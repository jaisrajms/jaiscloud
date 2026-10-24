package logging

import "jaiscloud/internal/model"

// invalidArgument builds the canonical InvalidArgument error the transports map
// to their wire encoding (gRPC status / REST envelope).
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// invalidMaskPath reports an update_mask path that names no writable field of the
// resource. Real GCP validates the mask against the resource's field set and
// answers INVALID_ARGUMENT (AIP-134 / AIP-161), not an unimplemented operation,
// so every Logging merge rejects an unmappable path this way.
func invalidMaskPath(path string) error {
	return invalidArgument("unsupported update_mask path: " + path)
}
