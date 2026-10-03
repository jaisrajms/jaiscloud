package logging

import "jaiscloud/internal/model"

// invalidArgument builds the canonical InvalidArgument error the transports map
// to their wire encoding (gRPC status / REST envelope).
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}
