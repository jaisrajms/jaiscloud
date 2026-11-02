package providererror

import "jaiscloud/internal/model"

func bad() error {
	return &model.ProviderError{ // want `\[provider-error-ctor\]`
		Code:       "Aborted",
		Message:    "etag mismatch",
		HTTPStatus: 409,
		Status:     "ABORTED",
	}
}

func good() error {
	return model.NewProviderError("Aborted", "etag mismatch", 409)
}

func goodChain() error {
	return model.NewProviderError("Aborted", "etag mismatch", 409).
		WithStatus("ABORTED").
		WithDetails(nil)
}
