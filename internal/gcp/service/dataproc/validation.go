package dataproc

import (
	"errors"
	"strings"

	"jaiscloud/internal/model"
)

// validationErrors collects the InvalidArgument failures of one create so the
// request can be answered with all of them at once, the way real Dataproc does.
//
// Real GCP returns one 400 INVALID_ARGUMENT whose message is
// "Multiple validation errors:\n - …\n - …" when a create has more than one
// problem, and the bare message when it has exactly one (captured in
// docs/gcp/dataproc-aggregate-validation-errors.md). Keeping the single case as
// its own message means every existing one-error response stays byte-identical.
//
// The order is the order the checks run in, which is fixed in the create path,
// so the joined message is deterministic. The individual sentences are the
// emulator's own (real GCP's wording differs per check); only the envelope is a
// real-GCP capture.
type validationErrors []string

// add records err's provider message when err is non-nil. It stores the message,
// not Error(), so the collected text is the wire message rather than
// "InvalidArgument: … (HTTP 400)".
func (v *validationErrors) add(err error) {
	if err == nil {
		return
	}
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		*v = append(*v, pe.Message)
		return
	}
	*v = append(*v, err.Error())
}

// addAll records every error in errs.
func (v *validationErrors) addAll(errs validationErrors) {
	*v = append(*v, errs...)
}

// err renders the collected failures as one InvalidArgument, or nil when there
// are none. One failure keeps its original message byte for byte.
func (v validationErrors) err() error {
	switch len(v) {
	case 0:
		return nil
	case 1:
		return invalidArgument(v[0])
	default:
		return invalidArgument("Multiple validation errors:\n - " + strings.Join(v, "\n - "))
	}
}
