package sentinel

import (
	"errors"

	"jaiscloud/internal/store"
)

func badSwitch(err error) string {
	switch err {
	case store.ErrNotFound: // want `\[store-sentinel\]`
		return "missing"
	case store.ErrAlreadyExists: // want `\[store-sentinel\]`
		return "exists"
	default:
		return ""
	}
}

func badMultiCase(err error) string {
	switch err {
	case store.ErrNotFound, store.ErrAlreadyExists: // want `\[store-sentinel\]` `\[store-sentinel\]`
		return "sentinel"
	default:
		return ""
	}
}

func goodTaglessSwitch(err error) string {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return "missing"
	default:
		return ""
	}
}
