package sentinel

import (
	"errors"

	"jaiscloud/internal/store"
)

func bad(err error) bool {
	return err == store.ErrNotFound // want `\[store-sentinel\]`
}

func badNE(err error) bool {
	return err != store.ErrAlreadyExists // want `\[store-sentinel\]`
}

func good(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}
