package ui

import (
	"context"
	"os"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/pluginchain"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// microClock is the real clock cut to microseconds, the precision of a timestamp on the
// REST wire. The passthrough tests compare a model read back through the REST rendering
// with the stored one; on a clock that ticks in nanoseconds (Linux) the stored value would
// carry digits the wire never does, and the two would differ for no real reason.
type microClock struct{}

func (microClock) Now() time.Time { return clock.RealNow().Truncate(time.Microsecond) }

func TestMain(m *testing.M) {
	clock.SetGlobalClock(microClock{})
	os.Exit(m.Run())
}

// fakeHost stands in for the plugin host: it covers the keys in wrap and installs
// each wrapper on whatever registry the chain attaches it to.
type fakeHost struct {
	wrap map[string]func(next provider.HandlerFunc) provider.HandlerFunc
}

func (h fakeHost) Covers(key string) bool { _, ok := h.wrap[key]; return ok }
func (h fakeHost) CoveredKeys() []string {
	var out []string
	for k := range h.wrap {
		out = append(out, k)
	}
	return out
}
func (h fakeHost) Attach(reg *provider.Registry) {
	for k, w := range h.wrap {
		reg.Intercept(k, w)
	}
}

// typedFixture builds a caller whose chain covers the given keys.
func typedFixture(builtins map[string]provider.HandlerFunc, wrap map[string]func(provider.HandlerFunc) provider.HandlerFunc) typedCaller {
	return typedCaller{
		chain:    pluginchain.New(fakeHost{wrap: wrap}),
		builtins: builtins,
		cfg:      &config.Config{},
	}
}

// observe records the request a key sees and then lets the built-in answer.
type seenCall struct {
	key    string
	origin model.Origin
	params map[string]any
}

func passthrough(into *[]seenCall, key string) func(provider.HandlerFunc) provider.HandlerFunc {
	return func(next provider.HandlerFunc) provider.HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			*into = append(*into, seenCall{key: key, origin: nr.Origin, params: nr.Params})
			return next(ctx, nr)
		}
	}
}

func replaceWith(data map[string]any) func(provider.HandlerFunc) provider.HandlerFunc {
	return func(provider.HandlerFunc) provider.HandlerFunc {
		return func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return provider.OK(data), nil
		}
	}
}
