package pluginchain

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// fakeHost covers the keys in wrap and installs each wrapper on the registry it is attached to.
type fakeHost struct {
	wrap map[string]func(provider.HandlerFunc) provider.HandlerFunc
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

func ok(data map[string]any) *model.ProviderResponse {
	return &model.ProviderResponse{HTTPStatus: 200, Data: data}
}

func TestChainWithoutAHostCoversNothing(t *testing.T) {
	var nilChain *Chain
	if nilChain.Covers("PubSub.TopicGet") {
		t.Error("a nil chain must cover nothing")
	}
	if New(nil).Covers("PubSub.TopicGet") {
		t.Error("a chain with no host must cover nothing")
	}
}

func TestChainCoversExactlyWhatThePluginsTouch(t *testing.T) {
	c := New(fakeHost{wrap: map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"PubSub.TopicGet": func(next provider.HandlerFunc) provider.HandlerFunc { return next },
	}})
	if !c.Covers("PubSub.TopicGet") || c.Covers("PubSub.TopicList") {
		t.Error("Covers must be true for a key a plugin touches and false for the others")
	}
}

// A plugin that wraps the key sees the call, and the supplied built-in runs innermost.
func TestRunPutsTheCallersBuiltInUnderThePlugin(t *testing.T) {
	var order []string
	c := New(fakeHost{wrap: map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"PubSub.TopicGet": func(next provider.HandlerFunc) provider.HandlerFunc {
			return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
				order = append(order, "plugin-before")
				resp, err := next(ctx, nr)
				order = append(order, "plugin-after")
				return resp, err
			}
		},
	}})
	got, err := c.Run(context.Background(), "PubSub.TopicGet", &model.NormalizedRequest{}, func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
		order = append(order, "built-in")
		return ok(map[string]any{"name": "t"}), nil
	})
	if err != nil || got.Data["name"] != "t" {
		t.Fatalf("Run = %+v, %v", got, err)
	}
	if want := "plugin-before,built-in,plugin-after"; join(order) != want {
		t.Fatalf("order = %s, want %s", join(order), want)
	}
}

// A plugin that answers instead never reaches the built-in.
func TestRunLetsAReplacingPluginSkipTheBuiltIn(t *testing.T) {
	c := New(fakeHost{wrap: map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"PubSub.TopicGet": func(provider.HandlerFunc) provider.HandlerFunc {
			return func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
				return ok(map[string]any{"name": "from-plugin"}), nil
			}
		},
	}})
	got, err := c.Run(context.Background(), "PubSub.TopicGet", &model.NormalizedRequest{}, func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
		t.Error("the built-in must not run when a plugin answers")
		return nil, nil
	})
	if err != nil || got.Data["name"] != "from-plugin" {
		t.Fatalf("Run = %+v, %v", got, err)
	}
}

func join(s []string) string {
	out := ""
	for i, x := range s {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}
