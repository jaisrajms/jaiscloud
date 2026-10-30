package ui

import (
	"context"
	"testing"

	"jaiscloud/internal/aws/provider/queue"
	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// The console must reach a service through the registry (origin "ui") whenever
// the call is a registry action, so a plugin that overrides the service answers
// the console too. Console-only helpers stay on the built-in.
func TestConsoleCallsGoThroughTheRegistry(t *testing.T) {
	reg := provider.NewRegistry()
	var seen []model.Origin
	reg.RegisterAll(map[string]provider.HandlerFunc{
		"Queue.ListQueues": func(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			seen = append(seen, nr.Origin)
			return provider.OK(map[string]any{"QueueUrls": []any{"from-registry"}}), nil
		},
	})

	routed := routeQueue(queue.New(nil, nil, nil), uiCaller{reg: reg})
	resp, err := routed.ListQueues(context.Background(), &model.NormalizedRequest{Params: map[string]any{}})
	if err != nil || resp.Data["QueueUrls"].([]any)[0] != "from-registry" {
		t.Fatalf("ListQueues = %v, %v; want the registry's answer", resp, err)
	}
	if len(seen) != 1 || seen[0] != model.OriginUI {
		t.Fatalf("origins = %v, want [ui]", seen)
	}

	// Without a registry (unit tests of the console) the provider is used directly.
	if _, ok := routeQueue(queue.New(nil, nil, nil), uiCaller{}).(*queue.QueueProvider); !ok {
		t.Fatal("a nil registry must leave the provider unwrapped")
	}
}

// A plugin's response is JSON-decoded: lists arrive as []any and numbers as
// float64. The console must render them like the built-in's typed values.
func TestConsoleHelpersAcceptPluginShapedData(t *testing.T) {
	pluginShaped := map[string]any{
		"Buckets": []any{map[string]any{"Name": "b1"}, "not-an-object"},
		"Names":   []any{"a", 7, "b"},
		"Count":   float64(3),
		"Attrs":   map[string]any{"k": "v", "n": 1},
	}
	builtinShaped := map[string]any{
		"Buckets": []map[string]any{{"Name": "b1"}},
		"Names":   []string{"a", "b"},
		"Count":   3,
		"Attrs":   map[string]string{"k": "v"},
	}
	for name, d := range map[string]map[string]any{"plugin": pluginShaped, "builtin": builtinShaped} {
		if got := uihelper.Maps(d["Buckets"]); len(got) != 1 || got[0]["Name"] != "b1" {
			t.Errorf("%s: Maps = %v", name, got)
		}
		if got := uihelper.Strings(d["Names"]); len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Errorf("%s: Strings = %v", name, got)
		}
		if got := uihelper.Int(d["Count"]); got != 3 {
			t.Errorf("%s: Int = %d", name, got)
		}
		if got := uihelper.StringMap(d["Attrs"]); got["k"] != "v" {
			t.Errorf("%s: StringMap = %v", name, got)
		}
	}
}
