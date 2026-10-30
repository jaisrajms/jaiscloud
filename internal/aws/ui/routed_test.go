package ui

import (
	"context"
	"testing"

	"jaiscloud/internal/aws/provider/queue"
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
