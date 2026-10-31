package provider

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

func tag(s string) func(next HandlerFunc) HandlerFunc {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			resp, err := next(ctx, nr)
			if err == nil {
				resp.Data["trail"] = resp.Data["trail"].(string) + s
			}
			return resp, err
		}
	}
}

func TestInterceptWrapsOriginalInOrder(t *testing.T) {
	r := NewRegistry()
	r.RegisterAll(map[string]HandlerFunc{
		"Queue.SendMessage": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return OK(map[string]any{"trail": "core,"}), nil
		},
	})
	r.Intercept("Queue.SendMessage", tag("inner,"))
	r.Intercept("Queue.SendMessage", tag("outer,"))

	resp, err := r.Dispatch(context.Background(), "Queue.SendMessage", &model.NormalizedRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Data["trail"]; got != "core,inner,outer," {
		t.Fatalf("trail = %q, want core,inner,outer, (last Intercept is outermost)", got)
	}
}

func TestInterceptMissingKeyBecomesCoreAndNextIsUnknownAction(t *testing.T) {
	r := NewRegistry()
	var nextErr error
	r.Intercept("Queue.AcmeNew", func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
			_, nextErr = next(ctx, nr)
			return OK(map[string]any{"ok": true}), nil
		}
	})
	if !r.Has("Queue.AcmeNew") {
		t.Fatal("Has must see an intercepted key")
	}
	if _, err := r.Dispatch(context.Background(), "Queue.AcmeNew", &model.NormalizedRequest{Service: "sqs", Action: "AcmeNew"}); err != nil {
		t.Fatal(err)
	}
	pe, ok := nextErr.(*model.ProviderError)
	if !ok || pe.Code != "UnknownAction" || pe.HTTPStatus != 400 {
		t.Fatalf("next of a missing key = %v, want UnknownAction/400", nextErr)
	}
}

func TestInterceptFallbackOnlySeesUnregisteredActions(t *testing.T) {
	r := NewRegistry()
	r.RegisterAll(map[string]HandlerFunc{
		"Queue.Known": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return OK(map[string]any{"from": "builtin"}), nil
		},
	})
	r.InterceptFallback("Queue", func(HandlerFunc) HandlerFunc {
		return func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
			return OK(map[string]any{"from": "plugin"}), nil
		}
	})
	known, _ := r.Dispatch(context.Background(), "Queue.Known", &model.NormalizedRequest{})
	unknown, _ := r.Dispatch(context.Background(), "Queue.Other", &model.NormalizedRequest{})
	if known.Data["from"] != "builtin" || unknown.Data["from"] != "plugin" {
		t.Fatalf("known=%v unknown=%v", known.Data, unknown.Data)
	}
}

func TestKeysFiltersByPrefixAndIsSorted(t *testing.T) {
	r := NewRegistry()
	noop := func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) { return nil, nil }
	r.RegisterAll(map[string]HandlerFunc{"Queue.B": noop, "Queue.A": noop, "QueueX.C": noop, "Table.D": noop})
	if got := strings.Join(r.Keys("Queue"), ","); got != "Queue.A,Queue.B" {
		t.Fatalf("Keys(Queue) = %q", got)
	}
}

// Background workers dispatch while plugin loading still wraps handlers; the
// registry must tolerate that (run with -race).
func TestDispatchIsSafeWhileHandlersAreIntercepted(t *testing.T) {
	r := NewRegistry()
	r.RegisterAll(map[string]HandlerFunc{"Queue.SendMessage": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return OK(map[string]any{}), nil
	}})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = r.Dispatch(context.Background(), "Queue.SendMessage", &model.NormalizedRequest{})
				_ = r.Has("Queue.SendMessage")
			}
		}
	}()
	for i := 0; i < 200; i++ {
		r.Intercept("Queue.SendMessage", func(next HandlerFunc) HandlerFunc { return next })
		r.InterceptFallback("Queue", func(next HandlerFunc) HandlerFunc { return next })
	}
	close(stop)
	<-done
}
