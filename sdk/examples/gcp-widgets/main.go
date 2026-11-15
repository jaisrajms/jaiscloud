// Command gcp-widgets is a complete jaiscloud plugin for GCP that adds a wire
// service the emulator does not have: widgets, at
// /v1/projects/{project}/locations/{location}/widgets. A client pointed at the
// emulator origin gets the shapes a Google API returns: a widget object
// ({"name", "state", "labels"}), an empty {} for create and delete, and a
// completed long-running operation for :start.
//
//	go run ./examples/gcp-widgets        # listens on 127.0.0.1:9102
//	jaiscloud-gcp start --plugin http://127.0.0.1:9102
//
// Then, against the emulator on its default port:
//
//	EP=http://localhost:8080/v1/projects/p/locations/l
//	curl -s -X POST "$EP/widgets?widgetId=w1" -H 'Content-Type: application/json' \
//	    -d '{"labels":{"team":"acme"}}'
//	curl -s "$EP/widgets/w1"
//	curl -s -X POST "$EP/widgets/w1:start" -H 'Content-Type: application/json' -d '{}'
//	curl -s -X DELETE "$EP/widgets/w1"
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/jaisrajms/jaiscloud/sdk"
)

func main() {
	m := sdk.NewMux("acme-gcp-widgets", "0.1.0")

	// One declaration per service: the REST paths it answers, and the wildcard
	// replace that serves them. Mux.Manifest adds "services" to Requires, so an
	// emulator that predates plugin services refuses this plugin at startup
	// instead of serving half of it.
	m.Service("gcp", "widgets").
		Get("/v1/projects/{project}/locations/{location}/widgets/{widget}", "WidgetsGet").
		Post("/v1/projects/{project}/locations/{location}/widgets", "WidgetsCreate").
		Delete("/v1/projects/{project}/locations/{location}/widgets/{widget}", "WidgetsDelete").
		Post("/v1/projects/{project}/locations/{location}/widgets/{widget}:start", "WidgetsStart").
		Replace(handle)

	log.Fatal(sdk.ListenAndServe(":9102", m))
}

// handle answers every action of the widgets service. Path captures arrive in
// Params under their names (project, location, widget), a JSON body under
// "body", and each query parameter by name.
func handle(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
	project, _ := c.Req.Params["project"].(string)
	location, _ := c.Req.Params["location"].(string)
	id, _ := c.Req.Params["widget"].(string)

	switch c.Req.Action {
	case "WidgetsCreate":
		return create(ctx, c, project, location)

	case "WidgetsGet":
		w, err := fetch(ctx, c, id)
		if err != nil {
			return sdk.Result{}, err
		}
		if w == nil {
			return notFound(project, location, id), nil
		}
		return sdk.Result{Resp: &sdk.Response{Data: w}}, nil

	case "WidgetsDelete":
		w, err := fetch(ctx, c, id)
		if err != nil {
			return sdk.Result{}, err
		}
		if w == nil {
			return notFound(project, location, id), nil
		}
		if err := c.Store.Delete(ctx, "widget", id); err != nil {
			return sdk.Result{}, err
		}
		return sdk.Result{Resp: &sdk.Response{Data: map[string]any{}}}, nil

	case "WidgetsStart":
		w, err := fetch(ctx, c, id)
		if err != nil {
			return sdk.Result{}, err
		}
		if w == nil {
			return notFound(project, location, id), nil
		}
		w["state"] = "RUNNING"
		if err := put(ctx, c, id, w); err != nil {
			return sdk.Result{}, err
		}
		// A completed long-running operation. Its ID carries the "widgets-"
		// prefix, so a path declared as
		// .../operations/{operation=widgets-*} can claim just this slice of the
		// shared ".../operations" collection.
		return sdk.Result{Resp: &sdk.Response{Data: map[string]any{
			"name":     opName(project, location, id),
			"done":     true,
			"response": w,
		}}}, nil
	}

	return sdk.Result{Err: &sdk.Error{
		Code:    "NotFound",
		Message: fmt.Sprintf("unknown action %q", c.Req.Action),
		Status:  404,
	}}, nil
}

// create stores a new widget. The client names it with ?widgetId=w1, as the
// Google create methods do, or with the last segment of a full name in the
// body. Like the real create methods it answers {}.
func create(ctx context.Context, c *sdk.Call, project, location string) (sdk.Result, error) {
	body, _ := c.Req.Params["body"].(map[string]any)
	id, _ := c.Req.Params["widgetId"].(string)
	if id == "" {
		if name, ok := body["name"].(string); ok {
			id = name[strings.LastIndex(name, "/")+1:]
		}
	}
	if id == "" {
		return sdk.Result{Err: &sdk.Error{
			Code:    "InvalidArgument",
			Message: "widgetId is required",
			Status:  400,
		}}, nil
	}

	var labels map[string]string
	if m, ok := body["labels"].(map[string]any); ok {
		labels = map[string]string{}
		for k, v := range m {
			if s, ok := v.(string); ok {
				labels[k] = s
			}
		}
	}
	state, _ := body["state"].(string)
	if state == "" {
		state = "READY"
	}

	if err := put(ctx, c, id, newWidget(project, location, id, state, labels)); err != nil {
		return sdk.Result{}, err
	}
	return sdk.Result{Resp: &sdk.Response{Data: map[string]any{}}}, nil
}

// newWidget builds the resource in the shape a Google API returns.
func newWidget(project, location, id, state string, labels map[string]string) map[string]any {
	w := map[string]any{"name": widgetName(project, location, id), "state": state}
	if len(labels) > 0 {
		w["labels"] = labels
	}
	return w
}

// fetch returns the stored widget, or nil when there is none.
func fetch(ctx context.Context, c *sdk.Call, id string) (map[string]any, error) {
	ent, found, err := c.Store.Get(ctx, "widget", id)
	if err != nil || !found {
		return nil, err
	}
	var w map[string]any
	if err := json.Unmarshal(ent.Data, &w); err != nil {
		return nil, err
	}
	return w, nil
}

// put stores the widget as it will be returned later.
func put(ctx context.Context, c *sdk.Call, id string, w map[string]any) error {
	row, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return c.Store.Put(ctx, "widget", id, row)
}

func notFound(project, location, id string) sdk.Result {
	return sdk.Result{Err: &sdk.Error{
		Code:    "NotFound",
		Message: fmt.Sprintf("widget %s not found", widgetName(project, location, id)),
		Status:  404,
	}}
}

func widgetName(project, location, id string) string {
	return fmt.Sprintf("projects/%s/locations/%s/widgets/%s", project, location, id)
}

func opName(project, location, id string) string {
	return fmt.Sprintf("projects/%s/locations/%s/operations/widgets-%s", project, location, id)
}
