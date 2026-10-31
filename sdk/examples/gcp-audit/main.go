// Command gcp-audit is a complete jaiscloud plugin for GCP: it audits every
// Pub/Sub topic creation and publish, labels new topics, and rejects publishes
// whose payload contains "BLOCK". One plugin covers both REST and gRPC clients,
// because both reach the same key (for example PubSub.TopicPublish); only the
// shape of Params differs, and Request.Transport says which one you got.
//
//	go run ./examples/gcp-audit        # listens on 127.0.0.1:9101
//	jaiscloud-gcp start --plugin http://127.0.0.1:9101
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"strings"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// payloads returns the decoded message payloads of a publish, in either shape.
//
//	REST:  params.body.messages[].data   (base64)
//	gRPC:  params.messages[].data        (base64; protojson of PublishRequest)
func payloads(c *sdk.Call) []string {
	msgs, _ := c.Req.Params["messages"].([]any)
	if c.Req.Transport != "grpc" {
		body, _ := c.Req.Params["body"].(map[string]any)
		msgs, _ = body["messages"].([]any)
	}
	var out []string
	for _, m := range msgs {
		mm, _ := m.(map[string]any)
		data, _ := mm["data"].(string)
		if raw, err := base64.StdEncoding.DecodeString(data); err == nil {
			out = append(out, string(raw))
		}
	}
	return out
}

// withAudit returns a copy of labels with the audit label added.
func withAudit(labels any) map[string]any {
	out := map[string]any{}
	if l, ok := labels.(map[string]any); ok {
		for k, v := range l {
			out[k] = v
		}
	}
	out["audited-by"] = "acme"
	return out
}

func main() {
	m := sdk.NewMux("acme-gcp-audit", "0.1.0")

	// Runs before the built-in TopicPublish, whichever transport the client used.
	m.Before("gcp", "pubsub", "TopicPublish", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
		row, _ := json.Marshal(map[string]any{"at": c.Req.Now, "transport": c.Req.Transport, "origin": c.Req.Origin})
		if err := c.Store.Put(ctx, "publish", c.Req.CallID, row); err != nil {
			return sdk.Result{}, err
		}
		for _, p := range payloads(c) {
			if strings.Contains(p, "BLOCK") {
				return sdk.Result{Err: &sdk.Error{Code: "FailedPrecondition", Message: "blocked by acme-gcp-audit", Status: 400}}, nil
			}
		}
		return sdk.Result{}, nil // continue to the built-in
	})

	// A patch: label every new topic. Over REST the Topic is params.body; over
	// gRPC it is the request message itself. A patch is shallow: a key you
	// return replaces that whole value, so copy the existing labels (or body)
	// and edit the copy.
	m.Before("gcp", "pubsub", "TopicCreate", func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		if c.Req.Transport == "grpc" {
			return sdk.Result{Params: map[string]any{"labels": withAudit(c.Req.Params["labels"])}}, nil
		}
		body := map[string]any{}
		if b, ok := c.Req.Params["body"].(map[string]any); ok {
			for k, v := range b {
				body[k] = v
			}
		}
		body["labels"] = withAudit(body["labels"])
		return sdk.Result{Params: map[string]any{"body": body}}, nil
	})

	log.Fatal(sdk.ListenAndServe(":9101", m))
}
