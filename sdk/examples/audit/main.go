// Command audit is a complete jaiscloud plugin: it audits every SQS SendMessage,
// rejects bodies containing "BLOCK", and adds a new action, AcmeAuditCount.
//
//	go run ./examples/audit            # listens on :9100
//	jaiscloud-aws start --plugin http://127.0.0.1:9100
package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/jaisrajms/jaiscloud/sdk"
)

func main() {
	m := sdk.NewMux("acme-audit", "0.1.0")

	// Middleware: runs before the built-in SendMessage.
	m.Before("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
		row, _ := json.Marshal(map[string]any{"queue": c.Req.Params["QueueUrl"], "at": c.Req.Now})
		if err := c.Store.Put(ctx, "audit", c.Req.RequestID, row); err != nil {
			return sdk.Result{}, err
		}
		if body, _ := c.Req.Params["MessageBody"].(string); strings.Contains(body, "BLOCK") {
			return sdk.Result{Err: &sdk.Error{Code: "InvalidParameterValue", Message: "blocked by acme-audit", Status: 400}}, nil
		}
		return sdk.Result{}, nil // continue to the built-in
	})

	// A new action on an existing service. NotHandled lets every other SQS
	// action keep using the built-in.
	m.Replace("aws", "sqs", "*", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
		if c.Req.Action != "AcmeAuditCount" {
			return sdk.Result{NotHandled: true}, nil
		}
		rows, err := c.Store.List(ctx, "audit", "")
		if err != nil {
			return sdk.Result{}, err
		}
		return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"Audits": len(rows)}}}, nil
	})

	log.Fatal(sdk.ListenAndServe(":9100", m))
}
