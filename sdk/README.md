# jaiscloud plugin SDK

Extend a running `jaiscloud-aws` without forking it: audit or rewrite calls, replace
one action or a whole service, or add actions the emulator does not have. Your state is
stored by jaiscloud itself, so `reset`, `export`/`import`, snapshots and the frozen clock
(`POST /_jaiscloud/clock`) cover it automatically.

## Write a plugin (five minutes, plain Go)

```
mkdir acme-audit && cd acme-audit
go mod init example.com/acme-audit
go get github.com/jaisrajms/jaiscloud/sdk
```

`main.go` — see [examples/audit](examples/audit/main.go) for a complete one:

```go
m := sdk.NewMux("acme-audit", "0.1.0")
m.Before("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
    // c.Req.Params, c.Req.Now, c.Store ...
    return sdk.Result{}, nil // continue to the built-in
})
log.Fatal(sdk.ListenAndServe(":9100", m)) // ":9100" means 127.0.0.1:9100
```

Run it and point the emulator at it:

```
go run .
jaiscloud-aws start --plugin http://127.0.0.1:9100
```

No cgo, no matching toolchain, no special build flags. Any language works: the transport
is HTTP/JSON (the exact shapes are in [wire.go](wire.go)).

If the plugin is not up yet, the emulator keeps retrying for `--plugin-wait` (default 2m)
before it fails to start, and the error names the plugin address. If the plugin restarts
while the emulator runs, calls are retried for `--plugin-retry` (default 30s) and the
emulator re-sends its `/init` automatically.

## What you can do

| Mode | Meaning |
|---|---|
| `m.Before(cloud, service, action, fn)` | runs first; return `Result{Params: …}` to edit the call, or `Resp`/`Err` to stop it |
| `m.After(cloud, service, action, fn)` | sees the result (`c.Resp` or `c.Err`); may replace it |
| `m.Replace(cloud, service, action, fn)` | you answer instead of the built-in; `Result{NotHandled: true}` inherits it |

`service` is the wire service name (`sqs`, `dynamodb`, …); `action` may be `"*"` for the whole
service. Plugins listed first run outermost. `Before`/`After` on a misspelled action fail
startup instead of silently never firing.

### Override one action, keep the rest

Claim just the action you want; every other action of the service stays on the built-in:

```go
m.Replace("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
    if !mine(c) {
        return sdk.Result{NotHandled: true}, nil // this call uses the built-in
    }
    return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"MessageId": id}}}, nil
})
```

`Replace` cannot edit the params and then fall through; to tweak a call and still run the
built-in, use `Before` and return `Result{Params: …}`. Be careful with data the built-in reads
back: if your `SendMessage` keeps messages in your own store, the built-in `ReceiveMessage`
will not see them. Either replace the actions that share the data together, or let the built-in
store the message.

### Overriding a service covers internal flows

When a plugin overrides a built-in, every caller inside the emulator uses it too, and sees `Req.Origin`:

| Origin | Callers |
|---|---|
| `internal` | SNS, S3 notifications, EventBridge targets, CloudWatch alarms, Lambda DLQ and rotation delivering to SQS/SNS/Lambda; the Lambda event-source pollers (SQS and DynamoDB Streams); log writes from Lambda, ECS, EMR and Step Functions; log metric filters publishing metrics; EMR and Firehose writing to S3; Glue crawlers reading S3; SSM reading Secrets Manager references |
| `service` | Step Functions task states and CloudFormation resource handlers, with the parameters a client would send |
| `ui` | the console |

`Req.SourceArn` / `Req.ServicePrincipal` identify the calling service where there is one. Two
things to know when you override a service:

* Internal S3 reads and writes carry the whole body as `_body` (bytes; base64 for an out-of-process
  plugin). A wire `GetObject` streams and cannot be replaced by an out-of-process plugin.
* A few console-only helpers (for example SQS "peek messages") are not API actions; they keep
  reading the built-in's state.

## State

`c.Store` keeps rows per `(kind, id)`; every row has a `Version`.

```go
ent, found, _ := c.Store.Get(ctx, "counter", "n")
err := c.Store.PutIf(ctx, "counter", "n", data, ent.Version) // 0 = must not exist
if errors.Is(err, sdk.ErrConflict) { /* someone else wrote first: re-read and retry */ }
```

`Put` is last-writer-wins; use `PutIf` for counters and quotas. `List(kind, prefix)` is a real
prefix match. Store calls are only accepted while the call that carries your `CallID` is in
flight, and only for that call's account and region.

If you cache anything in memory, drop it when the emulator tells you state changed:

```go
m.HandleLifecycle(func(ctx context.Context, event string) { /* "reset" or "restore" */ })
```

## Rules

* Use `c.Req.Now` for time, never `time.Now()` — that is what makes a frozen clock work.
  Note that `POST /_jaiscloud/reset` and `/import` put the clock back to real time.
* Keep state in `c.Store`, not in a private database or global variable.
* The built-in codec serialises your `Data`; use the same field names the real service does.
* `c.Req.RequestID` may repeat (clients choose it); use `c.Req.CallID` when you need a unique key.
* If your plugin is down for longer than the retry window, requests it covers fail with
  `503 PluginUnavailable`; the emulator never silently falls back to the built-in.

## Security

The SDK refuses browser requests (anything with an `Origin` header) and non-JSON POSTs, and
`ListenAndServe(":port")` binds loopback only. To listen on other interfaces, say so
(`0.0.0.0:9100`) and set a shared secret on both sides:

```go
sdk.ListenAndServe("0.0.0.0:9100", m, sdk.WithSecret(os.Getenv("PLUGIN_SECRET")))
```
```
jaiscloud-aws start --plugin http://plugin:9100 --plugin-secret "$PLUGIN_SECRET"
```

## Limits (out-of-process)

* Binary and streaming data does not cross the JSON boundary. Params and response keys that start
  with `_` (S3 object bodies and streams) are never shown to a plugin and are always preserved; a
  plugin can observe such a response but cannot replace it (`501 PluginUnsupportedPayload`).
  Full S3 interception needs the in-process layer (planned).
* Reset/import: the emulator notifies you (`HandleLifecycle`), and the rows you wrote are wiped or
  restored with everything else.

## Status

Experimental (ABI 1). AWS routes only; new services, REST path routes, GCP and in-process
(`.so`) plugins are planned, not implemented.
