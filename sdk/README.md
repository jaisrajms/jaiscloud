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

### Overriding a service covers internal flows

When a plugin overrides a built-in, flows that originate *inside* the emulator use the plugin
too. Today SNS→SQS, S3 notification→SQS and Lambda DLQ→SQS deliveries reach an SQS plugin as a
`SendMessage` with `Req.SourceArn` / `Req.ServicePrincipal` set (the built-in delivery is the
innermost step). Other internal producers and consumers (EventBridge→SQS, Lambda event-source
polling, DynamoDB streams, …) are being moved onto the same seam one at a time; until then
they still call the built-in directly. See the coverage table in the design docs.

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
