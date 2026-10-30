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
log.Fatal(sdk.ListenAndServe(":9100", m))
```

Run it and point the emulator at it:

```
go run .
jaiscloud-aws start --plugin http://127.0.0.1:9100
```

That's it: no cgo, no matching toolchain, no special build flags. Any language works —
the transport is HTTP/JSON (see `wire.go` for the exact shapes).

## What you can do

| Mode | Meaning |
|---|---|
| `m.Before(cloud, service, action, fn)` | runs first; edit `Params`, or return a response/error to stop |
| `m.After(cloud, service, action, fn)` | sees the result (`c.Resp` or `c.Err`); may replace it |
| `m.Replace(cloud, service, action, fn)` | you answer instead of the built-in; `Result{NotHandled: true}` inherits it |

`service` is the wire service name (`sqs`, `dynamodb`, …); `action` may be `"*"` for the
whole service. Plugins listed first run outermost.

## Rules

* Use `c.Req.Now` for time, never `time.Now()` — that is what makes a frozen clock work.
* Keep state in `c.Store`, not in a private database or global variable.
* The built-in codec serialises your `Data`; use the same field names the real service does.
* If your plugin is down, requests it covers fail with `503 PluginUnavailable`; the
  emulator never silently falls back to the built-in.

## Status

Experimental (ABI 1). AWS routes only; new services, REST path routes, GCP and
in-process (`.so`) plugins are planned, not implemented.
