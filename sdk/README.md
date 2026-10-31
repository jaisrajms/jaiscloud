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

Until a `sdk/v0.x.y` tag is published, `go get` cannot resolve the module. Use a checkout instead:
`go mod edit -replace github.com/jaisrajms/jaiscloud/sdk=/path/to/jaiscloud/sdk`.

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
| `m.Before(cloud, service, action, fn)` | runs first; return `Result{Params: …}` to patch the call, or `Resp`/`Err` to stop it |
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
built-in, use `Before` and return `Result{Params: …}`.

`Result.Params` is a **patch**: return only the keys you change. Keys you leave out stay as the client
sent them, and a key whose value is `null` is removed:

```go
return sdk.Result{Params: map[string]any{"MessageBody": "rewritten", "DelaySeconds": nil}}, nil
// MessageBody is replaced, DelaySeconds is removed, QueueUrl and everything else are untouched.
```
A value the emulator cannot show you (a stream) is never touched, even by `null`. Be careful with data the built-in reads
back: if your `SendMessage` keeps messages in your own store, the built-in `ReceiveMessage`
will not see them. Either replace the actions that share the data together, or let the built-in
store the message.

### Overriding a service covers internal flows

When a plugin overrides a built-in, every caller inside the emulator uses it too, and sees `Req.Origin`:

| Origin | Callers |
|---|---|
| `internal` | SNS, S3 notifications (to SQS, SNS, Lambda and EventBridge), EventBridge targets, CloudWatch alarms, Lambda DLQ and rotation delivering to SQS/SNS/Lambda; the Lambda event-source pollers (SQS and DynamoDB Streams); log writes from the Lambda (docker/k8s), ECS, EMR on EKS and Step Functions executors; log metric filters publishing metrics; EMR and Firehose writing to S3; Glue crawlers reading S3; SSM reading Secrets Manager references; Secrets Manager and SSM encrypting through KMS; EMR bootstrap scripts and the Firehose bucket check reading S3; the gateway's S3 CORS preflight reading bucket rules; the Lambda executors and `/_jaiscloud/lambda/code` reading function and layer packages |
| `service` | Step Functions task states and CloudFormation resource handlers, with the parameters a client would send |
| `ui` | the console |

`Req.SourceArn` / `Req.ServicePrincipal` identify the calling service where there is one.

### Transports (GCP gRPC)

On `jaiscloud-gcp` the same key is reached over REST and over gRPC. For a gRPC call
`Req.Transport` is `"grpc"` and `Params` (and `Resp.Data`, and what a `replace` must return) are the
protojson form of the RPC messages: camelCase fields, int64 as strings, enums by name, no `body` wrapper.
Over REST `Req.Transport` is empty and `Params` is the REST shape (path parameters plus `body`).
A plugin that only understands one shape declares it, and calls over the other transport skip it:

```go
m := sdk.NewMux("acme", "0.1.0").Transports("rest") // or "grpc"; the default is both
```

The manifest carries it as `transports` on each route. A `before` patch is shallow in both shapes.

**An internal call to an S3 or CloudWatch Logs key is an ordinary client call.** The emulator
makes the same calls an agent would, so one key means one thing whoever calls it:

* Writing an object to a bucket that does not exist: `PutObject` answers `NoSuchBucket`, and the
  emulator then calls `CreateBucket` and `PutObject` again.
* Writing log events to a stream that does not exist: `PutLogEvents` answers
  `ResourceNotFoundException`, and the emulator calls `CreateLogStream` and `PutLogEvents` again.
  Which thing is missing is in the error's `Data["resource"]`: `"log-stream"` (or no field at all) gets
  the create-and-retry; `"log-group"` drops the batch, because a workload can outlive its group. A
  plugin that replaces Logs should set the field the same way. Batches are split at 10 000 events and
  messages cut at 262 144 bytes (on a character boundary).
* `CreateLogGroup` for a group that exists answers `ResourceAlreadyExistsException`, which the
  emulator treats as success.
* Because they are real calls, internal writes now trigger bucket notifications and log
  subscription and metric filters, as they do on AWS.

Things to know when you override a service:

* A built-in `GetObject` answers with a stream; a plugin answers with `_body` (bytes;
  `{"$base64": "..."}` over JSON). The emulator reads either for its own callers, but an
  out-of-process plugin cannot answer a *client's* `GetObject` this way, so a wire `GetObject`
  cannot be replaced by an out-of-process plugin.
* `Function.GetFunctionCodeZip` and `Function.GetLayerCodeZip` are internal-only actions (they
  are not AWS API actions): the container executors and the admin code endpoint use them to read
  the package they mount. If your plugin owns the function store but leaves `Invoke` to the
  built-in executor, answer them with `_body` bytes. A plugin that replaces the whole service
  needs nothing.
* Internal and wire calls to the same SQS key differ in one respect: an internal `SendMessage` may name
  the queue by ARN in `QueueUrl` and skips the validation a client gets (FIFO group ids, size limits),
  because the emulator's own producers never had it. It answers with `MessageId` and `MD5OfMessageBody`
  like a wire call. The Lambda poller's `ReceiveMessage` and `DeleteMessageBatch` are ordinary client
  calls by `QueueUrl` (it asks for all attributes and deletes at most ten handles per call).
* A few console-only helpers (for example SQS "peek messages") are not API actions; they keep
  reading the built-in's state.
* A few flows are not implemented in the emulator at all, so no plugin sees them (EMR on EC2 log
  forwarding).

### Event-source mappings and an unreachable plugin

When the plugin that owns `ReceiveMessage`, `GetRecords` (and the other Streams actions) or the Lambda function cannot be reached (down,
restarting, or past `--plugin-timeout`; the host reports `PluginUnavailable` or `PluginTimeout`),
the pollers back off from 1 s to 30 s and retry. The failure is not counted against the mapping,
nothing is sent to the failure destination (DLQ) and nothing is deleted, so the messages come back
after their visibility timeout. Any other failure is handled exactly as it is for a built-in.

### Test hooks and plugin-owned services

`POST /_jaiscloud/eb-tick`, `cw-evaluate`, `ttl-sweep` and `firehose/flush` run a pass over the
built-in's own state. If your plugin owns the schedule, alarms, TTL or buffers, it gets a
lifecycle event after the built-in pass, `tick:<service>` (`tick:events`, `tick:monitoring`,
`tick:dynamodb`, `tick:firehose`), and runs its own pass:

```go
m.HandleLifecycle(func(ctx context.Context, event string) {
    if svc, ok := sdk.TickService(event); ok && svc == "events" {
        runDueRules(ctx)
    }
})
```

Only plugins with a route for that service are told, and the hook returns after your handler does.
A plugin does not need these events to do background work in production: it is a process, so it
can run its own timers (read the time from `Req.Now` when you handle a call).

## In-process plugins

The same `Mux` code can be built as a Go plugin (`-buildmode=plugin`) and loaded into the emulator with
`--plugin so:<path>`. In-process, `c.Next` runs the rest of the chain (so a `Replace` can change a request, call
the built-in and change its answer), `c.ResourceID` formats an ARN or resource name, `Params` and `Data`
hold the emulator's own values (`[]byte`, `io.Reader`) instead of JSON, and a plugin may implement
`sdk.Stateful` to keep state in memory that reset, export/import and snapshots include. A plugin that uses
these lists the capability, so loading it as an HTTP sidecar fails at startup:

```go
func New() sdk.Plugin {
    m := sdk.NewMux("acme-redact", "0.1.0")
    m.Require(sdk.CapInProcess)
    m.Replace("aws", "sqs", "SendMessage", func(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
        // copy c.Req.Params, change it, then:
        resp, perr := c.Next(ctx, &sdk.Request{Params: changed})
        if perr != nil {
            return sdk.Result{Err: perr}, nil
        }
        return sdk.Result{Resp: resp}, nil
    })
    return m
}

func main() {}
```

It needs a plugin-capable emulator (`make build-plugins`, linux or darwin, cgo) and a plugin built to match:
the same Go release, `-trimpath`, `CGO_ENABLED=1`, and the same SDK source, in a module of its own that
requires the SDK. See [examples/inprocess](examples/inprocess/main.go) (AWS and GCP) and
[docs/PLUGINS.md](../docs/PLUGINS.md#in-process-plugins) for the build, the container image and the rules.

## State

`c.Store` keeps rows per `(kind, id)`; every row has a `Version`. An `id` is at most 1 KiB with no NUL
byte; a `List` that would return more than 10 000 rows or 32 MiB is refused (`413`), so narrow it with
a longer `idPrefix`.

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
* A panic in the emulator's plugin seam surfaces as `500 PluginPanic` naming your plugin; a panic in
  your own process is yours to recover (the SDK's server does, and answers 500, which the emulator
  reports as `PluginProtocolError`).
* After a restart, a plugin answers `/handle` with `409 {"error":"not_initialized"}` until it gets
  `/init` again (`sdk.ErrNotInitialized`). The emulator re-sends `/init` and retries the call on that
  code; the wording is not part of the contract.

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

## Limits (out-of-process; an in-process plugin has no JSON or size limit)

* Everything crosses as JSON. Raw bytes (S3 object bodies, Lambda payloads) arrive as
  `{"$base64": "..."}` inside the params, under keys that start with `_` (the emulator's own channel
  for such data), and must be returned in the same form; unchanged values keep their original type.
  A value that cannot be JSON (a stream, such as a built-in `GetObject` answer) is never shown to you
  and is always preserved: you can observe such a response with `After` but not replace it
  (`501 PluginUnsupportedPayload`). Full S3 interception needs the in-process layer (planned).
* A request is at most 32 MiB once encoded (base64 makes bytes a third larger). A bigger one is refused
  with `501 PluginUnsupportedPayload`, naming the largest param, before it reaches you; so a plugin
  with a route on `s3` cannot see a `PutObject` of more than about 24 MiB. Responses are bounded the
  same way.
* Reset/import: the emulator notifies you (`HandleLifecycle`), and the rows you wrote are wiped or
  restored with everything else.
* `Manifest.Requires` lists host capabilities you cannot work without. This build provides none, so any
  entry stops the emulator at startup and the error says what it does provide.

## Status

Experimental (ABI 1). Out-of-process and in-process (`.so`) plugins for `jaiscloud-aws` and `jaiscloud-gcp` (GCP REST
and gRPC). New services, REST path routes and a raw-response escape hatch are planned, not implemented. See [docs/PLUGINS.md](../docs/PLUGINS.md) for the architecture and the full list of limits.
