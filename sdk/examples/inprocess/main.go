// Command inprocess is an in-process plugin for jaiscloud-aws and jaiscloud-gcp: Go code
// loaded into the emulator's own process. It shows what only that layer can do:
//
//   - wrap the built-in with Call.Next (change a request, call the built-in, change the answer);
//   - answer with the emulator's own values, a stream, which an HTTP sidecar cannot;
//   - keep state in memory and still have reset, export/import and snapshots cover it (sdk.Stateful).
//
// Build it with the same Go toolchain and flags as the emulator (see docs/PLUGINS.md).
// From a jaiscloud checkout:
//
//	make build-plugins-aws
//	make plugin-example OUT=inprocess.so
//	./jaiscloud-aws-plugins start --plugin so:$PWD/inprocess.so
//
// A plugin of your own is a module of its own that requires the SDK; copy this file into
// one and run `make plugin SRC=<your module directory> OUT=<file.so>` (or the go build
// command it prints). It must not be built inside the SDK module itself.
//
// The same file serves both clouds: each binary uses the routes for its own cloud.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// New is the entry point the emulator looks up in the .so.
func New() sdk.Plugin {
	p := &plugin{Mux: sdk.NewMux("inprocess-demo", "0.1.0"), calls: map[string]int64{}}
	// Next, streams and Stateful exist only in-process; say so, so that loading this as an
	// HTTP sidecar fails at startup with a clear message instead of misbehaving.
	p.Require(sdk.CapInProcess)

	// AWS: wrap the built-in. Redact a secret from the message body, let SQS store the
	// redacted message, and count the call.
	p.Replace("aws", "sqs", "SendMessage", p.redactAndSend)
	// AWS: a new action served by the plugin, to read what it counted.
	p.Replace("aws", "sqs", "InProcessCount", p.count)
	// AWS: answer part of S3 with a stream the plugin makes up; everything else is the built-in's.
	// A plugin that owns objects serves HeadObject as well: clients (aws s3 cp) ask for it first.
	p.Replace("aws", "s3", "GetObject", p.getObject)
	p.Replace("aws", "s3", "HeadObject", p.headObject)

	// GCP: wrap Pub/Sub publish and stamp each message with an attribute. Over REST the
	// params carry the request in "body"; over gRPC they are the PublishRequest itself.
	p.Replace("gcp", "pubsub", "TopicPublish", p.stampPublish)
	return p
}

type plugin struct {
	*sdk.Mux
	mu    sync.Mutex
	calls map[string]int64 // state kept in memory, reached by reset, export and snapshots
}

func (p *plugin) bump(what string) {
	p.mu.Lock()
	p.calls[what]++
	p.mu.Unlock()
}

var secret = regexp.MustCompile(`password=\S+`)

func (p *plugin) redactAndSend(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
	p.bump("SendMessage")
	params := map[string]any{}
	for k, v := range c.Req.Params {
		params[k] = v
	}
	if body, ok := params["MessageBody"].(string); ok {
		params["MessageBody"] = secret.ReplaceAllString(body, "password=***")
	}
	resp, perr := c.Next(ctx, &sdk.Request{Params: params}) // the built-in stores the redacted message
	if perr != nil {
		return sdk.Result{Err: perr}, nil
	}
	return sdk.Result{Resp: resp}, nil
}

func (p *plugin) count(context.Context, *sdk.Call) (sdk.Result, error) {
	p.mu.Lock()
	n := p.calls["SendMessage"]
	p.mu.Unlock()
	return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"SendMessageCalls": n}}}, nil
}

const virtualPrefix = "inprocess/"

// virtualObject is the object the plugin serves for key, or false for any other key.
func virtualObject(c *sdk.Call) (key string, body []byte, ok bool) {
	key, _ = c.Req.Params["_key"].(string) // S3 REST params: _bucket, _key
	if !strings.HasPrefix(key, virtualPrefix) {
		return "", nil, false
	}
	return key, []byte("served by the in-process plugin: " + key), true
}

// getObject serves objects under "inprocess/" from the plugin, as a stream.
func (p *plugin) getObject(_ context.Context, c *sdk.Call) (sdk.Result, error) {
	_, body, ok := virtualObject(c)
	if !ok {
		return sdk.Result{NotHandled: true}, nil // any other object is the built-in's
	}
	p.bump("GetObject")
	return sdk.Result{Resp: &sdk.Response{Data: map[string]any{
		"_stream":       io.NopCloser(bytes.NewReader(body)),
		"_passthrough":  true,
		"_content_type": "text/plain",
		"ContentLength": int64(len(body)),
		"ETag":          `"inprocess"`,
		"LastModified":  "Mon, 02 Jan 2006 15:04:05 GMT",
	}}}, nil
}

// headObject answers the same object's metadata.
func (p *plugin) headObject(_ context.Context, c *sdk.Call) (sdk.Result, error) {
	_, body, ok := virtualObject(c)
	if !ok {
		return sdk.Result{NotHandled: true}, nil
	}
	return sdk.Result{Resp: &sdk.Response{Data: map[string]any{
		"ContentLength": int64(len(body)),
		"ContentType":   "text/plain",
		"ETag":          `"inprocess"`,
		"LastModified":  "Mon, 02 Jan 2006 15:04:05 GMT",
	}}}, nil
}

func (p *plugin) stampPublish(ctx context.Context, c *sdk.Call) (sdk.Result, error) {
	p.bump("TopicPublish")
	params := map[string]any{}
	for k, v := range c.Req.Params {
		params[k] = v
	}
	holder := params // gRPC: the message list is at the top
	if c.Req.Transport != "grpc" {
		body, _ := params["body"].(map[string]any)
		holder = map[string]any{}
		for k, v := range body {
			holder[k] = v
		}
		params["body"] = holder // a copy: the original request is not edited in place
	}
	var msgs []any
	for _, m := range asSlice(holder["messages"]) {
		msg, _ := m.(map[string]any)
		stamped := map[string]any{}
		for k, v := range msg {
			stamped[k] = v
		}
		attrs := map[string]any{}
		if old, ok := stamped["attributes"].(map[string]any); ok {
			for k, v := range old {
				attrs[k] = v
			}
		}
		attrs["via"] = "inprocess-demo"
		stamped["attributes"] = attrs
		msgs = append(msgs, stamped)
	}
	holder["messages"] = msgs
	resp, perr := c.Next(ctx, &sdk.Request{Params: params})
	if perr != nil {
		return sdk.Result{Err: perr}, nil
	}
	return sdk.Result{Resp: resp}, nil
}

func asSlice(v any) []any { s, _ := v.([]any); return s }

// ---- sdk.Stateful: what the plugin keeps in memory joins reset, export and snapshots ----

func (p *plugin) Reset(context.Context) {
	p.mu.Lock()
	p.calls = map[string]int64{}
	p.mu.Unlock()
}

func (p *plugin) Snapshot(_ context.Context, w io.Writer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	keys := make([]string, 0, len(p.calls))
	for k := range p.calls {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		b.WriteString(strconv.FormatInt(p.calls[k], 10))
	}
	b.WriteByte('}')
	_, err := w.Write(b.Bytes())
	return err
}

func (p *plugin) Restore(_ context.Context, r io.Reader) error {
	calls := map[string]int64{}
	if err := json.NewDecoder(r).Decode(&calls); err != nil {
		return err
	}
	p.mu.Lock()
	p.calls = calls
	p.mu.Unlock()
	return nil
}

func (p *plugin) IsEmpty(context.Context) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls) == 0, nil
}

// main is required by -buildmode=plugin and never runs.
func main() {}
