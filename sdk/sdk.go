// Package sdk is the public contract between a jaiscloud emulator and a plugin.
//
// A plugin is a Manifest (which (cloud, service, action) keys it claims, and in
// which Mode) plus one function, Handle. The same Handle contract is served over
// HTTP/JSON by Serve (out-of-process plugins, any language).
//
// This package depends on the Go standard library only, on purpose: the
// in-process (Go plugin) layer requires host and plugin to be built from
// identical copies of every shared package.
package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ABI is the contract version negotiated at load time. It is bumped only for
// breaking changes; adding optional fields does not bump it.
const ABI = 1

// Mode says how a Route participates in a request.
type Mode string

const (
	// Replace: the plugin answers instead of the built-in. Returning
	// Result.NotHandled falls through to the built-in (inherit).
	Replace Mode = "replace"
	// Before: runs ahead of the core; may rewrite Params or short-circuit.
	Before Mode = "before"
	// After: runs after the core; may replace the response or the error.
	After Mode = "after"
)

// Manifest declares what a plugin claims.
type Manifest struct {
	ABI     int     `json:"abi"`
	Name    string  `json:"name"`    // [a-z0-9-]+ ; state namespace and log label
	Version string  `json:"version"` // informational
	Routes  []Route `json:"routes"`
}

// Route claims actions of a WIRE service (e.g. "sqs") for a cloud.
type Route struct {
	Cloud   string   `json:"cloud"`   // "aws" | "gcp"
	Service string   `json:"service"` // wire service name, never a registry prefix
	Actions []string `json:"actions"` // action names, or ["*"] for the whole service
	Mode    Mode     `json:"mode"`
}

// Request is the projection of one API call a plugin sees.
//
// Params holds the decoded wire parameters. Keys beginning with "_" are
// emulator-internal (streams, raw bytes) and are never shown to out-of-process
// plugins; the emulator always preserves them.
type Request struct {
	Cloud     string         `json:"cloud"`
	Service   string         `json:"service"`
	Action    string         `json:"action"`
	Params    map[string]any `json:"params"`
	AccountID string         `json:"accountId"`
	Region    string         `json:"region"`
	AccessKey string         `json:"accessKey,omitempty"`
	// Origin says where the call came from: "wire" (a client), "internal"
	// (another part of the emulator, e.g. SNS delivering to SQS, a poller or a
	// scheduler) or "ui" (the console). A plugin that overrides a key receives
	// all of them; branch on Origin only if you must treat them differently.
	Origin string `json:"origin"`
	// SourceArn / ServicePrincipal are set when another service in the emulator
	// made this call (for example SNS delivering to SQS).
	SourceArn        string `json:"sourceArn,omitempty"`
	ServicePrincipal string `json:"servicePrincipal,omitempty"`
	// RequestID identifies the originating request for correlation and logs.
	// It may repeat (clients can choose their own); never use it as a key.
	RequestID string `json:"requestId"`
	// CallID is unique per call to the plugin and authorises its store access.
	CallID string `json:"callId"`
	// Now is the emulator's business clock (honours POST /_jaiscloud/clock).
	// It is the ONLY time source a plugin should use.
	Now time.Time `json:"now"`
}

// Response is a successful result. Data is serialised by the BUILT-IN codec.
type Response struct {
	Status int            `json:"status,omitempty"` // 0 means 200; otherwise 100..599
	Data   map[string]any `json:"data"`
}

// Error is a structured failure, rendered by the built-in codec's error encoder.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Status  int            `json:"status"` // HTTP status, 100..599
	Data    map[string]any `json:"data,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s (HTTP %d)", e.Code, e.Message, e.Status) }

// Call is the input of Handle.
type Call struct {
	Phase Mode
	Req   *Request
	Resp  *Response // After only: the core's success result
	Err   *Error    // After only: the core's error
	Store Store     // namespaced, bound to this call's account+region
}

// Result is the output of Handle. Resp and Err are mutually exclusive.
//
//	Replace: Resp or Err answers; NotHandled falls through to the built-in.
//	Before:  Params (when non-nil) replaces the request's visible params;
//	         Resp/Err short-circuit and the built-in is not run.
//	After:   Resp/Err override the core's outcome; all nil passes it through.
type Result struct {
	NotHandled bool           `json:"notHandled,omitempty"`
	Params     map[string]any `json:"params"`
	Resp       *Response      `json:"resp,omitempty"`
	Err        *Error         `json:"err,omitempty"`
}

// Host is what the emulator tells a plugin at Init.
type Host struct {
	Cloud   string
	Version string
	Log     *slog.Logger
}

// Plugin is implemented by every plugin. Mux provides a ready implementation.
type Plugin interface {
	Manifest() Manifest
	Init(ctx context.Context, h Host) error
	Handle(ctx context.Context, c *Call) (Result, error)
	Shutdown(ctx context.Context) error
}

// LifecycleListener is an optional interface. The emulator calls it after
// state is reset ("reset") or replaced by an import or snapshot revert
// ("restore"). Plugins that cache state must drop their caches here.
type LifecycleListener interface {
	OnLifecycle(ctx context.Context, event string)
}

// Lifecycle events delivered to a LifecycleListener.
const (
	EventReset   = "reset"
	EventRestore = "restore"
)

// Store is the plugin's state. Rows live in the emulator's own ResourceStore
// under the type "plugin:<name>:<kind>", so reset, export/import and named
// snapshots cover them without any plugin code.
//
// Every row carries a Version that increases on each write. Use PutIf for
// read-modify-write (counters, quotas); Put is last-writer-wins.
type Store interface {
	Get(ctx context.Context, kind, id string) (Entry, bool, error)
	// Put writes unconditionally.
	Put(ctx context.Context, kind, id string, data json.RawMessage) error
	// PutIf writes only if the row's current Version equals ifVersion
	// (0 means "the row must not exist"); otherwise it returns ErrConflict.
	PutIf(ctx context.Context, kind, id string, data json.RawMessage, ifVersion int64) error
	Delete(ctx context.Context, kind, id string) error
	// List returns rows whose id starts with idPrefix, in no particular order.
	List(ctx context.Context, kind, idPrefix string) ([]Entry, error)
}

// ErrConflict is returned by PutIf when the row changed since it was read.
var ErrConflict = errors.New("sdk: version conflict")

// Entry is one stored row.
type Entry struct {
	ID      string          `json:"id"`
	Data    json.RawMessage `json:"data"`
	Version int64           `json:"version"`
}
