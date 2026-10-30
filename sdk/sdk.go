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
type Request struct {
	Cloud     string         `json:"cloud"`
	Service   string         `json:"service"`
	Action    string         `json:"action"`
	Params    map[string]any `json:"params"`
	AccountID string         `json:"accountId"`
	Region    string         `json:"region"`
	AccessKey string         `json:"accessKey,omitempty"`
	RequestID string         `json:"requestId"`
	// Now is the emulator's business clock (honours POST /_jaiscloud/clock).
	// It is the ONLY time source a plugin should use.
	Now time.Time `json:"now"`
}

// Response is a successful result. Data is serialised by the BUILT-IN codec.
type Response struct {
	Status int            `json:"status,omitempty"` // 0 means 200
	Data   map[string]any `json:"data"`
}

// Error is a structured failure, rendered by the built-in codec's error encoder.
type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Status  int            `json:"status"` // HTTP status
	Data    map[string]any `json:"data,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s (HTTP %d)", e.Code, e.Message, e.Status) }

// Call is the input of Handle.
type Call struct {
	Phase Mode
	Req   *Request
	Resp  *Response // After only: the core's success result
	Err   *Error    // After only: the core's error
	Store Store     // namespaced, bound to this request's account+region
}

// Result is the output of Handle.
//
//	Replace: Resp or Err answers; NotHandled falls through to the built-in.
//	Before:  Req (only Params is honoured) edits the call; Resp/Err short-circuit.
//	After:   Resp/Err override the core's outcome; all nil passes it through.
type Result struct {
	NotHandled bool      `json:"notHandled,omitempty"`
	Req        *Request  `json:"req,omitempty"`
	Resp       *Response `json:"resp,omitempty"`
	Err        *Error    `json:"err,omitempty"`
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

// Store is the plugin's state. Rows live in the emulator's own ResourceStore
// under the type "plugin:<name>:<kind>", so reset, export/import and named
// snapshots cover them without any plugin code.
type Store interface {
	Get(ctx context.Context, kind, id string) (data json.RawMessage, found bool, err error)
	Put(ctx context.Context, kind, id string, data json.RawMessage) error
	Delete(ctx context.Context, kind, id string) error
	List(ctx context.Context, kind, idPrefix string) ([]Entry, error)
}

// Entry is one stored row.
type Entry struct {
	ID   string          `json:"id"`
	Data json.RawMessage `json:"data"`
}
