package sdk

import "encoding/json"

// Wire types of the out-of-process (HTTP/JSON) transport. Non-Go plugins
// implement exactly these JSON shapes:
//
//	GET  /manifest   -> Manifest
//	POST /init       <- InitRequest            (204; may be sent again after an emulator restart)
//	POST /handle     <- HandleRequest          -> Result
//	POST /lifecycle  <- LifecycleRequest       (204; optional)
//
// and call the emulator's store endpoints (InitRequest.Store.URL + "/get",
// "/put", "/delete", "/list") with StoreRequest / StoreResponse, sending
// "Authorization: Bearer <token>". Requests must use Content-Type
// application/json. When a shared secret is configured the emulator also sends
// "Authorization: Bearer <secret>" on every call to the plugin.

// InitRequest is sent at startup and again if the plugin restarts.
type InitRequest struct {
	ABI   int           `json:"abi"`
	Host  InitHost      `json:"host"`
	Store StoreEndpoint `json:"store"`
}

type InitHost struct {
	Cloud   string `json:"cloud"`
	Version string `json:"version"`
}

// StoreEndpoint tells the plugin where and how to reach its state.
type StoreEndpoint struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

// HandleRequest is one call. Resp/Err are set only for the After phase.
type HandleRequest struct {
	Phase Mode      `json:"phase"`
	Req   *Request  `json:"req"`
	Resp  *Response `json:"resp,omitempty"`
	Err   *Error    `json:"err,omitempty"`
}

// LifecycleRequest notifies the plugin that state changed underneath it.
type LifecycleRequest struct {
	Event string `json:"event"` // EventReset | EventRestore
}

// StoreRequest is the body of every store callback. CallID must be the CallID
// of a call that is currently in flight, and AccountID/Region must be those of
// that call; the emulator rejects everything else, so a plugin cannot touch
// state from a background thread or another tenant.
type StoreRequest struct {
	CallID    string          `json:"callId"`
	AccountID string          `json:"accountId"`
	Region    string          `json:"region"`
	Kind      string          `json:"kind"`
	ID        string          `json:"id,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	IDPrefix  string          `json:"idPrefix,omitempty"`
	// IfVersion makes "put" conditional (0 = row must not exist). HTTP 409 on conflict.
	IfVersion *int64 `json:"ifVersion,omitempty"`
}

type StoreResponse struct {
	Found   bool    `json:"found,omitempty"`
	Entry   *Entry  `json:"entry,omitempty"`
	Entries []Entry `json:"entries,omitempty"`
}
