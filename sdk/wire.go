package sdk

import "encoding/json"

// Wire types of the out-of-process (HTTP/JSON) transport. Non-Go plugins
// implement exactly these JSON shapes:
//
//	GET  /manifest  -> Manifest
//	POST /init      <- InitRequest            (204)
//	POST /handle    <- HandleRequest          -> Result
//
// and call the emulator's store endpoints (InitRequest.Store.URL + "/get",
// "/put", "/delete", "/list") with StoreRequest / StoreResponse, sending
// "Authorization: Bearer <token>".

// InitRequest is sent once at startup.
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

// StoreRequest is the body of every store callback. RequestID must be the
// RequestID of a call that is currently in flight; the emulator rejects the
// rest, so a plugin cannot touch state from a background thread.
type StoreRequest struct {
	RequestID string          `json:"requestId"`
	AccountID string          `json:"accountId"`
	Region    string          `json:"region"`
	Kind      string          `json:"kind"`
	ID        string          `json:"id,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	IDPrefix  string          `json:"idPrefix,omitempty"`
}

type StoreResponse struct {
	Found   bool            `json:"found,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Entries []Entry         `json:"entries,omitempty"`
}
