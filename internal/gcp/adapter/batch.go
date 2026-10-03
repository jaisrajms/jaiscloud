package gcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"jaiscloud/internal/gateway"
	"jaiscloud/internal/model"
)

// GCP exposes the JSON batch endpoint (POST /batch/{service}/{version}) for the
// google-api-client batch protocol used by e.g. google-cloud-storage's
// StorageBatch. The adapter owns the multipart/mixed wire parsing and response
// formatting; the gateway supplies process to run each embedded sub-request
// through the normal pipeline (see gateway.BatchHandler).

// IsBatchRequest reports whether r targets a GCP JSON batch endpoint. The
// canonical path is /batch/storage/v1; the service/version segments are
// irrelevant because each embedded part is routed by its own full path, so any
// POST under /batch/ is accepted.
func (a *GCPAdapter) IsBatchRequest(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	p := "/" + strings.TrimLeft(r.URL.Path, "/")
	return strings.HasPrefix(p, "/batch/")
}

// ServeBatch parses the multipart/mixed batch body, dispatches every embedded
// sub-request through process, and writes the multiplexed multipart/mixed
// response. Sub-requests are replayed in order — the google-api-client
// response parser matches responses to requests positionally, so ordering and
// one part per request are required.
func (a *GCPAdapter) ServeBatch(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte, process gateway.BatchProcessFunc) {
	decoded, err := decodeGzippedBody(r, body)
	if err != nil {
		if pe, ok := err.(*model.ProviderError); ok {
			writeBatchError(w, pe.HTTPStatus, pe.Message)
			return
		}
		writeBatchError(w, http.StatusBadRequest, "malformed batch request body")
		return
	}

	boundary := extractBoundary(r.Header.Get("Content-Type"))
	if boundary == "" {
		writeBatchError(w, http.StatusBadRequest, "expected multipart batch body")
		return
	}

	type partResult struct {
		contentID string
		status    int
		headers   http.Header
		body      []byte
	}

	outerAuth := r.Header.Get("Authorization")
	mr := multipart.NewReader(bytes.NewReader(decoded), boundary)
	var results []partResult
	for {
		part, perr := mr.NextPart()
		if perr == io.EOF {
			break
		}
		if perr != nil {
			writeBatchError(w, http.StatusBadRequest, "malformed multipart batch body")
			return
		}
		contentID := part.Header.Get("Content-ID")
		raw, rerr := io.ReadAll(part)
		if rerr != nil {
			writeBatchError(w, http.StatusBadRequest, "malformed multipart batch body")
			return
		}
		sub, rerr := http.ReadRequest(bufio.NewReader(bytes.NewReader(raw)))
		if rerr != nil {
			results = append(results, partResult{
				contentID: contentID,
				status:    http.StatusBadRequest,
				headers:   jsonHeaders(),
				body:      errorEnvelope(http.StatusBadRequest, rerr.Error()),
			})
			continue
		}
		subBody, _ := io.ReadAll(sub.Body)
		sub.Body.Close()
		sub.Body = io.NopCloser(bytes.NewReader(subBody))
		// The outer request carries the OAuth credential; the embedded request
		// usually does too, but fall back to the outer header so identity is
		// never lost.
		if sub.Header.Get("Authorization") == "" && outerAuth != "" {
			sub.Header.Set("Authorization", outerAuth)
		}
		status, headers, respBody := process(ctx, sub, subBody)
		results = append(results, partResult{
			contentID: contentID,
			status:    status,
			headers:   headers,
			body:      respBody,
		})
	}

	respBoundary := newBatchBoundary()
	var buf bytes.Buffer
	for i, res := range results {
		cid := res.contentID
		if cid == "" {
			cid = strconv.Itoa(i + 1)
		}
		cid = strings.Trim(cid, "<>")
		fmt.Fprintf(&buf, "--%s\r\n", respBoundary)
		buf.WriteString("Content-Type: application/http\r\n")
		fmt.Fprintf(&buf, "Content-ID: <response-%s>\r\n", cid)
		buf.WriteString("\r\n")
		fmt.Fprintf(&buf, "HTTP/1.1 %d %s\r\n", res.status, statusText(res.status))
		if res.headers != nil {
			if ct := res.headers.Get("Content-Type"); ct != "" {
				fmt.Fprintf(&buf, "Content-Type: %s\r\n", ct)
			}
			// Preserve retry hints on throttled sub-responses so a client
			// handling a batch can still back off per part.
			if ra := res.headers.Get("Retry-After"); ra != "" {
				fmt.Fprintf(&buf, "Retry-After: %s\r\n", ra)
			}
		}
		fmt.Fprintf(&buf, "Content-Length: %d\r\n", len(res.body))
		buf.WriteString("\r\n")
		buf.Write(res.body)
		buf.WriteString("\r\n")
	}
	fmt.Fprintf(&buf, "--%s--\r\n", respBoundary)

	w.Header().Set("Content-Type", "multipart/mixed; boundary="+respBoundary)
	w.Header().Set("Content-Length", strconv.Itoa(buf.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// newBatchBoundary returns a fresh, collision-resistant multipart boundary for
// a batch response.
func newBatchBoundary() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is not expected; fall back to a constant prefix so
		// the response is still well-formed.
		return "batch_response"
	}
	return "batch_" + hex.EncodeToString(b[:])
}

// statusText returns the HTTP reason phrase for a status code, or a stable
// placeholder for codes Go does not know.
func statusText(code int) string {
	if t := http.StatusText(code); t != "" {
		return t
	}
	return "Status"
}

// jsonHeaders returns response headers for a JSON error part.
func jsonHeaders() http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json; charset=UTF-8")
	return h
}

// errorEnvelope mirrors the GCS error shape (no "status" field) for errors the
// adapter itself produces before dispatch.
func errorEnvelope(status int, msg string) []byte {
	b, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"errors": []any{
				map[string]any{"domain": "global", "reason": "invalid", "message": msg},
			},
			"code":    status,
			"message": msg,
		},
	})
	return b
}

// writeBatchError writes a top-level (non-multipart) batch error response. This
// is returned when the batch envelope itself cannot be parsed, which the client
// surfaces as a failed batch request.
func writeBatchError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(status)
	_, _ = w.Write(errorEnvelope(status, msg))
}
