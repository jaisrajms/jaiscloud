//go:build gcp_conformance

package gcpconformance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Entry is one captured HTTP exchange against the emulator.
type Entry struct {
	Service string          `json:"service"`
	Method  string          `json:"method"`
	Path    string          `json:"path"`
	Status  int             `json:"status"`
	Request json.RawMessage `json:"request,omitempty"`
	Body    json.RawMessage `json:"body,omitempty"`
	// Stream holds a server-streaming response body. Google's REST transcoder
	// renders a server-streaming RPC as newline-delimited JSON (e.g. Firestore
	// documents:runQuery), so the whole body is not one JSON document; each
	// element is validated against the method's response schema in order.
	Stream []json.RawMessage `json:"stream,omitempty"`
}

// Transcript is the on-disk recorded-response format.
type Transcript struct {
	Entries []Entry `json:"entries"`
}

// WriteTranscript writes a transcript as JSON with one compact entry per line.
// The entries array is the whole file, so a pretty-printed layout makes every
// re-record rewrite the entire fixture (inserting one service's entries shifts
// the tail); one entry per line turns that into a line-level diff of only the
// entries that changed.
func WriteTranscript(path string, tr Transcript) error {
	var b bytes.Buffer
	b.WriteString("{\n  \"entries\": [\n")
	for i, e := range tr.Entries {
		line, err := json.Marshal(e)
		if err != nil {
			return err
		}
		b.WriteString("    ")
		b.Write(line)
		if i < len(tr.Entries)-1 {
			b.WriteByte(',')
		}
		b.WriteByte('\n')
	}
	b.WriteString("  ]\n}\n")
	return os.WriteFile(path, b.Bytes(), 0o644)
}

// jsonStream splits a newline-delimited JSON body into its elements. It reports
// false unless every non-empty line is a JSON object or array (a raw media
// download or an HTML error page is not a stream and stays uncaptured).
func jsonStream(body []byte) ([]json.RawMessage, bool) {
	var out []json.RawMessage
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if !json.Valid(line) {
			return nil, false
		}
		switch line[0] {
		case '{', '[':
		default:
			return nil, false
		}
		out = append(out, json.RawMessage(line))
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// ReadTranscript reads a transcript from JSON. A missing file is reported as a
// non-nil error wrapping os.ErrNotExist so callers can skip.
func ReadTranscript(path string) (Transcript, error) {
	var tr Transcript
	data, err := os.ReadFile(path)
	if err != nil {
		return tr, err
	}
	if err := json.Unmarshal(data, &tr); err != nil {
		return tr, fmt.Errorf("parse transcript %s: %w", path, err)
	}
	return tr, nil
}

// ValidateTranscripts validates every captured entry: 2xx responses against the
// matched Discovery method's response schema, non-2xx responses against the GCP
// error envelope. Operations with no matching Discovery method surface as
// unmatched_method divergences so coverage gaps stay visible.
func ValidateTranscripts(docs map[string]*DiscoveryDoc, tr Transcript) []Divergence {
	ix := BuildMethodIndex(docs)
	var divs []Divergence

	for i, e := range tr.Entries {
		ctx := fmt.Sprintf("entry[%d] %s %s %s", i, e.Service, e.Method, e.Path)

		if e.Status >= 200 && e.Status < 300 {
			doc, m, ok := ix.MatchMethodInService(e.Service, e.Method, e.Path)
			if !ok {
				divs = append(divs, Divergence{
					Path:     ctx,
					Kind:     kindUnmatchedMethod,
					Expected: "a Discovery method for " + e.Method + " " + e.Path,
					Actual:   "no match",
					Severity: "low",
					Service:  e.Service,
					Method:   e.Method,
				})
				continue
			}
			// Request-side: validate the captured request body against the
			// method's declared request schema. Types and unknown fields are
			// checked; `required` is not enforced (partial/PATCH bodies are
			// legitimate, and the emulator is deliberately lenient).
			if m.Request != nil && m.Request.Ref != "" && len(bytes.TrimSpace(e.Request)) > 0 {
				if reqSchema, ok := doc.ResolveRef(m.Request.Ref); ok {
					var rv any
					if err := json.Unmarshal(e.Request, &rv); err == nil {
						for _, d := range ValidateRequestValue(doc, reqSchema, rv, m.ID+".request") {
							d.Service = e.Service
							d.Method = m.ID
							d.Path = ctx + " " + d.Path
							divs = append(divs, d)
						}
					}
				}
			}

			var schema *Schema
			if m.Response != nil && m.Response.Ref != "" {
				schema, _ = doc.ResolveRef(m.Response.Ref)
			}
			if schema == nil {
				// No declared response body (e.g. delete returning Empty with an
				// empty 204 body) — nothing to validate.
				continue
			}
			if len(e.Stream) > 0 {
				// A transposed server-streaming response: every element is one
				// response message, so each is validated against the same schema.
				for i, raw := range e.Stream {
					var v any
					if err := json.Unmarshal(raw, &v); err != nil {
						divs = append(divs, Divergence{
							Path: fmt.Sprintf("%s %s.stream[%d]", ctx, m.ID, i), Kind: kindWrongType,
							Expected: "valid JSON", Actual: err.Error(), Severity: "high",
							Service: e.Service, Method: m.ID,
						})
						continue
					}
					for _, d := range ValidateValue(doc, schema, v, fmt.Sprintf("%s.stream[%d]", m.ID, i)) {
						d.Service = e.Service
						d.Method = m.ID
						d.Path = ctx + " " + d.Path
						divs = append(divs, d)
					}
				}
				continue
			}
			if len(bytes.TrimSpace(e.Body)) == 0 {
				continue
			}
			var v any
			if err := json.Unmarshal(e.Body, &v); err != nil {
				divs = append(divs, Divergence{
					Path: ctx + " " + m.ID, Kind: kindWrongType,
					Expected: "valid JSON", Actual: err.Error(), Severity: "high",
					Service: e.Service, Method: m.ID,
				})
				continue
			}
			for _, d := range ValidateValue(doc, schema, v, m.ID) {
				d.Service = e.Service
				d.Method = m.ID
				d.Path = ctx + " " + d.Path
				divs = append(divs, d)
			}
			continue
		}

		for _, d := range ValidateErrorEnvelope(e.Status, e.Body) {
			d.Service = e.Service
			d.Path = ctx + " " + d.Path
			divs = append(divs, d)
		}
	}

	return divs
}

// Record runs the curated scenario list against a live emulator and captures
// every request/response pair. It is the body of the -record workflow.
func Record(jaiscloudHost string) (Transcript, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	suffix := runSuffix()

	docs, err := LoadSnapshots("discovery")
	if err != nil {
		return Transcript{}, fmt.Errorf("load discovery snapshots: %w", err)
	}
	scenarios := append(Scenarios(suffix), SynthPrelude()...)
	scenarios = append(scenarios, SynthScenarios(docs, suffix)...)

	var tr Transcript
	vars := map[string]string{}
	clockMoved := false
	defer func() {
		// A failed record run must not leave the shared emulator in the future.
		if clockMoved {
			_ = setEmulatorClock(client, jaiscloudHost, "real")
		}
	}()
	for _, sc := range scenarios {
		path := expandVars(sc.Path, vars)
		reqBody := expandVars(sc.Body, vars)
		if sc.Prepare != nil {
			prepared, err := sc.Prepare(vars)
			if err != nil {
				return tr, fmt.Errorf("%s %s: prepare body: %w", sc.Method, path, err)
			}
			reqBody = prepared
		}
		if sc.AdvanceClock != "" {
			if err := setEmulatorClock(client, jaiscloudHost, sc.AdvanceClock); err != nil {
				return tr, fmt.Errorf("%s %s: set clock %q: %w", sc.Method, path, sc.AdvanceClock, err)
			}
			clockMoved = sc.AdvanceClock != "real"
		}
		if strings.Contains(reqBody, "${") {
			return tr, fmt.Errorf("%s %s: unresolved variable in request body", sc.Method, path)
		}
		url := jaiscloudHost + path
		var body io.Reader
		if reqBody != "" {
			body = bytes.NewReader([]byte(reqBody))
		}
		req, err := http.NewRequest(sc.Method, url, body)
		if err != nil {
			return tr, fmt.Errorf("%s %s: %w", sc.Method, path, err)
		}
		// A host-discriminated scenario (a service sharing a canonical path on
		// one origin) sends its own Host header; the transport still dials
		// jaiscloudHost.
		if sc.Host != "" {
			req.Host = sc.Host
		}
		if reqBody != "" {
			ct := sc.ContentType
			if ct == "" {
				ct = "application/json"
			}
			req.Header.Set("Content-Type", ct)
		}
		resp, err := client.Do(req)
		if err != nil {
			return tr, fmt.Errorf("%s %s: %w", sc.Method, path, err)
		}
		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return tr, fmt.Errorf("%s %s: read body: %w", sc.Method, path, readErr)
		}
		entry := Entry{
			Service: sc.Service,
			Method:  sc.Method,
			Path:    path,
			Status:  resp.StatusCode,
		}
		if len(bytes.TrimSpace(respBody)) > 0 {
			if json.Valid(respBody) {
				entry.Body = json.RawMessage(respBody)
			} else if stream, ok := jsonStream(respBody); ok {
				// A server-streaming response (newline-delimited JSON): keep every
				// element so the schema check can validate the stream instead of
				// silently counting the status code as evidence.
				entry.Stream = stream
			}
		}
		// Only JSON request bodies map to a Discovery `request` schema; media
		// uploads and other raw bodies are skipped.
		if reqBody != "" && json.Valid([]byte(reqBody)) {
			entry.Request = json.RawMessage(reqBody)
		}
		if len(sc.Save) > 0 {
			var decoded any
			if err := json.Unmarshal(respBody, &decoded); err == nil {
				for name, jsonPath := range sc.Save {
					if val, ok := captureVar(decoded, jsonPath); ok {
						vars[name] = val
					}
				}
			}
		}
		tr.Entries = append(tr.Entries, entry)
	}
	return tr, nil
}
