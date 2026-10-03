//go:build gcp_conformance

package gcpconformance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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
}

// Transcript is the on-disk recorded-response format.
type Transcript struct {
	Entries []Entry `json:"entries"`
}

// WriteTranscript writes a transcript as indented JSON.
func WriteTranscript(path string, tr Transcript) error {
	data, err := json.MarshalIndent(tr, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
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

	var tr Transcript
	vars := map[string]string{}
	for _, sc := range Scenarios(suffix) {
		path := expandVars(sc.Path, vars)
		reqBody := expandVars(sc.Body, vars)
		url := jaiscloudHost + path
		var body io.Reader
		if reqBody != "" {
			body = bytes.NewReader([]byte(reqBody))
		}
		req, err := http.NewRequest(sc.Method, url, body)
		if err != nil {
			return tr, fmt.Errorf("%s %s: %w", sc.Method, path, err)
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
			entry.Body = json.RawMessage(respBody)
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
