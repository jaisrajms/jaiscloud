//go:build gcp_differential

package gcpdifferential

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// sessionPath reduces a captured response-header URL to the path+query a later
// scenario addresses through the target's service/origin mapping. A resumable
// upload's Location header is absolute on both real GCP and the emulator, but
// their hosts and path prefixes differ, so only the origin-independent part is
// kept. A value that is not an absolute URL is returned unchanged.
func sessionPath(raw string) string {
	if u, err := url.Parse(raw); err == nil && u.IsAbs() {
		return u.RequestURI()
	}
	return raw
}

// Target describes where a scenario run is sent. Record mode targets real GCP
// with an ADC bearer token; replay targets the local emulator with no
// credentials and no external network.
type Target struct {
	Name          string
	Project       string
	ProjectNumber string
	Suffix        string
	Names         ResourceNames
	Token         string
	HTTP          *http.Client
	URLFor        func(service, path string) string
	// SavedVars holds the last Run's captured ${name} variables (e.g. a
	// server-generated alert-policy or notification-channel name) so Cleanup can
	// delete the exact resources a capture created. It is nil until Run.
	SavedVars map[string]string
}

// Run executes every scenario against the target in order, capturing a
// normalized Exchange for each. Saved variables feed later scenarios.
func (t *Target) Run(scenarios []Scenario) ([]Exchange, error) {
	if t.HTTP == nil {
		t.HTTP = &http.Client{Timeout: 60 * time.Second}
	}
	norm := NewNormalizer(t.Project, t.ProjectNumber, t.Suffix, t.Names)

	vars := map[string]string{}
	exs := make([]Exchange, 0, len(scenarios))
	for i, sc := range scenarios {
		path := expandVars(sc.Path, vars)
		reqBody := expandVars(sc.Body, vars)

		var respHeader http.Header
		send := func() (int, []byte, error) {
			url := t.URLFor(sc.Service, path)
			var body io.Reader
			if reqBody != "" {
				body = bytes.NewReader([]byte(reqBody))
			}
			req, err := http.NewRequest(sc.Method, url, body)
			if err != nil {
				return 0, nil, fmt.Errorf("%s %s: %w", sc.Method, path, err)
			}
			if reqBody != "" {
				ct := sc.ContentType
				if ct == "" {
					ct = "application/json"
				}
				req.Header.Set("Content-Type", ct)
			}
			for k, v := range sc.Headers {
				req.Header.Set(k, v)
			}
			if t.Token != "" && !sc.NoAuth {
				req.Header.Set("Authorization", "Bearer "+t.Token)
			}
			req.Header.Set("User-Agent", "jaiscloud-gcp-differential/1")
			resp, err := t.HTTP.Do(req)
			if err != nil {
				return 0, nil, fmt.Errorf("%s %s: %w", sc.Method, path, err)
			}
			defer resp.Body.Close()
			respBody, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return 0, nil, fmt.Errorf("%s %s: read body: %w", sc.Method, path, readErr)
			}
			respHeader = resp.Header
			return resp.StatusCode, respBody, nil
		}

		var (
			status   int
			respBody []byte
			err      error
		)
		if sc.Wait != nil {
			status, respBody, err = t.waitFor(send, sc.Wait)
		} else {
			status, respBody, err = send()
		}
		if err != nil {
			return exs, err
		}

		// A Setup scenario drives state a later golden depends on but is not
		// itself recorded or diffed (its response is best-effort).
		if !sc.Setup {
			ex := Exchange{
				Index:   i,
				Service: sc.Service,
				Op:      sc.Op,
				Method:  sc.Method,
				Path:    norm.Path(path),
				Status:  status,
			}
			if len(bytes.TrimSpace(respBody)) > 0 {
				ex.Response = norm.Bytes(respBody)
			}
			if reqBody != "" && !sc.NoRequestCapture {
				ex.Request = norm.RequestBytes([]byte(reqBody))
			}
			exs = append(exs, ex)
		}

		for name, header := range sc.SaveHeader {
			if v := respHeader.Get(header); v != "" {
				vars[name] = sessionPath(v)
			}
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
	}
	t.SavedVars = vars
	return exs, nil
}

// waitFor polls send until the spec's condition holds or spec.Timeout expires,
// returning the last observed status/body. It always sends at least once so a
// response already at the terminal state is captured immediately.
func (t *Target) waitFor(send func() (int, []byte, error), spec *WaitSpec) (int, []byte, error) {
	interval := spec.Interval
	if interval <= 0 {
		interval = time.Second
	}
	timeout := spec.Timeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		status, body, err := send()
		if err != nil {
			return status, body, err
		}
		var decoded any
		if json.Unmarshal(body, &decoded) == nil {
			if v, ok := jsonPathValue(decoded, spec.Field); ok {
				switch {
				case spec.MinLen > 0:
					if jsonLen(v) >= spec.MinLen {
						return status, body, nil
					}
				case spec.Contains != "":
					if b, err := json.Marshal(v); err == nil && strings.Contains(string(b), spec.Contains) {
						return status, body, nil
					}
				case truthyJSON(v):
					return status, body, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return status, body, nil
		}
		time.Sleep(interval)
	}
}

// jsonPathValue resolves a dotted path into a decoded JSON value. A numeric
// segment indexes an array ("receivedMessages.0.ackId"); any other segment is a
// map key.
func jsonPathValue(v any, path string) (any, bool) {
	cur := v
	for _, seg := range strings.Split(path, ".") {
		if arr, ok := cur.([]any); ok {
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(arr) {
				return nil, false
			}
			cur = arr[i]
			continue
		}
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// jsonLen reports the element count of a decoded JSON array or object, and 0
// for anything else.
func jsonLen(v any) int {
	switch t := v.(type) {
	case []any:
		return len(t)
	case map[string]any:
		return len(t)
	default:
		return 0
	}
}

// truthyJSON reports whether a decoded JSON value is a true/non-empty scalar or
// a non-empty collection.
func truthyJSON(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != "" && t != "false" && t != "0"
	case float64:
		return t != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return false
	}
}

// RealTarget builds a record target for real GCP. token must be a fresh ADC
// access token; it is held in memory only and never logged.
func RealTarget(project, projectNumber, suffix string, names ResourceNames, token string) *Target {
	return &Target{
		Name:          "real-gcp",
		Project:       project,
		ProjectNumber: projectNumber,
		Suffix:        suffix,
		Names:         names,
		Token:         token,
		HTTP:          &http.Client{Timeout: 60 * time.Second},
		URLFor: func(service, path string) string {
			base, ok := serviceBaseURL[service]
			if !ok {
				// Fall back to storage-style origin; every included service must
				// have an entry, so this is a programming error surfaced loudly.
				base = "https://" + service + ".googleapis.com"
			}
			return base + path
		},
	}
}

// EmulatorTarget builds a replay target for the local emulator. All services are
// served from one origin, so the per-scenario path (which already carries the
// service prefix) is appended unchanged.
func EmulatorTarget(endpoint, project, suffix string, names ResourceNames) *Target {
	return &Target{
		Name:          "emulator",
		Project:       project,
		ProjectNumber: "",
		Suffix:        suffix,
		Names:         names,
		HTTP:          &http.Client{Timeout: 60 * time.Second},
		URLFor: func(_, path string) string {
			return endpoint + path
		},
	}
}
