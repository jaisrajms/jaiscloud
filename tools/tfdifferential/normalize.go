package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// foldFields maps a state/plan attribute name to the placeholder that replaces
// its value. Presence is still compared (a field the emulator omits shows up as
// a missing key); only the inherently run-specific value is folded.
var foldFields = map[string]string{
	"etag":               "<etag>",
	"fingerprint":        "<fingerprint>",
	"create_time":        "<timestamp>",
	"creation_time":      "<timestamp>",
	"update_time":        "<timestamp>",
	"time_created":       "<timestamp>",
	"time_updated":       "<timestamp>",
	"delete_time":        "<timestamp>",
	"expire_time":        "<timestamp>",
	"effective_time":     "<timestamp>",
	"last_modified_time": "<timestamp>",
	"updated":            "<timestamp>",
	"generation":         "<generation>",
	"metageneration":     "<generation>",
	"unique_id":          "<unique-id>",
	"project_number":     "<project>",
	"number":             "<number>",
}

// dropFields are values that are never part of the comparison.
var dropFields = map[string]bool{
	"timeouts": true,
}

// opaqueIDFields fold a pure-numeric value (server-generated) to one placeholder.
var opaqueIDFields = map[string]bool{
	"id":         true,
	"number":     true,
	"unique_id":  true,
	"generation": true,
}

var originPrefix = regexp.MustCompile(`^https?://[^/\s]+`)

// mediaLinkGeneration folds the generation query parameter's value (a
// server-generated number) while keeping its presence: an emulator that omits
// the parameter entirely still differs from the recording.
var mediaLinkGeneration = regexp.MustCompile(`([?&]generation=)[0-9]+`)

// personalEmail folds the operator's consumer-account address that real GCP
// puts into a default ACL (the emulator is unauthenticated). Goldens must not
// carry a personal identifier, and the difference itself stays visible.
var personalEmail = regexp.MustCompile(`^[A-Za-z0-9._%+\-]+@(gmail|googlemail)\.com$`)

// Normalizer makes a Terraform state/plan document comparable across a real-GCP
// recording and an emulator replay: project identifiers, the run suffix and
// server-generated values are folded to placeholders.
type Normalizer struct {
	repls   [][2]string
	origins []string // "<scheme>://<host>" prefixes to fold to <origin>
}

// NewNormalizer builds a normalizer for one run. endpoint is the emulator base
// URL; pass "" for a real-GCP run (googleapis.com origins are always folded).
func NewNormalizer(projectID, projectNumber, suffix, endpoint string) *Normalizer {
	n := &Normalizer{}
	add := func(from, to string) {
		if from != "" {
			n.repls = append(n.repls, [2]string{from, to})
		}
	}
	add(projectNumber, "<project>")
	add(projectID, "<project>")
	add(suffix, "<suffix>")
	sort.SliceStable(n.repls, func(i, j int) bool {
		return len(n.repls[i][0]) > len(n.repls[j][0])
	})
	if endpoint != "" {
		if loc := originPrefix.FindString(endpoint); loc != "" {
			n.origins = append(n.origins, loc)
		}
	}
	return n
}

// String applies the textual folds to a scalar.
func (n *Normalizer) String(s string) string {
	for _, r := range n.repls {
		s = strings.ReplaceAll(s, r[0], r[1])
	}
	return s
}

// foldOrigin replaces a self-link/media-link origin with <origin>. Real GCP
// serves googleapis.com; the emulator serves its own base URL. The path is
// preserved, so a resource-name shape difference is still visible.
func (n *Normalizer) foldOrigin(s string) string {
	for _, o := range n.origins {
		if strings.HasPrefix(s, o) {
			return "<origin>" + strings.TrimPrefix(s, o)
		}
	}
	if loc := originPrefix.FindString(s); loc != "" {
		host := strings.TrimPrefix(strings.TrimPrefix(loc, "https://"), "http://")
		if strings.HasSuffix(host, ".googleapis.com") || host == "googleapis.com" {
			return "<origin>" + s[len(loc):]
		}
	}
	return s
}

// Value normalizes one value. sensitive mirrors Terraform's sensitive_values
// tree; a true leaf is dropped entirely.
func (n *Normalizer) Value(key string, v, sensitive any) (any, bool) {
	if b, ok := sensitive.(bool); ok && b {
		return nil, false
	}
	switch vv := v.(type) {
	case map[string]any:
		sens, _ := sensitive.(map[string]any)
		out := make(map[string]any, len(vv))
		keys := make([]string, 0, len(vv))
		for k := range vv {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if dropFields[k] {
				continue
			}
			var s any
			if sens != nil {
				s = sens[k]
			}
			nv, keep := n.Value(k, vv[k], s)
			if keep {
				out[k] = nv
			}
		}
		return out, true
	case []any:
		sens, _ := sensitive.([]any)
		out := make([]any, len(vv))
		for i, item := range vv {
			var s any
			if i < len(sens) {
				s = sens[i]
			}
			nv, keep := n.Value(key, item, s)
			if keep {
				out[i] = nv
			}
		}
		return out, true
	case string:
		return n.foldScalar(key, vv), true
	case json.Number:
		if placeholder, ok := n.placeholder(key, string(vv)); ok {
			return placeholder, true
		}
		return vv, true
	case float64:
		return n.numeric(key, vv), true
	case bool, nil:
		return v, true
	default:
		return v, true
	}
}

func (n *Normalizer) numeric(key string, v float64) any {
	if placeholder, ok := n.placeholder(key, strconv.FormatFloat(v, 'f', -1, 64)); ok {
		return placeholder
	}
	return json.Number(strconv.FormatFloat(v, 'f', -1, 64))
}

func (n *Normalizer) foldScalar(key, s string) any {
	if placeholder, ok := n.placeholder(key, s); ok {
		return placeholder
	}
	s = mediaLinkGeneration.ReplaceAllString(s, "${1}<generation>")
	s = n.foldOrigin(n.String(s))
	if personalEmail.MatchString(s) {
		return "<user>"
	}
	return s
}

func (n *Normalizer) placeholder(key, s string) (string, bool) {
	if p, ok := foldFields[strings.ToLower(key)]; ok {
		return p, true
	}
	if opaqueIDFields[strings.ToLower(key)] && isDigits(s) {
		return "<numeric-id>", true
	}
	return "", false
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ── Terraform state ─────────────────────────────────────────────────────────

type tfState struct {
	Values *tfStateValues `json:"values"`
}

type tfStateValues struct {
	RootModule *tfStateModule `json:"root_module"`
}

type tfStateModule struct {
	Resources    []tfStateResource `json:"resources"`
	ChildModules []tfStateModule   `json:"child_modules"`
}

type tfStateResource struct {
	Address         string         `json:"address"`
	Mode            string         `json:"mode"`
	Type            string         `json:"type"`
	Name            string         `json:"name"`
	Index           any            `json:"index"`
	Values          map[string]any `json:"values"`
	SensitiveValues any            `json:"sensitive_values"`
}

// ParseState flattens `terraform show -json` state into address -> normalized
// values.
func ParseState(raw []byte, n *Normalizer) (map[string]any, error) {
	var st tfState
	if err := decodeJSON(raw, &st); err != nil {
		return nil, fmt.Errorf("parse state: %w", err)
	}
	out := map[string]any{}
	if st.Values == nil || st.Values.RootModule == nil {
		return out, nil
	}
	var walk func(m *tfStateModule)
	walk = func(m *tfStateModule) {
		for _, r := range m.Resources {
			addr := r.Address
			if addr == "" {
				addr = r.Type + "." + r.Name
			}
			v, keep := n.Value("", r.Values, r.SensitiveValues)
			if keep {
				out[addr] = v
			}
		}
		for i := range m.ChildModules {
			walk(&m.ChildModules[i])
		}
	}
	walk(st.Values.RootModule)
	return out, nil
}

// ── Terraform plan ──────────────────────────────────────────────────────────

type tfPlan struct {
	ResourceChanges []tfResourceChange `json:"resource_changes"`
}

type tfResourceChange struct {
	Address string       `json:"address"`
	Mode    string       `json:"mode"`
	Type    string       `json:"type"`
	Name    string       `json:"name"`
	Index   any          `json:"index"`
	Change  tfPlanChange `json:"change"`
}

type tfPlanChange struct {
	Actions        []string `json:"actions"`
	AfterUnknown   any      `json:"after_unknown"`
	AfterSensitive any      `json:"after_sensitive"`
}

// ParsePlan reduces `terraform show -json <planfile>` to the plan's diff set:
// address -> {actions, unknown}. Only unknown-field *paths* are recorded (never
// values), so a plan golden cannot carry a secret.
func ParsePlan(raw []byte) (map[string]any, error) {
	var p tfPlan
	if err := decodeJSON(raw, &p); err != nil {
		return nil, fmt.Errorf("parse plan: %w", err)
	}
	out := map[string]any{}
	for _, rc := range p.ResourceChanges {
		addr := rc.Address
		if addr == "" {
			addr = rc.Type + "." + rc.Name
		}
		var unknown = []string{}
		collectUnknown(rc.Change.AfterUnknown, "", &unknown)
		sort.Strings(unknown)
		actions := rc.Change.Actions
		if actions == nil {
			actions = []string{}
		}
		out[addr] = map[string]any{
			"actions": actions,
			"unknown": unknown,
		}
	}
	return out, nil
}

// collectUnknown records the dotted paths where after_unknown is true.
func collectUnknown(v any, prefix string, out *[]string) {
	switch vv := v.(type) {
	case bool:
		if vv && prefix != "" {
			*out = append(*out, prefix)
		}
	case map[string]any:
		keys := make([]string, 0, len(vv))
		for k := range vv {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			collectUnknown(vv[k], path, out)
		}
	case []any:
		for i, item := range vv {
			collectUnknown(item, fmt.Sprintf("%s[%d]", prefix, i), out)
		}
	}
}

func decodeJSON(raw []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	return dec.Decode(v)
}
