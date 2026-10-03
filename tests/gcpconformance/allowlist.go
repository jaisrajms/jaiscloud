//go:build gcp_conformance

package gcpconformance

import "strings"

// AllowRule suppresses a divergence that reflects documented, API-specific
// wire-shape variance rather than an emulator defect. A rule matches on the
// divergence Kind and a Path suffix (ValidateTranscripts prefixes Path with the
// entry context, so exact matching would miss), optionally narrowed to a set of
// services.
type AllowRule struct {
	Kind     string
	Path     string
	Services []string // empty means "any service"
	Reason   string
}

func (r AllowRule) matches(d Divergence) bool {
	if r.Kind != d.Kind || !strings.HasSuffix(d.Path, r.Path) {
		return false
	}
	if len(r.Services) == 0 {
		return true
	}
	for _, s := range r.Services {
		if s == d.Service {
			return true
		}
	}
	return false
}

// errorEnvelopeAllowlist is intentionally EMPTY.
//
// The envelope rule used to demand BOTH the google.rpc "status" and the legacy
// errors[] array, which flagged correct responses from legacy APIs (Cloud
// Storage: errors[] without status) and modern ones (Pub/Sub, Secret Manager,
// KMS, IAM, Cloud DNS, BigQuery: status without errors[]). ValidateErrorEnvelope
// now models that per-generation variance directly — requiring code + message,
// validating whichever of status/errors[] is present, and demanding at least
// one of them — so no shape-variance finding needs suppressing.
//
// The mechanism is kept for genuinely-divergent cases: add a rule here WITH a
// reason, rather than weakening the validator.
var errorEnvelopeAllowlist = []AllowRule{}

// allowRuleFor returns the first rule in rules matching d, or nil.
func allowRuleFor(d Divergence, rules []AllowRule) *AllowRule {
	for i := range rules {
		if rules[i].matches(d) {
			return &rules[i]
		}
	}
	return nil
}

// ApplyAllowlist partitions divergences using the package allowlist.
func ApplyAllowlist(divs []Divergence) (kept, suppressed []Divergence) {
	return applyAllowlist(divs, errorEnvelopeAllowlist)
}

// applyAllowlist partitions divergences using an explicit rule set (testable).
func applyAllowlist(divs []Divergence, rules []AllowRule) (kept, suppressed []Divergence) {
	for _, d := range divs {
		if allowRuleFor(d, rules) != nil {
			suppressed = append(suppressed, d)
			continue
		}
		kept = append(kept, d)
	}
	return kept, suppressed
}
