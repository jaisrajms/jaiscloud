// Package tfcoverage owns the Terraform provider coverage inventory: one row
// per hashicorp/google resource type that maps to an emulated service, with
// its service, emulator state, the fixture/module case that exercises it and a
// classification. tools/tfcoverage/scope.yaml is the hand-maintained source of
// truth; docs/fidelity/terraform-coverage.csv is generated from it plus the
// committed provider schema snapshot and the resources the fixture and pinned
// module cases declare.
package main

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Emulator states a resource type can carry.
const (
	// StateServed: the provider's calls for this resource reach a served API.
	StateServed = "served"
	// StateLimited: the API is served with a declared caveat (the fidelity
	// matrix grades the relevant operations `limited`).
	StateLimited = "limited"
	// StateUnsupported: the resource targets an API surface the emulator does
	// not serve; the reason must say which surface.
	StateUnsupported = "unsupported"
	// StateClientSide: the provider performs no emulator API call for the
	// resource (a state-only resource or a client-side quirk); the reason must
	// say why.
	StateClientSide = "client-side"
)

// Classifications a generated row can carry.
const (
	ClassServed      = "served"
	ClassPlanOnly    = "plan-only"
	ClassUnsupported = "unsupported"
	ClassClientSide  = "client-side"
)

// ServiceScope records one emulated service and the fidelity-matrix state it
// had when the rules were last reviewed. A promotion to `ga` invalidates the
// "not served / served with caveats" reasoning below it, so Check fails and
// asks for a review instead of silently keeping a stale classification.
type ServiceScope struct {
	// MatrixService is the service name in docs/fidelity/fidelity-matrix.json;
	// it defaults to the map key.
	MatrixService string `yaml:"matrix_service"`
	// MatrixState is the state recorded at the last review: ga | limited |
	// unsupported.
	MatrixState string `yaml:"matrix_state"`
}

// Rule maps provider resource types to an emulated service and state. Rules are
// evaluated in file order and the first match wins, so exact resource types and
// narrow globs must precede a service-wide glob.
type Rule struct {
	// Match is a path.Match glob on the provider resource type
	// (e.g. "google_storage_bucket" or "google_storage_*").
	Match string `yaml:"match"`
	// Service is a key in Scope.Services.
	Service string `yaml:"service"`
	// State is served | limited | unsupported | client-side.
	State string `yaml:"state"`
	// Reason is required for unsupported and client-side rules; for a
	// served/limited rule it is the row's reason (a synthesized one is used
	// when the resource is not exercised and this is empty).
	Reason string `yaml:"reason"`
	// ReviewAfter overrides Scope.DefaultReviewAfter for this rule (YYYY-MM-DD).
	ReviewAfter string `yaml:"review_after"`
}

// OutOfScopeRule exempts a provider subsystem from the inventory with a reason.
// Every provider resource type must match either a Rule or an OutOfScopeRule;
// an unmatched type fails the gate instead of disappearing.
type OutOfScopeRule struct {
	Match  string `yaml:"match"`
	Reason string `yaml:"reason"`
}

// Scope is tools/tfcoverage/scope.yaml.
type Scope struct {
	// Provider is the provider the rules are written against.
	Provider string `yaml:"provider"`
	// DefaultReviewAfter is the row review date when a rule sets none.
	DefaultReviewAfter string `yaml:"default_review_after"`
	// Services maps an emulator service name to its matrix facts.
	Services map[string]ServiceScope `yaml:"services"`
	// Rules classifies provider resource types that map to an emulated service.
	Rules []Rule `yaml:"rules"`
	// OutOfScope exempts provider subsystems the emulator does not serve.
	OutOfScope []OutOfScopeRule `yaml:"out_of_scope"`
}

// LoadScope reads and minimally validates a scope file.
func LoadScope(file string) (*Scope, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read scope (%s): %w", file, err)
	}
	var s Scope
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("parse scope (%s): %w", file, err)
	}
	if err := s.validate(); err != nil {
		return nil, fmt.Errorf("scope (%s): %w", file, err)
	}
	return &s, nil
}

func (s *Scope) validate() error {
	if strings.TrimSpace(s.Provider) == "" {
		return fmt.Errorf("provider is empty")
	}
	if s.DefaultReviewAfter == "" {
		return fmt.Errorf("default_review_after is empty")
	}
	if _, err := parseDate(s.DefaultReviewAfter); err != nil {
		return fmt.Errorf("default_review_after: %w", err)
	}
	if len(s.Services) == 0 {
		return fmt.Errorf("no services declared")
	}
	for name, svc := range s.Services {
		if svc.MatrixState == "" {
			return fmt.Errorf("services.%s: matrix_state is empty", name)
		}
		if !validMatrixState(svc.MatrixState) {
			return fmt.Errorf("services.%s: matrix_state %q is not ga|limited|unsupported", name, svc.MatrixState)
		}
		if svc.MatrixService == "" {
			svc.MatrixService = name
			s.Services[name] = svc
		}
	}
	if len(s.Rules) == 0 {
		return fmt.Errorf("no rules declared")
	}
	for i, r := range s.Rules {
		if _, err := path.Match(r.Match, "x"); err != nil {
			return fmt.Errorf("rules[%d].match %q: %w", i, r.Match, err)
		}
		if _, ok := s.Services[r.Service]; !ok {
			return fmt.Errorf("rules[%d] (%s): service %q is not declared", i, r.Match, r.Service)
		}
		if !validState(r.State) {
			return fmt.Errorf("rules[%d] (%s): state %q is not served|limited|unsupported|client-side", i, r.Match, r.State)
		}
		if (r.State == StateUnsupported || r.State == StateClientSide) && strings.TrimSpace(r.Reason) == "" {
			return fmt.Errorf("rules[%d] (%s): state %q requires a reason", i, r.Match, r.State)
		}
		if r.ReviewAfter != "" {
			if _, err := parseDate(r.ReviewAfter); err != nil {
				return fmt.Errorf("rules[%d] (%s).review_after: %w", i, r.Match, err)
			}
		}
	}
	for i, r := range s.OutOfScope {
		if _, err := path.Match(r.Match, "x"); err != nil {
			return fmt.Errorf("out_of_scope[%d].match %q: %w", i, r.Match, err)
		}
		if strings.TrimSpace(r.Reason) == "" {
			return fmt.Errorf("out_of_scope[%d] (%s): reason is empty", i, r.Match)
		}
	}
	return nil
}

func validState(state string) bool {
	switch state {
	case StateServed, StateLimited, StateUnsupported, StateClientSide:
		return true
	}
	return false
}

func validMatrixState(state string) bool {
	switch state {
	case "ga", "limited", "unsupported":
		return true
	}
	return false
}

// MatchRule returns the first rule matching a provider resource type.
func (s *Scope) MatchRule(resourceType string) (*Rule, bool) {
	for i := range s.Rules {
		if matches(s.Rules[i].Match, resourceType) {
			return &s.Rules[i], true
		}
	}
	return nil, false
}

// MatchOutOfScope returns the first out-of-scope exemption matching a provider
// resource type.
func (s *Scope) MatchOutOfScope(resourceType string) (*OutOfScopeRule, bool) {
	for i := range s.OutOfScope {
		if matches(s.OutOfScope[i].Match, resourceType) {
			return &s.OutOfScope[i], true
		}
	}
	return nil, false
}

func matches(pattern, name string) bool {
	ok, err := path.Match(pattern, name)
	return err == nil && ok
}

// usage records how often each rule matched, so a rule that matches nothing (a
// removed resource type or a typo) fails the gate instead of lingering.
type usage struct {
	Rules      []int    // matches per Rules entry
	OutOfScope []int    // matches per OutOfScope entry
	Unmatched  []string // resource types no rule or exemption covers
}

func (s *Scope) usage(resourceTypes []string) usage {
	u := usage{
		Rules:      make([]int, len(s.Rules)),
		OutOfScope: make([]int, len(s.OutOfScope)),
	}
	for _, rt := range resourceTypes {
		matched := false
		// Out-of-scope exemptions win: an exempted subsystem is removed from
		// the inventory even if a broad rule also matches (for example
		// google_storage_transfer_* under a google_storage_* rule).
		for i := range s.OutOfScope {
			if matches(s.OutOfScope[i].Match, rt) {
				u.OutOfScope[i]++
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		for i := range s.Rules {
			if matches(s.Rules[i].Match, rt) {
				u.Rules[i]++
				matched = true
				break
			}
		}
		if !matched {
			u.Unmatched = append(u.Unmatched, rt)
		}
	}
	sort.Strings(u.Unmatched)
	return u
}
