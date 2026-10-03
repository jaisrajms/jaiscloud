//go:build gcp_conformance

package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	conf "jaiscloud/tests/gcpconformance"
)

// overridesDoc mirrors docs/fidelity-overrides.yaml.
type overridesDoc struct {
	Defaults   map[string]overrideEntry `yaml:"defaults"`
	Operations []operationOverride      `yaml:"operations"`
}

type overrideEntry struct {
	State        string `yaml:"state"`
	Reason       string `yaml:"reason"`
	AllowUpgrade bool   `yaml:"allow_upgrade"`
}

type operationOverride struct {
	Service      string `yaml:"service"`
	Operation    string `yaml:"operation"`
	State        string `yaml:"state"`
	Reason       string `yaml:"reason"`
	AllowUpgrade bool   `yaml:"allow_upgrade"`
}

// Overrides is a validated override set with per-service and per-operation
// lookups. Operation-level entries win over service-level defaults.
type Overrides struct {
	byService map[string]Override
	byOp      map[string]Override
}

// LoadOverrides reads and validates path against the enumerated registry.
// An override that names an unknown service/operation, uses an unknown state,
// is non-ga without a reason, or upgrades to ga without allow_upgrade is an
// error — the file is the published contract, so it must be exact.
//
// ops is the REST registry; grpc is the enumerated gRPC surface. Both are
// validated so per-operation overrides may target either transport (gRPC
// operations are keyed by their plain proto method name).
func LoadOverrides(path string, ops []conf.Operation, grpc []conf.GRPCService) (*Overrides, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc overridesDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	services := map[string]bool{}
	operations := map[string]bool{}
	for _, o := range ops {
		services[o.Service] = true
		operations[o.Service+"/"+o.Key()] = true
	}
	for _, s := range grpc {
		services[s.Service] = true
		for _, m := range s.Methods {
			operations[s.Service+"/"+m] = true
		}
	}

	o := &Overrides{byService: map[string]Override{}, byOp: map[string]Override{}}

	// Service-level defaults.
	svcNames := make([]string, 0, len(doc.Defaults))
	for svc := range doc.Defaults {
		svcNames = append(svcNames, svc)
	}
	sort.Strings(svcNames)
	for _, svc := range svcNames {
		e := doc.Defaults[svc]
		if !services[svc] {
			return nil, fmt.Errorf("%s: defaults: unknown service %q", path, svc)
		}
		ov, err := validateEntry(path, "defaults."+svc, e)
		if err != nil {
			return nil, err
		}
		o.byService[svc] = ov
	}

	// Per-operation overrides.
	for i, op := range doc.Operations {
		where := fmt.Sprintf("%s: operations[%d] %s/%s", path, i, op.Service, op.Operation)
		key := op.Service + "/" + op.Operation
		if !operations[key] {
			return nil, fmt.Errorf("%s: unknown operation (not in the registry)", where)
		}
		ov, err := validateEntry(path, where, overrideEntry{
			State: op.State, Reason: op.Reason, AllowUpgrade: op.AllowUpgrade,
		})
		if err != nil {
			return nil, err
		}
		o.byOp[key] = ov
	}

	return o, nil
}

func validateEntry(path, where string, e overrideEntry) (Override, error) {
	if e.State == "" {
		return Override{}, fmt.Errorf("%s: %s: missing state", path, where)
	}
	if !ValidState(e.State) {
		return Override{}, fmt.Errorf("%s: %s: unknown state %q", path, where, e.State)
	}
	if e.State != StateGA && strings.TrimSpace(e.Reason) == "" {
		return Override{}, fmt.Errorf("%s: %s: state %q requires a reason", path, where, e.State)
	}
	if e.State == StateGA && !e.AllowUpgrade {
		return Override{}, fmt.Errorf("%s: %s: state ga requires allow_upgrade: true", path, where)
	}
	return Override{State: e.State, Reason: e.Reason, AllowUpgrade: e.AllowUpgrade}, nil
}

// For returns the effective override for a cell, or nil when none applies.
func (o *Overrides) For(service, operation string) *Override {
	if ov, ok := o.byOp[service+"/"+operation]; ok {
		c := ov
		return &c
	}
	if ov, ok := o.byService[service]; ok {
		c := ov
		return &c
	}
	return nil
}

// Len reports how many override entries were loaded.
func (o *Overrides) Len() int { return len(o.byService) + len(o.byOp) }
