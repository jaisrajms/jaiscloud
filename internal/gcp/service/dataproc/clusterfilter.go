package dataproc

import (
	"fmt"
	"strings"

	"jaiscloud/internal/gcp/aip160"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// clusterFilter is the compiled subset of the clusters.list filter grammar
// (dataproc.v1.ListClustersRequest.filter): a conjunction of clauses over
// status.state, clusterName and labels.<key>. An empty filter matches every
// cluster. Anything outside the grammar is InvalidArgument — a filter is never
// silently ignored, which would return an unfiltered list to a client that
// asked for a subset.
type clusterFilter struct {
	preds []clusterPredicate
}

// match reports whether c satisfies every clause.
func (f clusterFilter) match(c dpstore.Cluster) bool {
	for _, p := range f.preds {
		if !p.match(c) {
			return false
		}
	}
	return true
}

// clusterFilterStateOrder is the documented status.state value set, in the
// order the API lists it (used for error messages and to build the lookup set).
var clusterFilterStateOrder = []string{
	"CREATING",
	"RUNNING",
	"ERROR",
	"DELETING",
	"UPDATING",
	"STOPPING",
	"STOPPED",
}

// clusterFilterStates are the status.state values the API documents for the
// clusters.list filter (ACTIVE and INACTIVE are handled as aggregate sets).
var clusterFilterStates = func() map[string]bool {
	m := make(map[string]bool, len(clusterFilterStateOrder))
	for _, s := range clusterFilterStateOrder {
		m[s] = true
	}
	return m
}()

// clusterActiveStates is the documented expansion of `status.state = ACTIVE`
// ("ACTIVE contains the CREATING, UPDATING, and RUNNING states").
var clusterActiveStates = map[string]bool{
	"CREATING": true,
	"UPDATING": true,
	"RUNNING":  true,
}

// clusterInactiveStates is the documented expansion of `status.state =
// INACTIVE` ("INACTIVE contains the DELETING, ERROR, STOPPING, and STOPPED
// states").
var clusterInactiveStates = map[string]bool{
	"DELETING": true,
	"ERROR":    true,
	"STOPPING": true,
	"STOPPED":  true,
}

// compileClusterFilter parses the bounded clusters.list filter grammar:
//
//	clause := field op value
//	filter := clause ( ("AND" | implicit whitespace) clause )*
//	field  := "status.state" | "clusterName" | "labels." <key>
//	value  := <ident> | <double-quoted string> | "*"
//
// Every field takes the equality operator; status.state accepts the documented
// ACTIVE/INACTIVE aggregates or a concrete cluster state, clusterName accepts a
// name or * (any name), and labels.<key> accepts a value or * (the key is
// present with any value). Clauses are joined by the logical AND only, written
// explicitly (" AND ") or implicitly (whitespace), as the API documents.
func compileClusterFilter(s string) (clusterFilter, error) {
	if strings.TrimSpace(s) == "" {
		return clusterFilter{}, nil
	}
	toks, err := aip160.Tokenize(s)
	if err != nil {
		return clusterFilter{}, invalidArgument("dataproc ListClusters filter: " + err.Error())
	}
	preds, err := aip160.NewParser(toks).ParseClauses(parseClusterClause)
	if err != nil {
		return clusterFilter{}, invalidArgument("dataproc ListClusters filter: " + err.Error())
	}
	f := clusterFilter{preds: make([]clusterPredicate, 0, len(preds))}
	for _, p := range preds {
		f.preds = append(f.preds, p.(clusterPredicate))
	}
	return f, nil
}

// ─── predicates ───────────────────────────────────────────────────────────────

// clusterPredicate is one compiled clusters.list filter clause.
type clusterPredicate interface {
	match(dpstore.Cluster) bool
}

// clusterStatePredicate matches an ACTIVE/INACTIVE aggregate or a concrete
// status.state value.
type clusterStatePredicate struct {
	state string
}

func (p clusterStatePredicate) match(c dpstore.Cluster) bool {
	switch p.state {
	case "ACTIVE":
		return clusterActiveStates[c.Status.State]
	case "INACTIVE":
		return clusterInactiveStates[c.Status.State]
	default:
		return c.Status.State == p.state
	}
}

// clusterNamePredicate matches a cluster's name; an empty name is the `*` form
// (any name).
type clusterNamePredicate struct {
	name string
}

func (p clusterNamePredicate) match(c dpstore.Cluster) bool {
	return p.name == "" || c.Name == p.name
}

// clusterLabelExistsPredicate matches clusters carrying the label key with any
// value (the `labels.<key> = *` form).
type clusterLabelExistsPredicate struct {
	key string
}

func (p clusterLabelExistsPredicate) match(c dpstore.Cluster) bool {
	_, ok := c.Labels[p.key]
	return ok
}

// clusterLabelEqualsPredicate matches clusters whose label key equals value. A
// cluster missing the key never matches, even against an empty value.
type clusterLabelEqualsPredicate struct {
	key   string
	value string
}

func (p clusterLabelEqualsPredicate) match(c dpstore.Cluster) bool {
	v, ok := c.Labels[p.key]
	return ok && v == p.value
}

// ─── parser ───────────────────────────────────────────────────────────────────

// parseClusterClause parses one clusters.list filter clause into the matching
// clusterPredicate, returned as an `any` for the shared clause sequencer.
func parseClusterClause(p *aip160.Parser) (any, error) {
	keyTok := p.Next()
	if keyTok.Kind != aip160.KindIdent {
		return nil, fmt.Errorf("expected field, got %s", aip160.TokenText(keyTok))
	}
	key := keyTok.Text

	opTok := p.Next()
	if opTok.Kind != aip160.KindOp {
		return nil, fmt.Errorf("expected operator after %q, got %s", key, aip160.TokenText(opTok))
	}
	op := opTok.Text
	if op != "=" {
		return nil, fmt.Errorf("unsupported operator %q on %s (only \"=\" is supported)", op, key)
	}

	switch {
	case key == "status.state":
		valTok := p.Next()
		if valTok.Kind != aip160.KindIdent {
			return nil, fmt.Errorf("status.state value must be a state name, got %s", aip160.TokenText(valTok))
		}
		switch valTok.Text {
		case "ACTIVE":
			return clusterStatePredicate{state: "ACTIVE"}, nil
		case "INACTIVE":
			return clusterStatePredicate{state: "INACTIVE"}, nil
		default:
			if !clusterFilterStates[valTok.Text] {
				return nil, fmt.Errorf("unsupported status.state %q (want ACTIVE, INACTIVE or one of %s)",
					valTok.Text, clusterStateList())
			}
			return clusterStatePredicate{state: valTok.Text}, nil
		}

	case key == "clusterName":
		valTok := p.Next()
		if valTok.Kind == aip160.KindStar {
			return clusterNamePredicate{}, nil
		}
		val, ok := aip160.TokenValue(valTok)
		if !ok || val == "" {
			return nil, fmt.Errorf("clusterName value must be a name or \"*\", got %s", aip160.TokenText(valTok))
		}
		return clusterNamePredicate{name: val}, nil

	case strings.HasPrefix(key, "labels."):
		lk := strings.TrimPrefix(key, "labels.")
		if lk == "" {
			return nil, fmt.Errorf("empty label key in %q", key)
		}
		valTok := p.Next()
		if valTok.Kind == aip160.KindStar {
			return clusterLabelExistsPredicate{key: lk}, nil
		}
		val, ok := aip160.TokenValue(valTok)
		if !ok {
			return nil, fmt.Errorf("label value must be a value or \"*\", got %s", aip160.TokenText(valTok))
		}
		return clusterLabelEqualsPredicate{key: lk, value: val}, nil

	default:
		return nil, fmt.Errorf("unsupported filter key %q (only status.state, clusterName and labels.<key> are supported)", key)
	}
}

// clusterStateList renders the supported concrete status.state values for an
// error message, in the API's documented order.
func clusterStateList() string {
	return strings.Join(clusterFilterStateOrder, ", ")
}
