package dataproc

import (
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/gcp/aip160"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// JobStateMatcher narrows ListJobs to active or non-active jobs. It mirrors
// dataproc.v1.ListJobsRequest.JobStateMatcher: ACTIVE selects non-terminal jobs
// (PENDING, SETUP_DONE, RUNNING, CANCEL_PENDING, CANCEL_STARTED,
// ATTEMPT_FAILURE) and NON_ACTIVE the terminal ones (DONE, ERROR, CANCELLED).
// Per the API it is ignored when a filter is supplied.
type JobStateMatcher int

const (
	// JobStateMatcherAll matches jobs in any state.
	JobStateMatcherAll JobStateMatcher = iota
	// JobStateMatcherActive matches jobs in a non-terminal state.
	JobStateMatcherActive
	// JobStateMatcherNonActive matches jobs in a terminal state.
	JobStateMatcherNonActive
)

// ParseJobStateMatcher parses the REST jobStateMatcher query value
// (ALL/ACTIVE/NON_ACTIVE; empty means ALL). The enum name is case-sensitive,
// matching the API's enum decoding; an unknown value is InvalidArgument.
func ParseJobStateMatcher(s string) (JobStateMatcher, error) {
	v := strings.TrimSpace(s)
	switch v {
	case "", "ALL":
		return JobStateMatcherAll, nil
	case "ACTIVE":
		return JobStateMatcherActive, nil
	case "NON_ACTIVE":
		return JobStateMatcherNonActive, nil
	default:
		return JobStateMatcherAll, invalidArgument("unsupported jobStateMatcher " + v)
	}
}

// jobPredicate is one compiled ListJobs filter clause.
type jobPredicate interface {
	match(dpstore.Job) bool
}

// jobFilter is the compiled subset of the ListJobs filter grammar
// (dataproc.v1.ListJobsRequest.filter): a conjunction of clauses over
// status.state, labels.<key> and insertTime. An empty filter matches every job.
type jobFilter struct {
	preds []jobPredicate
}

// match reports whether j satisfies every clause.
func (f jobFilter) match(j dpstore.Job) bool {
	for _, p := range f.preds {
		if !p.match(j) {
			return false
		}
	}
	return true
}

// compileJobFilter parses the bounded Dataproc ListJobs filter grammar:
//
//	clause := field op value
//	filter := clause ( ("AND" | implicit whitespace) clause )*
//	field  := "status.state" | "labels." <key> | "insertTime"
//	value  := <ident> | <double-quoted string> | "*"
//
// status.state accepts ACTIVE (non-terminal) or NON_ACTIVE (terminal) with an
// equality operator; labels.<key> accepts = against a value or * (the key is
// present with any value); insertTime accepts =, !=, <, <=, > or >= against a
// double-quoted RFC3339 timestamp. Clauses are joined by the logical AND
// operator only, written explicitly (" AND ") or implicitly (whitespace), as
// the proto documents. Anything outside the grammar fails with InvalidArgument
// rather than silently matching everything.
func compileJobFilter(s string) (jobFilter, error) {
	if strings.TrimSpace(s) == "" {
		return jobFilter{}, nil
	}
	toks, err := aip160.Tokenize(s)
	if err != nil {
		return jobFilter{}, invalidArgument("dataproc ListJobs filter: " + err.Error())
	}
	preds, err := aip160.NewParser(toks).ParseClauses(parseJobClause)
	if err != nil {
		return jobFilter{}, invalidArgument("dataproc ListJobs filter: " + err.Error())
	}
	f := jobFilter{preds: make([]jobPredicate, 0, len(preds))}
	for _, p := range preds {
		f.preds = append(f.preds, p.(jobPredicate))
	}
	return f, nil
}

// matchJobStateMatcher reports whether j satisfies the jobStateMatcher.
// ListJobs applies it only when no filter was supplied.
func matchJobStateMatcher(j dpstore.Job, m JobStateMatcher) bool {
	switch m {
	case JobStateMatcherActive:
		return jobStatePredicate{active: true}.match(j)
	case JobStateMatcherNonActive:
		return jobStatePredicate{active: false}.match(j)
	default:
		return true
	}
}

// ─── predicates ───────────────────────────────────────────────────────────────

// jobStatePredicate matches ACTIVE (non-terminal) or NON_ACTIVE (terminal) jobs.
type jobStatePredicate struct {
	active bool
}

func (p jobStatePredicate) match(j dpstore.Job) bool {
	if p.active {
		return !jobTerminal(j.Status.State)
	}
	return jobTerminal(j.Status.State)
}

// jobLabelExistsPredicate matches jobs carrying the label key with any value
// (the `labels.<key> = *` form).
type jobLabelExistsPredicate struct {
	key string
}

func (p jobLabelExistsPredicate) match(j dpstore.Job) bool {
	_, ok := j.Labels[p.key]
	return ok
}

// jobLabelEqualsPredicate matches jobs whose label key equals value. A job
// missing the key never matches, even against an empty value.
type jobLabelEqualsPredicate struct {
	key   string
	value string
}

func (p jobLabelEqualsPredicate) match(j dpstore.Job) bool {
	v, ok := j.Labels[p.key]
	return ok && v == p.value
}

// jobInsertTimeOps is the operator set accepted on insertTime.
var jobInsertTimeOps = map[string]bool{
	"=": true, "!=": true, "<": true, "<=": true, ">": true, ">=": true,
}

// jobInsertTimePredicate matches a job's creation (insert) time against a
// RFC3339 timestamp.
type jobInsertTimePredicate struct {
	op string
	ts time.Time
}

func (p jobInsertTimePredicate) match(j dpstore.Job) bool {
	t := j.CreateTime
	switch p.op {
	case "=":
		return t.Equal(p.ts)
	case "!=":
		return !t.Equal(p.ts)
	case "<":
		return t.Before(p.ts)
	case "<=":
		return !t.After(p.ts)
	case ">":
		return t.After(p.ts)
	case ">=":
		return !t.Before(p.ts)
	}
	return false
}

// ─── parser ───────────────────────────────────────────────────────────────────

// parseJobClause parses one jobs.list filter clause (`field op value`) into the
// matching jobPredicate, returned as an `any` for the shared clause sequencer.
func parseJobClause(p *aip160.Parser) (any, error) {
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

	switch {
	case key == "status.state":
		if op != "=" {
			return nil, fmt.Errorf("unsupported operator %q on status.state (only \"=\" is supported)", op)
		}
		valTok := p.Next()
		if valTok.Kind != aip160.KindIdent {
			return nil, fmt.Errorf("status.state value must be ACTIVE or NON_ACTIVE, got %s", aip160.TokenText(valTok))
		}
		switch valTok.Text {
		case "ACTIVE":
			return jobStatePredicate{active: true}, nil
		case "NON_ACTIVE":
			return jobStatePredicate{active: false}, nil
		default:
			return nil, fmt.Errorf("unsupported status.state %q (only ACTIVE and NON_ACTIVE are supported)", valTok.Text)
		}

	case key == "insertTime":
		if !jobInsertTimeOps[op] {
			return nil, fmt.Errorf("unsupported operator %q on insertTime", op)
		}
		valTok := p.Next()
		if valTok.Kind != aip160.KindString {
			return nil, fmt.Errorf("insertTime value must be a double-quoted RFC3339 timestamp, got %s", aip160.TokenText(valTok))
		}
		ts, err := time.Parse(time.RFC3339, valTok.Text)
		if err != nil {
			return nil, fmt.Errorf("invalid insertTime timestamp %q", valTok.Text)
		}
		return jobInsertTimePredicate{op: op, ts: ts}, nil

	case strings.HasPrefix(key, "labels."):
		lk := strings.TrimPrefix(key, "labels.")
		if lk == "" {
			return nil, fmt.Errorf("empty label key in %q", key)
		}
		if op != "=" {
			return nil, fmt.Errorf("unsupported operator %q on %s (only \"=\" is supported)", op, key)
		}
		valTok := p.Next()
		if valTok.Kind == aip160.KindStar {
			return jobLabelExistsPredicate{key: lk}, nil
		}
		val, ok := aip160.TokenValue(valTok)
		if !ok {
			return nil, fmt.Errorf("label value must be a value or \"*\", got %s", aip160.TokenText(valTok))
		}
		return jobLabelEqualsPredicate{key: lk, value: val}, nil

	default:
		return nil, fmt.Errorf("unsupported filter key %q (only status.state, labels.<key> and insertTime are supported)", key)
	}
}
