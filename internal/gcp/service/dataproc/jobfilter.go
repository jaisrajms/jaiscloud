package dataproc

import (
	"fmt"
	"strings"
	"time"

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
	toks, err := tokenizeJobFilter(s)
	if err != nil {
		return jobFilter{}, invalidArgument("dataproc ListJobs filter: " + err.Error())
	}
	f, err := (&jobFilterParser{toks: toks}).parse()
	if err != nil {
		return jobFilter{}, invalidArgument("dataproc ListJobs filter: " + err.Error())
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

// ─── tokenizer ────────────────────────────────────────────────────────────────

type jobFilterTokenKind int

const (
	jfEOF jobFilterTokenKind = iota
	jfIdent
	jfString
	jfOp
	jfStar
)

type jobFilterToken struct {
	kind jobFilterTokenKind
	text string
}

// tokenizeJobFilter splits an expression into identifiers, quoted strings,
// comparison operators and the `*` wildcard. Whitespace separates tokens; an
// unterminated string or an unexpected character is an error.
func tokenizeJobFilter(s string) ([]jobFilterToken, error) {
	var toks []jobFilterToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '*':
			toks = append(toks, jobFilterToken{kind: jfStar, text: "*"})
			i++
		case c == '"':
			j := i + 1
			var b strings.Builder
			closed := false
			for j < len(s) {
				if s[j] == '\\' && j+1 < len(s) {
					b.WriteByte(s[j+1])
					j += 2
					continue
				}
				if s[j] == '"' {
					closed = true
					j++
					break
				}
				b.WriteByte(s[j])
				j++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated quoted string")
			}
			toks = append(toks, jobFilterToken{kind: jfString, text: b.String()})
			i = j
		case c == '=' || c == '!' || c == '<' || c == '>':
			if i+1 < len(s) {
				switch s[i : i+2] {
				case "!=", "<=", ">=":
					toks = append(toks, jobFilterToken{kind: jfOp, text: s[i : i+2]})
					i += 2
					continue
				}
			}
			if c == '!' {
				return nil, fmt.Errorf("unexpected character %q", string(c))
			}
			toks = append(toks, jobFilterToken{kind: jfOp, text: string(c)})
			i++
		case isJobFilterIdentStart(c):
			j := i + 1
			for j < len(s) && isJobFilterIdentPart(s[j]) {
				j++
			}
			toks = append(toks, jobFilterToken{kind: jfIdent, text: s[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q", string(c))
		}
	}
	toks = append(toks, jobFilterToken{kind: jfEOF})
	return toks, nil
}

func isJobFilterIdentStart(c byte) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

func isJobFilterIdentPart(c byte) bool {
	return isJobFilterIdentStart(c) || c == '-' || c == '.'
}

// ─── parser ───────────────────────────────────────────────────────────────────

type jobFilterParser struct {
	toks []jobFilterToken
	i    int
}

func (p *jobFilterParser) peek() jobFilterToken { return p.toks[p.i] }

func (p *jobFilterParser) next() jobFilterToken {
	t := p.toks[p.i]
	if t.kind != jfEOF {
		p.i++
	}
	return t
}

// parse consumes a conjunction of clauses separated by explicit "AND" keywords
// or by whitespace (implicit AND).
func (p *jobFilterParser) parse() (jobFilter, error) {
	var f jobFilter
	for {
		pred, err := p.parseClause()
		if err != nil {
			return jobFilter{}, err
		}
		f.preds = append(f.preds, pred)

		t := p.peek()
		switch {
		case t.kind == jfEOF:
			return f, nil
		case t.kind == jfIdent && strings.EqualFold(t.text, "AND"):
			p.next()
			if p.peek().kind == jfEOF {
				return jobFilter{}, fmt.Errorf("dangling AND")
			}
			// Loop and parse the next clause.
		case t.kind == jfIdent:
			// Implicit AND: the next token starts another field.
		default:
			return jobFilter{}, fmt.Errorf("unexpected token %s", tokenText(t))
		}
	}
}

func (p *jobFilterParser) parseClause() (jobPredicate, error) {
	keyTok := p.next()
	if keyTok.kind != jfIdent {
		return nil, fmt.Errorf("expected field, got %s", tokenText(keyTok))
	}
	key := keyTok.text

	opTok := p.next()
	if opTok.kind != jfOp {
		return nil, fmt.Errorf("expected operator after %q, got %s", key, tokenText(opTok))
	}
	op := opTok.text

	switch {
	case key == "status.state":
		if op != "=" {
			return nil, fmt.Errorf("unsupported operator %q on status.state (only \"=\" is supported)", op)
		}
		valTok := p.next()
		if valTok.kind != jfIdent {
			return nil, fmt.Errorf("status.state value must be ACTIVE or NON_ACTIVE, got %s", tokenText(valTok))
		}
		switch valTok.text {
		case "ACTIVE":
			return jobStatePredicate{active: true}, nil
		case "NON_ACTIVE":
			return jobStatePredicate{active: false}, nil
		default:
			return nil, fmt.Errorf("unsupported status.state %q (only ACTIVE and NON_ACTIVE are supported)", valTok.text)
		}

	case key == "insertTime":
		if !jobInsertTimeOps[op] {
			return nil, fmt.Errorf("unsupported operator %q on insertTime", op)
		}
		valTok := p.next()
		if valTok.kind != jfString {
			return nil, fmt.Errorf("insertTime value must be a double-quoted RFC3339 timestamp, got %s", tokenText(valTok))
		}
		ts, err := time.Parse(time.RFC3339, valTok.text)
		if err != nil {
			return nil, fmt.Errorf("invalid insertTime timestamp %q", valTok.text)
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
		valTok := p.next()
		if valTok.kind == jfStar {
			return jobLabelExistsPredicate{key: lk}, nil
		}
		val, ok := tokenValue(valTok)
		if !ok {
			return nil, fmt.Errorf("label value must be a value or \"*\", got %s", tokenText(valTok))
		}
		return jobLabelEqualsPredicate{key: lk, value: val}, nil

	default:
		return nil, fmt.Errorf("unsupported filter key %q (only status.state, labels.<key> and insertTime are supported)", key)
	}
}

// tokenValue returns an identifier or quoted-string token's text.
func tokenValue(t jobFilterToken) (string, bool) {
	switch t.kind {
	case jfIdent, jfString:
		return t.text, true
	}
	return "", false
}

// tokenText renders a token for an error message.
func tokenText(t jobFilterToken) string {
	if t.kind == jfEOF {
		return "end of filter"
	}
	return fmt.Sprintf("%q", t.text)
}
