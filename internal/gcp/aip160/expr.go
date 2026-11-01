package aip160

import (
	"fmt"
	"strings"
)

// Expr is a parsed AIP-160 boolean expression over surface-specific clauses.
// Leaf holds the clause value returned by the caller's clause function; the
// logical nodes are AND/OR/NOT. A nil *Expr matches everything.
type Expr struct {
	Op    string // "AND", "OR", "NOT", or "" for a leaf
	Leaf  any
	Left  *Expr
	Right *Expr
}

// Match evaluates the expression, using leaf to interpret each surface
// predicate. Surface predicates are typically function values boxed in any.
func (e *Expr) Match(leaf func(any) bool) bool {
	if e == nil {
		return true
	}
	switch e.Op {
	case "AND":
		return e.Left.Match(leaf) && e.Right.Match(leaf)
	case "OR":
		return e.Left.Match(leaf) || e.Right.Match(leaf)
	case "NOT":
		return !e.Left.Match(leaf)
	default:
		return leaf(e.Leaf)
	}
}

// ParseExpression parses a full AIP-160 expression:
//
//	expr    := and
//	and     := or ( ("AND" | implicit) or )*
//	or      := unary ( "OR" unary )*
//	unary   := ("NOT" | "-") unary | primary
//	primary := "(" expr ")" | clause
//
// Per AIP-160, OR binds tighter than AND. Whitespace between clauses is an
// implicit AND. The clause callback consumes one comparison and returns that
// surface's predicate as an any. Anything left after the expression (or a
// malformed group) is an error; an empty stream is rejected by clause.
func (p *Parser) ParseExpression(clause func(*Parser) (any, error)) (*Expr, error) {
	e, err := p.parseAndExpr(clause)
	if err != nil {
		return nil, err
	}
	if t := p.Peek(); t.Kind != KindEOF {
		return nil, fmt.Errorf("unexpected token %s", TokenText(t))
	}
	return e, nil
}

func (p *Parser) parseAndExpr(clause func(*Parser) (any, error)) (*Expr, error) {
	left, err := p.parseOrExpr(clause)
	if err != nil {
		return nil, err
	}
	for {
		t := p.Peek()
		switch {
		case t.Kind == KindIdent && strings.EqualFold(t.Text, "AND"):
			p.Next()
			right, err := p.parseOrExpr(clause)
			if err != nil {
				return nil, err
			}
			left = &Expr{Op: "AND", Left: left, Right: right}
		case t.Kind == KindEOF, t.Kind == KindOp && t.Text == ")":
			return left, nil
		case t.Kind == KindIdent && strings.EqualFold(t.Text, "OR"):
			// parseOrExpr consumes OR; reaching here means it was dangling.
			return nil, fmt.Errorf("unexpected token %s", TokenText(t))
		default:
			// Whitespace-separated clauses are an implicit AND.
			right, err := p.parseOrExpr(clause)
			if err != nil {
				return nil, err
			}
			left = &Expr{Op: "AND", Left: left, Right: right}
		}
	}
}

func (p *Parser) parseOrExpr(clause func(*Parser) (any, error)) (*Expr, error) {
	left, err := p.parseUnaryExpr(clause)
	if err != nil {
		return nil, err
	}
	for {
		t := p.Peek()
		if t.Kind != KindIdent || !strings.EqualFold(t.Text, "OR") {
			return left, nil
		}
		p.Next()
		right, err := p.parseUnaryExpr(clause)
		if err != nil {
			return nil, err
		}
		left = &Expr{Op: "OR", Left: left, Right: right}
	}
}

func (p *Parser) parseUnaryExpr(clause func(*Parser) (any, error)) (*Expr, error) {
	t := p.Peek()
	if (t.Kind == KindIdent && strings.EqualFold(t.Text, "NOT")) || (t.Kind == KindOp && t.Text == "-") {
		p.Next()
		inner, err := p.parseUnaryExpr(clause)
		if err != nil {
			return nil, err
		}
		return &Expr{Op: "NOT", Left: inner}, nil
	}
	return p.parsePrimaryExpr(clause)
}

func (p *Parser) parsePrimaryExpr(clause func(*Parser) (any, error)) (*Expr, error) {
	if t := p.Peek(); t.Kind == KindOp && t.Text == "(" {
		p.Next()
		inner, err := p.parseAndExpr(clause)
		if err != nil {
			return nil, err
		}
		if t := p.Next(); t.Kind != KindOp || t.Text != ")" {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		return inner, nil
	}
	leaf, err := clause(p)
	if err != nil {
		return nil, err
	}
	return &Expr{Leaf: leaf}, nil
}
