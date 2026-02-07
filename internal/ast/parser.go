package ast

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

// Parser is a recursive descent SQL parser.
type Parser struct {
	lex       *Lexer
	scanned   []*Token
	unscanned []*Token
}

// NewParser returns a new Parser that reads from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{lex: NewLexer(r)}
}

// Parse parses the input into a list of statements.
func (p *Parser) Parse() ([]Statement, error) {
	var stmts []Statement

	for {
		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}

		switch t.Type {
		case SEMICOLON:
			continue
		case EOF:
			return stmts, nil
		case SELECT:
			p.unscan()
			sel, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, Statement{Select: sel})
		default:
			return nil, fmt.Errorf("unexpected token %q at line %d position %d", t.String(), t.Line, t.Pos)
		}
	}
}

func (p *Parser) scan() (*Token, error) {
	if len(p.unscanned) > 0 {
		t := p.unscanned[len(p.unscanned)-1]
		p.unscanned = p.unscanned[:len(p.unscanned)-1]
		p.scanned = append(p.scanned, t)
		return t, nil
	}
	t, err := p.lex.Scan()
	if err != nil {
		return nil, err
	}
	p.scanned = append(p.scanned, t)
	return t, nil
}

func (p *Parser) scanSkipWS() (*Token, error) {
	for {
		t, err := p.scan()
		if err != nil {
			return nil, err
		}
		if t.Type != WS && t.Type != COMMENT {
			return t, nil
		}
	}
}

func (p *Parser) unscan() {
	if len(p.scanned) == 0 {
		return
	}
	t := p.scanned[len(p.scanned)-1]
	p.scanned = p.scanned[:len(p.scanned)-1]
	p.unscanned = append(p.unscanned, t)
}

// peek returns the next non-WS token without consuming it.
func (p *Parser) peek() (*Token, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return &Token{Type: ILLEGAL}, err
	}
	// Put the token back into unscanned directly
	p.unscanned = append(p.unscanned, t)
	// Remove it from scanned (it's the last element since scanSkipWS just added it)
	if len(p.scanned) > 0 {
		p.scanned = p.scanned[:len(p.scanned)-1]
	}
	return t, nil
}

func (p *Parser) expect(typ TokenType) (*Token, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	if t.Type != typ {
		return nil, fmt.Errorf("expected %s but got %q at line %d position %d", typ, t.String(), t.Line, t.Pos)
	}
	return t, nil
}

func (p *Parser) parseSelect() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	if _, err := p.expect(SELECT); err != nil {
		return nil, err
	}

	// DISTINCT
	if t, _ := p.peek(); t.Type == DISTINCT {
		p.scanSkipWS()
		stmt.Distinct = true
	}

	// columns
	cols, err := p.parseColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM
	if t, _ := p.peek(); t.Type != FROM {
		return stmt, nil
	}
	p.scanSkipWS() // consume FROM

	from, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt.From = &FromClause{Table: *from}

	// JOINs
	for {
		t, err := p.peek()
		if err != nil {
			return nil, err
		}

		var joinType JoinType
		switch t.Type {
		case JOIN:
			p.scanSkipWS()
			joinType = InnerJoin
		case LEFT:
			p.scanSkipWS()
			joinType = LeftJoin
			if _, err := p.expect(JOIN); err != nil {
				return nil, err
			}
		case RIGHT:
			p.scanSkipWS()
			joinType = RightJoin
			if _, err := p.expect(JOIN); err != nil {
				return nil, err
			}
		default:
			goto afterJoins
		}

		jt, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(ON); err != nil {
			return nil, err
		}
		cond, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Joins = append(stmt.Joins, JoinClause{
			Type:      joinType,
			Table:     *jt,
			Condition: cond,
		})
	}
afterJoins:

	// WHERE
	if t, _ := p.peek(); t.Type == WHERE {
		p.scanSkipWS()
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// GROUP BY
	if t, _ := p.peek(); t.Type == GROUP {
		p.scanSkipWS()
		if _, err := p.expect(BY); err != nil {
			return nil, err
		}
		exprs, err := p.parseExpressionList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = exprs
	}

	// ORDER BY
	if t, _ := p.peek(); t.Type == ORDER {
		p.scanSkipWS()
		if _, err := p.expect(BY); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// OVER duration
	if t, _ := p.peek(); t.Type == OVER {
		p.scanSkipWS()
		durTok, err := p.expect(DURATION)
		if err != nil {
			return nil, err
		}
		d, err := time.ParseDuration(durTok.String())
		if err != nil {
			return nil, fmt.Errorf("invalid duration %q at line %d position %d", durTok.String(), durTok.Line, durTok.Pos)
		}
		stmt.Over = d
	}

	// EVERY duration
	if t, _ := p.peek(); t.Type == EVERY {
		p.scanSkipWS()
		durTok, err := p.expect(DURATION)
		if err != nil {
			return nil, err
		}
		d, err := time.ParseDuration(durTok.String())
		if err != nil {
			return nil, fmt.Errorf("invalid duration %q at line %d position %d", durTok.String(), durTok.Line, durTok.Pos)
		}
		stmt.Every = d
	}

	// LIMIT n
	if t, _ := p.peek(); t.Type == LIMIT {
		p.scanSkipWS()
		numTok, err := p.expect(NUMERIC)
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(numTok.String())
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value %q at line %d position %d", numTok.String(), numTok.Line, numTok.Pos)
		}
		stmt.Limit = &n
	}

	return stmt, nil
}

func (p *Parser) parseColumns() ([]Column, error) {
	var cols []Column

	for {
		col, err := p.parseColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, *col)

		if t, _ := p.peek(); t.Type != COMMA {
			return cols, nil
		}
		p.scanSkipWS() // consume comma
	}
}

func (p *Parser) parseColumn() (*Column, error) {
	// Use peek to avoid scan/unscan WS issues
	t, err := p.peek()
	if err != nil {
		return nil, err
	}

	// Check for bare *
	if t.Type == STAR {
		p.scanSkipWS() // consume the star
		return &Column{Star: true}, nil
	}

	// Parse as expression (handles ident, ident.col, functions, etc.)
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	// Check if the expression is "table.*" via the ColumnRef with Column="*"
	if ref, ok := expr.(*ColumnRef); ok && ref.Column == "*" {
		return &Column{Star: true, TableRef: ref.Table}, nil
	}

	col := &Column{Expr: expr}

	// Optional AS alias
	t, err = p.peek()
	if err != nil {
		return nil, err
	}
	if t.Type == AS {
		p.scanSkipWS() // consume AS
		alias, err := p.expect(IDENT)
		if err != nil {
			return nil, err
		}
		col.Alias = alias.String()
	} else if t.Type == IDENT {
		// implicit alias (no AS keyword) — but only if it's not a keyword
		p.scanSkipWS()
		col.Alias = t.String()
	}

	return col, nil
}

func (p *Parser) parseTableRef() (*TableRef, error) {
	name, err := p.expect(IDENT)
	if err != nil {
		return nil, err
	}

	ref := &TableRef{Name: name.String()}

	// Optional alias
	t, err := p.peek()
	if err != nil {
		return nil, err
	}
	if t.Type == AS {
		p.scanSkipWS() // consume AS
		alias, err := p.expect(IDENT)
		if err != nil {
			return nil, err
		}
		ref.Alias = alias.String()
	} else if t.Type == IDENT {
		p.scanSkipWS() // consume the alias
		ref.Alias = t.String()
	}
	// Otherwise, no alias — token stays for the next clause

	return ref, nil
}

func (p *Parser) parseOrderBy() ([]OrderByExpr, error) {
	var orders []OrderByExpr

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		order := OrderByExpr{Expr: expr}
		if t, _ := p.peek(); t.Type == ASC {
			p.scanSkipWS()
		} else if t.Type == DESC {
			p.scanSkipWS()
			order.Desc = true
		}
		orders = append(orders, order)

		if t, _ := p.peek(); t.Type != COMMA {
			return orders, nil
		}
		p.scanSkipWS() // consume comma
	}
}

func (p *Parser) parseExpressionList() ([]Expression, error) {
	var exprs []Expression

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)

		if t, _ := p.peek(); t.Type != COMMA {
			return exprs, nil
		}
		p.scanSkipWS() // consume comma
	}
}
