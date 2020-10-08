package csql

import (
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/pkg/errors"
)

func line() string {
	_, file, l, _ := runtime.Caller(1)
	file = filepath.Base(file)
	return fmt.Sprintf("%s:%d", file, l)
}

type Parser struct {
	lex       *Lexer
	pos       int
	scanned   []*Token
	unscanned []*Token
}

func NewParser(r io.Reader) *Parser {
	return &Parser{lex: NewLexer(r)}
}

func (p *Parser) Parse() ([]Statement, error) {
	var stmts []Statement

	for {
		var stmt Statement
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
			s, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			stmt.Select = s
		default:
			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
		}

		stmts = append(stmts, stmt)
	}
}

func (p *Parser) scan() (*Token, error) {
	var t *Token
	if len(p.unscanned) > 0 {
		t = p.unscanned[len(p.unscanned)-1]
		p.unscanned = p.unscanned[:len(p.unscanned)-1]
	} else {
		tok, err := p.lex.Scan()
		if err != nil {
			return nil, err
		}
		t = tok
	}
	p.scanned = append(p.scanned, t)
	p.pos += len(t.Raw)
	return t, nil
}

func (p *Parser) scanSkipWS() (*Token, error) {
	for {
		t, err := p.scan()
		if err != nil {
			return nil, err
		}
		if t.Type != WS {
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
	p.pos -= len(t.Raw)
}

func (p *Parser) unscanSkipWS() {
	for {
		p.unscan()
		if len(p.scanned) == 0 {
			return
		}
		if p.scanned[len(p.scanned)-1].Type != WS {
			return
		}
	}
}

func (p *Parser) parseSelect() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}

	// SELECT
	if t.Type != SELECT {
		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}

	if !p.hasMore() {
		return stmt, nil
	}

	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}

	// DISTINCT
	if t.Type == DISTINCT {
		stmt.Distinct = true
	} else {
		p.unscan()
		if !p.hasMore() {
			return stmt, nil
		}
	}

	// column, column, ...
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Cols = cols

	if !p.hasMore() {
		return stmt, nil
	}

	// FROM table [, table...]
	from, err := p.parseFrom()
	if err != nil {
		return nil, err
	}
	stmt.From = from

	// TODO: FROM, WHERE, GROUP BY, LIMIT, WHEN
	// from, err := p.parseFrom()
	// if err != nil {
	// 	return nil, err
	// }
	// stmt.From = from

	for {
		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}
		switch t.Type {
		case EOF, SEMICOLON:
			return stmt, nil
		default:
			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
		}
	}
}

func (p *Parser) hasMore() bool {
	t, err := p.scanSkipWS()
	if err != nil {
		panic(err)
	}
	p.unscan()
	return t.Type != EOF && t.Type != SEMICOLON
}

func (p *Parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn

	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		cols = append(cols, *col)

		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}

		if t.Type != COMMA {
			p.unscan()
			return cols, nil
		}
	}
}

func (p *Parser) parseSelectColumn() (*SelectColumn, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}

	// * | expression
	var col *SelectColumn
	switch t.Type {
	case ASTERISK:
		return &SelectColumn{
			Star: true,
		}, nil
	default:
		p.unscan()
		col, err = p.parseSelectExpressionColumn()
		if err != nil {
			return nil, err
		}
	}

	// AS
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	if t.Type != AS {
		p.unscan()
		return col, nil
	}

	// IDENT
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	if t.Type != IDENT {
		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}
	col.As = unquote(t.Raw)

	return col, nil
}

func (p *Parser) parseSelectStarColumn() (*SelectColumn, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err

	}

	if t.Type != ASTERISK {
		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}

	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	p.unscan()

	if t.Type != COMMA && t.Type != FROM {
		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}

	return &SelectColumn{
		Star: true,
	}, nil
}

func (p *Parser) parseSelectExpressionColumn() (*SelectColumn, error) {
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	return &SelectColumn{
		Expr: expr,
	}, nil
}

func (p *Parser) parseExpression() (Expression, error) {
	ep := ExpressionParser{}
	if err := ep.Consume(p); err != nil {
		return nil, err
	}
	return ep.Parse()

	// infix, err := p.scanInfixTerms()
	// if err != nil {
	// 	return nil, err
	// }

	// sh := &shuntingYard{}

	// if err := sh.PushInfix(infix); err != nil {
	// 	return nil, err
	// }

	// expr, err := sh.ParseExpression()
	// if err != nil {
	// 	return nil, err
	// }

	// op, err := p.scanComparisonOp()
	// if err != nil {
	// 	return nil, err
	// }
	// if op == nil {
	// 	return expr, nil
	// }

	// rhs, err := p.parseExpression()
	// if err != nil {
	// 	return nil, err
	// }

	// expr = &ComparisonExpression{
	// 	// Op: ,
	// 	Left:  expr,
	// 	Right: rhs,
	// }

	// return expr, nil
}

// func (p *Parser) scanExpression() ([]*Token, error) {
// 	var tokens []*Token
// 	for {
// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}
// 		switch t.Type {
// 		case NUMERIC, STRING, IDENT, COUNT, SUM, AVG, MIN, MAX, PLUS, MINUS, ASTERISK, SLASH, PERCENT, LPAREN, RPAREN:
// 			tokens = append(tokens, t)
// 		default:
// 			return tokens, nil
// 		}
// 	}
// }

// func (p *Parser) scanInfixExpression() ([]*Token, error) {
// 	var tokens []*Token
// 	var (
// 		compare bool
// 	)
// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return nil, err
// 	}

// 	tokens = append(tokens, t)

// 	switch t.Type {
// 	case NUMERIC:
// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}
// 		tokens = append(tokens, t)
// 		switch t.Type {
// 		case PLUS, MINUS, ASTERISK, SLASH, PERCENT:
// 			next, err := p.scanInfixExpression()
// 			if err != nil {
// 				return nil, err
// 			}
// 			tokens = append(tokens, next...)
// 		}
// 	case STRING:
// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}
// 		tokens = append(tokens, t)
// 		switch t.Type {
// 		case PLUS:
// 			next, err := p.scanInfixExpression()
// 			if err != nil {
// 				return nil, err
// 			}
// 			tokens = append(tokens, next...)
// 		}
// 	case IDENT:
// 		panic("todo: IDENT[.IDENT]")
// 	case COUNT:
// 		panic("todo: COUNT([ALL | DISTINCT] expression)")
// 	case SUM, AVG, MIN, MAX:
// 		lparen, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}
// 		if lparen.Type != LPAREN {
// 			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 		}
// 		p.unscan()
// 		next, err := p.scanInfixExpression()
// 		if err != nil {
// 			return nil, err
// 		}
// 		tokens = append(tokens, next...)
// 	case LPAREN:
// 		inner, err := p.scanInfixExpression()
// 		if err != nil {
// 			return nil, err
// 		}
// 		rparen, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}
// 		if rparen.Type != RPAREN {
// 			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 		}
// 		tokens = append(tokens, inner...)
// 		tokens = append(tokens, rparen)
// 	}

// 	return tokens, nil
// }

// func (p *Parser) scanInfixTerms() ([]exprTerm, error) {

// 	// operands and operators
// 	var expr []exprTerm
// 	var prev TokenType = ILLEGAL

// 	for {
// 		// last Token in the last exprTerm
// 		if len(expr) > 0 {
// 			prev = expr[len(expr)-1].typ()
// 		}

// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}

// 		term := exprTerm{t}

// 		switch t.Type {
// 		case PLUS, MINUS:
// 			if !tokenIn(prev, ILLEGAL, PLUS, MINUS, NUMERIC, STRING, IDENT, LPAREN, RPAREN) {
// 				return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 			}
// 		case ASTERISK, SLASH:
// 			if !tokenIn(prev, NUMERIC, STRING, IDENT, RPAREN) {
// 				return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 			}
// 		case NUMERIC, STRING, COUNT, SUM, MIN, MAX, AVG:
// 			if !tokenIn(prev, ILLEGAL, PLUS, MINUS, ASTERISK, SLASH, LPAREN) {
// 				return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 			}
// 		case IDENT:
// 			if !tokenIn(prev, ILLEGAL, PLUS, MINUS, ASTERISK, SLASH, LPAREN) {
// 				return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 			}
// 			dot, err := p.scan()
// 			if err != nil {
// 				return nil, err
// 			}
// 			if dot.Type != DOT {
// 				p.unscan()
// 				break
// 			}
// 			ident, err := p.scan()
// 			if err != nil {
// 				return nil, err
// 			}
// 			if ident.Type != IDENT {
// 				return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 			}
// 			term = append(term, dot, ident)
// 		case LPAREN:
// 			if !tokenIn(prev, ILLEGAL, PLUS, MINUS, ASTERISK, SLASH, COUNT, SUM, AVG, MIN, MAX, LPAREN) {
// 				return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 			}

// 			expr = append(expr, term)

// 			// recursively descend into the parenthetical
// 			inner, err := p.scanInfixTerms()
// 			if err != nil {
// 				return nil, err
// 			}
// 			expr = append(expr, inner...)

// 			rparen, err := p.scan()
// 			if err != nil {
// 				return nil, err
// 			}
// 			if rparen.Type != RPAREN {
// 				return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 			}

// 			term = exprTerm{rparen}
// 		default:
// 			p.unscan()
// 			return expr, nil
// 		}

// 		expr = append(expr, term)
// 	}
// }

func (p *Parser) scanComparisonOp() (*Token, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	switch t.Type {
	// case
	}
	panic("todo")
}

func (p *Parser) parseFrom() (*FromClause, error) {
	var from FromClause

	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	if t.Type != FROM {
		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}

	tables, err := p.parseTablesExpression()
	if err != nil {
		return nil, err
	}
	from.Tables = *tables

	return &from, nil
}

func (p *Parser) parseTablesExpression() (*TablesExpression, error) {
	tables := &TablesExpression{}

	// IDENT | LPAREN
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	switch t.Type {
	case LPAREN:
		inner, err := p.parseTablesExpression()
		if err != nil {
			return nil, err
		}
		t, err = p.scanSkipWS()
		if err != nil {
			return nil, err
		}
		if t.Type != RPAREN {
			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
		}
		tables.Expr = inner
	case IDENT:
		table := unquote(t.Raw)
		tables.Table = &table
	default:
		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}

	// AS
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	switch t.Type {
	case AS:
		alias, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}
		if alias.Type != IDENT && alias.Type != STRING {
			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
		}
		tables.As = unquote(alias.Raw)
	case IDENT, STRING:
		tables.As = unquote(t.Raw)
	default:
		p.unscan()
	}

	// JOIN expression
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}

	switch t.Type {
	case LPAREN:
		p.unscan()
		return p.parseTablesExpression()
	case COMMA:
		join, err := p.parseTablesExpression()
		if err != nil {
			return nil, err
		}
		tables.CrossJoin = join
	case CROSS_JOIN:
		join, err := p.parseTablesExpression()
		if err != nil {
			return nil, err
		}
		tables.CrossJoin = join
	default:
		p.unscan()
	}
	// TODO: Other join cases

	// AS
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	switch t.Type {
	case AS:
		alias, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}
		if alias.Type != IDENT && alias.Type != STRING {
			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
		}
		tables.As = unquote(alias.Raw)
	case IDENT, STRING:
		tables.As = unquote(t.Raw)
	default:
		p.unscan()
	}

	// ON
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	switch t.Type {
	case ON:
		on, err := p.parseJoinOnPredicate()
		if err != nil {
			return nil, err
		}
		// tables.On
		panic(on)
	default:
		p.unscan()
	}

	return tables, nil
}

func (p *Parser) parseJoinOnPredicate() (*JoinOnPredicate, error) {
	left, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	panic(left)
}

func (p *Parser) scanSkipWSAssertNext(expected TokenType, varargs ...TokenType) error {
	for _, expectedType := range append([]TokenType{expected}, varargs...) {
		t, err := p.scanSkipWS()
		if err != nil {
			return err
		}
		if t.Type != expectedType {
			return errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
		}
	}
	return nil

}

func tokenIn(tok TokenType, in ...TokenType) bool {
	for _, t := range in {
		if tok == t {
			return true
		}
	}
	return false
}

func unquote(orig []byte) string {
	if len(orig) == 0 {
		return ""
	}

	quote := orig[0]
	switch quote {
	case '"':
		uq, err := strconv.Unquote(string(orig))
		if err != nil {
			panic(errors.Errorf("%q: %+v", orig, err))
		}
		return uq
	case '\'':
		// convert single quotes to double quotes and try again
		dq := []byte{'"'}
		for i := 1; i < len(orig); i++ {
			ch := orig[i]
			switch ch {
			case '\\':
				escape := orig[i+1]
				if '\'' == escape {
					dq = append(dq, '\'')
					i++
					continue
				}
				dq = append(dq, ch)
			case '\'':
				dq = append(dq, '"')
			default:
				dq = append(dq, ch)
			}
		}
		return unquote(dq)
	default:
		return string(orig)
	}
}
