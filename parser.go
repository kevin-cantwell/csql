package csql

import (
	"fmt"
	"io"
	"strconv"
)

type token struct {
	tok Token
	raw []byte
}

type Parser struct {
	lex       *Lexer
	pos       int
	scanned   []*token
	unscanned []*token
}

func NewParser(r io.Reader) *Parser {
	return &Parser{lex: NewLexer(r)}
}

func (p *Parser) Parse() (*Statement, error) {
	stmt := &Statement{}

	t, err := p.scan()
	if err != nil {
		return nil, err
	}

	switch t.tok {
	case SELECT:
		p.unscan()
		s, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = s
	default:
		return nil, fmt.Errorf("unsupported statement at position %d: %q", p.pos, t.raw)
	}

	return stmt, nil
}

func (p *Parser) scan() (*token, error) {
	var t *token
	if len(p.unscanned) > 0 {
		t = p.unscanned[len(p.unscanned)-1]
		p.unscanned = p.unscanned[:len(p.unscanned)-1]
	} else {
		tok, raw, err := p.lex.Scan()
		if err != nil {
			return nil, err
		}
		t = &token{tok: tok, raw: raw}
	}
	p.scanned = append(p.scanned, t)
	p.pos += len(t.raw)
	return t, nil
}

func (p *Parser) scanSkipWS() (*token, error) {
	for {
		t, err := p.scan()
		if err != nil {
			return nil, err
		}
		if t.tok != WS {
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
	p.pos -= len(t.raw)
}

func (p *Parser) unscanSkipWS() {
	for {
		p.unscan()
		if len(p.scanned) == 0 {
			return
		}
		if p.scanned[len(p.scanned)-1].tok != WS {
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

	if t.tok != SELECT {
		return nil, fmt.Errorf("expected SELECT at position %d: %q", p.pos, t.raw)
	}

	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}

	if t.tok == DISTINCT {
		stmt.Distinct = true
	} else {
		p.unscan()
	}

	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// TODO: FROM, WHERE, GROUP BY, LIMIT, WHEN

	return stmt, nil
}

func (p *Parser) parseSelectColumns() ([]SelectColumn, error) {
	var cols []SelectColumn

	for {
		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}

		var col *SelectColumn
		switch t.tok {
		case ASTERISK:
			p.unscan()
			col, err = p.parseSelectStarColumn()
			if err != nil {
				return nil, err
			}
		case AS:
			if col == nil {
				p.unscan()
				return nil, fmt.Errorf("missing column expression at position %d: %q", p.pos, t.raw)
			}
			t, err := p.scanSkipWS()
			if err != nil {
				return nil, err
			}
			if t.tok != STRING && t.tok != IDENT {
				return nil, fmt.Errorf("missing alias name at position %d: %q", p.pos, t.raw)
			}
			col.Alias = unquote(string(t.raw))
		case COMMA:
			if col == nil {
				return nil, fmt.Errorf("missing column expression at position %d: %q", p.pos, t.raw)
			}
			cols = append(cols, *col)
		case FROM:
			p.unscan()
			cols = append(cols, *col)
			return cols, nil
		default:
			p.unscan()
			col, err = p.parseSelectExpressionColumn()
			if err != nil {
				return nil, err
			}
		}
	}
}

func (p *Parser) parseSelectStarColumn() (*SelectColumn, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err

	}

	if t.tok != ASTERISK {
		return nil, fmt.Errorf("missing \"*\" at position %d: %q", p.pos, t.raw)
	}

	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	p.unscan()

	if t.tok != COMMA && t.tok != FROM {
		return nil, fmt.Errorf("expected \",\" or \"FROM\" at position %d: %q", p.pos, t.raw)
	}

	return &SelectColumn{
		Star: true,
	}, nil
}

func (p *Parser) parseSelectExpressionColumn() (*SelectColumn, error) {
	terms, err := p.scanInfixExpression()
	if err != nil {
		return nil, err
	}

	sh := &shuntingYard{}

	for _, term := range terms {
		sh.Push(term)
	}

	expr, err := sh.ParseExpression()
	if err != nil {
		return nil, err
	}

	return &SelectColumn{
		Expr: expr,
	}, nil
}

// term represents an expression term: a operand, operator, function, or parenthesis
type term []*token

func (t term) isOperand() bool {
	return tokenIn(t.tok(), NUMERIC, STRING, IDENT)
}

func (t term) isOperator() bool {
	return tokenIn(t.tok(), PLUS, MINUS, ASTERISK, SLASH, PERCENT)
}

func (t term) isFunction() bool {
	return tokenIn(t.tok(), COUNT, SUM, AVG, MIN, MAX)
}

func (t term) isLeftParen() bool {
	return tokenIn(t.tok(), LPAREN)
}

func (t term) isRightParen() bool {
	return tokenIn(t.tok(), RPAREN)
}

func (t term) tok() Token {
	if len(t) == 0 {
		return NONE
	}
	return t.tok()
}

func (p *Parser) scanInfixExpression() ([]term, error) {
	// operands and operators
	var expr []term
	var prev Token = NONE

	for {
		// last Token in the last term
		if len(expr) > 0 {
			prev = expr[len(expr)-1].tok()
		}

		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}

		term := term{t}

		switch t.tok {
		case PLUS, MINUS:
			if !tokenIn(prev, NONE, PLUS, MINUS, NUMERIC, STRING, IDENT, LPAREN, RPAREN) {
				p.unscan()
				return nil, fmt.Errorf("bad syntax at position %d: %q", p.pos, t.raw)
			}
		case ASTERISK, SLASH:
			if !tokenIn(prev, NUMERIC, STRING, IDENT, RPAREN) {
				p.unscan()
				return nil, fmt.Errorf("bad syntax at position %d: %q", p.pos, t.raw)
			}
		case NUMERIC, STRING, COUNT, SUM, MIN, MAX, AVG:
			if !tokenIn(prev, NONE, PLUS, MINUS, ASTERISK, SLASH, LPAREN) {
				p.unscan()
				return nil, fmt.Errorf("bad syntax at position %d: %q", p.pos, t.raw)
			}
		case IDENT:
			if !tokenIn(prev, NONE, PLUS, MINUS, ASTERISK, SLASH, LPAREN) {
				p.unscan()
				return nil, fmt.Errorf("bad syntax at position %d: %q", p.pos, t.raw)
			}
			dot, err := p.scan()
			if err != nil {
				return nil, err
			}
			if dot.tok != DOT {
				p.unscan()
				break
			}
			ident, err := p.scan()
			if err != nil {
				return nil, err
			}
			if ident.tok != IDENT {
				p.unscan()
				return nil, fmt.Errorf("bad expression syntax at position %d: %q", p.pos, ident.raw)
			}
			term = append(term, dot, ident)
		case LPAREN:
			if !tokenIn(prev, NONE, PLUS, MINUS, ASTERISK, SLASH, COUNT, SUM, AVG, MIN, MAX, LPAREN) {
				p.unscan()
				return nil, fmt.Errorf("bad syntax at position %d: %q", p.pos, t.raw)
			}
			inner, err := p.scanInfixExpression()
			if err != nil {
				return nil, err
			}
			rparen, err := p.scan()
			if err != nil {
				return nil, err
			}
			if rparen.tok != RPAREN {
				p.unscan()
				return nil, fmt.Errorf("bad expression syntax at position %d: %q", p.pos, rparen.raw)
			}
			for _, term := range inner {
				term = append(term, term...)
			}
			term = append(term, rparen)
		default:
			p.unscan()
			return expr, nil
		}

		expr = append(expr, term)
	}
}

func tokenIn(tok Token, in ...Token) bool {
	for _, t := range in {
		if tok == t {
			return true
		}
	}
	return false
}

// shuntingYard processes an infix expression and
type shuntingYard struct {
	output []term
	stack  []term
}

// while there are terms to be read:
//     read a term.
func (s *shuntingYard) Push(term term) {
	switch {
	// if the term is a operand, then:
	//     push it to the output queue.
	case term.isOperand():
		s.pushOutput(term)
	// else if the term is a function then:
	//     push it onto the operator stack
	case term.isFunction():
		s.pushStack(term)
	// else if the term is a left parenthesis (i.e. "("), then:
	//     push it onto the operator stack.
	case term.isLeftParen():
		s.pushStack(term)
	// else if the term is a right parenthesis (i.e. ")"), then:
	//     while the operator at the top of the operator stack is not a left parenthesis:
	//         pop the operator from the operator stack onto the output queue.
	//     if there is a left parenthesis at the top of the operator stack, then:
	//         pop the operator from the operator stack and discard it
	case term.isRightParen():
		top := s.popStack()
		for ; top != nil && !top.isLeftParen(); top = s.popStack() {
			s.pushOutput(top)
		}
		/* If the stack runs out without finding a left parenthesis, then there are mismatched parentheses. */
		if top == nil {
			panic("mismatched parenthesis")
		}
		if !top.isLeftParen() {
			s.pushStack(top)
		}
	// else if the term is an operator then:
	//     while ((there is an operator at the top of the operator stack)
	//             and ((the operator at the top of the operator stack has greater precedence)
	// 	               or (the operator at the top of the operator stack has equal precedence and the term is left associative))
	//             and (the operator at the top of the operator stack is not a left parenthesis)):
	//         pop operators from the operator stack onto the output queue.
	//     push it onto the operator stack.
	case term.isOperator():
		for top := s.popStack(); top != nil && s.opPrecedes(top, term) && !top.isLeftParen(); top = s.popStack() {
			s.pushOutput(top)
		}
		s.pushStack(term)
	}
}

func (s *shuntingYard) ParseExpression() (Expression, error) {
	var stack []Expression

	for _, term := range s.output {
		var expr Expression
		switch t := term.tok(); t {
		case STRING:
			token := term[0]
			str := unquote(string(token.raw))
			expr = &OperandExpression{
				String: &str,
			}
		case NUMERIC:
			token := term[0]
			f, err := strconv.ParseFloat(string(token.raw), 64)
			if err != nil {
				return nil, err
			}
			expr = &OperandExpression{
				Numeric: &f,
			}
		case IDENT:
			var ident Ident
			if len(term) == 3 {
				ident.Table = unquote(string(term[0].raw))
				ident.Field = unquote(string(term[2].raw))
			} else {
				ident.Field = unquote(string(term[0].raw))
			}
			expr = &OperandExpression{
				Ident: &ident,
			}
		case PLUS, MINUS, ASTERISK, SLASH, PERCENT:
			right := stack[len(stack)-1]
			left := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			expr = &OperatorExpression{
				Op:    t,
				Left:  left,
				Right: right,
			}
		case COUNT, SUM, MIN, MAX, AVG:
			arg := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			expr = &FunctionExpression{
				Func: t,
				Args: []Expression{arg},
			}
		default:
			return nil, fmt.Errorf("invalid expression syntax")
		}

		stack = append(stack, expr)
	}

	if len(stack) != 1 {
		panic("reverse polish notation mismatch")
	}

	return stack[0], nil
}

func (s *shuntingYard) pushOutput(term term) {
	s.output = append(s.output, term)
}

func (s *shuntingYard) pushStack(term term) {
	s.stack = append(s.stack, term)
}

func (s *shuntingYard) opPrecedes(a, b term) bool {
	if !a.isOperator() || !b.isOperator() {
		panic("non-operator precedence check")
	}
	atok := a[len(a)-1].tok
	btok := b[len(b)-1].tok
	if tokenIn(atok, PLUS, MINUS) {
		return false
	}
	if tokenIn(btok, PLUS, MINUS) {
		return true
	}
	return false
}

func (s *shuntingYard) popStack() term {
	if len(s.stack) == 0 {
		return nil
	}
	pop := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	return pop
}

func (p *Parser) parseSelectExpression() (Expression, error) {
	panic("TODO")

}

func (p *Parser) selectNoOpExpression() (Expression, error) {
	panic("TODO")

}

func unquote(orig string) string {
	if len(orig) == 0 {
		return orig
	}

	quoteChar := orig[0]
	switch quoteChar {
	case '`':
		return string(orig[1 : len(orig)-1])
	case '"':
		u, err := strconv.Unquote(orig)
		if err != nil {
			return orig
		}
		return u
	case '\'':
		panic("TODO")
	default:
		return orig
	}
}

var escapeChars = []byte{
	'\\', '\'', '\t', '\n', '\a', '\b', '\f',
}

// \x followed by exactly two hexadecimal digits,
// \ followed by exactly three octal digits,
// \u followed by exactly four hexadecimal digits,
// \U followed by exactly eight hexadecimal digits,

// \a	Alert or bell
// \b	Backspace
// \\	Backslash
// \t	Horizontal tab
// \n	Line feed or newline
// \f	Form feed
// \r	Carriage return
// \v	Vertical tab
// \'	Single quote (only in rune literals)
// \"	Double quote (only in string literals)
