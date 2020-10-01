package csql

import (
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strconv"
)

func line() string {
	_, file, l, _ := runtime.Caller(1)
	file = filepath.Base(file)
	return fmt.Sprintf("%s:%d", file, l)
}

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

func (p *Parser) Parse() ([]Statement, error) {
	var stmts []Statement

	for {
		var stmt Statement
		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}

		switch t.tok {
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
			return nil, fmt.Errorf("%s: unsupported statement at position %d: %q", line(), p.pos, t.raw)
		}

		stmts = append(stmts, stmt)
	}
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
		switch t.tok {
		case EOF, SEMICOLON:
			return stmt, nil
		default:
			return nil, fmt.Errorf("unexpected token at position %d: %q", p.pos, t.raw)
		}
	}
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

		if t.tok != COMMA {
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
	switch t.tok {
	case ASTERISK:
		return &SelectColumn{
			Star: true,
		}, nil
	case IDENT, STRING, NUMERIC, PLUS, MINUS, COUNT, SUM, MIN, MAX, AVG:
		p.unscan()
		col, err = p.parseSelectExpressionColumn()
		if err != nil {
			return nil, err
		}
	default:
		p.unscan()
		return nil, fmt.Errorf("unexpected token at position %d: %q", p.pos, t.raw)
	}

	// AS
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	if t.tok != AS {
		p.unscan()
		return col, nil
	}

	// STRING | IDENT
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	if t.tok != STRING && t.tok != IDENT {
		p.unscan()
		return nil, fmt.Errorf("unexpected token at position %d: %q", p.pos, t.raw)
	}
	col.Alias = unquote(string(t.raw))

	return col, nil
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
	infix, err := p.scanInfixTerms()
	if err != nil {
		return nil, err
	}

	for _, t := range infix {
		debugf("%q\n", t)
	}

	sh := &shuntingYard{}

	if err := sh.PushInfix(infix); err != nil {
		return nil, err
	}

	expr, err := sh.ParseExpression()
	if err != nil {
		return nil, err
	}

	return &SelectColumn{
		Expr: expr,
	}, nil
}

// exprTerm represents an expression exprTerm: a operand, operator, function, or parenthesis
type exprTerm []*token

func (t exprTerm) isOperand() bool {
	return tokenIn(t.tok(), NUMERIC, STRING, IDENT)
}

func (t exprTerm) isOperator() bool {
	return tokenIn(t.tok(), PLUS, MINUS, ASTERISK, SLASH, PERCENT)
}

func (t exprTerm) isFunction() bool {
	return tokenIn(t.tok(), COUNT, SUM, AVG, MIN, MAX)
}

func (t exprTerm) isLeftParen() bool {
	return tokenIn(t.tok(), LPAREN)
}

func (t exprTerm) isRightParen() bool {
	return tokenIn(t.tok(), RPAREN)
}

func (t exprTerm) tok() Token {
	if len(t) == 0 {
		return NONE
	}
	return t[0].tok
}

func (t exprTerm) String() string {
	var s string
	for _, token := range t {
		s += string(token.raw)
	}
	return s
}

func (p *Parser) scanInfixTerms() ([]exprTerm, error) {

	// operands and operators
	var expr []exprTerm
	var prev Token = NONE

	for {
		// last Token in the last exprTerm
		if len(expr) > 0 {
			prev = expr[len(expr)-1].tok()
		}

		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}

		term := exprTerm{t}

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
				return nil, fmt.Errorf("unexpected token at position %d: %q", p.pos, ident.raw)
			}
			term = append(term, dot, ident)
		case LPAREN:
			if !tokenIn(prev, NONE, PLUS, MINUS, ASTERISK, SLASH, COUNT, SUM, AVG, MIN, MAX, LPAREN) {
				p.unscan()
				return nil, fmt.Errorf("unexpected token at position %d: %q", p.pos, t.raw)
			}

			expr = append(expr, term)

			// recursively descend into the parenthetical
			inner, err := p.scanInfixTerms()
			if err != nil {
				return nil, err
			}
			expr = append(expr, inner...)

			rparen, err := p.scan()
			if err != nil {
				return nil, err
			}
			if rparen.tok != RPAREN {
				p.unscan()
				return nil, fmt.Errorf("unexpected token at position %d: %q", p.pos, rparen.raw)
			}

			term = exprTerm{rparen}
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
	output []exprTerm
	stack  []exprTerm
}

func (s *shuntingYard) PushInfix(infix []exprTerm) error {
	// while there are term to be read:
	//     read a term.
	for _, term := range infix {
		switch {
		// if the term is a operand, then:
		//     push it to the output queue.
		case term.isOperand():

			s.pushOutput(term)
		// else if the term is a function then:
		//     push it onto the operator stack
		case term.isFunction():
			s.pushStack(term)
		// else if the term is an operator then:
		//     while ((there is an operator at the top of the operator stack)
		//             and ((the operator at the top of the operator stack has greater precedence)
		// 	               or (the operator at the top of the operator stack has equal precedence and the term is left associative))
		//             and (the operator at the top of the operator stack is not a left parenthesis)):
		//         pop operators from the operator stack onto the output queue.
		//     push it onto the operator stack.
		case term.isOperator():
			for peek := s.peekStack(); peek != nil && !peek.isLeftParen() && s.opPrecedes(peek, term); peek = s.peekStack() {
				s.pushOutput(s.popStack())
			}
			// for top := s.popStack(); top != nil && !top.isLeftParen() && s.opPrecedes(top, term); top = s.popStack() {
			// 	s.pushOutput(top)
			// }
			s.pushStack(term)
		// else if the term is a left parenthesis (i.e. "("), then:
		//     push it onto the operator stack.
		case term.isLeftParen():
			s.pushStack(term)
		// else if the term is a right parenthesis (i.e. ")"), then:
		//     while the operator at the top of the operator stack is not a left parenthesis:
		//         pop the operator from the operator stack onto the output queue.
		//	   /* If the stack runs out without finding a left parenthesis, then there are mismatched parentheses. */
		//     if there is a left parenthesis at the top of the operator stack, then:
		//         pop the operator from the operator stack and discard it
		case term.isRightParen():
			// debugf("output:\n")
			// for _, t := range s.output {
			// 	debugf("\toutput:%v\n", t)
			// }
			// debugf("stack:\n")
			// for _, t := range s.stack {
			// 	debugf("\tstack:%v\n", t)
			// }
			top := s.popStack()
			for ; top != nil && !top.isLeftParen(); top = s.popStack() {
				s.pushOutput(top)
			}
			if top == nil {
				return fmt.Errorf("missing left paren")
			}
			if !top.isLeftParen() {
				s.pushStack(top)
			}
		}
	}
	// if there are no more tokens to read then:
	//     while there are still operator tokens on the stack:
	//         /* If the operator token on the top of the stack is a parenthesis, then there are mismatched parentheses. */
	//         pop the operator from the operator stack onto the output queue.
	for pop := s.popStack(); pop != nil; pop = s.popStack() {
		if pop.isLeftParen() || pop.isRightParen() {
			return fmt.Errorf("extra parenthesis %q", pop[0].raw)
		}
		s.pushOutput(pop)
	}

	return nil
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
		for _, expr := range stack {
			fmt.Printf("%+v\n", expr)
		}
		panic("reverse polish notation mismatch")
	}

	return stack[0], nil
}

func (s *shuntingYard) pushOutput(term exprTerm) {
	debugf("push output: %+v\n", term)
	s.output = append(s.output, term)
}

func (s *shuntingYard) pushStack(term exprTerm) {
	debugf("push stack: %+v\n", term)
	s.stack = append(s.stack, term)
}

func (s *shuntingYard) popStack() exprTerm {
	if len(s.stack) == 0 {
		return nil
	}
	pop := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	debugf("pop stack: %+v\n", pop)
	return pop
}

func (s *shuntingYard) peekStack() exprTerm {
	if len(s.stack) == 0 {
		return nil
	}
	return s.stack[len(s.stack)-1]
}

func (s *shuntingYard) opPrecedes(a, b exprTerm) bool {
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
