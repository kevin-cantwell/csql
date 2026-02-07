package ast

import (
	"fmt"
	"strings"
)

// parseExpression is the entry point for expression parsing using precedence climbing.
func (p *Parser) parseExpression() (Expression, error) {
	return p.parsePrecedence(0)
}

// parsePrecedence implements Pratt parsing / precedence climbing.
func (p *Parser) parsePrecedence(minPrec int) (Expression, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		t, err := p.peek()
		if err != nil {
			return nil, err
		}

		prec := infixPrecedence(t.Type)
		if prec < minPrec {
			return left, nil
		}

		// Consume the operator
		p.scanSkipWS()

		switch t.Type {
		case AND, OR:
			right, err := p.parsePrecedence(prec + 1)
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Op: t.Type, Left: left, Right: right}

		case EQ, NEQ, LT, LTE, GT, GTE, LIKE:
			right, err := p.parsePrecedence(prec + 1)
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Op: t.Type, Left: left, Right: right}

		case PLUS, MINUS:
			right, err := p.parsePrecedence(prec + 1)
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Op: t.Type, Left: left, Right: right}

		case STAR, SLASH, PERCENT:
			right, err := p.parsePrecedence(prec + 1)
			if err != nil {
				return nil, err
			}
			left = &BinaryExpr{Op: t.Type, Left: left, Right: right}

		case IS:
			left, err = p.parseIsExpr(left)
			if err != nil {
				return nil, err
			}

		case NOT:
			left, err = p.parseNotPostfix(left)
			if err != nil {
				return nil, err
			}

		case IN:
			left, err = p.parseInExpr(left, false)
			if err != nil {
				return nil, err
			}

		case BETWEEN:
			left, err = p.parseBetweenExpr(left, false)
			if err != nil {
				return nil, err
			}

		default:
			return left, nil
		}
	}
}

func (p *Parser) parseUnary() (Expression, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}

	switch t.Type {
	case NOT:
		operand, err := p.parsePrecedence(precedenceNOT)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: NOT, Operand: operand}, nil

	case MINUS:
		operand, err := p.parsePrecedence(precedenceUnary)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: MINUS, Operand: operand}, nil

	case LPAREN:
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(RPAREN); err != nil {
			return nil, err
		}
		return expr, nil

	case IDENT:
		return p.parseIdentOrFunction(t)

	case NUMERIC:
		return &LiteralExpr{Type: NUMERIC, Value: t.String()}, nil

	case STRING:
		return &LiteralExpr{Type: STRING, Value: t.String()}, nil

	case TRUE:
		return &LiteralExpr{Type: TRUE, Value: "TRUE"}, nil

	case FALSE:
		return &LiteralExpr{Type: FALSE, Value: "FALSE"}, nil

	case NULL:
		return &LiteralExpr{Type: NULL, Value: "NULL"}, nil

	case STAR:
		return &StarExpr{}, nil

	// Aggregate/function keywords used as function names
	case COUNT, SUM, AVG, MIN, MAX:
		return p.parseFunctionCall(strings.ToUpper(t.String()))

	default:
		return nil, fmt.Errorf("unexpected token %q at line %d position %d", t.String(), t.Line, t.Pos)
	}
}

func (p *Parser) parseIdentOrFunction(ident *Token) (Expression, error) {
	name := ident.String()

	// The lexer may have already consumed "table.column" as a single IDENT token.
	// Split on dot to handle qualified names.
	if dotIdx := strings.IndexByte(name, '.'); dotIdx >= 0 {
		table := name[:dotIdx]
		col := name[dotIdx+1:]
		return &ColumnRef{Table: table, Column: col}, nil
	}

	t, err := p.peek()
	if err != nil {
		return nil, err
	}

	// Check for function call: ident(
	if t.Type == LPAREN {
		p.scanSkipWS() // consume (
		return p.parseFunctionArgs(strings.ToUpper(name))
	}

	// Check for qualified name: ident.ident or ident.*
	if t.Type == DOT {
		p.scanSkipWS() // consume .
		next, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}
		if next.Type == IDENT {
			return &ColumnRef{Table: name, Column: next.String()}, nil
		}
		if next.Type == STAR {
			return &ColumnRef{Table: name, Column: "*"}, nil
		}
		return nil, fmt.Errorf("expected identifier or * after '.' but got %q at line %d position %d", next.String(), next.Line, next.Pos)
	}

	return &ColumnRef{Column: name}, nil
}

func (p *Parser) parseFunctionCall(name string) (Expression, error) {
	if _, err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	return p.parseFunctionArgs(name)
}

// parseFunctionArgs parses function arguments after the opening paren has been consumed.
func (p *Parser) parseFunctionArgs(name string) (Expression, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	if t.Type == RPAREN {
		return &FunctionExpr{Name: name, Args: nil}, nil
	}
	p.unscan()

	// Check for * argument (COUNT(*))
	t, err = p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	if t.Type == STAR {
		if _, err := p.expect(RPAREN); err != nil {
			return nil, err
		}
		return &FunctionExpr{Name: name, Args: []Expression{&StarExpr{}}}, nil
	}
	p.unscan()

	var args []Expression
	for {
		arg, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}
		if t.Type == RPAREN {
			break
		}
		if t.Type != COMMA {
			return nil, fmt.Errorf("expected ',' or ')' but got %q at line %d position %d", t.String(), t.Line, t.Pos)
		}
	}

	return &FunctionExpr{Name: name, Args: args}, nil
}

func (p *Parser) parseIsExpr(left Expression) (Expression, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	not := false
	if t.Type == NOT {
		not = true
		t, err = p.scanSkipWS()
		if err != nil {
			return nil, err
		}
	}
	if t.Type != NULL {
		return nil, fmt.Errorf("expected NULL after IS but got %q at line %d position %d", t.String(), t.Line, t.Pos)
	}
	return &IsNullExpr{Expr: left, Not: not}, nil
}

func (p *Parser) parseNotPostfix(left Expression) (Expression, error) {
	t, err := p.scanSkipWS()
	if err != nil {
		return nil, err
	}
	switch t.Type {
	case IN:
		return p.parseInExpr(left, true)
	case BETWEEN:
		return p.parseBetweenExpr(left, true)
	case LIKE:
		right, err := p.parsePrecedence(precedenceComparison + 1)
		if err != nil {
			return nil, err
		}
		return &LikeExpr{Expr: left, Pattern: right, Not: true}, nil
	default:
		return nil, fmt.Errorf("expected IN, BETWEEN, or LIKE after NOT but got %q at line %d position %d", t.String(), t.Line, t.Pos)
	}
}

func (p *Parser) parseInExpr(left Expression, not bool) (Expression, error) {
	if _, err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	var values []Expression
	for {
		val, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		values = append(values, val)

		t, err := p.scanSkipWS()
		if err != nil {
			return nil, err
		}
		if t.Type == RPAREN {
			break
		}
		if t.Type != COMMA {
			return nil, fmt.Errorf("expected ',' or ')' but got %q at line %d position %d", t.String(), t.Line, t.Pos)
		}
	}
	return &InExpr{Expr: left, Values: values, Not: not}, nil
}

func (p *Parser) parseBetweenExpr(left Expression, not bool) (Expression, error) {
	low, err := p.parsePrecedence(precedenceComparison + 1)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(AND); err != nil {
		return nil, err
	}
	high, err := p.parsePrecedence(precedenceComparison + 1)
	if err != nil {
		return nil, err
	}
	return &BetweenExpr{Expr: left, Low: low, High: high, Not: not}, nil
}

// Precedence levels (higher = binds tighter)
const (
	precedenceOR         = 1
	precedenceAND        = 2
	precedenceNOT        = 3
	precedenceComparison = 4
	precedenceAddSub     = 5
	precedenceMulDiv     = 6
	precedenceUnary      = 7
)

func infixPrecedence(t TokenType) int {
	switch t {
	case OR:
		return precedenceOR
	case AND:
		return precedenceAND
	case NOT:
		return precedenceComparison // for NOT IN, NOT BETWEEN, NOT LIKE
	case EQ, NEQ, LT, LTE, GT, GTE, LIKE, IS, IN, BETWEEN:
		return precedenceComparison
	case PLUS, MINUS:
		return precedenceAddSub
	case STAR, SLASH, PERCENT:
		return precedenceMulDiv
	default:
		return -1 // not an infix operator
	}
}
