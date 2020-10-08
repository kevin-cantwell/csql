package csql

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
)

// ExpressionParser scans and then parses an
// infix expression using the Shunting Yard algorithm.
type ExpressionParser struct {
	output []*Token
	stack  []*Token
}

func (s *ExpressionParser) scanShuntingYard(p *Parser) error {
	s.output = nil
	s.stack = nil

	pushOutput := func(t *Token) {
		s.output = append(s.output, t)
	}
	pushStack := func(t *Token) {
		s.stack = append(s.stack, t)
	}
	popStack := func() *Token {
		if len(s.stack) == 0 {
			return nil
		}
		pop := s.stack[len(s.stack)-1]
		s.stack = s.stack[:len(s.stack)-1]
		return pop
	}
	peekStack := func() *Token {
		if len(s.stack) == 0 {
			return nil
		}
		return s.stack[len(s.stack)-1]
	}

	err := func() error {
		// while there are term to be read:
		//     read a term.
		for {
			t, err := p.scanSkipWS()
			if err != nil {
				return err
			}

			switch t.Type {
			// if the term is a operand, then:
			//     push it to the output queue.
			case NUMERIC, STRING, NULL, TRUE, FALSE, IDENT, DOT:
				pushOutput(t)
			// else if the term is a function then:
			//     push it onto the operator stack
			case COUNT, SUM, AVG, MIN, MAX:
				pushStack(t)
			// else if the term is an operator then:
			//     while ((there is an operator at the top of the operator stack)
			//             and ((the operator at the top of the operator stack has greater precedence)
			// 	               or (the operator at the top of the operator stack has equal precedence and the term is left associative))
			//             and (the operator at the top of the operator stack is not a left parenthesis)):
			//         pop operators from the operator stack onto the output queue.
			//     push it onto the operator stack.
			case PLUS, MINUS, ASTERISK, SLASH, PERCENT, AND, OR, EQ, NEQ, LT, LTE, GT, GTE:
				for top := peekStack(); top != nil && top.Type != LPAREN && precedence(top) > precedence(t); top = peekStack() {
					pushOutput(popStack())
				}
				pushStack(t)
			// else if the term is a left parenthesis (i.e. "("), then:
			//     push it onto the operator stack.
			case LPAREN:
				pushStack(t)
			// else if the term is a right parenthesis (i.e. ")"), then:
			//     while the operator at the top of the operator stack is not a left parenthesis:
			//         pop the operator from the operator stack onto the output queue.
			//	   /* If the stack runs out without finding a left parenthesis, then there are mismatched parentheses. */
			//     if there is a left parenthesis at the top of the operator stack, then:
			//         pop the operator from the operator stack and discard it
			case RPAREN:
				for top := peekStack(); top.Type != LPAREN; top = peekStack() {
					pushOutput(popStack())
				}
				popStack() // discard LPAREN

				// top := popStack()
				// for ; top != nil && top.Type != LPAREN; top = popStack() {
				// 	pushOutput(top)
				// }
				// if top == nil {
				// 	return errors.Errorf("mismatched parenthesis at line %d position %d", t.Line, t.Pos)
				// }
				// if top.Type != LPAREN {
				// 	pushStack(top)
				// }
			default:
				p.unscan()
				return nil
			}
		}
	}()
	if err != nil {
		return err
	}

	// if there are no more tokens to read then:
	//     while there are still operator tokens on the stack:
	//         /* If the operator token on the top of the stack is a parenthesis, then there are mismatched parentheses. */
	//         pop the operator from the operator stack onto the output queue.
	for pop := popStack(); pop != nil; pop = popStack() {
		if pop.Type == LPAREN || pop.Type == RPAREN {
			return errors.Errorf("mismatched parenthesis at line %d position %d", pop.Line, pop.Pos)
		}
		pushOutput(pop)
	}

	return nil
}

// Parse scans and parses an infix expression using the Shunting Yard algorithm
// to produce an Expression AST.
func (s *ExpressionParser) Parse(p *Parser) (Expression, error) {
	if err := s.scanShuntingYard(p); err != nil {
		return nil, err
	}

	if len(s.output) == 0 {
		return nil, nil
	}

	// Parse the reverse polish notation and construct the expression
	var exprs []Expression

	for i := 0; i < len(s.output); i++ {
		t := s.output[i]
		var expr Expression
		switch t.Type {
		case STRING:
			str := unquote(t.Raw)
			expr = &OperandExpression{
				String: &str,
			}
		case NUMERIC:
			f, err := strconv.ParseFloat(string(t.Raw), 64)
			if err != nil {
				return nil, err
			}
			expr = &OperandExpression{
				Numeric: &f,
			}
		case TRUE, FALSE:
			b, err := strconv.ParseBool(string(t.Raw))
			if err != nil {
				return nil, err
			}
			expr = &OperandExpression{
				Boolean: &b,
			}
		case NULL:
			expr = &OperandExpression{
				Null: true,
			}
		case IDENT:
			ident := Ident{
				Field: unquote(t.Raw),
			}
			if i < len(s.output)-2 && s.output[i+1].Type == DOT {
				ident.Table = unquote(s.output[i+2].Raw)
				i += 2
			}
			expr = &OperandExpression{
				Ident: &ident,
			}
		case PLUS, MINUS, ASTERISK, SLASH, PERCENT:
			right := exprs[len(exprs)-1]
			left := exprs[len(exprs)-2]
			exprs = exprs[:len(exprs)-2]
			expr = &OperatorExpression{
				Op:    t.Type,
				Left:  left,
				Right: right,
			}
		case AND, OR:
			right := exprs[len(exprs)-1]
			left := exprs[len(exprs)-2]
			exprs = exprs[:len(exprs)-2]
			if _, ok := left.(*PredicateExpression); !ok {

			}
			expr = &PredicateExpression{
				Predicate: t.Type,
				Left:      left,
				Right:     right,
			}
		case EQ, NEQ, LT, LTE, GT, GTE:
			right := exprs[len(exprs)-1]
			left := exprs[len(exprs)-2]
			exprs = exprs[:len(exprs)-2]
			expr = &PredicateExpression{
				Predicate: t.Type,
				Left:      left,
				Right:     right,
			}
		case COUNT, SUM, MIN, MAX, AVG:
			arg := exprs[len(exprs)-1]
			exprs = exprs[:len(exprs)-1]
			expr = &FunctionExpression{
				Func: t.Type,
				Args: []Expression{arg},
			}
		default:
			return nil, fmt.Errorf("invalid expression token %s at line %d position %d", t, t.Line, t.Pos)
		}

		exprs = append(exprs, expr)
	}

	if len(exprs) != 1 {
		panic("reverse polish notation mismatch")
	}

	return exprs[0], nil
}

// larger numbers indicate greater precedence
func precedence(a *Token) int {
	switch a.Type {
	case OR:
		return 1
	case AND:
		return 2
	case EQ, NEQ, LT, LTE, GT, GTE:
		return 3
	case PLUS, MINUS:
		return 4
	case ASTERISK, SLASH, PERCENT:
		return 5
	case COUNT, SUM, MIN, MAX, AVG:
		return 6
	default:
		return 0
	}
}