package csql

import (
	"fmt"

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
		var prev TokenType

		// while there are term to be read:
		//     read a term.
		for {
			t, err := p.scanSkipWS()
			if err != nil {
				return err
			}

			// fmt.Println(jsonify(t))

			switch t.Type {
			// if the term is a operand, then:
			//     push it to the output queue.
			case IDENT:
				pushOutput(t)
				dot, err := p.scanSkipWS()
				if err != nil {
					return err
				}
				if dot.Type != DOT {
					p.unscan()
					continue
				}
				pushOutput(dot)
				ident, err := p.scanSkipWS()
				if err != nil {
					return err
				}
				if ident.Type != IDENT {
					return errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
				}
				pushOutput(ident)
			case DOT:
				if prev != IDENT {
					return nil
				}
			case NUMERIC, STRING, NULL, TRUE, FALSE:
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
			case PLUS, MINUS, STAR, SLASH, PERCENT, AND, OR, EQ, NEQ, LT, LTE, GT, GTE:
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

			prev = t.Type
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

	return s.parseRPN(len(s.output) - 1)
}

func (s *ExpressionParser) parseRPN(i int) (Expression, error) {
	if i < 0 {
		t := s.output[0]
		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}

	var expr Expression

	t := s.output[i]
	switch t.Type {
	case STRING:
		expr = &OperandExpression{
			String: t,
		}
	case NUMERIC:
		expr = &OperandExpression{
			Numeric: t,
		}
	case TRUE, FALSE:
		expr = &OperandExpression{
			Boolean: t,
		}
	case NULL:
		expr = &OperandExpression{
			Null: t,
		}
	case IDENT:
		ident := Ident{
			Field: *t,
		}
		if i >= 2 && s.output[i-1].Type == DOT {
			ident.Table = s.output[i-2]
		}
		expr = &OperandExpression{
			Ident: &ident,
		}
	case PLUS, MINUS, STAR, SLASH, PERCENT:
		left, err := s.parseRPN(i - 2)
		if err != nil {
			return nil, err
		}
		right, err := s.parseRPN(i - 1)
		if err != nil {
			return nil, err
		}
		expr = &OperatorExpression{
			Op:    *t,
			Left:  left,
			Right: right,
		}
	case AND, OR, EQ, NEQ, LT, LTE, GT, GTE:
		left, err := s.parseRPN(i - 2)
		if err != nil {
			return nil, err
		}
		right, err := s.parseRPN(i - 1)
		if err != nil {
			return nil, err
		}
		if _, ok := left.(*PredicateExpression); !ok {
			panic("todo")
		}
		expr = &PredicateExpression{
			Predicate: *t,
			Left:      left,
			Right:     right,
		}
	case COUNT, SUM, MIN, MAX, AVG:
		arg, err := s.parseRPN(i - 1)
		if err != nil {
			return nil, err
		}
		expr = &FunctionExpression{
			Func: *t,
			Args: []Expression{arg},
		}
	default:
		fmt.Println(jsonify(s.output))
		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}

	return expr, nil
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
	case STAR, SLASH, PERCENT:
		return 5
	case COUNT, SUM, MIN, MAX, AVG:
		return 6
	default:
		return 0
	}
}
