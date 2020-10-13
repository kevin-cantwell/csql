package csql

import (
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
			case IDENT:
				pushOutput(t)
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

	_, expr, err := s.parseRPN(len(s.output) - 1)
	if err != nil {
		return nil, err
	}
	return expr, nil
}

func (s *ExpressionParser) parseRPN(i int) (int, Expression, error) {
	if i < 0 {
		t := s.output[0]
		return 0, nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}

	var expr Expression

	t := s.output[i]
	switch t.Type {
	case STRING:
		expr = &OperandExpression{
			String: t,
		}
		return i - 1, expr, nil
	case NUMERIC:
		expr = &OperandExpression{
			Numeric: t,
		}
		return i - 1, expr, nil
	case TRUE, FALSE:
		expr = &OperandExpression{
			Boolean: t,
		}
		return i - 1, expr, nil
	case NULL:
		expr = &OperandExpression{
			Null: t,
		}
		return i - 1, expr, nil
	case IDENT:
		expr = &OperandExpression{
			Ident: t,
		}
		return i - 1, expr, nil
	case PLUS, MINUS, STAR, SLASH, PERCENT:
		j, right, err := s.parseRPN(i - 1)
		if err != nil {
			return j, nil, err
		}
		j, left, err := s.parseRPN(j)
		if err != nil {
			return j, nil, err
		}
		expr = &OperatorExpression{
			Op:    *t,
			Left:  left,
			Right: right,
		}
		return j, expr, nil
	case AND, OR, EQ, NEQ, LT, LTE, GT, GTE:
		j, right, err := s.parseRPN(i - 1)
		if err != nil {
			return 0, nil, err
		}
		j, left, err := s.parseRPN(j)
		if err != nil {
			return 0, nil, err
		}
		if _, ok := left.(*PredicateExpression); !ok {
			panic("todo")
		}
		expr = &PredicateExpression{
			Predicate: *t,
			Left:      left,
			Right:     right,
		}
		return j, expr, nil
	case COUNT, SUM, MIN, MAX, AVG:
		j, arg, err := s.parseRPN(i - 1)
		if err != nil {
			return 0, nil, err
		}
		expr = &FunctionExpression{
			Func: *t,
			Args: []Expression{arg},
		}
		return j, expr, nil
	default:
		return 0, nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
	}
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
