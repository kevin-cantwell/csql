package csql

import (
	"fmt"
	"strconv"
)

// exprTerm represents an expression exprTerm: a operand, operator, function, or parenthesis
type exprTerm []*Token

func (t exprTerm) isOperand() bool {
	return tokenIn(t.typ(), NUMERIC, STRING, IDENT)
}

func (t exprTerm) isOperator() bool {
	return tokenIn(t.typ(), PLUS, MINUS, ASTERISK, SLASH, PERCENT)
}

func (t exprTerm) isFunction() bool {
	return tokenIn(t.typ(), COUNT, SUM, AVG, MIN, MAX)
}

func (t exprTerm) isLeftParen() bool {
	return tokenIn(t.typ(), LPAREN)
}

func (t exprTerm) isRightParen() bool {
	return tokenIn(t.typ(), RPAREN)
}

func (t exprTerm) typ() TokenType {
	if len(t) == 0 {
		return ILLEGAL
	}
	return t[0].Type
}

func (t exprTerm) String() string {
	var s string
	for _, token := range t {
		s += string(token.Raw)
	}
	return s
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
			return fmt.Errorf("extra parenthesis %q", pop[0].Raw)
		}
		s.pushOutput(pop)
	}

	return nil
}

func (s *shuntingYard) ParseExpression() (Expression, error) {
	var stack []Expression

	for _, term := range s.output {
		var expr Expression
		switch t := term.typ(); t {
		case STRING:
			token := term[0]
			str := unquote(string(token.Raw))
			expr = &OperandExpression{
				String: &str,
			}
		case NUMERIC:
			token := term[0]
			f, err := strconv.ParseFloat(string(token.Raw), 64)
			if err != nil {
				return nil, err
			}
			expr = &OperandExpression{
				Numeric: &f,
			}
		case IDENT:
			var field Field
			if len(term) == 3 {
				field.Table = unquote(string(term[0].Raw))
				field.Name = unquote(string(term[2].Raw))
			} else {
				field.Name = unquote(string(term[0].Raw))
			}
			expr = &OperandExpression{
				Field: &field,
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
	s.output = append(s.output, term)
}

func (s *shuntingYard) pushStack(term exprTerm) {
	s.stack = append(s.stack, term)
}

func (s *shuntingYard) popStack() exprTerm {
	if len(s.stack) == 0 {
		return nil
	}
	pop := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
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
	atok := a[len(a)-1].Type
	btok := b[len(b)-1].Type
	if tokenIn(atok, PLUS, MINUS) {
		return false
	}
	if tokenIn(btok, PLUS, MINUS) {
		return true
	}
	return false
}
