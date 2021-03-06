package ast

import "time"

type Source int

type Statement struct {
	SQL     string
	Every   time.Duration
	Consume Token // SELF|EDGE|TREE
}

type Select struct {
	Distinct bool          `json:"DISTINCT,omitempty"`
	Cols     []Column      `json:"columns,omitempty"`
	From     *FromClause   `json:"FROM,omitempty"`
	Where    *WhereClause  `json:"WHERE,omitempty"`
	Every    time.Duration `json:"EVERY,omitempty"`
}

type Insert struct {
}

type Column struct {
	// if true, other fields ignored
	Star bool       `json:"star,omitempty"`
	Expr Expression `json:"expression,omitempty"`
	As   string     `json:"AS,omitempty"`
}

type Expression interface {
	at() (Token, Token)
}

type OperatorExpression struct {
	Op    Token      `json:"operator"`
	Left  Expression `json:"left"`
	Right Expression `json:"right"`
}

func (e *OperatorExpression) at() (Token, Token) {
	left, _ := e.Left.at()
	_, right := e.Right.at()
	return left, right
}

type FunctionExpression struct {
	Func Token        `json:"function"`
	Args []Expression `json:"args"`
}

func (e *FunctionExpression) at() (Token, Token) {
	_, right := e.Args[len(e.Args)-1].at()
	return e.Func, right
}

type OperandExpression struct {
	String  *Token `json:"string,omitempty"`
	Numeric *Token `json:"numeric,omitempty"`
	Ident   *Token `json:"identity,omitempty"`
	Boolean *Token `json:"boolean,omitempty"`
	Null    *Token `json:"null,omitempty"`
}

func (e *OperandExpression) at() (Token, Token) {
	switch {
	case e.String != nil:
		return *e.String, *e.String
	case e.Numeric != nil:
		return *e.Numeric, *e.Numeric
	case e.Ident != nil:
		return *e.Ident, *e.Ident
	case e.Boolean != nil:
		return *e.Boolean, *e.Boolean
	case e.Null != nil:
		return *e.Null, *e.Null
	default:
		panic("nil operand expression")
	}
}

type PredicateExpression struct {
	Predicate Token      `json:"predicate"` // AND, OR, =, !=, <. <=, >, >=
	Left      Expression `json:"left"`
	Right     Expression `json:"right"`
}

func (e *PredicateExpression) at() (Token, Token) {
	left, _ := e.Left.at()
	_, right := e.Right.at()
	return left, right
}

type ComparisonExpression struct {
	Comparison Token      `json:"comparison"` // =, !=, <. <=, >, >=
	Left       Expression `json:"left"`
	Right      Expression `json:"right"`
}

func (e *ComparisonExpression) at() (Token, Token) {
	left, _ := e.Left.at()
	_, right := e.Right.at()
	return left, right
}

type FromClause struct {
	Tables []TableIdent  `json:"tables"`
	Within time.Duration `json:"over"`
}

type TableIdent struct {
	Name string `json:"name"`
	As   string `json:"as"`
}

type WhereClause struct {
}

// type Predicate interface {
// 	predicate()
// }

// type FunctionPredicate struct {
// 	Op   Token // NOT, IN, BETWEEN, IS
// 	Args []Expression
// }

// type AndPredicate struct {
// 	Op    Token `json:"op"`
// 	Left  Predicate
// 	Right Predicate
// }
