package ast

import "time"

// Statement is a top-level SQL statement.
type Statement struct {
	Select *SelectStatement
}

// SelectStatement represents a full SELECT query.
type SelectStatement struct {
	Distinct bool
	Columns  []Column
	From     *FromClause
	Joins    []JoinClause
	Where    Expression
	GroupBy  []Expression
	OrderBy  []OrderByExpr
	Limit    *int
	Over     time.Duration
	Every    time.Duration
}

// Column represents a single item in the SELECT list.
type Column struct {
	Star      bool
	TableRef  string // table alias for "t.*"
	Expr      Expression
	Alias     string
}

// FromClause represents the FROM clause.
type FromClause struct {
	Table TableRef
}

// TableRef is a table name with optional alias.
type TableRef struct {
	Name  string
	Alias string
}

// JoinClause represents a JOIN.
type JoinClause struct {
	Type      JoinType
	Table     TableRef
	Condition Expression
}

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

// OrderByExpr is a single ORDER BY expression.
type OrderByExpr struct {
	Expr Expression
	Desc bool
}

// Expression is a node in an expression tree.
type Expression interface {
	exprNode()
}

// BinaryExpr represents a binary operation (arithmetic, comparison, logical).
type BinaryExpr struct {
	Op    TokenType
	Left  Expression
	Right Expression
}

func (*BinaryExpr) exprNode() {}

// UnaryExpr represents a unary operation (NOT, unary minus).
type UnaryExpr struct {
	Op      TokenType
	Operand Expression
}

func (*UnaryExpr) exprNode() {}

// FunctionExpr represents a function call like COUNT(x), SUM(x), UPPER(x).
type FunctionExpr struct {
	Name string
	Args []Expression
}

func (*FunctionExpr) exprNode() {}

// ColumnRef is a reference to a column, possibly qualified (table.column).
type ColumnRef struct {
	Table  string
	Column string
}

func (*ColumnRef) exprNode() {}

// LiteralExpr is a literal value.
type LiteralExpr struct {
	Type  TokenType // STRING, NUMERIC, TRUE, FALSE, NULL
	Value string
}

func (*LiteralExpr) exprNode() {}

// StarExpr represents * in expressions (e.g., COUNT(*)).
type StarExpr struct{}

func (*StarExpr) exprNode() {}

// IsNullExpr represents "expr IS [NOT] NULL".
type IsNullExpr struct {
	Expr Expression
	Not  bool
}

func (*IsNullExpr) exprNode() {}

// BetweenExpr represents "expr [NOT] BETWEEN low AND high".
type BetweenExpr struct {
	Expr Expression
	Low  Expression
	High Expression
	Not  bool
}

func (*BetweenExpr) exprNode() {}

// InExpr represents "expr [NOT] IN (values...)".
type InExpr struct {
	Expr   Expression
	Values []Expression
	Not    bool
}

func (*InExpr) exprNode() {}

// LikeExpr represents "expr [NOT] LIKE pattern".
type LikeExpr struct {
	Expr    Expression
	Pattern Expression
	Not     bool
}

func (*LikeExpr) exprNode() {}
