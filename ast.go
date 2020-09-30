package csql

type Statement struct {
	Select *SelectStatement
}

type SelectStatement struct {
	Distinct bool
	Columns  []SelectColumn
}

type SelectColumn struct {
	Star  bool // if true, other fields ignored
	Expr  Expression
	Alias string
}

type Expression interface {
	private()
}

type OperatorExpression struct {
	Op    Token
	Left  Expression
	Right Expression
}

func (expr *OperatorExpression) private() {}

type FunctionExpression struct {
	Func Token
	Args []Expression
}

func (expr *FunctionExpression) private() {}

type OperandExpression struct {
	String  *string
	Numeric *float64
	Ident   *Ident
}

func (expr *OperandExpression) private() {}

type Ident struct {
	Table string
	Field string
}
