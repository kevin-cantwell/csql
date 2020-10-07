package csql

type Statement struct {
	Select *SelectStatement `json:"SELECT,omitempty"`
}

type SelectStatement struct {
	Distinct bool           `json:"DISTINCT,omitempty"`
	Cols     []SelectColumn `json:"cols"`
	From     FromClause     `json:"FROM"`
}

type SelectColumn struct {
	// if true, other fields ignored
	Star bool       `json:"star,omitempty"`
	Expr Expression `json:"expr,omitempty"`
	As   string     `json:"AS,omitempty"`
}

type Expression interface {
	expression()
}

type OperatorExpression struct {
	Op    TokenType  `json:"op"`
	Left  Expression `json:"left"`
	Right Expression `json:"right"`
}

func (expr *OperatorExpression) expression() {}

type FunctionExpression struct {
	Func TokenType    `json:"func"`
	Args []Expression `json:"args"`
}

func (expr *FunctionExpression) expression() {}

type OperandExpression struct {
	String  *string  `json:"string,omitempty"`
	Numeric *float64 `json:"numeric,omitempty"`
	Field   *Field   `json:"field,omitempty"`
}

func (expr *OperandExpression) expression() {}

type ComparisonExpression struct {
	Op    TokenType // =, !=, <. <=, >, >=
	Left  Expression
	Right Expression
}

func (expr *ComparisonExpression) expression() {}

type Field struct {
	Table string `json:"table,omitempty"`
	Name  string `json:"name"`
}

type FromClause struct {
	Tables TablesExpression `json:"tables"`
}

type TablesExpression struct {
	Table         *string           `json:"table,omitempty"`
	Expr          *TablesExpression `json:"expr,omitempty"`
	As            string            `json:"as,omitempty"`
	CrossJoin     *TablesExpression `json:"CROSS JOIN,omitempty"`
	InnerJoin     *TablesExpression `json:"INNER JOIN,omitempty"`
	OuterJoin     *TablesExpression `json:"OUTER JOIN,omitempty"`
	LeftJoin      *TablesExpression `json:"LEFT JOIN,omitempty"`
	RightJoin     *TablesExpression `json:"RIGHT JOIN,omitempty"`
	FullOuterJoin *TablesExpression `json:"FULL OUTER JOIN,omitempty"`
	On            Predicate         `json:"ON,omitempty"`
}

type JoinOnPredicate struct {
	Op    TokenType  `json:"op"`
	Left  Expression `json:"left"`
	Right Expression `json:"right"`
}

type Predicate interface {
	predicate()
}

type FunctionPredicate struct {
	Op   TokenType // NOT, IN, BETWEEN, IS
	Args []Expression
}

type AndPredicate struct {
	Op    TokenType `json:"op"`
	Left  Predicate
	Right Predicate
}
