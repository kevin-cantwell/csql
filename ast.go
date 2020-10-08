package csql

type Statement struct {
	Select *SelectStatement `json:"SELECT,omitempty"`
}

type SelectStatement struct {
	Distinct bool           `json:"DISTINCT,omitempty"`
	Cols     []SelectColumn `json:"columns,omitempty"`
	From     *FromClause    `json:"FROM,omitempty"`
}

type SelectColumn struct {
	// if true, other fields ignored
	Star bool       `json:"star,omitempty"`
	Expr Expression `json:"expression,omitempty"`
	As   string     `json:"AS,omitempty"`
}

type Expression interface {
	expression()
}

type OperatorExpression struct {
	Op    TokenType  `json:"operator"`
	Left  Expression `json:"left"`
	Right Expression `json:"right"`
}

func (expr *OperatorExpression) expression() {}

type FunctionExpression struct {
	Func TokenType    `json:"function"`
	Args []Expression `json:"args"`
}

func (expr *FunctionExpression) expression() {}

type OperandExpression struct {
	String  *string  `json:"string,omitempty"`
	Numeric *float64 `json:"numeric,omitempty"`
	Ident   *Ident   `json:"identity,omitempty"`
	Boolean *bool    `json:"boolean,omitempty"`
	Null    bool     `json:"null,omitempty"`
}

func (expr *OperandExpression) expression() {}

type PredicateExpression struct {
	Predicate TokenType  `json:"predicate"` // AND, OR, =, !=, <. <=, >, >=
	Left      Expression `json:"left"`
	Right     Expression `json:"right"`
}

func (expr *PredicateExpression) expression() {}

type ComparisonExpression struct {
	Comparison TokenType  `json:"comparison"` // =, !=, <. <=, >, >=
	Left       Expression `json:"left"`
	Right      Expression `json:"right"`
}

func (expr *ComparisonExpression) expression() {}

type Ident struct {
	Table string `json:"table,omitempty"`
	Field string `json:"name"`
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
