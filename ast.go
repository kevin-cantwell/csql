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
	Tables TablesExpression `json:"tables"`
}

type TablesExpression struct {
	Ident         Token             `json:"table"`
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
	Op    Token      `json:"op"`
	Left  Expression `json:"left"`
	Right Expression `json:"right"`
}

type Predicate interface {
	predicate()
}

type FunctionPredicate struct {
	Op   Token // NOT, IN, BETWEEN, IS
	Args []Expression
}

type AndPredicate struct {
	Op    Token `json:"op"`
	Left  Predicate
	Right Predicate
}
