package csql

import "fmt"

type Token struct {
	// TokenType categorizes the token.
	Type TokenType
	// Raw is the original bytes for this token.
	Raw []byte
	// Line is the 1-indexed line on which this token appears in the query.
	Line int
	// Pos is the 1-indexed position where this token appears on its line.
	Pos int
}

func (t *Token) String() string {
	return string(t.Raw)
}

// Token represents a lexical token.
type TokenType int

func (t TokenType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", t.String())), nil
}

const (
	// Special tokens
	ILLEGAL TokenType = iota
	EOF
	WS

	// Symbols
	ASTERISK  // *
	COMMA     // ,
	DOT       // .
	LPAREN    // (
	RPAREN    // )
	LBRACKET  // [
	RBRACKET  // ]
	EQ        // =
	NEQ       // !=
	LT        // <
	LTE       // <=
	GT        // >
	GTE       // >=
	PLUS      // +
	MINUS     // -
	SLASH     // /
	PERCENT   // %
	SEMICOLON // ;

	// Keywords
	SELECT
	DISTINCT
	COUNT
	SUM
	MAX
	MIN
	AVG
	AS
	FROM
	CROSS_JOIN
	INNER_JOIN
	LEFT_JOIN
	LEFT_OUTER_JOIN
	RIGHT_JOIN
	RIGHT_OUTER_JOIN
	FULL_OUTER_JOIN
	ON
	WHERE
	AND
	OR
	NOT
	IN
	IS
	BETWEEN
	WITHIN
	GROUP_BY
	EVERY
	LIMIT
	NULL
	TRUE
	FALSE

	// Literals
	STRING  // 'foo', "foo"
	NUMERIC // 123.456

	// Identifiers
	IDENT // table_name, field_name, alias, "ident"

	// Comment
	COMMENT
)

func (t TokenType) String() string {
	switch t {
	case ILLEGAL:
		return "ILLEGAL"
	case EOF:
		return "EOF"
	case WS:
		return "WS"
	case ASTERISK:
		return "*"
	case COMMA:
		return ","
	case DOT:
		return "."
	case LPAREN:
		return "("
	case RPAREN:
		return ")"
	case LBRACKET:
		return "["
	case RBRACKET:
		return "]"
	case EQ:
		return "="
	case NEQ:
		return "!="
	case LT:
		return "<"
	case LTE:
		return "<="
	case GT:
		return ">"
	case GTE:
		return ">="
	case PLUS:
		return "+"
	case MINUS:
		return "-"
	case SLASH:
		return "/"
	case PERCENT:
		return "%"
	case SEMICOLON:
		return ";"
	case SELECT:
		return "SELECT"
	case DISTINCT:
		return "DISTINCT"
	case COUNT:
		return "COUNT"
	case SUM:
		return "SUM"
	case MAX:
		return "MAX"
	case MIN:
		return "MIN"
	case AVG:
		return "AVG"
	case AS:
		return "AS"
	case FROM:
		return "FROM"
	case CROSS_JOIN:
		return "CROSS JOIN"
	case INNER_JOIN:
		return "INNER JOIN"
	case LEFT_JOIN:
		return "LEFT JOIN"
	case LEFT_OUTER_JOIN:
		return "LEFT OUTER JOIN"
	case RIGHT_JOIN:
		return "RIGHT JOIN"
	case RIGHT_OUTER_JOIN:
		return "RIGHT OUTER JOIN"
	case FULL_OUTER_JOIN:
		return "FULL OUTER JOIN"
	case ON:
		return "ON"
	case WHERE:
		return "WHERE"
	case AND:
		return "AND"
	case OR:
		return "OR"
	case NOT:
		return "NOT"
	case IN:
		return "IN"
	case IS:
		return "IS"
	case BETWEEN:
		return "BETWEEN"
	case WITHIN:
		return "WITHIN"
	case GROUP_BY:
		return "GROUP BY"
	case EVERY:
		return "EVERY"
	case LIMIT:
		return "LIMIT"
	case NULL:
		return "NULL"
	case TRUE:
		return "TRUE"
	case FALSE:
		return "FALSE"
	case STRING:
		return "STRING"
	case NUMERIC:
		return "NUMERIC"
	case IDENT:
		return "IDENT"
	case COMMENT:
		return "--"
	default:
		return ""
	}
}
