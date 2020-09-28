package csql

// Token represents a lexical token.
type Token string

const (
	// Special tokens
	ILLEGAL Token = ""
	EOF     Token = "EOF"
	// WS      Token = " "

	// Symbols
	ASTERISK Token = "*"
	COMMA    Token = ","
	DOT      Token = "."
	LPAREN   Token = "("
	RPAREN   Token = ")"
	LBRACKET Token = "["
	RBRACKET Token = "]"

	// Predicate Operators
	EQUAL    Token = "="
	NOTEQUAL Token = "!="
	LT       Token = "<"
	LTE      Token = "<="
	GT       Token = ">"
	GTE      Token = ">="

	// Expression Operators
	PLUS      Token = "+"
	MINUS     Token = "-"
	DIVIDE    Token = "/"
	PERCENT   Token = "%"
	SEMICOLON Token = ";"

	// Keywords
	SELECT   Token = "SELECT"
	DISTINCT Token = "DISTINCT"
	COUNT    Token = "COUNT"
	SUM      Token = "SUM"
	MAX      Token = "MAX"
	MIN      Token = "MIN"
	AVG      Token = "AVG"
	AS       Token = "AS"
	FROM     Token = "FROM"
	WHERE    Token = "WHERE"
	AND      Token = "AND"
	OR       Token = "OR"
	NOT      Token = "NOT"
	IN       Token = "IN"
	IS       Token = "IS"
	BETWEEN  Token = "BETWEEN"
	WITHIN   Token = "WITHIN"
	GROUP    Token = "GROUP"
	BY       Token = "BY"
	LIMIT    Token = "LIMIT"
	NULL     Token = "NULL"
	TRUE     Token = "TRUE"
	FALSE    Token = "FALSE"

	// Literals
	STRING  Token = "STRING"  // 'foo', "foo"
	NUMERIC Token = "NUMERIC" // 123.456

	// Identifiers
	IDENT Token = "IDENT" // table_name, field_name, alias
)
