package csql

// Token represents a lexical token.
type Token int

const (
	// Special tokens
	ERROR Token = iota
	SKIP
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
	EXCLAIM   // !
	EQUALS    // =
	LT        // <
	GT        // >
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
	WHERE
	AND
	OR
	NOT
	IN
	IS
	BETWEEN
	WITHIN
	GROUP
	BY
	LIMIT
	NULL
	TRUE
	FALSE

	// Literals
	STRING  // 'foo', "foo"
	NUMERIC // 123.456

	// Identifiers
	IDENT // table_name, field_name, alias
)
