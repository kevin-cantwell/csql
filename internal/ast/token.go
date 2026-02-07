package ast

import (
	"fmt"
)

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
	if t.Type == EOF {
		return "EOF"
	}
	return string(t.Raw)
}

func (t *Token) MarshalJSON() ([]byte, error) {
	return []byte(
		`{"type":` + fmt.Sprintf("%q", t.Type) +
			`,"raw":` + fmt.Sprintf("%q", t.Raw) +
			`,"line":` + fmt.Sprint(t.Line) +
			`,"pos":` + fmt.Sprint(t.Pos) +
			`}`,
	), nil
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
	COMMENT
	WS

	// Symbols
	STAR      // *
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
	OVER
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
	ORDER
	ASC
	DESC
	LIMIT
	NULL
	EVERY
	CONSUME
	SELF
	EDGE
	TREE
	JOIN
	ON
	LEFT
	RIGHT
	LIKE

	// Literals
	STRING   // 'foo', "foo"
	NUMERIC  // 123.456
	DURATION // 2m, 1s, 24h
	TRUE     // true|TRUE
	FALSE    // false|FALSE

	// Identifiers
	IDENT // table_name, field_name, alias, "ident"
)
