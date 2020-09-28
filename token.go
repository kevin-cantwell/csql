package csql

// Token represents a lexical token.
type Token int

const (
	// Special tokens
	ILLEGAL Token = iota
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

type token struct {
	tok  Token
	raw  string
	prev *token
	next *token
}

type tokenBuf struct {
	head *token
	tail *token
}

func (l *tokenBuf) push(tok Token, raw string) {
	node := &token{tok: tok}
	if l.head == nil {
		l.head = node
		l.tail = node
		return
	}
	node.prev = l.tail
	l.tail.next = node
	l.tail = node
}

func (l *tokenBuf) prev() Token {
	if l.tail == nil {
		return ILLEGAL
	}
	return l.tail.tok
}

func (l *tokenBuf) prevSkipWS() Token {
	for t := l.tail; t != nil; t = t.prev {
		if t.tok != WS {
			return t.tok
		}
	}
	return ILLEGAL
}

func (l *tokenBuf) get(n int) *token {
	var tok *token
	if n < 0 {
		curr := l.tail
		for i := n; i < 0 && curr != nil; i++ {
			tok = curr
			curr = curr.prev
		}
	} else {
		curr := l.head
		for i := 0; i < n && curr != nil; i++ {
			tok = curr
			curr = curr.next
		}
	}
	return tok
}
