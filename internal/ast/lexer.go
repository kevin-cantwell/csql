package ast

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// eof represents a marker byte for the end of the reader.
const (
	eof = byte(0)
)

// Lexer represents a lexical scanner.
type Lexer struct {
	r    *bufio.Reader
	loc  []int
	line int
	pos  int
	eof  bool
}

// NewLexer returns a new instance of Lexer.
func NewLexer(r io.Reader) *Lexer {
	return &Lexer{
		r:    bufio.NewReader(r),
		loc:  []int{1},
		line: 1,
		pos:  1,
	}
}

// Scan returns the next token and literal value.
func (l *Lexer) Scan() (*Token, error) {
	for _, scan := range []func() (*Token, error){
		l.scanEOF,
		l.scanComment,
		l.scanWS,
		l.scanString,
		l.scanDuration,
		l.scanNumeric,
		l.scanSymbol,
		l.scanKeyword,
		l.scanIdent,
	} {
		tok, err := scan()
		if err != nil {
			return nil, err
		}
		if tok != nil {
			return tok, nil
		}
	}
	return l.scanIllegal()
}

func (l *Lexer) Line(b4 int) int {
	line := len(l.loc)
	pos := l.loc[line-1]
	for pos <= b4 {
		b4 = b4 - pos
		line--
		pos = l.loc[line-1] - 1
	}
	return line
}

func (l *Lexer) Pos(b4 int) int {
	line := len(l.loc)
	pos := l.loc[line-1]
	for pos <= b4 {
		b4 = b4 - pos
		line--
		pos = l.loc[line-1] - 1
	}
	return pos - b4
}

func (l *Lexer) newToken(tok TokenType, raw []byte) (*Token, error) {
	return &Token{
		Type: tok,
		Raw:  raw,
		// Line: len(l.loc) - 1,
		// Pos:  l.loc[len(l.loc)-1],
		Line: l.Line(len(raw)),
		Pos:  l.Pos(len(raw)),
	}, nil
}

func (l *Lexer) scanEOF() (*Token, error) {
	ch, err := l.read()
	if err != nil {
		return nil, err
	}

	if ch != eof {
		if err := l.unread(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	return l.newToken(EOF, nil)
}

func (l *Lexer) scanComment() (*Token, error) {
	peek, err := l.peekN(2)
	if err != nil {
		return nil, err
	}
	if string(peek) != "--" {
		return nil, nil
	}
	var raw []byte
	for {
		ch, err := l.read()
		if err != nil {
			return nil, err
		}
		switch ch {
		case '\n', eof:
			l.unread()
			return l.newToken(COMMENT, raw)
		default:
			raw = append(raw, ch)
		}
	}
}

func (l *Lexer) scanWS() (*Token, error) {
	ch, err := l.read()
	if err != nil {
		return nil, err
	}

	if !isWS(ch) {
		if err := l.unread(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	raw := []byte{ch}

	for {
		ch, err := l.read()
		if err != nil {
			return nil, err
		}

		if ch == eof {
			return l.newToken(WS, raw)
		}

		if !isWS(ch) {
			if err := l.unread(); err != nil {
				return nil, err
			}
			return l.newToken(WS, raw)
		}

		raw = append(raw, ch)
	}
}

// scans a quoted string literal
func (l *Lexer) scanString() (*Token, error) {
	quote, err := l.peek()
	if err != nil {
		return nil, err
	}

	switch quote {
	case '\'':
		raw, err := l.scanQuote()
		if err != nil {
			return nil, err
		}
		return l.newToken(STRING, raw)
	default:
		return nil, nil
	}
}

// A duration string is a possibly signed sequence of decimal numbers, each with optional fraction
// and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"),
// "ms", "s", "m", "h".
func (l *Lexer) scanDuration() (*Token, error) {
	var (
		duration []byte
	)

	// "ns", "us", "µs", "ms", "s", "m", "h"
	appendUnit := func() (bool, error) {
		peek, err := l.peekAfter(len(duration))
		if err != nil {
			return false, err
		}
		switch peek {
		case 's', 'h':
			duration = append(duration, peek)
			return true, nil
		case 'n', 'u', 'µ':
			duration = append(duration, peek)
			peek, err := l.peekAfter(len(duration))
			if err != nil {
				return false, err
			}
			if peek != 's' {
				return false, nil
			}
			duration = append(duration, 's')
			return true, nil
		case 'm':
			duration = append(duration, peek)
			peek, err := l.peekAfter(len(duration))
			if err != nil {
				return false, err
			}
			if peek != 's' {
				return true, nil
			}
			duration = append(duration, 's')
			return true, nil
		default:
			return false, nil
		}
	}

	appendDuration := func() (bool, error) {
		var (
			decimal  bool
			numerals int
		)
		for {
			peek, err := l.peekAfter(len(duration))
			if err != nil {
				return false, err
			}
			switch {
			case peek == '-':
				if len(duration) > 0 {
					return false, nil
				}
			case peek == '.':
				if decimal {
					return false, nil
				}
				decimal = true
			case isDigit(peek):
				numerals++
			case !isDigit(peek):
				if numerals == 0 {
					return false, nil
				}
				return appendUnit()
			}
			duration = append(duration, peek)
		}
	}

	for i := 0; ; i++ {
		ok, err := appendDuration()
		if err != nil {
			return nil, err
		}
		if !ok {
			if i == 0 {
				return nil, nil
			}
			if _, err := time.ParseDuration(string(duration)); err != nil {
				return nil, nil
			}
			raw, err := l.readN(len(duration))
			if err != nil {
				return nil, err
			}
			return l.newToken(DURATION, raw)
		}
	}
}

// scans a number literal
func (l *Lexer) scanNumeric() (*Token, error) {
	var (
		number   []byte
		decimal  bool
		numerals int
	)

loop:
	for {
		peek, err := l.peekAfter(len(number))
		if err != nil {
			return nil, err
		}
		switch {
		case peek == '-':
			if len(number) > 0 {
				break loop
			}
		case peek == '.':
			if decimal {
				break loop
			}
			decimal = true
		case isDigit(peek):
			numerals++
		case !isDigit(peek):
			if numerals == 0 {
				return nil, nil
			}
			break loop
		}
		number = append(number, peek)
	}

	if _, err := strconv.ParseFloat(string(number), 64); err != nil {
		return nil, nil
	}

	raw, err := l.readN(len(number))
	if err != nil {
		return nil, err
	}
	return l.newToken(NUMERIC, raw)
}

func (l *Lexer) scanSymbol() (*Token, error) {
	for _, sym := range symbols {
		n := len([]byte(sym.str))

		peek, err := l.peekN(n)
		if err != nil {
			return nil, err
		}

		if string(peek) == sym.str {
			raw, err := l.readN(n)
			if err != nil {
				return nil, err
			}
			return l.newToken(sym.typ, raw)
		}
	}

	return nil, nil
}

func (l *Lexer) scanKeyword() (*Token, error) {
	for typ, keyword := range keywords {
		n := len([]byte(keyword))

		peek, err := l.peekN(n)
		if err != nil {
			return nil, err
		}

		// if peek doesn't match the keyword, then skip
		if strings.ToUpper(string(peek)) != keyword {
			continue
		}

		// If there's more to the word, then it can't be the keyword
		ch, err := l.peekAfter(n)
		if err != nil {
			return nil, err
		}
		if !isIdent(ch) {
			// It's the keyword
			raw, err := l.readN(n)
			if err != nil {
				return nil, err
			}
			return l.newToken(typ, raw)
		}

	}

	return nil, nil
}

func (l *Lexer) scanIdent() (t *Token, e error) {
	var raw []byte

	peek, err := l.peek()
	if err != nil {
		return nil, err
	}
	if peek == '"' {
		raw, err = l.scanQuote()
		if err != nil {
			return nil, err
		}
	} else {
		for i := 0; ; i++ {
			peek, err := l.peekAfter(i)
			if err != nil {
				return nil, err
			}
			if isIdent(peek) {
				raw = append(raw, peek)
				continue
			}
			if i == 0 {
				return nil, nil
			}
			if isKeyword(string(raw)) {
				return nil, nil
			}
			raw, err = l.readN(i)
			if err != nil {
				return nil, err
			}
			break
		}
	}

	dot, err := l.read()
	if err != nil {
		e = err
		return
	}
	if dot != '.' {
		if err := l.unread(); err != nil {
			return nil, err
		}
		return l.newToken(IDENT, raw)
	}

	suffix, err := l.scanIdent()
	if suffix == nil {
		// No valid ident after dot — put the dot back and return what we have
		if err := l.unread(); err != nil {
			return nil, err
		}
		return l.newToken(IDENT, raw)
	}
	raw = append(raw, '.')
	raw = append(raw, suffix.Raw...)
	return l.newToken(IDENT, raw)
}

func (l *Lexer) scanQuote() ([]byte, error) {
	quote, err := l.read()
	if err != nil {
		return nil, err
	}

	raw := []byte{quote}

	for {
		ch, err := l.read()
		if err != nil {
			return nil, err
		}
		switch ch {
		case eof:
			return nil, fmt.Errorf("mismatched quote")
		case '\\':
			escaped, err := l.read()
			if err != nil {
				return nil, err
			}
			raw = append(raw, '\\', escaped)
			if escaped == quote {
				continue
			}
			n, ok := escapeChars[escaped]
			if !ok {
				return nil, fmt.Errorf("unkown escape sequence: '\\%s'", string(escaped))
			}
			seq, err := l.readN(n)
			if err != nil {
				return nil, err
			}
			raw = append(raw, seq...)
		case quote:
			raw = append(raw, quote)
			return raw, nil
		default:
			raw = append(raw, ch)
		}
	}
}

func (l *Lexer) scanIllegal() (*Token, error) {
	ch, err := l.read()
	if err != nil {
		return nil, err
	}

	return l.newToken(ILLEGAL, []byte{ch})
}

// read reads the next byte from the buffered reader.
// Returns the byte(0) if an error occurs (or io.EOF is returned).
func (l *Lexer) read() (b byte, e error) {
	if l.eof {
		return eof, nil
	}

	defer func() {
		if b == eof || e != nil {
			return
		}
		l.loc[len(l.loc)-1]++
		if b == '\n' {
			l.loc = append(l.loc, 1)
		}
		// fmt.Println(string(b), jsonify(l.loc))
	}()

	ch, err := l.r.ReadByte()
	if err == io.EOF {
		l.eof = true
		return eof, nil
	}
	if err != nil {
		return eof, err
	}
	return ch, nil
}

func (l *Lexer) readN(n int) ([]byte, error) {
	var read []byte
	for i := 0; i < n; i++ {
		ch, err := l.read()
		if err != nil {
			return nil, err
		}
		if ch == eof {
			break
		}
		read = append(read, ch)
	}
	return read, nil
}

func (l *Lexer) peek() (byte, error) {
	peek, err := l.r.Peek(1)
	if err == io.EOF {
		return eof, nil
	}
	if err != nil {
		return eof, err
	}
	return peek[0], nil
}

// Peeks up to n bytes. If EOF is read, then the returned string may be less than length n.
func (l *Lexer) peekN(n int) ([]byte, error) {
	// peek only as far as the next EOF or ';'. Don't wait for the input to advance.
	var peek []byte
	for i := 0; i < n; i++ {
		var err error
		peek, err = l.r.Peek(i + 1)
		if err == io.EOF {
			return peek, nil
		}
		if err != nil {
			return nil, err
		}
		if peek[len(peek)-1] == ';' {
			return peek, nil
		}
	}
	return peek, nil
}

func (l *Lexer) peekAfter(n int) (byte, error) {
	peekPlusOne, err := l.peekN(n + 1)
	if err != nil {
		return eof, nil
	}
	if len(peekPlusOne) != n+1 {
		return eof, nil
	}
	return peekPlusOne[n], nil
}

// unread places the previously read byte back on the reader.
func (l *Lexer) unread() error {
	if l.eof {
		return nil
	}
	if err := l.r.UnreadByte(); err != nil {
		return err
	}
	if l.loc[len(l.loc)-1] <= 1 {
		l.loc = l.loc[:len(l.loc)-1]
	}
	l.loc[len(l.loc)-1]--
	return nil
}

func in(ch byte, list ...byte) bool {
	for _, r := range list {
		if ch == r {
			return true
		}
	}
	return false
}

func isWS(ch byte) bool {
	return unicode.IsSpace(rune(ch))
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isParen(ch byte) bool {
	return in(ch, '(', ')')
}

func isOperator(ch byte) bool {
	return in(ch, '+', '-', '/', '*', '%')
}

func isLetter(ch byte) bool {
	return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z')
}

func isIdent(ch byte) bool {
	return isLetter(ch) || isDigit(ch) || in(ch, '_')
}

func isSymbol(pattern string) bool {
	for _, sym := range symbols {
		if strings.ToUpper(pattern) == sym.str {
			return true
		}
	}
	return false
}

func isKeyword(pattern string) bool {
	for _, keyword := range keywords {
		if strings.ToUpper(pattern) == keyword {
			return true
		}
	}
	return false
}

type symbolEntry struct {
	typ TokenType
	str string
}

var (
	// Ordered longest-first so multi-char symbols match before single-char prefixes.
	symbols = []symbolEntry{
		{NEQ, "!="},
		{LTE, "<="},
		{GTE, ">="},
		{STAR, "*"},
		{COMMA, ","},
		{DOT, "."},
		{LPAREN, "("},
		{RPAREN, ")"},
		{LBRACKET, "["},
		{RBRACKET, "]"},
		{EQ, "="},
		{LT, "<"},
		{GT, ">"},
		{PLUS, "+"},
		{MINUS, "-"},
		{SLASH, "/"},
		{PERCENT, "%"},
		{SEMICOLON, ";"},
	}

	keywords = map[TokenType]string{
		SELECT:   "SELECT",
		DISTINCT: "DISTINCT",
		COUNT:    "COUNT",
		SUM:      "SUM",
		MAX:      "MAX",
		MIN:      "MIN",
		AVG:      "AVG",
		AS:       "AS",
		FROM:     "FROM",
		OVER:     "OVER",
		WHERE:    "WHERE",
		AND:      "AND",
		OR:       "OR",
		NOT:      "NOT",
		IN:       "IN",
		IS:       "IS",
		BETWEEN:  "BETWEEN",
		WITHIN:   "WITHIN",
		GROUP:    "GROUP",
		BY:       "BY",
		ORDER:    "ORDER",
		ASC:      "ASC",
		DESC:     "DESC",
		EVERY:    "EVERY",
		LIMIT:    "LIMIT",
		NULL:     "NULL",
		TRUE:     "TRUE",
		FALSE:    "FALSE",
		JOIN:     "JOIN",
		ON:       "ON",
		LEFT:     "LEFT",
		RIGHT:    "RIGHT",
		LIKE:     "LIKE",
	}

	escapeChars = map[byte]int{
		'x':  2, // followed by exactly two hexadecimal digits
		'u':  4, // followed by exactly four hexadecimal digits
		'U':  8, // followed by exactly eight hexadecimal digits
		'a':  0, // Alert or bell
		'b':  0, // Backspace
		'\\': 0, // Backslash
		't':  0, // Horizontal tab
		'n':  0, // Line feed or newline
		'f':  0, // Form feed
		'r':  0, // Carriage return
		'v':  0, // Veritical tab
		// \' and \" must be handled specially within the context of single or double quoted strings
	}
)
