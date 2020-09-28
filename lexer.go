package csql

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
)

// eof represents a marker byte for the end of the reader.
const (
	eof = byte(0)
)

// Lexer represents a lexical scanner.
type Lexer struct {
	r   *bufio.Reader
	buf *tokenBuf
	pos int
	tok Token
	raw []byte
	err error
}

// NewLexer returns a new instance of Lexer.
func NewLexer(r io.Reader) *Lexer {
	return &Lexer{
		r:   bufio.NewReader(r),
		buf: &tokenBuf{},
	}
}

// Scan returns the next token and literal value.
func (l *Lexer) Scan() (Token, []byte, error) {
	for _, scan := range []func() bool{
		l.scanEOF,
		l.scanWS,
		l.scanString,
		l.scanNumeric,
		l.scanSymbol,
		l.scanKeyword,
		l.scanIdent,
	} {
		if scan() {
			return l.tok, l.raw, nil
		}
	}
	if l.err != nil {
		return ILLEGAL, nil, l.err
	}
	return ILLEGAL, nil, fmt.Errorf("No legal token found at position %d", l.pos)
}

func (l *Lexer) scanEOF() bool {
	ch := l.read()

	if ch != eof {
		l.unread()
		return false
	}

	l.push(EOF, []byte{eof})
	return true
}

func (l *Lexer) scanWS() bool {
	ch := l.read()

	if !isWS(ch) {
		l.unread()
		return false
	}

	l.push(WS, []byte{ch})
	return true
}

// scans a quoted string literal
func (l *Lexer) scanString() bool {
	peek := l.peek()

	switch peek {
	case '\'', '"':
	default:
		return false
	}

	quote := peek
	prev := peek
	for i := 1; ; i++ {
		peek := l.peekAfter(i)

		switch peek {
		case eof:
			if l.err == nil {
				l.err = fmt.Errorf("Unterminated quote at position %d", l.pos)
			}
			return false
		case quote:
			// if this is an unescaped terminating quote, then done
			if prev != '\\' {
				raw := l.readN(i + 1)
				l.push(STRING, raw)
				return true
			}
		}

		prev = peek
	}
}

// scans a number literal
func (l *Lexer) scanNumeric() bool {
	peek := l.peek()

	// fail fast
	if peek != '-' && peek != '.' && !isDigit(peek) {
		return false
	}

	// numbers cannot directly follow keywords or idents
	prev := l.tok
	switch {
	case isKeywordTok(prev), prev == IDENT:
		return false
	}

	var (
		peeked []byte
	)

	for i := 0; ; i++ {
		peek := l.peekAfter(i)
		if peek != '-' && peek != '.' && !isDigit(peek) {
			break
		}
		peeked = append(peeked, peek)
	}

	_, err := strconv.ParseFloat(string(peeked), 64)
	if err != nil {
		return false
	}

	raw := l.readN(len(peeked))
	l.push(NUMERIC, raw)
	return true
}

func (l *Lexer) scanSymbol() bool {
	ch := l.read()

	tok, ok := symbols[ch]
	if !ok {
		l.unread()
		return false
	}

	l.push(tok, []byte{ch})
	return true
}

func (l *Lexer) scanKeyword() bool {
	// fail fast
	peek := l.peek()
	if !isLetter(peek) {
		return false
	}

	for _, keyword := range keywords {
		n := len([]byte(keyword.String()))

		peek := l.peekN(n)

		// if these bytes explicitely don't match the keyword, then skip
		if strings.ToUpper(string(peek)) != keyword.String() {
			continue
		}

		/*
			At this point we have a string of characters that appears to match a keyword.
		*/

		// functions must be followed by a LPAREN
		if isFunctionTok(keyword) {
			lparen := l.peekAfterSkipWS(n)
			if lparen != '(' {
				return false
			}
			l.push(keyword, l.readN(n))
			return true
		}

		// examine the next byte after peek
		ch := l.peekAfter(n)
		switch {
		// Only IDENTS and NUMERIC chars may followed by a dot
		case ch == '.':
			return false
		case ch == eof, isWS(ch), isSymbol(ch), isQuote(ch):
			l.push(keyword, l.readN(n))
			return true
		}
	}

	return false
}

func (l *Lexer) scanIdent() bool {
	peek := l.peek()
	if peek == '`' {
		return l.scanBacktickedIdent()
	}

	var raw []byte
	for i := 0; ; i++ {
		peek := l.peekAfter(i)
		if isIdent(peek) {
			raw = append(raw, peek)
			continue
		}
		if len(raw) > 0 {
			l.push(IDENT, l.readN(len(raw)))
			return true
		}
		return false
	}
}

func (l *Lexer) scanBacktickedIdent() bool {
	if l.peek() != '`' {
		return false
	}

	prev := byte('`')
	for i := 1; ; i++ {
		peek := l.peekAfter(i)

		switch peek {
		case eof:
			if l.err == nil {
				l.err = fmt.Errorf("Unterminated quote at position %d", l.pos)
			}
			return false
		case '`':
			// if this is an unescaped terminating quote, then done
			if prev != '\\' {
				raw := l.readN(i + 1)
				l.push(IDENT, raw)
				return true
			}
		}

		prev = peek
	}
}

// read reads the next byte from the buffered reader.
// Returns the byte(0) if an error occurs (or io.EOF is returned).
func (l *Lexer) read() byte {
	ch, err := l.r.ReadByte()
	if err == io.EOF {
		return eof
	}
	if err != nil {
		l.err = err
		return eof
	}
	l.pos++
	return ch
}

func (l *Lexer) readN(n int) []byte {
	var read []byte
	for i := 0; i < n; i++ {
		ch := l.read()
		if ch == eof {
			break
		}
		read = append(read, ch)
	}
	return read
}

func (l *Lexer) peek() byte {
	peek, err := l.r.Peek(1)
	if err != nil {
		return eof
	}
	return peek[0]
}

// Peeks up to n bytes. If EOF is read, then the returned string may be less than length n.
func (l *Lexer) peekN(n int) []byte {
	peek, err := l.r.Peek(n)
	if err != nil && err != io.EOF {
		l.err = err
		return nil
	}
	return peek
}

func (l *Lexer) peekAfter(n int) byte {
	peekPlusOne := l.peekN(n + 1)
	if len(peekPlusOne) != n+1 {
		return eof
	}
	return peekPlusOne[n]
}

func (l *Lexer) peekAfterSkipWS(n int) byte {
	ch := l.peekAfter(n)
	if isWS(ch) {
		return l.peekAfterSkipWS(n + 1)
	}
	return ch
}

// unread places the previously read byte back on the reader.
func (l *Lexer) unread() {
	if err := l.r.UnreadByte(); err != nil {
		l.err = fmt.Errorf("unread: %+v", err)
		return
	}
	l.pos--
}

func (l *Lexer) push(tok Token, raw []byte) {
	l.tok = tok
	l.raw = raw
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

func isQuote(ch byte) bool {
	return in(ch, '\'', '"', '`')
}

func isParen(ch byte) bool {
	return in(ch, '(', ')')
}

func isOperator(ch byte) bool {
	return in(ch, '+', '-', '/', '*', '%')
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}

func isIdent(ch byte) bool {
	return isLetter(ch) || isDigit(ch) || in(ch, '_')
}

func isSymbol(ch byte) bool {
	_, ok := symbols[ch]
	return ok
}

func isFunctionTok(keyword Token) bool {
	for _, function := range functions {
		if keyword == function {
			return true
		}
	}
	return false
}

func isKeywordTok(tok Token) bool {
	for _, keyword := range keywords {
		if tok == keyword {
			return true
		}
	}
	return false
}

var (
	symbols = map[byte]Token{
		'*': ASTERISK,
		',': COMMA,
		'.': DOT,
		'(': LPAREN,
		')': RPAREN,
		'[': LBRACKET,
		']': RBRACKET,
		'!': EXCLAIM,
		'=': EQUALS,
		'<': LT,
		'>': GT,
		'+': PLUS,
		'-': MINUS,
		'/': SLASH,
		'%': PERCENT,
		';': SEMICOLON,
	}

	functions = []Token{
		COUNT,
		SUM,
		MAX,
		MIN,
		AVG,
	}

	keywords = []Token{
		SELECT,
		DISTINCT,
		COUNT,
		SUM,
		MAX,
		MIN,
		AVG,
		AS,
		FROM,
		WHERE,
		AND,
		OR,
		NOT,
		IN,
		IS,
		BETWEEN,
		WITHIN,
		GROUP,
		BY,
		LIMIT,
		NULL,
		TRUE,
		FALSE,
	}
)
