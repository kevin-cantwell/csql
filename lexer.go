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
	tok Token
	raw []byte
	pos int
	err error
}

// NewLexer returns a new instance of Lexer.
func NewLexer(r io.Reader) *Lexer {
	return &Lexer{
		r: bufio.NewReader(r),
	}
}

// Scan returns the next token and literal value.
func (l *Lexer) Scan() (Token, []byte, error) {
	for _, scan := range []func() (Token, []byte, error){
		l.scanEOF,
		l.scanWS,
		l.scanString,
		l.scanNumeric,
		l.scanSymbol,
		l.scanKeyword,
		l.scanIdent,
	} {
		tok, raw, err := scan()
		if err != nil {
			peek, _ := l.peekN(10)
			return ERROR, nil, fmt.Errorf("error at position %d: %q: %v", l.pos, peek, err)
		}
		if tok == SKIP {
			continue
		}
		return tok, raw, nil
	}
	peek, _ := l.peekN(10)
	return ERROR, nil, fmt.Errorf("illegal token found at position %d: %q", l.pos, peek)
}

func (l *Lexer) scanEOF() (Token, []byte, error) {
	ch, err := l.read()
	if err != nil {
		return ERROR, nil, err
	}

	if ch != eof {
		if err := l.unread(); err != nil {
			return ERROR, nil, err
		}
		return SKIP, nil, nil
	}

	return EOF, []byte{eof}, nil
}

func (l *Lexer) scanWS() (Token, []byte, error) {
	ch, err := l.read()
	if err != nil {
		return ERROR, nil, err
	}

	if !isWS(ch) {
		if err := l.unread(); err != nil {
			return ERROR, nil, err
		}
		return SKIP, nil, nil
	}

	raw := []byte{ch}

	for {
		ch, err := l.read()
		if err != nil {
			return ERROR, nil, err
		}

		if ch == eof {
			return WS, raw, nil
		}

		if !isWS(ch) {
			if err := l.unread(); err != nil {
				return ERROR, nil, err
			}
			return WS, raw, nil
		}

		raw = append(raw, ch)
	}
}

// scans a quoted string literal
func (l *Lexer) scanString() (Token, []byte, error) {
	peek, err := l.peek()
	if err != nil {
		return ERROR, nil, err
	}

	switch peek {
	case '\'', '"':
	default:
		return SKIP, nil, nil
	}

	quote := peek
	prev := peek
	for i := 1; ; i++ {
		peek, err := l.peekAfter(i)
		if err != nil {
			return ERROR, nil, err
		}

		switch peek {
		case eof:
			return ERROR, nil, fmt.Errorf("Unterminated quote")
		case quote:
			// if this is an unescaped terminating quote, then done
			if prev != '\\' {
				raw, err := l.readN(i + 1)
				if err != nil {
					return ERROR, nil, err
				}
				return STRING, raw, nil
			}
		}

		prev = peek
	}
}

// scans a number literal
func (l *Lexer) scanNumeric() (Token, []byte, error) {
	peek, err := l.peek()
	if err != nil {
		return ERROR, nil, err
	}

	// fail fast
	if peek != '-' && peek != '.' && !isDigit(peek) {
		return SKIP, nil, nil
	}

	// // numbers cannot directly follow keywords or idents
	// prev := l.tok
	// switch {
	// case isKeywordTok(prev), prev == IDENT:
	// 	return SKIP, nil, nil
	// }

	var (
		peeked []byte
	)

	for i := 0; ; i++ {
		peek, err := l.peekAfter(i)
		if err != nil {
			return ERROR, nil, err
		}
		if peek != '-' && peek != '.' && !isDigit(peek) {
			break
		}
		peeked = append(peeked, peek)
	}

	if _, err := strconv.ParseFloat(string(peeked), 64); err != nil {
		return SKIP, nil, nil
	}

	raw, err := l.readN(len(peeked))
	if err != nil {
		return ERROR, nil, err
	}
	return NUMERIC, raw, nil
}

func (l *Lexer) scanSymbol() (Token, []byte, error) {
	ch, err := l.peek()
	if err != nil {
		return ERROR, nil, err
	}

	if _, ok := symbols[ch]; !ok {
		return SKIP, nil, nil
	}

	ch, err = l.read()
	if err != nil {
		return ERROR, nil, err
	}
	return symbols[ch], []byte{ch}, nil
}

func (l *Lexer) scanKeyword() (Token, []byte, error) {
	// fail fast
	peek, err := l.peek()
	if err != nil {
		return ERROR, nil, err
	}

	if !isLetter(peek) {
		return SKIP, nil, nil
	}

	for _, keyword := range keywords {
		n := len([]byte(keyword.String()))

		peek, err := l.peekN(n)
		if err != nil {
			return ERROR, nil, err
		}

		// if these bytes explicitely don't match the keyword, then skip
		if strings.ToUpper(string(peek)) != keyword.String() {
			continue
		}

		/*
			At this point we have a string of characters that appears to match a keyword.
		*/

		// examine the next byte after peek
		ch, err := l.peekAfter(n)
		if err != nil {
			return ERROR, nil, err
		}
		switch {
		// Only IDENTS and NUMERIC chars may followed by a dot
		case ch == '.':
			return SKIP, nil, nil
		case ch == eof, isWS(ch), isSymbol(ch), isQuote(ch):
			raw, err := l.readN(n)
			if err != nil {
				return ERROR, nil, err
			}
			return keyword, raw, nil
		}
	}

	return SKIP, nil, nil
}

func (l *Lexer) scanIdent() (Token, []byte, error) {
	peek, err := l.peek()
	if err != nil {
		return ERROR, nil, err
	}
	if peek == '`' {
		return l.scanBacktickedIdent()
	}

	var raw []byte
	for i := 0; ; i++ {
		peek, err := l.peekAfter(i)
		if err != nil {
			return ERROR, nil, err
		}
		if isIdent(peek) {
			raw = append(raw, peek)
			continue
		}
		if i > 0 {
			raw, err := l.readN(i)
			if err != nil {
				return ERROR, nil, err
			}
			return IDENT, raw, nil
		}
		return SKIP, nil, nil
	}
}

func (l *Lexer) scanBacktickedIdent() (Token, []byte, error) {
	tick, err := l.peek()
	if err != nil {
		return ERROR, nil, err
	}
	if tick != '`' {
		return SKIP, nil, nil
	}

	prev := tick
	for i := 1; ; i++ {
		peek, err := l.peekAfter(i)
		if err != nil {
			return ERROR, nil, err
		}

		switch peek {
		case eof:
			if err != nil {
				return ERROR, nil, fmt.Errorf("Non-terminated quote")
			}
			return SKIP, nil, nil
		case tick:
			// if this is an unescaped terminating quote, then done
			if prev != '\\' {
				raw, err := l.readN(i + 1)
				if err != nil {
					return ERROR, nil, err
				}
				return IDENT, raw, nil
			}
		}

		prev = peek
	}
}

// read reads the next byte from the buffered reader.
// Returns the byte(0) if an error occurs (or io.EOF is returned).
func (l *Lexer) read() (byte, error) {
	ch, err := l.r.ReadByte()
	if err == io.EOF {
		return eof, nil
	}
	if err != nil {
		return eof, err
	}
	l.pos++
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
	peek, err := l.r.Peek(n)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
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
	if err := l.r.UnreadByte(); err != nil {
		return err
	}
	l.pos--
	return nil
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
