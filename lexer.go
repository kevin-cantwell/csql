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
	r    *bufio.Reader
	line int
	pos  int
}

// NewLexer returns a new instance of Lexer.
func NewLexer(r io.Reader) *Lexer {
	return &Lexer{
		r:    bufio.NewReader(r),
		line: 1,
		pos:  1,
	}
}

// Scan returns the next token and literal value.
func (l *Lexer) Scan() (*Token, error) {
	for _, scan := range []func() (*Token, error){
		l.scanEOF,
		l.scanWS,
		l.scanString,
		l.scanNumeric,
		l.scanSymbol,
		l.scanIdent,
		l.scanKeyword,
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

	return &Token{
		Type: EOF,
		Raw:  nil,
		Line: l.line,
		Pos:  l.pos,
	}, nil
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
			return &Token{
				Type: WS,
				Raw:  raw,
				Line: l.line,
				Pos:  l.pos - len(raw),
			}, nil
		}

		if !isWS(ch) {
			if err := l.unread(); err != nil {
				return nil, err
			}
			return &Token{
				Type: WS,
				Raw:  raw,
				Line: l.line,
				Pos:  l.pos - len(raw),
			}, nil
		}

		raw = append(raw, ch)
	}
}

// scans a quoted string literal
func (l *Lexer) scanString() (*Token, error) {
	peek, err := l.peek()
	if err != nil {
		return nil, err
	}

	if peek != '\'' {
		return nil, nil
	}

	quote := peek
	prev := peek
	for i := 1; ; i++ {
		peek, err := l.peekAfter(i)
		if err != nil {
			return nil, err
		}

		switch peek {
		case eof:
			return nil, fmt.Errorf("mismatched quote")
		case quote:
			// TODO: Correctly identify single quote escape sequences (ie: \\\' should not match)
			// if this is an unescaped terminating quote, then done
			if prev != '\\' {
				raw, err := l.readN(i + 1)
				if err != nil {
					return nil, err
				}
				return &Token{
					Type: STRING,
					Raw:  raw,
					Line: l.line,
					Pos:  l.pos - len(raw),
				}, nil
			}
		}

		prev = peek
	}
}

// scans a number literal
func (l *Lexer) scanNumeric() (*Token, error) {
	peek, err := l.peek()
	if err != nil {
		return nil, err
	}

	// fail fast
	if peek != '-' && peek != '.' && !isDigit(peek) {
		return nil, nil
	}

	var (
		peeked []byte
	)

	for i := 0; ; i++ {
		peek, err := l.peekAfter(i)
		if err != nil {
			return nil, err
		}
		if peek != '-' && peek != '.' && !isDigit(peek) {
			break
		}
		peeked = append(peeked, peek)
	}

	if _, err := strconv.ParseFloat(string(peeked), 64); err != nil {
		return nil, nil
	}

	raw, err := l.readN(len(peeked))
	if err != nil {
		return nil, err
	}
	return &Token{
		Type: NUMERIC,
		Raw:  raw,
		Line: l.line,
		Pos:  l.pos - len(raw),
	}, nil
}

func (l *Lexer) scanSymbol() (*Token, error) {
	for _, symbol := range symbols {
		n := len([]byte(symbol.String()))

		peek, err := l.peekN(n)
		if err != nil {
			return nil, err
		}

		if string(peek) == symbol.String() {
			raw, err := l.readN(n)
			if err != nil {
				return nil, err
			}
			return &Token{
				Type: symbol,
				Raw:  raw,
				Line: l.line,
				Pos:  l.pos - len(raw),
			}, nil
		}
	}

	return nil, nil
}

func (l *Lexer) scanKeyword() (*Token, error) {
	for _, keyword := range keywords {
		n := len([]byte(keyword.String()))

		peek, err := l.peekN(n)
		if err != nil {
			return nil, err
		}

		// if peek doesn't match the keyword, then skip
		if strings.ToUpper(string(peek)) != keyword.String() {
			continue
		}

		// If there's more to the word, then it can't be the keyword
		ch, err := l.peekAfter(n)
		if err != nil {
			return nil, err
		}
		if !isLetter(ch) {
			// It's the keyword
			raw, err := l.readN(n)
			if err != nil {
				return nil, err
			}
			return &Token{
				Type: keyword,
				Raw:  raw,
				Line: l.line,
				Pos:  l.pos - len(raw),
			}, nil
		}

	}

	return nil, nil
}

func (l *Lexer) scanIdent() (*Token, error) {
	peek, err := l.peek()
	if err != nil {
		return nil, err
	}
	if peek == '"' {
		return l.scanQuotedIdent()
	}

	var raw []byte
	for i := 0; ; i++ {
		peek, err := l.peekAfter(i)
		if err != nil {
			return nil, err
		}
		if isIdent(peek) {
			raw = append(raw, peek)
			continue
		}
		if i > 0 {
			if isKeyword(string(raw)) {
				return nil, nil
			}
			raw, err := l.readN(i)
			if err != nil {
				return nil, err
			}
			return &Token{
				Type: IDENT,
				Raw:  raw,
				Line: l.line,
				Pos:  l.pos - len(raw),
			}, nil
		}
		return nil, nil
	}
}

func (l *Lexer) scanQuotedIdent() (*Token, error) {
	quote, err := l.peek()
	if err != nil {
		return nil, err
	}
	if quote != '"' {
		return nil, nil
	}

	prev := quote
	for i := 1; ; i++ {
		peek, err := l.peekAfter(i)
		if err != nil {
			return nil, err
		}

		switch peek {
		case eof:
			if err != nil {
				return nil, fmt.Errorf("mismatched quote")
			}
			return nil, nil
		case quote:
			// if this is an unescaped terminating quote, then done
			if prev != '\\' {
				raw, err := l.readN(i + 1)
				if err != nil {
					return nil, err
				}
				return &Token{
					Type: IDENT,
					Raw:  raw,
					Line: l.line,
					Pos:  l.pos - len(raw),
				}, nil
			}
		}

		prev = peek
	}
}

func (l *Lexer) scanIllegal() (*Token, error) {
	ch, err := l.read()
	if err != nil {
		return nil, err
	}

	return &Token{
		Type: ILLEGAL,
		Raw:  []byte{ch},
		Line: l.line,
		Pos:  l.pos - 1,
	}, nil
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
	if ch == '\n' {
		l.line++
		l.pos = 1
	} else {
		l.pos++
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
	if err := l.r.UnreadByte(); err != nil {
		return err
	}
	l.pos--
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

func isSymbol(str string) bool {
	for _, symbol := range symbols {
		if strings.ToUpper(str) == symbol.String() {
			return true
		}
	}
	return false
}

func isKeyword(str string) bool {
	for _, keyword := range keywords {
		if strings.ToUpper(str) == keyword.String() {
			return true
		}
	}
	return false
}

var (
	symbols = []TokenType{
		ASTERISK,
		COMMA,
		DOT,
		LPAREN,
		RPAREN,
		LBRACKET,
		RBRACKET,
		EQ,
		NEQ,
		LT,
		LTE,
		GT,
		GTE,
		PLUS,
		MINUS,
		SLASH,
		PERCENT,
		SEMICOLON,
	}

	keywords = []TokenType{
		SELECT,
		DISTINCT,
		COUNT,
		SUM,
		MAX,
		MIN,
		AVG,
		AS,
		FROM,
		CROSS_JOIN,
		INNER_JOIN,
		LEFT_JOIN,
		LEFT_OUTER_JOIN,
		RIGHT_JOIN,
		RIGHT_OUTER_JOIN,
		FULL_OUTER_JOIN,
		ON,
		WHERE,
		AND,
		OR,
		NOT,
		IN,
		IS,
		BETWEEN,
		WITHIN,
		GROUP_BY,
		EVERY,
		LIMIT,
		NULL,
		TRUE,
		FALSE,
	}
)
