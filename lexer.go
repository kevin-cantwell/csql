package csql

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/pkg/errors"
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
	eof  bool
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
		l.scanIdent,
		l.scanSymbol,
		l.scanKeyword,
	} {
		tok, err := scan()
		if err != nil {
			return nil, err
		}
		if tok != nil {
			// fmt.Println(jsonify(tok))
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
		return &Token{
			Type: STRING,
			Raw:  raw,
			Line: l.line,
			Pos:  l.pos - len(raw),
		}, nil
	default:
		return nil, nil
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

func (l *Lexer) scanIdent() (t *Token, e error) {
	// defer func() {
	// 	if t != nil {
	// 		fmt.Printf("scanIdent:%q\n", t.Raw)
	// 	} else {
	// 		fmt.Printf("scanIdent:%v\n", e)
	// 	}
	// }()
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
		return &Token{
			Type: IDENT,
			Raw:  raw,
			Line: l.line,
			Pos:  l.pos - len(raw),
		}, nil
	}
	raw = append(raw, '.')

	suffix, err := l.scanIdent()
	if suffix == nil {
		e = err
		return
	}
	raw = append(raw, suffix.Raw...)
	return &Token{
		Type: IDENT,
		Raw:  raw,
		Line: l.line,
		Pos:  l.pos - len(raw),
	}, nil
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
			return nil, errors.Errorf("mismatched quote")
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
				return nil, errors.Errorf("unkown escape sequence: '\\%s'", string(escaped))
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
	if l.eof {
		return eof, nil
	}
	ch, err := l.r.ReadByte()
	if err == io.EOF {
		l.eof = true
		l.pos++
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
	if l.eof {
		return nil
	}
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
		// DOT,
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

// map of escape characters to number of bytes in the escape sequence
var escapeChars = map[byte]int{
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
