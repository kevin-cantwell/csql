package ast

// import (
// 	"fmt"
// 	"io"
// 	"path/filepath"
// 	"runtime"
// 	"strconv"
// 	"time"

// 	"github.com/pkg/errors"
// )

// func line() string {
// 	_, file, l, _ := runtime.Caller(1)
// 	file = filepath.Base(file)
// 	return fmt.Sprintf("%s:%d", file, l)
// }

// type Parser struct {
// 	lex       *Lexer
// 	pos       int
// 	scanned   []*Token
// 	unscanned []*Token
// }

// func NewParser(r io.Reader) *Parser {
// 	return &Parser{lex: NewLexer(r)}
// }

// func (p *Parser) Parse() ([]Statement, error) {
// 	var stmts []Statement

// 	for {
// 		var stmt Statement
// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}

// 		switch t.Type {
// 		case SEMICOLON:
// 			continue
// 		case EOF:
// 			return stmts, nil
// 		case SELECT:
// 			p.unscan()
// 			s, err := p.parseSelect()
// 			if err != nil {
// 				return nil, err
// 			}
// 			stmt.Select = s
// 		default:
// 			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 		}

// 		stmts = append(stmts, stmt)
// 	}
// }

// func (p *Parser) scan() (*Token, error) {
// 	var t *Token
// 	if len(p.unscanned) > 0 {
// 		t = p.unscanned[len(p.unscanned)-1]
// 		p.unscanned = p.unscanned[:len(p.unscanned)-1]
// 	} else {
// 		tok, err := p.lex.Scan()
// 		if err != nil {
// 			return nil, err
// 		}
// 		t = tok
// 	}
// 	p.scanned = append(p.scanned, t)
// 	p.pos += len(t.Raw)
// 	return t, nil
// }

// func (p *Parser) scanSkipWS() (*Token, error) {
// 	for {
// 		t, err := p.scan()
// 		if err != nil {
// 			return nil, err
// 		}
// 		if t.Type != WS {
// 			return t, nil
// 		}
// 	}
// }

// func (p *Parser) unscan() {
// 	if len(p.scanned) == 0 {
// 		return
// 	}
// 	t := p.scanned[len(p.scanned)-1]
// 	p.scanned = p.scanned[:len(p.scanned)-1]
// 	p.unscanned = append(p.unscanned, t)
// 	p.pos -= len(t.Raw)
// }

// func (p *Parser) unscanSkipWS() {
// 	for {
// 		p.unscan()
// 		if len(p.scanned) == 0 {
// 			return
// 		}
// 		if p.scanned[len(p.scanned)-1].Type != WS {
// 			return
// 		}
// 	}
// }

// func (p *Parser) parseSelect() (*Select, error) {
// 	stmt := &Select{}

// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return nil, err
// 	}

// 	// SELECT
// 	if t.Type != SELECT {
// 		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 	}

// 	if !p.hasMore() {
// 		return stmt, nil
// 	}

// 	t, err = p.scanSkipWS()
// 	if err != nil {
// 		return nil, err
// 	}

// 	// DISTINCT
// 	if t.Type == DISTINCT {
// 		stmt.Distinct = true
// 	} else {
// 		p.unscan()
// 		if !p.hasMore() {
// 			return stmt, nil
// 		}
// 	}

// 	// column, column, ...
// 	cols, err := p.parseColumns()
// 	if err != nil {
// 		return nil, err
// 	}
// 	stmt.Cols = cols

// 	if !p.hasMore() {
// 		return stmt, nil
// 	}

// 	// FROM table [, table...]
// 	from, err := p.parseFrom()
// 	if err != nil {
// 		return nil, err
// 	}
// 	stmt.From = from

// 	// TODO: FROM, WHERE, GROUP BY, LIMIT, WHEN
// 	// from, err := p.parseFrom()
// 	// if err != nil {
// 	// 	return nil, err
// 	// }
// 	// stmt.From = from

// 	for {
// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}
// 		switch t.Type {
// 		case EOF, SEMICOLON:
// 			return stmt, nil
// 		default:
// 			return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 		}
// 	}
// }

// func (p *Parser) hasMore() bool {
// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		panic(err)
// 	}
// 	p.unscan()
// 	return t.Type != EOF && t.Type != SEMICOLON
// }

// func (p *Parser) parseColumns() ([]Column, error) {
// 	var cols []Column

// 	for {
// 		col, err := p.parseColumn()
// 		if err != nil {
// 			return nil, err
// 		}
// 		cols = append(cols, *col)

// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}

// 		if t.Type != COMMA {
// 			p.unscan()
// 			return cols, nil
// 		}
// 	}
// }

// func (p *Parser) parseColumn() (*Column, error) {
// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return nil, err
// 	}

// 	// * | expression
// 	var col *Column
// 	switch t.Type {
// 	case STAR:
// 		return &Column{
// 			Star: true,
// 		}, nil
// 	default:
// 		p.unscan()
// 		col, err = p.parseSelectExpressionColumn()
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	// AS
// 	alias, err := p.parseAs()
// 	if err != nil {
// 		return nil, err
// 	}
// 	col.As = alias

// 	return col, nil
// }

// func (p *Parser) parseSelectExpressionColumn() (*Column, error) {
// 	expr, err := (&ExpressionParser{}).Parse(p)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &Column{
// 		Expr: expr,
// 	}, nil
// }

// func (p *Parser) scanComparisonOp() (*Token, error) {
// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return nil, err
// 	}
// 	switch t.Type {
// 	// case
// 	}
// 	panic("todo")
// }

// func (p *Parser) parseFrom() (*FromClause, error) {
// 	var from FromClause

// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return nil, err
// 	}
// 	if t.Type != FROM {
// 		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 	}

// 	// IDENT [AS] IDENT [, IDENT [AS] IDENT]...
// 	table, err := p.parseTable()
// 	if err != nil {
// 		return nil, err
// 	}
// 	from.Tables = append(from.Tables, *table)

// 	for {
// 		table, err := p.parseTable()
// 		if err != nil {
// 			return nil, err
// 		}
// 		from.Tables = append(from.Tables, *table)

// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return nil, err
// 		}
// 		if t.Type != COMMA {
// 			break
// 		}
// 	}

// 	over, err := p.parseOver()
// 	if err != nil {
// 		return nil, err
// 	}
// 	from.Over = over

// 	return &from, nil
// }

// func (p *Parser) parseTable() (*TableIdent, error) {
// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return nil, err
// 	}
// 	if t.Type != IDENT {
// 		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 	}
// 	ident := splitIdent(t.Raw)
// 	if len(ident) > 1 {
// 		return nil, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 	}
// 	as, err := p.parseAs()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &TableIdent{
// 		Name: ident[0],
// 		As:   as,
// 	}, nil
// }

// func (p *Parser) parseOver() (time.Duration, error) {
// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return 0, err
// 	}
// 	if t.Type != OVER {
// 		return 0, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 	}
// 	t, err = p.scanSkipWS()
// 	if err != nil {
// 		return 0, err
// 	}
// 	if t.Type != DURATION {
// 		return 0, errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 	}
// 	return time.ParseDuration(t.String())
// }

// func (p *Parser) parseAs() (string, error) {
// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return "", err
// 	}
// 	switch t.Type {
// 	case AS:
// 		alias, err := p.parseAlias()
// 		if err != nil {
// 			return "", err
// 		}
// 		return alias, nil
// 	case IDENT:
// 		p.unscan()
// 		alias, err := p.parseAlias()
// 		if err != nil {
// 			return "", err
// 		}
// 		return alias, nil
// 	default:
// 		p.unscan()
// 	}
// 	return "", nil
// }

// func (p *Parser) parseAlias() (string, error) {
// 	t, err := p.scanSkipWS()
// 	if err != nil {
// 		return "", err
// 	}
// 	if t.Type != IDENT {
// 		return "", errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 	}
// 	ident := splitIdent(t.Raw)
// 	if len(ident) > 1 {
// 		return "", errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 	}
// 	return ident[0], nil
// }

// func (p *Parser) scanSkipWSAssertNext(expected TokenType, varargs ...TokenType) error {
// 	for _, expectedType := range append([]TokenType{expected}, varargs...) {
// 		t, err := p.scanSkipWS()
// 		if err != nil {
// 			return err
// 		}
// 		if t.Type != expectedType {
// 			return errors.Errorf("unexpected token %s at line %d position %d", t, t.Line, t.Pos)
// 		}
// 	}
// 	return nil

// }

// func tokenIn(tok TokenType, in ...TokenType) bool {
// 	for _, t := range in {
// 		if tok == t {
// 			return true
// 		}
// 	}
// 	return false
// }

// // splits an dot-deliminated identifier
// func splitIdent(raw []byte) []string {
// 	if len(raw) == 0 {
// 		return nil
// 	}

// 	var split []string

// loop:
// 	for i := 0; i < len(raw); i++ {
// 		switch raw[i] {
// 		case '"':
// 			curr := []byte{'"'}
// 			i++
// 			for ; i < len(raw); i++ {
// 				ch := raw[i]
// 				switch ch {
// 				case '\\':
// 					curr = append(curr, ch)
// 					escape := raw[i+1]
// 					curr = append(curr, escape)
// 				case '"':
// 					curr = append(curr, '"')
// 					split = append(split, mustUnquote(curr))
// 					i++ // skip past the next dot
// 					continue loop
// 				default:
// 					curr = append(curr, ch)
// 				}
// 			}
// 		default:
// 			var curr []byte
// 			for ; i < len(raw); i++ {
// 				ch := raw[i]
// 				if ch == '.' {
// 					break
// 				}
// 				curr = append(curr, ch)
// 			}
// 			split = append(split, string(curr))
// 		}
// 	}

// 	return split
// }

// func mustUnquote(orig []byte) string {
// 	uq, err := unquote(orig)
// 	if err != nil {
// 		panic(errors.Errorf("%q: %+v", orig, err))
// 	}
// 	return uq
// }

// func unquote(orig []byte) (string, error) {
// 	if len(orig) == 0 {
// 		return "", nil
// 	}

// 	quote := orig[0]
// 	switch quote {
// 	case '"':
// 		uq, err := strconv.Unquote(string(orig))
// 		if err != nil {
// 			return "", err
// 		}
// 		return uq, nil
// 	case '\'':
// 		// convert single quotes to double quotes and try again
// 		dq := []byte{'"'}
// 		for i := 1; i < len(orig); i++ {
// 			ch := orig[i]
// 			switch ch {
// 			case '\\':
// 				escape := orig[i+1]
// 				if '\'' == escape {
// 					dq = append(dq, '\'')
// 					i++
// 					continue
// 				}
// 				dq = append(dq, ch)
// 			case '\'':
// 				dq = append(dq, '"')
// 			default:
// 				dq = append(dq, ch)
// 			}
// 		}
// 		return unquote(dq)
// 	default:
// 		return string(orig), nil
// 	}
// }
