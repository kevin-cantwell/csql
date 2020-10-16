package csql

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Database struct {
	mu          sync.Mutex
	subscribers []*subscriber
}

func (db *Database) Exec(ctx context.Context, stmt *Statement) <-chan Row {
	results := make(chan Row)
	go func() {
		defer close(results)

		tables := db.tablesFrom(stmt.Select.From)

		for row := range db.subscribeAll(ctx, tables) {
			row := db.transformRow(ctx, stmt.Select.Cols, row)
			results <- row // TODO: This may block on slow clients. Detect and handle.
		}
	}()
	return results
}

func (db *Database) tablesFrom(from *FromClause) []string {
	panic("TODO")
	// var tables []string
	// var expr *TablesExpression
	// expr = &from.Tables // todo: nil check
	// for expr != nil {
	// 	idents := splitIdent(expr.Ident.Raw)
	// 	switch len(idents) {
	// 	case 1:
	// 		tables = append(tables, idents[0])
	// 	default:
	// 		panic("deliminated table identifiers not supported")
	// 	}
	// 	expr = expr.CrossJoin
	// }
	// return tables
}

func (db *Database) subscribeAll(ctx context.Context, names []string) <-chan Row {
	var subs []<-chan Row
	for _, name := range names {
		subs = append(subs, db.Subscribe(ctx, name))
	}
	return merge(ctx, subs)
}

func (db *Database) transformRow(ctx context.Context, cols []SelectColumn, row Row) Row {
	data := Data{}
	for i, col := range cols {
		var (
			name  string
			value interface{}
		)
		if col.Star {
			for k, v := range row.Data {
				data[k] = v
			}
			continue
		}
		value, err := evaluateExpression(col.Expr, row)
		if err != nil {
			panic(err)
		}
		if e, ok := col.Expr.(*OperandExpression); ok && e.Ident != nil {
			idents := splitIdent(e.Ident.Raw)
			name = idents[len(idents)-1]
		}
		if col.As != "" {
			name = col.As
		}
		if name == "" {
			name = fmt.Sprintf("column%d", i+1)
		}
		data[name] = value
	}
	return Row{
		Table: row.Table,
		TS:    row.TS,
		Data:  data,
	}
}

func (db *Database) Publish(ctx context.Context, name string, data Data) int {
	ts := time.Now().UnixNano()

	db.mu.Lock()
	subs := db.subscribers
	db.mu.Unlock()

	var sent int
	for _, sub := range subs {
		if matches, _ := filepath.Match(sub.pattern, name); !matches {
			continue
		}
		select {
		case <-ctx.Done():
			return sent
		case sub.buf <- Row{Table: name, TS: ts, Data: data}:
			sent++
		case <-sub.closed:
		}
	}
	return sent
}

func (db *Database) Subscribe(ctx context.Context, pattern string) <-chan Row {
	newSub := &subscriber{
		pattern: pattern,
		buf:     make(chan Row, math.MaxInt32), // big fucker
		closed:  make(chan struct{}),
	}
	results := make(chan Row)
	go func() {
		defer func() {
			db.mu.Lock()
			defer db.mu.Unlock()

			close(newSub.closed)
			close(results)

			for i, sub := range db.subscribers {
				if newSub == sub {
					db.subscribers = append(db.subscribers[:i], db.subscribers[i+1:]...)
					return
				}
			}
		}()
		for row := range newSub.buf {
			select {
			case <-ctx.Done():
				return
			case results <- row: // TODO: This may block on slow clients. Detect and handle.
			}
		}
	}()
	return results
}

type Row struct {
	Table string `json:"stream"`
	TS    int64  `json:"ts"`
	Data  Data   `json:"data"`
	Err   error  `json:"error"`
}

type Data map[string]interface{}

type subscriber struct {
	pattern string
	buf     chan Row
	closed  chan struct{}
}

func merge(ctx context.Context, streams []<-chan Row) <-chan Row {
	results := make(chan Row)
	for _, stream := range streams {
		stream := stream
		go func() {
			defer close(results)
			for row := range stream {
				select {
				case <-ctx.Done():
					return
				case results <- row:
				}
			}
		}()
	}
	return results
}

func evaluateExpression(expr Expression, row Row) (interface{}, error) {
	switch e := expr.(type) {
	case *FunctionExpression:
		switch e.Func.Type {
		case COUNT:
		case SUM:
			// case
		}
	case *OperandExpression:
		switch {
		case e.Numeric != nil:
			return strconv.ParseFloat(string(e.Numeric.String()), 64)
		case e.Boolean != nil:
			return strconv.ParseBool(e.Boolean.String())
		case e.String != nil:
			return e.String.String(), nil
		case e.Null != nil:
			return nil, nil
		case e.Ident != nil:
			idents := splitIdent(e.Ident.Raw)
			switch len(idents) {
			case 1:
				return row.Data[idents[0]], nil
			case 2:
				table, field := idents[0], idents[1]
				if row.Table != table {
					return nil, nil
				}
				return row.Data[field], nil
			default:
				return nil, errors.Errorf("field identifiers delimited more than once are unsupported: %q", e.Ident.String())
			}
		default:
			panic("missing operand token") // TODO: return error instead
		}
	}
	panic("todo")
}
