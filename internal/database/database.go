package database

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/kevin-cantwell/csql/internal/ast"
	"github.com/pkg/errors"
)

// import (
// 	"fmt"
// 	"reflect"
// 	"strconv"
// 	"strings"

// 	"github.com/pkg/errors"
// )

// type DB struct {
// 	tables map[string]Table
// }

// func (db *DB) Insert(tableName string, tuple Tuple) {
// 	table, ok := db.tables[tableName]
// 	if !ok {
// 		table = Table{Name: tableName}
// 		db.tables[tableName] = table
// 	}
// 	table.Tuples = append(table.Tuples, tuple)
// }

// func (db *DB) Select(cols []SelectColumn, from *FromClause, where interface{}) []Tuple {
// 	join := db.nestedJoin(from, func(map[string]Tuple) bool {
// 		// TODO: apply ON and WHERE clauses here
// 		return true
// 	})
// 	return db.selectTuples(cols, join)
// }

// // walks every possible combination of tuples across all tables and returns any that satisfy
// // the predicate
// func (db *DB) nestedJoin(from *FromClause, predicate func(map[string]Tuple) bool) []Tuple {
// 	var (
// 		tables = from.Tables
// 		joined []Tuple
// 		cur    []int // relational join tuple cursor indicates the current tuple position on each table
// 	)

// 	cp := map[string]Tuple{}

// 	// iterates the relational join tuple cursor
// 	next := func() bool {
// 		if cur == nil {
// 			cur = make([]int, len(tables))
// 			return true
// 		}
// 		// walk backwards through the tables and iterate over the rhs as many times as needed
// 		for i := len(cur) - 1; i >= 0; i-- {
// 			max := len(db.tables[tables[i].Name].Tuples)
// 			if cur[i]+1 < max {
// 				cur[i]++
// 				return true
// 			}
// 			cur[i] = 0
// 		}
// 		return false
// 	}

// 	for next() {
// 		var cp Tuple
// 		for t, r := range cur {
// 			cp = append(cp, db.tables[tables[t].Name].Tuples[r]...)
// 		}
// 		if predicate(cp) {
// 			joined = append(joined, cp)
// 		}
// 	}

// 	return joined
// }

// func (db *DB) selectTuples(cols []SelectColumn, join []Tuple) [][]Pair {
// 	var result [][]Pair
// 	for _, tuple := range join {
// 		var selected []Pair
// 		for i, col := range cols {
// 			if col.Star {
// 				for _, attr := range tuple {
// 					selected = append(selected, attr.Pair)
// 				}
// 				continue
// 			}
// 			var (
// 				column = fmt.Sprintf("%d", i+1)
// 				value  interface{}
// 			)
// 			switch {
// 			case isAggregateExpr(col.Expr):
// 				// todo add tuple aggregation logic
// 			case isIdentExpr(col.Expr):
// 				e := col.Expr.(*OperandExpression)
// 				idents := splitIdent(e.Ident.Raw)
// 				switch len(idents) {
// 				case 1:
// 					for _, attr := range tuple {
// 						if attr.Pair.Column == idents[0] {
// 							value = attr.Pair.Value
// 							break
// 						}
// 					}
// 				case 2:
// 					for _, attr := range tuple {
// 						if attr.Table == idents[0] && attr.Pair.Column == idents[1] {
// 							value = attr.Pair.Value
// 							break
// 						}
// 					}
// 				default:
// 					panic(errors.Errorf("field identifiers delimited more than once are unsupported: %q", e.Ident.String()))
// 				}
// 			default:
// 				val, err := evaluateExpression(col.Expr, tuple)
// 				if err != nil {
// 					panic("TODO")
// 				}
// 				value = val
// 			}
// 			if col.As != "" {
// 				column = col.As
// 			}
// 			selected = append(selected, Pair{
// 				Column: column,
// 				Value:  value,
// 			})
// 		}
// 		result = append(result, selected)
// 	}
// 	return result
// }

type TableSchema struct {
	Name     string
	ColsMap  map[string]*ColumnSchem
	ColsIter []ColumnSchema
}

type ColumnSchema struct {
	Name     string
	Type     string
	Position int
}

type Table struct {
	Name   string
	Tuples []Tuple
}

type Tuple []Attr

type Attr struct {
	Name  string
	Value interface{}
}

// type Pair struct {
// 	Column string
// 	Value  interface{}
// }

func isAggregateExpr(expr ast.Expression) bool {
	return false
}

func isIdentExpr(expr ast.Expression) bool {
	oe, ok := expr.(*ast.OperandExpression)
	return ok && oe.Ident != nil
}

func evaluateExpression(expr ast.Expression, row Tuple) (interface{}, error) {
	var value interface{}

	switch e := expr.(type) {
	case *ast.FunctionExpression:
		panic("TODO")
	case *ast.OperatorExpression:
		left, err := evaluateExpression(e.Left, row)
		if err != nil {
			return nil, err
		}
		right, err := evaluateExpression(e.Right, row)
		if err != nil {
			return nil, err
		}
		leftType := reflect.TypeOf(left)
		rightType := reflect.TypeOf(right)
		if leftType != rightType {
			return nil, errors.Errorf("mismatched data types at line %d position %d", e.Op.Line, e.Op.Pos)
		}
		switch left.(type) {
		case float64:
			switch e.Op.Type {
			case ast.PLUS:
				return left.(float64) + right.(float64), nil
			case ast.MINUS:
				return left.(float64) - right.(float64), nil
			case ast.STAR:
				return left.(float64) * right.(float64), nil
			case ast.SLASH:
				return left.(float64) / right.(float64), nil
			case ast.PERCENT:
				return int64(left.(float64)) % int64(right.(float64)), nil
			default:
				return nil, errors.Errorf("cannot apply operator %s to numeric at line %d position %d", e.Op, e.Op.Line, e.Op.Pos)
			}
			return left.(float64) + right.(float64), nil
		case string:
			switch e.Op.Type {
			case ast.PLUS:
				return left.(string) + right.(string), nil
			default:
				return nil, errors.Errorf("cannot apply operator %s to string at line %d position %d", e.Op, e.Op.Line, e.Op.Pos)
			}
		case bool:
			return nil, errors.Errorf("cannot apply operator %s to boolean at line %d position %d", e.Op, e.Op.Line, e.Op.Pos)
		case nil:
			// Special case. Operating on nils always returns nil.
			return nil, nil
		default:
			return nil, errors.Errorf("cannot apply operator %s to %v at line %d position %d", e.Op, leftType, e.Op.Line, e.Op.Pos)
		}
	case *ast.OperandExpression:
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
			// idents := splitIdent(e.Ident.Raw)
			// switch len(idents) {
			// case 1:
			// 	for _, attr := range row {
			// 		if attr.Pair.Column == idents[0] {
			// 			return attr.Pair.Value, nil
			// 		}
			// 	}
			// 	return nil, nil
			// case 2:
			// 	// TODO: Return error if table ident doesn't exist
			// 	table, field := idents[0], idents[1]
			// 	if row.Table != table {
			// 		return nil, nil
			// 	}
			// 	return row.Data[field], nil
			// default:
			// 	return nil, errors.Errorf("field identifiers delimited more than once are unsupported: %q", e.Ident.String())
			// }
		default:
			panic("missing operand token") // TODO: return error instead
		}
	default:
		panic("todo")
	}

	return value, nil
}

type query struct {
	groupings map[string]*groupby
	// names and aliases mapped to tables
	catalog map[string]Table
}

type ident struct {
	name  string
	alias string
	typ   string
}

type groupby struct {
	group Tuple
	aggs  Tuple
}

func (by *groupby) hash() string {
	hash := make([]string, len(by.group))
	for i, attr := range by.group {
		hash[i] = fmt.Sprintf("%v", attr.Value)
	}
	return strings.Join(hash, ":")
}

type aggregation struct {
}

type tuple struct {
	aliases map[string]string
	m       map[string]map[string]int
	l       []interface{}
}

func newTuple(tables []ast.TableIdent) *tuple {
	aliases := map[string]string{}
	for _, table := range tables {
		aliases[table.Name] = table.Name
		// aliases[table.As]
	}
	tt := tuple{
		aliases: aliases,
		// m:
	}
	return &tt
}

func (t *tuple) Append(table ast.TableIdent, column string, val interface{}) {
	// t.m[table][a.Pair.Column] = len(t.l)
	// t.l = append(t.l, a)
}

// func (t *tuple) Iterate() []Attribute

func (t *tuple) Get(table, column string) interface{} {
	row, ok := t.m[table]
	if !ok {
		return nil
	}
	i, ok := row[column]
	if !ok {
		return nil
	}
	return t.l[i]
}
