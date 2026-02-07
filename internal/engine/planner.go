package engine

import (
	"fmt"
	"strings"

	"github.com/kevin-cantwell/csql/internal/ast"
)

// ToSQL converts an AST SelectStatement into a SQLite SQL string.
// staticTables is the set of table names that live in the "static" attached DB.
func ToSQL(sel *ast.SelectStatement, staticTables map[string]bool) string {
	var b strings.Builder

	b.WriteString("SELECT ")
	if sel.Distinct {
		b.WriteString("DISTINCT ")
	}

	// Columns
	for i, col := range sel.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		if col.Star {
			if col.TableRef != "" {
				b.WriteString(col.TableRef)
				b.WriteString(".*")
			} else {
				b.WriteString("*")
			}
		} else {
			b.WriteString(exprToSQL(col.Expr, staticTables))
			if col.Alias != "" {
				b.WriteString(" AS ")
				b.WriteString(quoteIdent(col.Alias))
			}
		}
	}

	// FROM
	if sel.From != nil {
		b.WriteString(" FROM ")
		b.WriteString(tableRefToSQL(sel.From.Table, staticTables))
	}

	// JOINs
	for _, j := range sel.Joins {
		switch j.Type {
		case ast.LeftJoin:
			b.WriteString(" LEFT JOIN ")
		case ast.RightJoin:
			b.WriteString(" RIGHT JOIN ")
		default:
			b.WriteString(" JOIN ")
		}
		b.WriteString(tableRefToSQL(j.Table, staticTables))
		b.WriteString(" ON ")
		b.WriteString(exprToSQL(j.Condition, staticTables))
	}

	// WHERE
	if sel.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(exprToSQL(sel.Where, staticTables))
	}

	// GROUP BY
	if len(sel.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, expr := range sel.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(exprToSQL(expr, staticTables))
		}
	}

	// ORDER BY
	if len(sel.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, ob := range sel.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(exprToSQL(ob.Expr, staticTables))
			if ob.Desc {
				b.WriteString(" DESC")
			}
		}
	}

	// LIMIT
	if sel.Limit != nil {
		b.WriteString(fmt.Sprintf(" LIMIT %d", *sel.Limit))
	}

	return b.String()
}

func tableRefToSQL(t ast.TableRef, staticTables map[string]bool) string {
	name := t.Name
	if staticTables[name] {
		name = "static." + quoteIdent(name)
	} else {
		name = quoteIdent(name)
	}
	if t.Alias != "" {
		return name + " " + quoteIdent(t.Alias)
	}
	return name
}

func exprToSQL(expr ast.Expression, st map[string]bool) string {
	switch e := expr.(type) {
	case *ast.BinaryExpr:
		left := exprToSQL(e.Left, st)
		right := exprToSQL(e.Right, st)
		op := tokenToSQLOp(e.Op)
		return fmt.Sprintf("(%s %s %s)", left, op, right)

	case *ast.UnaryExpr:
		operand := exprToSQL(e.Operand, st)
		if e.Op == ast.NOT {
			return fmt.Sprintf("(NOT %s)", operand)
		}
		return fmt.Sprintf("(-%s)", operand)

	case *ast.ColumnRef:
		if e.Table != "" {
			return quoteIdent(e.Table) + "." + quoteIdent(e.Column)
		}
		return quoteIdent(e.Column)

	case *ast.LiteralExpr:
		switch e.Type {
		case ast.STRING:
			return e.Value // already quoted with single quotes
		case ast.NUMERIC:
			return e.Value
		case ast.TRUE:
			return "1"
		case ast.FALSE:
			return "0"
		case ast.NULL:
			return "NULL"
		}

	case *ast.StarExpr:
		return "*"

	case *ast.FunctionExpr:
		var args []string
		for _, arg := range e.Args {
			args = append(args, exprToSQL(arg, st))
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))

	case *ast.IsNullExpr:
		inner := exprToSQL(e.Expr, st)
		if e.Not {
			return fmt.Sprintf("(%s IS NOT NULL)", inner)
		}
		return fmt.Sprintf("(%s IS NULL)", inner)

	case *ast.InExpr:
		inner := exprToSQL(e.Expr, st)
		var vals []string
		for _, v := range e.Values {
			vals = append(vals, exprToSQL(v, st))
		}
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("(%s %sIN (%s))", inner, not, strings.Join(vals, ", "))

	case *ast.BetweenExpr:
		inner := exprToSQL(e.Expr, st)
		low := exprToSQL(e.Low, st)
		high := exprToSQL(e.High, st)
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("(%s %sBETWEEN %s AND %s)", inner, not, low, high)

	case *ast.LikeExpr:
		inner := exprToSQL(e.Expr, st)
		pattern := exprToSQL(e.Pattern, st)
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("(%s %sLIKE %s)", inner, not, pattern)
	}

	return "?"
}

func tokenToSQLOp(t ast.TokenType) string {
	switch t {
	case ast.EQ:
		return "="
	case ast.NEQ:
		return "!="
	case ast.LT:
		return "<"
	case ast.LTE:
		return "<="
	case ast.GT:
		return ">"
	case ast.GTE:
		return ">="
	case ast.PLUS:
		return "+"
	case ast.MINUS:
		return "-"
	case ast.STAR:
		return "*"
	case ast.SLASH:
		return "/"
	case ast.PERCENT:
		return "%"
	case ast.AND:
		return "AND"
	case ast.OR:
		return "OR"
	case ast.LIKE:
		return "LIKE"
	default:
		return "?"
	}
}

func quoteIdent(s string) string {
	if s == "*" {
		return "*"
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
