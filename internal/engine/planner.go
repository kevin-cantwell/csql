package engine

import (
	"fmt"
	"strings"

	"github.com/kevin-cantwell/csql/internal/ast"
	"github.com/kevin-cantwell/csql/internal/source"
)

// BatchAccess classifies how a batch (static) source is accessed.
type BatchAccess int

const (
	AccessFullScan BatchAccess = iota // pre-load into shared static DB
	AccessIndexed                     // Go hash map, insert matching rows on demand
	AccessAttached                    // ATTACH original SQLite file directly
)

// BatchTablePlan describes how a single batch source should be loaded.
type BatchTablePlan struct {
	Access    BatchAccess
	Schema    string // SQL schema prefix ("static", "_src_<name>", or "" for indexed)
	SQLTable  string // actual table name in the schema
	JoinCol   string // batch table column (for AccessIndexed)
	StreamCol string // streaming record column (for AccessIndexed)
	AttachPath string // file path (for AccessAttached)
}

// AnalyzeBatchAccess walks the AST and classifies each batch source's access pattern.
// Only called in streaming mode where there is a clear distinction between streaming and batch sources.
func AnalyzeBatchAccess(stmt *ast.SelectStatement, sources map[string]source.Source) map[string]*BatchTablePlan {
	plan := make(map[string]*BatchTablePlan)

	// Build alias → source name mapping from the query
	aliasToSource := make(map[string]string) // alias (or name) → source name
	if stmt.From != nil {
		name := stmt.From.Table.Name
		alias := stmt.From.Table.Alias
		if alias == "" {
			alias = name
		}
		aliasToSource[alias] = name
	}
	for _, j := range stmt.Joins {
		name := j.Table.Name
		alias := j.Table.Alias
		if alias == "" {
			alias = name
		}
		aliasToSource[alias] = name
	}

	// Identify which sources are streaming vs batch
	streamingNames := make(map[string]bool)
	batchNames := make(map[string]bool)
	for name, src := range sources {
		if src.Type() == source.Streaming {
			streamingNames[name] = true
		} else {
			batchNames[name] = true
		}
	}

	// Check if source is a SQLite source with ATTACH support
	type attachable interface {
		DBPath() string
		TableName() string
	}

	// For each batch source, determine access pattern
	for name := range batchNames {
		src := sources[name]

		// Check if this is a SQLite source → always ATTACH
		if att, ok := src.(attachable); ok {
			schema := "_src_" + name
			plan[name] = &BatchTablePlan{
				Access:     AccessAttached,
				Schema:     schema,
				SQLTable:   att.TableName(),
				AttachPath: att.DBPath(),
			}
			continue
		}

		// Check if the batch source is only used in a JOIN with a simple equi-condition
		if joinPlan := findEquiJoin(stmt, name, aliasToSource, streamingNames); joinPlan != nil {
			plan[name] = joinPlan
			continue
		}

		// Default: full scan
		plan[name] = &BatchTablePlan{
			Access:   AccessFullScan,
			Schema:   "static",
			SQLTable: name,
		}
	}

	return plan
}

// findEquiJoin checks if the given batch source is only referenced in a JOIN with a
// single equi-condition (col = col) against a streaming or non-batch source.
// Returns a BatchTablePlan with AccessIndexed if found, nil otherwise.
func findEquiJoin(stmt *ast.SelectStatement, batchName string, aliasToSource map[string]string, streamingNames map[string]bool) *BatchTablePlan {
	// If the batch table is in the FROM clause, it's the primary scan target → full scan
	if stmt.From != nil && aliasToSource[stmt.From.Table.Name] == batchName {
		return nil
	}
	if stmt.From != nil && stmt.From.Table.Alias != "" && aliasToSource[stmt.From.Table.Alias] == batchName {
		return nil
	}

	// Find the JOIN clause for this batch source
	for _, j := range stmt.Joins {
		joinAlias := j.Table.Alias
		if joinAlias == "" {
			joinAlias = j.Table.Name
		}
		if aliasToSource[joinAlias] != batchName {
			continue
		}

		// Check if the ON condition is a simple equi-condition: colA = colB
		batchCol, streamCol := extractEquiJoinCols(j.Condition, joinAlias, aliasToSource, streamingNames)
		if batchCol == "" {
			return nil
		}

		return &BatchTablePlan{
			Access:    AccessIndexed,
			Schema:    "", // lives in window DB directly
			SQLTable:  batchName,
			JoinCol:   batchCol,
			StreamCol: streamCol,
		}
	}

	return nil
}

// extractEquiJoinCols extracts columns from a simple equi-condition (col = col).
// Returns (batchCol, streamCol) if found, or ("", "") otherwise.
// batchAlias is the alias (or name) used in the query for the batch table.
func extractEquiJoinCols(cond ast.Expression, batchAlias string, aliasToSource map[string]string, streamingNames map[string]bool) (string, string) {
	bin, ok := cond.(*ast.BinaryExpr)
	if !ok || bin.Op != ast.EQ {
		return "", ""
	}

	leftCol, ok := bin.Left.(*ast.ColumnRef)
	if !ok {
		return "", ""
	}
	rightCol, ok := bin.Right.(*ast.ColumnRef)
	if !ok {
		return "", ""
	}

	// Determine which side is the batch table and which is non-batch
	leftIsBatch := leftCol.Table == batchAlias
	rightIsBatch := rightCol.Table == batchAlias

	if leftIsBatch && !rightIsBatch {
		return leftCol.Column, rightCol.Column
	}
	if rightIsBatch && !leftIsBatch {
		return rightCol.Column, leftCol.Column
	}

	return "", ""
}

// BuildTableSchemas builds a tableSchemas map from a batch table plan.
func BuildTableSchemas(plan map[string]*BatchTablePlan) map[string]string {
	schemas := make(map[string]string)
	for name, p := range plan {
		schemas[name] = p.Schema
	}
	return schemas
}

// ToSQL converts an AST SelectStatement into a SQLite SQL string.
// tableSchemas maps source name → schema prefix (e.g., "static", "_src_users", or "" for window-local).
// If tableSchemas is nil, no schema prefixing is applied (batch mode).
func ToSQL(sel *ast.SelectStatement, tableSchemas map[string]string) string {
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
			b.WriteString(exprToSQL(col.Expr, tableSchemas))
			if col.Alias != "" {
				b.WriteString(" AS ")
				b.WriteString(quoteIdent(col.Alias))
			}
		}
	}

	// FROM
	if sel.From != nil {
		b.WriteString(" FROM ")
		b.WriteString(tableRefToSQL(sel.From.Table, tableSchemas))
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
		b.WriteString(tableRefToSQL(j.Table, tableSchemas))
		b.WriteString(" ON ")
		b.WriteString(exprToSQL(j.Condition, tableSchemas))
	}

	// WHERE
	if sel.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(exprToSQL(sel.Where, tableSchemas))
	}

	// GROUP BY
	if len(sel.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, expr := range sel.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(exprToSQL(expr, tableSchemas))
		}
	}

	// ORDER BY
	if len(sel.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, ob := range sel.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(exprToSQL(ob.Expr, tableSchemas))
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

func tableRefToSQL(t ast.TableRef, tableSchemas map[string]string) string {
	name := t.Name
	if schema, ok := tableSchemas[name]; ok && schema != "" {
		name = quoteIdent(schema) + "." + quoteIdent(name)
	} else {
		name = quoteIdent(name)
	}
	if t.Alias != "" {
		return name + " " + quoteIdent(t.Alias)
	}
	return name
}

// tableRefToSQLWithPlan is used when we need to substitute the actual SQL table name
// for attached sources (where DB table name may differ from source name).
func tableRefToSQLWithPlan(t ast.TableRef, tableSchemas map[string]string, plans map[string]*BatchTablePlan) string {
	name := t.Name
	if schema, ok := tableSchemas[name]; ok && schema != "" {
		sqlTable := name
		if p, ok := plans[name]; ok && p.SQLTable != "" {
			sqlTable = p.SQLTable
		}
		name = quoteIdent(schema) + "." + quoteIdent(sqlTable)
	} else {
		name = quoteIdent(name)
	}
	if t.Alias != "" {
		return name + " " + quoteIdent(t.Alias)
	}
	return name
}

// ToSQLWithPlans generates SQL using batch table plans to handle table name remapping
// for attached SQLite sources where the DB table name may differ from the source name.
func ToSQLWithPlans(sel *ast.SelectStatement, tableSchemas map[string]string, plans map[string]*BatchTablePlan) string {
	var b strings.Builder

	b.WriteString("SELECT ")
	if sel.Distinct {
		b.WriteString("DISTINCT ")
	}

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
			b.WriteString(exprToSQL(col.Expr, tableSchemas))
			if col.Alias != "" {
				b.WriteString(" AS ")
				b.WriteString(quoteIdent(col.Alias))
			}
		}
	}

	if sel.From != nil {
		b.WriteString(" FROM ")
		b.WriteString(tableRefToSQLWithPlan(sel.From.Table, tableSchemas, plans))
	}

	for _, j := range sel.Joins {
		switch j.Type {
		case ast.LeftJoin:
			b.WriteString(" LEFT JOIN ")
		case ast.RightJoin:
			b.WriteString(" RIGHT JOIN ")
		default:
			b.WriteString(" JOIN ")
		}
		b.WriteString(tableRefToSQLWithPlan(j.Table, tableSchemas, plans))
		b.WriteString(" ON ")
		b.WriteString(exprToSQL(j.Condition, tableSchemas))
	}

	if sel.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(exprToSQL(sel.Where, tableSchemas))
	}

	if len(sel.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, expr := range sel.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(exprToSQL(expr, tableSchemas))
		}
	}

	if len(sel.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, ob := range sel.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(exprToSQL(ob.Expr, tableSchemas))
			if ob.Desc {
				b.WriteString(" DESC")
			}
		}
	}

	if sel.Limit != nil {
		b.WriteString(fmt.Sprintf(" LIMIT %d", *sel.Limit))
	}

	return b.String()
}

func exprToSQL(expr ast.Expression, st map[string]string) string {
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

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
