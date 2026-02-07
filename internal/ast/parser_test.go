package ast

import (
	"strings"
	"testing"
)

func TestParseBasicSelect(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(*testing.T, *SelectStatement)
	}{
		{
			name:  "select star",
			input: "SELECT * FROM stdin",
			check: func(t *testing.T, sel *SelectStatement) {
				if len(sel.Columns) != 1 || !sel.Columns[0].Star {
					t.Errorf("expected single star column, got %+v", sel.Columns)
				}
				if sel.From == nil || sel.From.Table.Name != "stdin" {
					t.Errorf("expected FROM stdin, got %+v", sel.From)
				}
			},
		},
		{
			name:  "select column with where",
			input: "SELECT name FROM stdin WHERE age > 25",
			check: func(t *testing.T, sel *SelectStatement) {
				if len(sel.Columns) != 1 {
					t.Fatalf("expected 1 column, got %d", len(sel.Columns))
				}
				ref, ok := sel.Columns[0].Expr.(*ColumnRef)
				if !ok || ref.Column != "name" {
					t.Errorf("expected column ref 'name', got %+v", sel.Columns[0].Expr)
				}
				if sel.Where == nil {
					t.Error("expected WHERE clause")
				}
			},
		},
		{
			name:  "select with join",
			input: "SELECT s.action, u.name FROM stdin s JOIN users u ON s.uid = u.id",
			check: func(t *testing.T, sel *SelectStatement) {
				if len(sel.Columns) != 2 {
					t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
				}
				if len(sel.Joins) != 1 {
					t.Fatalf("expected 1 join, got %d", len(sel.Joins))
				}
				if sel.Joins[0].Table.Name != "users" {
					t.Errorf("expected join table 'users', got %q", sel.Joins[0].Table.Name)
				}
			},
		},
		{
			name:  "select with group by",
			input: "SELECT status, COUNT(*) FROM stdin GROUP BY status",
			check: func(t *testing.T, sel *SelectStatement) {
				if len(sel.Columns) != 2 {
					t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
				}
				if len(sel.GroupBy) != 1 {
					t.Fatalf("expected 1 group by, got %d", len(sel.GroupBy))
				}
			},
		},
		{
			name:  "select with over and every",
			input: "SELECT status, COUNT(*) FROM stdin GROUP BY status OVER 5m EVERY 10s",
			check: func(t *testing.T, sel *SelectStatement) {
				if sel.Over.String() != "5m0s" {
					t.Errorf("expected OVER 5m, got %v", sel.Over)
				}
				if sel.Every.String() != "10s" {
					t.Errorf("expected EVERY 10s, got %v", sel.Every)
				}
			},
		},
		{
			name:  "count star with alias",
			input: "SELECT COUNT(*) AS cnt FROM stdin",
			check: func(t *testing.T, sel *SelectStatement) {
				if len(sel.Columns) != 1 {
					t.Fatalf("expected 1 column, got %d", len(sel.Columns))
				}
				if sel.Columns[0].Alias != "cnt" {
					t.Errorf("expected alias 'cnt', got %q", sel.Columns[0].Alias)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tt.input))
			stmts, err := p.Parse()
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if len(stmts) != 1 || stmts[0].Select == nil {
				t.Fatalf("expected 1 SELECT statement, got %d statements", len(stmts))
			}
			tt.check(t, stmts[0].Select)
		})
	}
}
