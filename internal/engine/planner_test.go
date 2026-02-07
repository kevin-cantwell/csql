package engine

import (
	"strings"
	"testing"

	"github.com/kevin-cantwell/csql/internal/ast"
)

func TestToSQL(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "SELECT * FROM stdin",
			want:  `SELECT * FROM "stdin"`,
		},
		{
			input: "SELECT name FROM stdin WHERE age > 25",
			want:  `SELECT "name" FROM "stdin" WHERE ("age" > 25)`,
		},
		{
			input: "SELECT s.action, u.name FROM stdin s JOIN users u ON s.uid = u.id",
		},
	}

	for _, tt := range tests {
		p := ast.NewParser(strings.NewReader(tt.input))
		stmts, err := p.Parse()
		if err != nil {
			t.Fatalf("parse %q: %v", tt.input, err)
		}
		sql := ToSQL(stmts[0].Select, nil)
		t.Logf("input:  %s", tt.input)
		t.Logf("output: %s", sql)
		if tt.want != "" && sql != tt.want {
			t.Errorf("want: %s\ngot:  %s", tt.want, sql)
		}
	}
}
