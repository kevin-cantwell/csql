package ast

import (
	"strings"
	"testing"
)

func TestLexer(t *testing.T) {
	input := "SELECT name FROM stdin WHERE age > 25"
	l := NewLexer(strings.NewReader(input))
	for {
		tok, err := l.Scan()
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if tok.Type == EOF {
			break
		}
		if tok.Type == WS {
			continue
		}
		t.Logf("%-10s %q  line=%d pos=%d", tok.Type, string(tok.Raw), tok.Line, tok.Pos)
	}
}

func TestLexerSymbolOrder(t *testing.T) {
	// Test that >= is lexed as GTE, not as GT followed by EQ
	input := "age >= 25"
	l := NewLexer(strings.NewReader(input))
	var tokens []*Token
	for {
		tok, err := l.Scan()
		if err != nil {
			t.Fatalf("scan error: %v", err)
		}
		if tok.Type == EOF {
			break
		}
		if tok.Type == WS {
			continue
		}
		tokens = append(tokens, tok)
	}
	if len(tokens) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(tokens))
	}
	if tokens[1].Type != GTE {
		t.Errorf("expected GTE, got %s (%q)", tokens[1].Type, string(tokens[1].Raw))
	}
}
