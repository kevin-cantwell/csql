package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/kevin-cantwell/csql/internal/ast"
	"github.com/kevin-cantwell/csql/internal/engine"
	"github.com/kevin-cantwell/csql/internal/source"
)

type sourceFlag []string

func (s *sourceFlag) String() string { return strings.Join(*s, ", ") }
func (s *sourceFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func main() {
	var sources sourceFlag
	flag.Var(&sources, "source", "Data source in name=uri format (e.g., users=file://users.csv)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: csql [--source name=uri ...] 'SQL query'")
		os.Exit(1)
	}
	query := args[0]

	// Parse the query
	parser := ast.NewParser(strings.NewReader(query))
	stmts, err := parser.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse error: %v\n", err)
		os.Exit(1)
	}
	if len(stmts) == 0 || stmts[0].Select == nil {
		fmt.Fprintln(os.Stderr, "no SELECT statement found")
		os.Exit(1)
	}
	sel := stmts[0].Select

	// Build engine
	eng := engine.New(os.Stdout)

	// Parse and add explicit sources
	explicitSources := map[string]bool{}
	for _, s := range sources {
		parts := strings.SplitN(s, "=", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "invalid source: %q (expected name=uri)\n", s)
			os.Exit(1)
		}
		name, uri := parts[0], parts[1]
		cfg, err := source.ParseURI(name, uri)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid source URI: %v\n", err)
			os.Exit(1)
		}
		src, err := source.NewSource(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot create source %s: %v\n", name, err)
			os.Exit(1)
		}
		eng.AddSource(src)
		explicitSources[name] = true
	}

	// Determine which tables the query references
	tables := collectTableNames(sel)

	// Any table not in explicit sources defaults to stdin
	hasStdin := false
	for _, t := range tables {
		if !explicitSources[t] {
			if hasStdin {
				fmt.Fprintf(os.Stderr, "multiple tables reference stdin; use --source to specify\n")
				os.Exit(1)
			}
			eng.AddSource(source.NewStdinSource(t))
			hasStdin = true
		}
	}

	// Execute
	if err := eng.Execute(sel); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// collectTableNames extracts all table names referenced in a SELECT statement.
func collectTableNames(sel *ast.SelectStatement) []string {
	seen := map[string]bool{}
	var names []string

	add := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}

	if sel.From != nil {
		add(sel.From.Table.Name)
	}
	for _, j := range sel.Joins {
		add(j.Table.Name)
	}

	return names
}
