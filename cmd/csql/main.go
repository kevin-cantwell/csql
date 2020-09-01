package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kevin-cantwell/csql"

	"github.com/chzyer/readline"
)

func doSelect(backend csql.Backend, slct *csql.SelectStatement) error {
	results, err := backend.Select(slct)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(os.Stdout)

	for resultRow := range results.Rows {
		jsonl := map[string]interface{}{}

		for i, cell := range resultRow {
			var (
				name = results.Columns[i].Name
				typ  = results.Columns[i].Type
				val  interface{}
			)

			switch typ {
			case csql.IntType:
				i := cell.AsInt()
				if i != nil {
					val = *i
				}
			case csql.TextType:
				s := cell.AsText()
				if s != nil {
					val = *s
				}
			case csql.BoolType:
				b := cell.AsBool()
				if b != nil {
					val = *b
				}
			}

			jsonl[name] = val
		}

		if err := encoder.Encode(jsonl); err != nil {
			return err
		}
	}

	return nil
}

// func debugTable(b csql.Backend, name string) {
// 	// psql behavior is to display all if no name is specified.
// 	if name == "" {
// 		debugTables(b)
// 		return
// 	}

// 	var tm *csql.TableMetadata = nil
// 	for _, t := range b.GetTables() {
// 		if t.Name == name {
// 			tm = &t
// 		}
// 	}

// 	if tm == nil {
// 		fmt.Printf(`Did not find any relation named "%s".\n`, name)
// 		return
// 	}

// 	fmt.Printf("Table \"%s\"\n", name)

// 	table := tablewriter.NewWriter(os.Stdout)
// 	table.SetHeader([]string{"Column", "Type", "Nullable"})
// 	table.SetAutoFormatHeaders(false)
// 	table.SetBorder(false)

// 	rows := [][]string{}
// 	for _, c := range tm.Columns {
// 		typeString := "integer"
// 		switch c.Type {
// 		case csql.TextType:
// 			typeString = "text"
// 		case csql.BoolType:
// 			typeString = "boolean"
// 		}
// 		nullable := ""
// 		if c.NotNull {
// 			nullable = "not null"
// 		}
// 		rows = append(rows, []string{c.Name, typeString, nullable})
// 	}

// 	table.AppendBulk(rows)
// 	table.Render()

// 	if len(tm.Indexes) > 0 {
// 		fmt.Println("Indexes:")
// 	}

// 	for _, index := range tm.Indexes {
// 		attributes := []string{}
// 		if index.PrimaryKey {
// 			attributes = append(attributes, "PRIMARY KEY")
// 		} else if index.Unique {
// 			attributes = append(attributes, "UNIQUE")
// 		}
// 		attributes = append(attributes, index.Type)

// 		fmt.Printf("\t\"%s\" %s (%s)\n", index.Name, strings.Join(attributes, ", "), index.Exp)
// 	}

// 	fmt.Println("")
// }

// func debugTables(b csql.Backend) {
// 	tables := b.GetTables()
// 	if len(tables) == 0 {
// 		fmt.Println("Did not find any relations.")
// 		return
// 	}

// 	fmt.Println("List of relations")

// 	table := tablewriter.NewWriter(os.Stdout)
// 	table.SetHeader([]string{"Name", "Type"})
// 	table.SetAutoFormatHeaders(false)
// 	table.SetBorder(false)

// 	rows := [][]string{}
// 	for _, t := range tables {
// 		rows = append(rows, []string{t.Name, "table"})
// 	}

// 	table.AppendBulk(rows)
// 	table.Render()

// 	fmt.Println("")
// }

var (
	query = flag.String("q", "", "Continuous SQL query statement.")
	// streams = flag.String("streams", "-", "Comma separated list of files to use as input streams. Use '-' to read from stdin (the default).")
)

func main() {
	flag.Parse()

	backend := csql.NewFileBackend()

	l, err := readline.NewEx(&readline.Config{
		Prompt:          "# ",
		HistoryFile:     "/tmp/csql.tmp",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	fmt.Println("Welcome to csql.")
repl:
	for {
		fmt.Print("# ")
		line, err := l.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			} else {
				continue repl
			}
		} else if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error while reading line:", err)
			continue repl
		}

		parser := csql.Parser{}

		trimmed := strings.TrimSpace(line)
		if trimmed == "quit" || trimmed == "exit" || trimmed == "\\q" {
			break
		}

		// if trimmed == "\\dt" {
		// 	debugTables(mb)
		// 	continue
		// }

		// if strings.HasPrefix(trimmed, "\\d") {
		// 	name := strings.TrimSpace(trimmed[len("\\d"):])
		// 	debugTable(mb, name)
		// 	continue
		// }

		parseOnly := false
		if strings.HasPrefix(trimmed, "\\p") {
			line = strings.TrimSpace(trimmed[len("\\p"):])
			parseOnly = true
		}

		ast, err := parser.Parse(line)
		if err != nil {
			fmt.Println("Error while parsing:", err)
			continue repl
		}

		for _, stmt := range ast.Statements {
			if parseOnly {
				fmt.Println(stmt.GenerateCode())
				continue
			}

			switch stmt.Kind {
			// case csql.CreateIndexKind:
			// 	err = mb.CreateIndex(ast.Statements[0].CreateIndexStatement)
			// 	if err != nil {
			// 		fmt.Println("Error adding index on table", err)
			// 		continue repl
			// 	}
			// case csql.CreateTableKind:
			// 	err = mb.CreateTable(ast.Statements[0].CreateTableStatement)
			// 	if err != nil {
			// 		fmt.Println("Error creating table", err)
			// 		continue repl
			// 	}
			// case csql.DropTableKind:
			// 	err = mb.DropTable(ast.Statements[0].DropTableStatement)
			// 	if err != nil {
			// 		fmt.Println("Error dropping table", err)
			// 		continue repl
			// 	}
			// case csql.InsertKind:
			// 	err = mb.Insert(stmt.InsertStatement)
			// 	if err != nil {
			// 		fmt.Println("Error inserting values:", err)
			// 		continue repl
			// 	}
			case csql.SelectKind:
				err := doSelect(backend, stmt.SelectStatement)
				if err != nil {
					fmt.Println("Error selecting values:", err)
					continue repl
				}
			}
		}

		fmt.Println("ok")
	}
}
