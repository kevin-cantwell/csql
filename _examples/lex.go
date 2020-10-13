package main

import (
	"fmt"
	"log"
	"os"

	"github.com/kevin-cantwell/csql"
	"github.com/olekukonko/tablewriter"
)

func main() {
	log.SetFlags(log.Lshortfile)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Token", "Raw", "Line", "Pos"})

	lex := csql.NewLexer(os.Stdin)

	for {
		for {
			tok, err := lex.Scan()
			if err != nil {
				panic(err)
			}
			table.Append([]string{
				tok.Type.String(),
				fmt.Sprintf("%q", tok.Raw),
				fmt.Sprintf("%d", tok.Line),
				fmt.Sprintf("%d", tok.Pos),
			})
			if tok.Type == csql.EOF {
				table.Render() // Send output
				table.ClearRows()
				os.Exit(0)
			}
			if tok.Type == csql.SEMICOLON {
				table.Render() // Send output
				table.ClearRows()
				continue
			}
		}
	}

}
