package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kevin-cantwell/csql"
	"github.com/olekukonko/tablewriter"
)

func main() {
	log.SetFlags(log.Lshortfile)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Token", "Raw"})

	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		lex := csql.NewLexer(strings.NewReader(s.Text()))
		for {
			tok, raw, err := lex.Scan()
			if tok == csql.EOF {
				break
			}
			if err != nil {
				fmt.Print("\n")
				log.Fatalf("%v", err)
			}
			table.Append([]string{tok.String(), fmt.Sprintf("%q", raw)})
		}
		table.Render() // Send output
		table.ClearRows()
	}

}
