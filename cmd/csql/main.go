package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/kevin-cantwell/csql"
)

func main() {
	var (
		query = flag.String("query", "", "SQL query.")
	)
	flag.Parse()

	parser := csql.NewParser(strings.NewReader(*query))
	stmts, err := parser.Parse()
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}

	// just use the first one for now
	stmt := stmts[0]
	sel := stmt.Select
	go processSelect(sel)

	dec := json.NewDecoder(os.Stdin)
	for {
		var row map[string]interface{}
		if err := dec.Decode(&row); err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(1)
		}
	}
}

type SelectResults struct {
	Select *csql.SelectStatement
}
