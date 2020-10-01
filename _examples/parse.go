package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"

	"github.com/kevin-cantwell/csql"
)

func main() {
	log.SetFlags(log.Lshortfile)

	enc := json.NewEncoder(os.Stdout)

	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		p := csql.NewParser(strings.NewReader(s.Text()))
		stmts, err := p.Parse()
		if err != nil {
			panic(err)
		}
		for _, stmt := range stmts {
			if err := enc.Encode(stmt); err != nil {
				panic(err)
			}
		}
	}
}

func repl() io.Reader {
	pr, pw := io.Pipe()

	in := bufio.NewReader(os.Stdin)
	go func() {
		for {
			b, err := in.ReadByte()
			if err != nil {
				pw.CloseWithError(err)
			}
			_ = b
		}
	}()
	return pr
}
