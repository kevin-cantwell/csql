package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/kevin-cantwell/csql"
	"github.com/pkg/errors"
)

func main() {
	log.SetFlags(log.Lshortfile)

	enc := json.NewEncoder(os.Stdout)

	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		p := csql.NewParser(strings.NewReader(scan.Text()))
		stmts, err := p.Parse()
		if err != nil {
			fmt.Printf("%+v\n", err)
			os.Exit(0)
		}
		for _, stmt := range stmts {
			if err := enc.Encode(stmt); err != nil {
				panic(err)
			}
		}
	}
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func repl() io.Reader {
	pr, pw := io.Pipe()

	in := bufio.NewReader(os.Stdin)
	go func() {
		for {
			b, err := in.ReadByte()
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			_, err = pw.Write([]byte{b})
			if err != nil {
				panic(err)
			}
			if b == ';' {
				pw.Close()
				return
			}
		}
	}()
	return pr
}
