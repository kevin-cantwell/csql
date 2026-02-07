package source

import (
	"encoding/json"
	"fmt"
	"os"
)

// StdinSource reads JSON lines from stdin.
type StdinSource struct {
	name string
	ch   chan Record
	done chan struct{}
}

func NewStdinSource(name string) *StdinSource {
	if name == "" {
		name = "stdin"
	}
	s := &StdinSource{
		name: name,
		ch:   make(chan Record, 64),
		done: make(chan struct{}),
	}
	go s.read()
	return s
}

func (s *StdinSource) Type() SourceType { return Streaming }
func (s *StdinSource) Name() string     { return s.name }

func (s *StdinSource) Records() (<-chan Record, error) {
	return s.ch, nil
}

func (s *StdinSource) Close() error {
	close(s.done)
	return nil
}

func (s *StdinSource) read() {
	defer close(s.ch)
	dec := json.NewDecoder(os.Stdin)
	for dec.More() {
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			fmt.Fprintf(os.Stderr, "csql: stdin: %v\n", err)
			return
		}
		select {
		case s.ch <- rec:
		case <-s.done:
			return
		}
	}
}
