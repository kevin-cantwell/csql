package source

import (
	"bufio"
	"encoding/json"
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
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // skip malformed lines
		}
		select {
		case s.ch <- rec:
		case <-s.done:
			return
		}
	}
}
