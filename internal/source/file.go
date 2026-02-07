package source

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// FileSource reads records from a CSV or JSON file.
type FileSource struct {
	name string
	path string
	ch   chan Record
}

func NewFileSource(name, path string) (*FileSource, error) {
	ext := strings.ToLower(filepath.Ext(path))
	if ext != ".csv" && ext != ".json" && ext != ".jsonl" {
		return nil, fmt.Errorf("unsupported file type %q (use .csv, .json, or .jsonl)", ext)
	}

	s := &FileSource{
		name: name,
		path: path,
		ch:   make(chan Record, 64),
	}
	go s.read(ext)
	return s, nil
}

func (s *FileSource) Type() SourceType { return Static }
func (s *FileSource) Name() string     { return s.name }

func (s *FileSource) Records() (<-chan Record, error) {
	return s.ch, nil
}

func (s *FileSource) Close() error {
	return nil
}

func (s *FileSource) read(ext string) {
	defer close(s.ch)

	f, err := os.Open(s.path)
	if err != nil {
		return
	}
	defer f.Close()

	switch ext {
	case ".csv":
		s.readCSV(f)
	case ".json", ".jsonl":
		s.readJSONLines(f)
	}
}

func (s *FileSource) readCSV(r io.Reader) {
	reader := csv.NewReader(r)

	header, err := reader.Read()
	if err != nil {
		return
	}

	for {
		row, err := reader.Read()
		if err != nil {
			return
		}

		rec := make(Record, len(header))
		for i, col := range header {
			if i < len(row) {
				rec[col] = inferType(row[i])
			}
		}
		s.ch <- rec
	}
}

func (s *FileSource) readJSONLines(r io.Reader) {
	dec := json.NewDecoder(r)
	for {
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			return
		}
		s.ch <- rec
	}
}

// inferType converts a CSV string value to a typed value.
func inferType(s string) interface{} {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if !strings.Contains(s, ".") {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i
			}
		}
		return f
	}
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}
	return s
}
