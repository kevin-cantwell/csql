package output

import (
	"encoding/json"
	"fmt"
	"io"
)

// Writer writes query results.
type Writer interface {
	WriteRow(cols []string, vals []interface{}) error
	Flush() error
}

// JSONWriter writes JSON lines to an io.Writer.
type JSONWriter struct {
	w io.Writer
}

func NewJSONWriter(w io.Writer) *JSONWriter {
	return &JSONWriter{w: w}
}

func (jw *JSONWriter) WriteRow(cols []string, vals []interface{}) error {
	rec := make(map[string]interface{}, len(cols))
	for i, col := range cols {
		rec[col] = vals[i]
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(jw.w, string(b))
	return err
}

func (jw *JSONWriter) Flush() error {
	return nil
}
