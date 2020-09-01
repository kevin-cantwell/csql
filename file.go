package csql

import (
	"encoding/json"
	"io"
	"os"
	"sort"
)

type FileBackend struct {
	Backend
}

func NewFileBackend() *FileBackend {
	return &FileBackend{}
}

func (rb *FileBackend) Select(slct *SelectStatement) (*Stream, error) {
	if slct.from == nil {
		return nil, ErrNoStreamSpecified
	}

	path := slct.from.value
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrStreamDoesNotExist
		}
		return nil, err
	}
	stream := newFileStream(file)

	if slct.item == nil || len(*slct.item) == 0 {
		return nil, ErrInvalidSelectItem
	}

	// Expand SELECT * at the AST level into a SELECT on all columns
	finalItems := []*selectItem{}
	for _, item := range *slct.item {
		if item.asterisk {
			newItems := []*selectItem{}
			for j := 0; j < len(stream.Columns); j++ {
				newSelectItem := &selectItem{
					exp: &expression{
						literal: &token{
							value: stream.Columns[j].Name,
							kind:  identifierKind,
							loc:   location{0, uint(len("SELECT") + 1)},
						},
						binary: nil,
						kind:   literalKind,
					},
					asterisk: false,
					as:       nil,
				}
				newItems = append(newItems, newSelectItem)
			}
			finalItems = append(finalItems, newItems...)
		} else {
			finalItems = append(finalItems, item)
		}
	}

	resultRows := make(chan []Cell)

	result := Stream{
		Rows: resultRows,
	}

	for _, col := range finalItems {
		result.Columns = append(result.Columns, StreamColumn{
			Type: ColumnType(col.exp.kind),
			Name: col.exp.literal.value,
		})
	}

	go func() {
		for FileRow := range stream.Rows {
			// if slct.where != nil {
			// 	val, _, _, err := t.evaluateCell(uint(i), *slct.where)
			// 	if err != nil {
			// 		return nil, err
			// 	}

			// 	if !*val.AsBool() {
			// 		continue
			// 	}
			// }

			var selectedRow []Cell

			for i, FileCol := range stream.Columns {
				for _, selectedCol := range result.Columns {
					if FileCol.Name == selectedCol.Name {
						selectedRow = append(selectedRow, FileRow[i])
						break
					}
				}
			}

			resultRows <- selectedRow
		}
	}()

	return &result, nil
}

func newFileStream(file *os.File) *Stream {
	columns := []StreamColumn{}
	rows := make(chan []Cell, 1)

	s := Stream{
		Columns: columns,
		Rows:    rows,
	}

	decoder := json.NewDecoder(file)

	var row map[string]interface{}
	if err := decoder.Decode(&row); err != nil {
		if err != io.EOF {
			s.Err = err
		}
		close(rows)
		return &s
	}

	var names []string
	for name, _ := range row {
		names = append(names, name)
	}
	sort.Strings(names)

	var cells []Cell
	for _, name := range names {
		cell := jsonCell{}
		val := row[name]
		var typ ColumnType
		switch v := val.(type) {
		case float64:
			typ = IntType
			cell.n = &v
		case bool:
			typ = BoolType
			cell.b = &v
		case string:
			typ = TextType
			cell.s = &v
		case map[string]interface{}, nil:
			typ = TextType
		}
		s.Columns = append(s.Columns, StreamColumn{
			Type: typ,
			Name: name,
		})

		cells = append(cells, &cell)
	}

	rows <- cells

	go func() {
		defer close(rows)

		for {
			var cells []Cell
			var row map[string]interface{}
			if err := decoder.Decode(&row); err != nil {
				if err != io.EOF {
					msg := err.Error()
					rows <- append([]Cell{}, &jsonCell{
						s: &msg,
					})
					s.Err = err
				}
				return
			}
			for _, col := range s.Columns {
				cell := jsonCell{}
				val := row[col.Name]
				switch v := val.(type) {
				case float64:
					cell.n = &v
				case bool:
					cell.b = &v
				case string:
					cell.s = &v
				case map[string]interface{}, nil:
				}
				cells = append(cells, &cell)
			}
			rows <- cells
		}
	}()

	return &s
}

type jsonCell struct {
	b *bool
	s *string
	n *float64
	o *map[string]interface{}
}

func (c *jsonCell) AsText() *string {
	return c.s
}

func (c *jsonCell) AsInt() *int32 {
	if c.n != nil {
		i := int32(*c.n)
		return &i
	}
	return nil
}

func (c *jsonCell) AsBool() *bool {
	return c.b
}
