package csql

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

	s, ok := rb.Files[slct.from.value]
	if !ok {
		return nil, ErrStreamDoesNotExist
	}

	if slct.item == nil || len(*slct.item) == 0 {
		return nil, ErrInvalidSelectItem
	}

	// Expand SELECT * at the AST level into a SELECT on all columns
	finalItems := []*selectItem{}
	for _, item := range *slct.item {
		if item.asterisk {
			newItems := []*selectItem{}
			for j := 0; j < len(s.Columns); j++ {
				newSelectItem := &selectItem{
					exp: &expression{
						literal: &token{
							value: s.Columns[j].Name,
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

	result := File{
		Rows: resultRows,
	}

	for _, col := range finalItems {
		result.Columns = append(result.Columns, FileColumn{
			Type: ColumnType(col.exp.kind),
			Name: col.exp.literal.value,
		})
	}

	go func() {
		for FileRow := range s.Rows {
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

			for i, FileCol := range s.Columns {
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
