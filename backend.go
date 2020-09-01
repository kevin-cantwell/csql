package csql

type ColumnType uint

const (
	TextType ColumnType = iota
	IntType
	BoolType
)

func (c ColumnType) String() string {
	switch c {
	case TextType:
		return "TextType"
	case IntType:
		return "IntType"
	case BoolType:
		return "BoolType"
	default:
		return "Error"
	}
}

type Cell interface {
	AsText() *string
	AsInt() *int32
	AsBool() *bool
}

type Stream struct {
	Columns []StreamColumn
	Rows    <-chan []Cell
}

type StreamColumn struct {
	Type    ColumnType
	Name    string
	NotNull bool
}

type Index struct {
	Name       string
	Exp        string
	Type       string
	Unique     bool
	PrimaryKey bool
}

type TableMetadata struct {
	Name    string
	Columns []StreamColumn
	Indexes []Index
}

type Backend interface {
	// CreateTable(*CreateTableStatement) error
	// DropTable(*DropTableStatement) error
	// CreateIndex(*CreateIndexStatement) error
	// Insert(*InsertStatement) error
	Select(*SelectStatement) (*Stream, error)
	// GetTables() []TableMetadata
}
