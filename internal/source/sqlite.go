package source

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// SQLiteSource reads records from a table in a SQLite database file.
type SQLiteSource struct {
	name  string
	path  string
	table string
	ch    chan Record
	once  sync.Once
}

// NewSQLiteSource creates a source that reads all rows from a SQLite table.
// The path is the database file. The table defaults to name if not specified.
// The goroutine is started lazily on first call to Records().
func NewSQLiteSource(name, path, table string) (*SQLiteSource, error) {
	if table == "" {
		table = name
	}
	return &SQLiteSource{
		name:  name,
		path:  path,
		table: table,
	}, nil
}

func (s *SQLiteSource) Type() SourceType { return Static }
func (s *SQLiteSource) Name() string     { return s.name }

// DBPath returns the filesystem path to the SQLite database file.
func (s *SQLiteSource) DBPath() string { return s.path }

// TableName returns the table name within the SQLite database.
func (s *SQLiteSource) TableName() string { return s.table }

func (s *SQLiteSource) Records() (<-chan Record, error) {
	s.once.Do(func() {
		s.ch = make(chan Record, 64)
		go s.read()
	})
	return s.ch, nil
}

func (s *SQLiteSource) Close() error {
	return nil
}

func (s *SQLiteSource) read() {
	defer close(s.ch)

	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		return
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s", quoteIdent(s.table)))
	if err != nil {
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return
	}

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return
		}

		rec := make(Record, len(cols))
		for i, col := range cols {
			rec[col] = vals[i]
		}
		s.ch <- rec
	}
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
