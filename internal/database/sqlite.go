package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/kevin-cantwell/csql/internal/ast"
	_ "github.com/mattn/go-sqlite3"
)

type SQLite struct {
	db     *sql.DB
	pubsub *Broker
	schema map[string]TableSchema
}

// TODO: pass connection string here? Or perhaps just a data dir?
func OpenSQLite() (*SQLite, error) {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // sqlite does not support concurrent write access.
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(-1)

	return &SQLite{
		db:     db,
		pubsub: &Broker{},
	}, nil

}

func (db *SQLite) Insert(ctx context.Context, table string, tuple Tuple) (finalErr error) {
	placeholders := make([]string, len(tuple))
	args := make([]interface{}, 2*len(tuple))
	for i := 0; i < 2*len(tuple); i += 2 {
		attr := tuple[i/2]
		placeholders[i/2] = "?"
		args[i] = attr.Name
		args[i+1] = attr.Value
	}
	qms := strings.Join(placeholders, ",") // question marks

	// begin a transaction and defer the rollback/commit
	txn, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if finalErr != nil {
			_ = txn.Rollback()
		} else {
			finalErr = txn.Commit()
			if finalErr == nil {
				db.pubsub.Publish(ctx, table, tuple)
			}
		}
	}()

	// If the schema doesn't exist, create one
	schema, ok := db.schema[table]
	if !ok {
		schema = TableSchema{
			Name:     table,
			ColsMap:  map[string]*ColumnSchema{},
			ColsIter: make([]ColumnSchema, len(tuple)),
		}
		columns := make([]string, len(tuple))
		for i := 0; i < 2*len(tuple); i += 2 {
			attr := tuple[i/2]
			col := ColumnSchema{
				Name:     attr.Name,
				Type:     dataType(attr.Value),
				Position: i / 2,
			}
			schema.ColsIter[i/2] = col
			schema.ColsMap[col.Name] = &col
			columns[i/2] = fmt.Sprintf("%s %s", col.Name, col.Type)
		}
		if _, err := txn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (%s)", table, strings.Join(columns, ","))); err != nil {
			return err
		}
	} else {
		// Make sure the table has every column in the tuple and ADD COLUMN if none exist
		for _, attr := range tuple {
			if _, ok := schema.ColsMap[attr.Name]; !ok {
				col := ColumnSchema{
					Name:     attr.Name,
					Type:     dataType(attr.Value),
					Position: len(schema.ColsIter),
				}
				if _, err := txn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN \"%s\" %s", table, col.Name, col.Type)); err != nil {
					return err
				}
				schema.ColsIter = append(schema.ColsIter, col)
				schema.ColsMap[attr.Name] = &col
			}
		}
	}

	if _, err := txn.ExecContext(ctx, fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", table, qms, qms), args...); err != nil {
		return err
	}
	return nil
}

func dataType(val interface{}) string {
	switch val.(type) {
	case float64:
		return "decimal"
	case bool:
		return "boolean"
	default:
		return "string"
	}
}

func (db *SQLite) Select(ctx context.Context, query *ast.Select, args ...interface{}) (interface{}, error) {
	var viewName = "TODO_VIEW_UUID"
	_, err := db.db.ExecContext(ctx, fmt.Sprintf("CREATE TEMP VIEW \"%s\" AS "+q, viewName))
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_, err := db.db.ExecContext(ctx, "DROP VIEW \"%d\"", viewName)
		if err != nil {
			panic(err)
		}
	}()

}

func (db *SQLite) QueryEvery(ctx context.Context, every time.Duration, q string, args ...interface{}) (*Rows, error) {
	if every == 0 {
		rows, err := db.cr.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		return NewRows(rows), nil
	}
	time.Now().Nanosecond()
	time.Now().Unix()
}

type ResultSet struct {
	cols []string
	rows chan []interface{}
}

type Rows struct {
	rows  *sql.Rows
	nextc chan chan bool
	scanc chan chan func(...interface{}) error
	errc  chan chan error
}

func NewRows(rows *sql.Rows) *Rows {
	rr := Rows{
		rows:  rows,
		nextc: make(chan chan bool, 1),
		scanc: make(chan chan func(...interface{}) error),
		errc:  make(chan chan error, 1),
	}

	go func() {
		defer rows.Close()

		select {
		case notify := <-rr.errc:
			err := rows.Err()
			notify <- err
			if err != nil {
				return
			}
		case notify := <-rr.nextc:
			next := rows.Next()
			notify <- next
			if !next {
				return
			}
		case notify := <-rr.scanc:
			notify <- rows.Scan
		}

	}()

	return &rr
}

func (rr *Rows) Columns() ([]string, error) {
	return rr.rows.Columns()
}

func (rr *Rows) Next() bool {
	notify := make(chan bool)
	rr.nextc <- notify
	return <-notify
}

func (rr *Rows) Scan(dest ...interface{}) error {
	notify := make(chan func(...interface{}) error)
	rr.scanc <- notify
	return (<-notify)(dest...)
}

func (rr *Rows) Err() error {
	notify := make(chan error)
	rr.errc <- notify
	return <-notify
}
