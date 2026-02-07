package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/kevin-cantwell/csql/internal/ast"
	"github.com/kevin-cantwell/csql/internal/source"
	_ "modernc.org/sqlite"
)

// Engine orchestrates query execution.
type Engine struct {
	sources      map[string]source.Source
	staticTables map[string]bool
	output       io.Writer
}

// New creates a new Engine.
func New(out io.Writer) *Engine {
	return &Engine{
		sources:      make(map[string]source.Source),
		staticTables: make(map[string]bool),
		output:       out,
	}
}

// AddSource adds a data source mapped to a table name.
func (e *Engine) AddSource(s source.Source) {
	e.sources[s.Name()] = s
	if s.Type() == source.Static {
		e.staticTables[s.Name()] = true
	}
}

// Execute runs a parsed statement.
func (e *Engine) Execute(stmt *ast.SelectStatement) error {
	if stmt.Over > 0 {
		return e.executeStreaming(stmt)
	}
	return e.executeBatch(stmt)
}

func (e *Engine) executeBatch(stmt *ast.SelectStatement) error {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	defer db.Close()

	// Load all sources into the DB
	for name, src := range e.sources {
		ch, err := src.Records()
		if err != nil {
			return fmt.Errorf("source %s: %w", name, err)
		}
		for rec := range ch {
			if err := insertRecord(db, name, rec); err != nil {
				return fmt.Errorf("insert into %s: %w", name, err)
			}
		}
	}

	// Convert AST to SQL
	sqlStr := ToSQL(stmt, nil) // no static table distinction in batch mode

	// Execute
	rows, err := db.Query(sqlStr)
	if err != nil {
		return fmt.Errorf("query: %w\nSQL: %s", err, sqlStr)
	}
	defer rows.Close()

	return e.writeRows(rows)
}

func (e *Engine) executeStreaming(stmt *ast.SelectStatement) error {
	// Find the streaming source
	var streamSrc source.Source
	var streamName string
	for name, src := range e.sources {
		if src.Type() == source.Streaming {
			streamSrc = src
			streamName = name
			break
		}
	}
	if streamSrc == nil {
		return fmt.Errorf("OVER requires a streaming source (stdin)")
	}

	// Create static DB and load static sources
	var staticDB *sql.DB
	if len(e.staticTables) > 0 {
		var err error
		staticDB, err = sql.Open("sqlite", "file:static?mode=memory&cache=shared")
		if err != nil {
			return fmt.Errorf("open static db: %w", err)
		}
		defer staticDB.Close()

		for name, src := range e.sources {
			if src.Type() == source.Static {
				ch, err := src.Records()
				if err != nil {
					return fmt.Errorf("source %s: %w", name, err)
				}
				for rec := range ch {
					if err := insertRecord(staticDB, name, rec); err != nil {
						return fmt.Errorf("insert into %s: %w", name, err)
					}
				}
			}
		}
	}

	wm := NewWindowManager(stmt.Over, e.staticTables, staticDB)
	defer wm.Close()

	sqlStr := ToSQL(stmt, e.staticTables)

	ch, err := streamSrc.Records()
	if err != nil {
		return fmt.Errorf("source %s: %w", streamName, err)
	}

	if stmt.Every > 0 {
		return e.streamWithEvery(wm, sqlStr, streamName, ch, stmt.Every)
	}

	// Without EVERY: query after each insert
	for rec := range ch {
		win, err := wm.Current()
		if err != nil {
			return fmt.Errorf("get window: %w", err)
		}

		if err := insertRecord(win.DB, streamName, rec); err != nil {
			return fmt.Errorf("insert: %w", err)
		}

		rows, err := win.DB.Query(sqlStr)
		if err != nil {
			return fmt.Errorf("query: %w\nSQL: %s", err, sqlStr)
		}
		if err := e.writeRows(rows); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}

	return nil
}

func (e *Engine) streamWithEvery(wm *WindowManager, sqlStr, streamName string, ch <-chan source.Record, every time.Duration) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				// Stream ended, do final query
				win, err := wm.Current()
				if err != nil {
					return fmt.Errorf("get window: %w", err)
				}
				rows, err := win.DB.Query(sqlStr)
				if err != nil {
					return fmt.Errorf("query: %w", err)
				}
				err = e.writeRows(rows)
				rows.Close()
				return err
			}

			win, err := wm.Current()
			if err != nil {
				return fmt.Errorf("get window: %w", err)
			}
			if err := insertRecord(win.DB, streamName, rec); err != nil {
				return fmt.Errorf("insert: %w", err)
			}

		case <-ticker.C:
			win, err := wm.Current()
			if err != nil {
				return fmt.Errorf("get window: %w", err)
			}
			rows, err := win.DB.Query(sqlStr)
			if err != nil {
				// Table might not exist yet if no records inserted
				continue
			}
			if err := e.writeRows(rows); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
		}
	}
}

func (e *Engine) writeRows(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		rec := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			rec[col] = vals[i]
		}

		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Fprintln(e.output, string(b))
	}
	return rows.Err()
}

// insertRecord inserts a record into a table, creating the table if needed.
func insertRecord(db *sql.DB, table string, rec source.Record) error {
	if len(rec) == 0 {
		return nil
	}

	cols := make([]string, 0, len(rec))
	vals := make([]interface{}, 0, len(rec))
	placeholders := make([]string, 0, len(rec))

	for k, v := range rec {
		cols = append(cols, quoteIdent(k))
		// Serialize nested objects/arrays to JSON strings for SQLite
		switch v.(type) {
		case map[string]interface{}, []interface{}:
			b, err := json.Marshal(v)
			if err != nil {
				vals = append(vals, fmt.Sprintf("%v", v))
			} else {
				vals = append(vals, string(b))
			}
		default:
			vals = append(vals, v)
		}
		placeholders = append(placeholders, "?")
	}

	// Try insert first (fast path)
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdent(table),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "))

	_, err := db.Exec(insertSQL, vals...)
	if err == nil {
		return nil
	}

	// Table doesn't exist — create it
	colDefs := make([]string, len(cols))
	for i, col := range cols {
		colDefs[i] = col + " " + sqliteType(vals[i])
	}
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		quoteIdent(table),
		strings.Join(colDefs, ", "))
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Retry insert
	_, err = db.Exec(insertSQL, vals...)
	if err != nil {
		// Might need to add columns
		for i, col := range cols {
			alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
				quoteIdent(table), col, sqliteType(vals[i]))
			db.Exec(alterSQL) // ignore errors (column may already exist)
		}
		_, err = db.Exec(insertSQL, vals...)
	}
	return err
}

func sqliteType(v interface{}) string {
	switch v.(type) {
	case float64, float32, int, int64:
		return "REAL"
	case bool:
		return "INTEGER"
	default:
		return "TEXT"
	}
}
