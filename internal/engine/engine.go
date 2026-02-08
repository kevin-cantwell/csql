package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
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

// attachable is the interface for sources that can be ATTACHed directly.
type attachable interface {
	DBPath() string
	TableName() string
}

func (e *Engine) executeBatch(stmt *ast.SelectStatement) error {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	defer db.Close()

	// Check for SQLite sources that can be ATTACHed directly
	attachSchemas := make(map[string]string) // source name → schema for SQL generation
	for name, src := range e.sources {
		if att, ok := src.(attachable); ok {
			schema := "_src_" + name
			_, err := db.Exec(fmt.Sprintf(
				"ATTACH DATABASE %s AS %s",
				quoteLiteral(att.DBPath()), quoteIdent(schema)))
			if err != nil {
				return fmt.Errorf("attach %s: %w", name, err)
			}
			attachSchemas[name] = schema
			continue
		}

		// Load non-SQLite sources into the DB
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

	// Build plans for SQL generation with correct table names
	var plans map[string]*BatchTablePlan
	var tableSchemas map[string]string
	if len(attachSchemas) > 0 {
		plans = make(map[string]*BatchTablePlan)
		tableSchemas = make(map[string]string)
		for name, schema := range attachSchemas {
			att := e.sources[name].(attachable)
			plans[name] = &BatchTablePlan{
				Access:   AccessAttached,
				Schema:   schema,
				SQLTable: att.TableName(),
			}
			tableSchemas[name] = schema
		}
	}

	// Convert AST to SQL
	var sqlStr string
	if plans != nil {
		sqlStr = ToSQLWithPlans(stmt, tableSchemas, plans)
	} else {
		sqlStr = ToSQL(stmt, nil)
	}

	// Execute
	rows, err := db.Query(sqlStr)
	if err != nil {
		// A source with 0 records never creates its table — treat as empty result.
		if isNoSuchTableErr(err) {
			return nil
		}
		return fmt.Errorf("query: %w\nSQL: %s", err, sqlStr)
	}
	defer rows.Close()

	return e.writeRows(rows)
}

// IndexedTable holds a Go-side hash map for on-demand insertion of batch rows.
type IndexedTable struct {
	name      string                           // table name in window DB
	joinCol   string                           // column indexed on (in the batch table)
	streamCol string                           // column to look up from streaming record
	records   map[interface{}][]source.Record   // joinKey → matching batch records
}

// taggedRecord associates a record with its source table name.
type taggedRecord struct {
	table string
	rec   source.Record
}

// mergeStreams fans-in multiple streaming source channels into one tagged channel.
func mergeStreams(sources map[string]source.Source) (<-chan taggedRecord, error) {
	merged := make(chan taggedRecord, 64)
	var wg sync.WaitGroup
	for name, src := range sources {
		if src.Type() != source.Streaming {
			continue
		}
		ch, err := src.Records()
		if err != nil {
			return nil, fmt.Errorf("source %s: %w", name, err)
		}
		wg.Add(1)
		go func(name string, ch <-chan source.Record) {
			defer wg.Done()
			for rec := range ch {
				merged <- taggedRecord{table: name, rec: rec}
			}
		}(name, ch)
	}
	go func() {
		wg.Wait()
		close(merged)
	}()
	return merged, nil
}

func (e *Engine) executeStreaming(stmt *ast.SelectStatement) error {
	// Collect streaming sources
	streamingSources := make(map[string]source.Source)
	for name, src := range e.sources {
		if src.Type() == source.Streaming {
			streamingSources[name] = src
		}
	}
	if len(streamingSources) == 0 {
		return fmt.Errorf("OVER requires at least one streaming source")
	}

	// Analyze batch access patterns
	batchPlan := AnalyzeBatchAccess(stmt, e.sources)

	// Process each batch source according to its plan
	var staticDB *sql.DB
	var attachments []AttachInfo
	var indexedTables []*IndexedTable
	staticTables := make(map[string]bool)

	for name, plan := range batchPlan {
		src := e.sources[name]
		switch plan.Access {
		case AccessAttached:
			attachments = append(attachments, AttachInfo{
				Schema: plan.Schema,
				Path:   plan.AttachPath,
			})

		case AccessIndexed:
			// Read all records into a Go hash map indexed on the join column
			ch, err := src.Records()
			if err != nil {
				return fmt.Errorf("source %s: %w", name, err)
			}
			idx := &IndexedTable{
				name:      name,
				joinCol:   plan.JoinCol,
				streamCol: plan.StreamCol,
				records:   make(map[interface{}][]source.Record),
			}
			for rec := range ch {
				key := normalizeKey(rec[plan.JoinCol])
				if key != nil {
					idx.records[key] = append(idx.records[key], rec)
				}
			}
			indexedTables = append(indexedTables, idx)

		case AccessFullScan:
			// Pre-load into shared static DB
			if staticDB == nil {
				var err error
				staticDB, err = sql.Open("sqlite", "file:static?mode=memory&cache=shared")
				if err != nil {
					return fmt.Errorf("open static db: %w", err)
				}
			}
			staticTables[name] = true
			ch, err := src.Records()
			if err != nil {
				if staticDB != nil {
					staticDB.Close()
				}
				return fmt.Errorf("source %s: %w", name, err)
			}
			for rec := range ch {
				if err := insertRecord(staticDB, name, rec); err != nil {
					staticDB.Close()
					return fmt.Errorf("insert into %s: %w", name, err)
				}
			}
		}
	}

	if staticDB != nil {
		defer staticDB.Close()
	}

	// Build table schemas for SQL generation
	tableSchemas := BuildTableSchemas(batchPlan)

	// Generate SQL
	sqlStr := ToSQLWithPlans(stmt, tableSchemas, batchPlan)

	wm := NewWindowManager(stmt.Over, staticTables, staticDB, attachments)
	defer wm.Close()

	merged, err := mergeStreams(streamingSources)
	if err != nil {
		return err
	}

	if stmt.Every > 0 {
		return e.streamWithEvery(wm, sqlStr, merged, stmt.Every, indexedTables)
	}

	// Without EVERY: query after each insert
	for tr := range merged {
		win, err := wm.Current()
		if err != nil {
			return fmt.Errorf("get window: %w", err)
		}

		if err := insertRecord(win.DB, tr.table, tr.rec); err != nil {
			return fmt.Errorf("insert: %w", err)
		}

		// Populate indexed batch tables on demand
		for _, idx := range indexedTables {
			key := normalizeKey(tr.rec[idx.streamCol])
			if key == nil {
				continue
			}
			if win.hasKey(idx.name, key) {
				continue
			}
			for _, rec := range idx.records[key] {
				if err := insertRecord(win.DB, idx.name, rec); err != nil {
					return fmt.Errorf("insert indexed %s: %w", idx.name, err)
				}
			}
			win.markKey(idx.name, key)
		}

		rows, err := win.DB.Query(sqlStr)
		if err != nil {
			// In multi-stream mode, some tables may not exist yet
			if isNoSuchTableErr(err) {
				continue
			}
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

func (e *Engine) streamWithEvery(wm *WindowManager, sqlStr string, merged <-chan taggedRecord, every time.Duration, indexedTables []*IndexedTable) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case tr, ok := <-merged:
			if !ok {
				// All streams ended, do final query
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
			if err := insertRecord(win.DB, tr.table, tr.rec); err != nil {
				return fmt.Errorf("insert: %w", err)
			}

			// Populate indexed batch tables on demand
			for _, idx := range indexedTables {
				key := normalizeKey(tr.rec[idx.streamCol])
				if key == nil {
					continue
				}
				if win.hasKey(idx.name, key) {
					continue
				}
				for _, rec := range idx.records[key] {
					if err := insertRecord(win.DB, idx.name, rec); err != nil {
						return fmt.Errorf("insert indexed %s: %w", idx.name, err)
					}
				}
				win.markKey(idx.name, key)
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

// isNoSuchTableErr returns true if the error is a SQLite "no such table" error.
func isNoSuchTableErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table")
}

// normalizeKey converts numeric values to float64 so that int64(1) and float64(1)
// hash to the same map key. This is needed because CSV sources produce int64 while
// JSON/streaming sources produce float64 for the same logical value.
func normalizeKey(v interface{}) interface{} {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	default:
		return v
	}
}
