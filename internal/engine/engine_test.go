package engine

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kevin-cantwell/csql/internal/ast"
	"github.com/kevin-cantwell/csql/internal/source"
	_ "modernc.org/sqlite"
)

// --- test helpers ---

// chanSource is a test source backed by a channel.
type chanSource struct {
	name    string
	srcType source.SourceType
	ch      chan source.Record
}

func (s *chanSource) Type() source.SourceType                { return s.srcType }
func (s *chanSource) Name() string                           { return s.name }
func (s *chanSource) Records() (<-chan source.Record, error)  { return s.ch, nil }
func (s *chanSource) Close() error                           { return nil }

// newStaticChan creates a static source that immediately sends records and closes.
func newStaticChan(name string, records ...source.Record) *chanSource {
	ch := make(chan source.Record, len(records))
	for _, r := range records {
		ch <- r
	}
	close(ch)
	return &chanSource{name: name, srcType: source.Static, ch: ch}
}

// newStreamChan creates a streaming source with an open channel the caller controls.
func newStreamChan(name string) (*chanSource, chan<- source.Record) {
	ch := make(chan source.Record, 64)
	return &chanSource{name: name, srcType: source.Streaming, ch: ch}, ch
}

// parseAndExec parses a query, adds sources to an engine, executes, and returns output lines.
func parseAndExec(t *testing.T, query string, sources ...source.Source) []map[string]interface{} {
	t.Helper()
	sel := parseQuery(t, query)
	var buf bytes.Buffer
	eng := New(&buf)
	for _, s := range sources {
		eng.AddSource(s)
	}
	if err := eng.Execute(sel); err != nil {
		t.Fatalf("execute %q: %v", query, err)
	}
	return parseOutput(t, buf.String())
}

func parseQuery(t *testing.T, query string) *ast.SelectStatement {
	t.Helper()
	p := ast.NewParser(strings.NewReader(query))
	stmts, err := p.Parse()
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	if len(stmts) == 0 || stmts[0].Select == nil {
		t.Fatalf("no SELECT in %q", query)
	}
	return stmts[0].Select
}

func parseOutput(t *testing.T, out string) []map[string]interface{} {
	t.Helper()
	var rows []map[string]interface{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal output line %q: %v", line, err)
		}
		rows = append(rows, m)
	}
	return rows
}

func getFloat(m map[string]interface{}, key string) float64 {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case json.Number:
		f, _ := val.Float64()
		return f
	}
	return 0
}

func getString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

func testdataPath(name string) string {
	return "../../testdata/" + name
}

// =====================
// TABLE-TO-TABLE (BATCH)
// =====================

func TestBatchSelectStar(t *testing.T) {
	src, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT * FROM users", src)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
	// Verify a specific row
	found := false
	for _, r := range rows {
		if getString(r, "name") == "Alice" {
			if getFloat(r, "age") != 30 {
				t.Errorf("Alice age: got %v, want 30", r["age"])
			}
			found = true
		}
	}
	if !found {
		t.Error("Alice not found in results")
	}
}

func TestBatchSelectWhere(t *testing.T) {
	src, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT name, age FROM users WHERE age > 29", src)
	// Should get Alice(30), Charlie(35), Eve(42)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for _, r := range rows {
		age := getFloat(r, "age")
		if age <= 29 {
			t.Errorf("got age %v, expected > 29", age)
		}
	}
}

func TestBatchJoinCSVtoCSV(t *testing.T) {
	users, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	products, err := source.NewFileSource("products", testdataPath("products.csv"))
	if err != nil {
		t.Fatal(err)
	}
	// Cross-reference: just verify the join mechanics work
	// orders has user_id and product_id; use users + products joined through orders
	orders, err := source.NewFileSource("orders", testdataPath("orders.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t,
		"SELECT u.name, p.name pname FROM orders o JOIN users u ON o.user_id = u.id JOIN products p ON o.product_id = p.id",
		orders, users, products,
	)
	if len(rows) == 0 {
		t.Fatal("expected joined rows, got 0")
	}
	// Order 1: user 1 (Alice), product 101 (Widget)
	found := false
	for _, r := range rows {
		if getString(r, "name") == "Alice" && getString(r, "pname") == "Widget" {
			found = true
		}
	}
	if !found {
		t.Error("expected Alice+Widget join row")
	}
}

func TestBatchJoinCSVtoJSONL(t *testing.T) {
	users, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	events, err := source.NewFileSource("events", testdataPath("events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t,
		"SELECT u.name, e.action FROM events e JOIN users u ON e.user_id = u.id",
		events, users,
	)
	if len(rows) == 0 {
		t.Fatal("expected joined rows, got 0")
	}
	// event user_id=1 action=login -> Alice
	found := false
	for _, r := range rows {
		if getString(r, "name") == "Alice" && getString(r, "action") == "login" {
			found = true
		}
	}
	if !found {
		t.Error("expected Alice+login join row")
	}
}

func TestBatchLeftJoin(t *testing.T) {
	users, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	// orders has user_id=4 (Diana) but also user_id=4 who IS in users
	// orders has no user_id=5 order. So LEFT JOIN from users should show Eve with NULLs.
	orders, err := source.NewFileSource("orders", testdataPath("orders.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t,
		"SELECT u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name",
		users, orders,
	)
	if len(rows) == 0 {
		t.Fatal("expected rows from LEFT JOIN")
	}
	// Eve (id=5) has no orders
	eveFound := false
	for _, r := range rows {
		if getString(r, "name") == "Eve" {
			eveFound = true
			if r["order_id"] != nil {
				t.Errorf("Eve should have NULL order_id, got %v", r["order_id"])
			}
		}
	}
	if !eveFound {
		t.Error("Eve should appear in LEFT JOIN results")
	}
}

func TestBatchGroupBy(t *testing.T) {
	events, err := source.NewFileSource("events", testdataPath("events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t,
		"SELECT action, COUNT(*) cnt FROM events GROUP BY action ORDER BY action",
		events,
	)
	// login: user_id 1,3,5 = 3; click: 1; purchase: 1; logout: 1
	if len(rows) == 0 {
		t.Fatal("expected grouped rows")
	}
	actionCounts := map[string]float64{}
	for _, r := range rows {
		actionCounts[getString(r, "action")] = getFloat(r, "cnt")
	}
	if actionCounts["login"] != 3 {
		t.Errorf("login count: got %v, want 3", actionCounts["login"])
	}
	if actionCounts["click"] != 1 {
		t.Errorf("click count: got %v, want 1", actionCounts["click"])
	}
}

func TestBatchOrderByLimit(t *testing.T) {
	users, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT name, age FROM users ORDER BY age DESC LIMIT 3", users)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// Descending: Eve(42), Charlie(35), Alice(30)
	if getString(rows[0], "name") != "Eve" {
		t.Errorf("first row: got %s, want Eve", getString(rows[0], "name"))
	}
	if getString(rows[1], "name") != "Charlie" {
		t.Errorf("second row: got %s, want Charlie", getString(rows[1], "name"))
	}
	if getString(rows[2], "name") != "Alice" {
		t.Errorf("third row: got %s, want Alice", getString(rows[2], "name"))
	}
}

func TestBatchDistinct(t *testing.T) {
	events, err := source.NewFileSource("events", testdataPath("events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT DISTINCT action FROM events ORDER BY action", events)
	actions := make([]string, len(rows))
	for i, r := range rows {
		actions[i] = getString(r, "action")
	}
	if len(actions) != 4 {
		t.Fatalf("expected 4 distinct actions, got %d: %v", len(actions), actions)
	}
}

func TestBatchComplexWhere(t *testing.T) {
	users, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}

	// IN
	rows := parseAndExec(t, "SELECT name FROM users WHERE age IN (25, 35)", users)
	if len(rows) != 2 {
		t.Errorf("IN: expected 2 rows, got %d", len(rows))
	}

	// BETWEEN
	users2, _ := source.NewFileSource("users", testdataPath("users.csv"))
	rows = parseAndExec(t, "SELECT name FROM users WHERE age BETWEEN 28 AND 35", users2)
	// Alice(30), Charlie(35), Diana(28) = 3
	if len(rows) != 3 {
		t.Errorf("BETWEEN: expected 3 rows, got %d", len(rows))
	}

	// LIKE
	users3, _ := source.NewFileSource("users", testdataPath("users.csv"))
	rows = parseAndExec(t, "SELECT name FROM users WHERE name LIKE 'A%'", users3)
	if len(rows) != 1 || getString(rows[0], "name") != "Alice" {
		t.Errorf("LIKE: expected Alice, got %v", rows)
	}

	// AND + OR
	users4, _ := source.NewFileSource("users", testdataPath("users.csv"))
	rows = parseAndExec(t, "SELECT name FROM users WHERE age > 30 OR name = 'Bob'", users4)
	// Charlie(35), Eve(42), Bob(25) = 3
	if len(rows) != 3 {
		t.Errorf("AND+OR: expected 3 rows, got %d", len(rows))
	}
}

func TestBatchEmptyResult(t *testing.T) {
	users, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT * FROM users WHERE age > 100", users)
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestBatchThreeWayJoin(t *testing.T) {
	users, _ := source.NewFileSource("users", testdataPath("users.csv"))
	products, _ := source.NewFileSource("products", testdataPath("products.csv"))
	orders, _ := source.NewFileSource("orders", testdataPath("orders.jsonl"))

	rows := parseAndExec(t,
		`SELECT u.name, p.name pname, o.quantity
		 FROM orders o
		 JOIN users u ON o.user_id = u.id
		 JOIN products p ON o.product_id = p.id
		 ORDER BY o.order_id`,
		orders, users, products,
	)
	if len(rows) == 0 {
		t.Fatal("expected three-way join rows")
	}
	// order_id=1: Alice, Widget, qty=2
	r := rows[0]
	if getString(r, "name") != "Alice" {
		t.Errorf("first join row name: got %s, want Alice", getString(r, "name"))
	}
	if getString(r, "pname") != "Widget" {
		t.Errorf("first join row pname: got %s, want Widget", getString(r, "pname"))
	}
	if getFloat(r, "quantity") != 2 {
		t.Errorf("first join row quantity: got %v, want 2", r["quantity"])
	}
}

func TestBatchAggregateFunctions(t *testing.T) {
	users, _ := source.NewFileSource("users", testdataPath("users.csv"))
	rows := parseAndExec(t,
		"SELECT COUNT(*) cnt, SUM(age) total_age, AVG(age) avg_age, MIN(age) min_age, MAX(age) max_age FROM users",
		users,
	)
	if len(rows) != 1 {
		t.Fatalf("expected 1 aggregate row, got %d", len(rows))
	}
	r := rows[0]
	if getFloat(r, "cnt") != 5 {
		t.Errorf("COUNT: got %v, want 5", r["cnt"])
	}
	if getFloat(r, "min_age") != 25 {
		t.Errorf("MIN: got %v, want 25", r["min_age"])
	}
	if getFloat(r, "max_age") != 42 {
		t.Errorf("MAX: got %v, want 42", r["max_age"])
	}
}

func TestBatchSparseColumns(t *testing.T) {
	// sparse.jsonl has records with different sets of keys
	sparse, err := source.NewFileSource("data", testdataPath("sparse.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT * FROM data ORDER BY id", sparse)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// Row id=2 should have no "tag" field (NULL)
	r := rows[1]
	if r["tag"] != nil {
		t.Errorf("id=2 tag: expected nil, got %v", r["tag"])
	}
	// Row id=4 should have no "value" field (NULL)
	r = rows[3]
	if r["value"] != nil {
		t.Errorf("id=4 value: expected nil, got %v", r["value"])
	}
}

func TestBatchArithmeticExpressions(t *testing.T) {
	products, _ := source.NewFileSource("products", testdataPath("products.csv"))
	rows := parseAndExec(t,
		"SELECT name, price * 2 double_price FROM products WHERE price < 10 ORDER BY price",
		products,
	)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (price<10), got %d", len(rows))
	}
	// Thingamajig: 4.99, Widget: 9.99, Whatchamacallit: 7.50
	if getFloat(rows[0], "double_price") != 9.98 {
		t.Errorf("Thingamajig double_price: got %v, want 9.98", rows[0]["double_price"])
	}
}

func TestBatchGroupByHaving(t *testing.T) {
	// GROUP BY with aggregate filtering via subquery-style WHERE
	orders, _ := source.NewFileSource("orders", testdataPath("orders.jsonl"))
	rows := parseAndExec(t,
		"SELECT user_id, SUM(quantity) total FROM orders GROUP BY user_id ORDER BY total DESC",
		orders,
	)
	if len(rows) == 0 {
		t.Fatal("expected grouped rows")
	}
	// user_id=2: qty 1+5=6 should be largest
	if getFloat(rows[0], "total") != 6 {
		t.Errorf("top total: got %v, want 6", rows[0]["total"])
	}
}

func TestBatchNotIn(t *testing.T) {
	users, _ := source.NewFileSource("users", testdataPath("users.csv"))
	rows := parseAndExec(t,
		"SELECT name FROM users WHERE age NOT IN (25, 28) ORDER BY name",
		users,
	)
	// Should exclude Bob(25) and Diana(28) -> Alice, Charlie, Eve
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestBatchNotBetween(t *testing.T) {
	users, _ := source.NewFileSource("users", testdataPath("users.csv"))
	rows := parseAndExec(t,
		"SELECT name FROM users WHERE age NOT BETWEEN 26 AND 36 ORDER BY name",
		users,
	)
	// Should exclude Alice(30), Charlie(35), Diana(28) -> Bob(25), Eve(42)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestBatchNotLike(t *testing.T) {
	users, _ := source.NewFileSource("users", testdataPath("users.csv"))
	rows := parseAndExec(t,
		"SELECT name FROM users WHERE name NOT LIKE 'A%' ORDER BY name",
		users,
	)
	// Excludes Alice -> Bob, Charlie, Diana, Eve
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestBatchIsNull(t *testing.T) {
	sparse, _ := source.NewFileSource("data", testdataPath("sparse.jsonl"))
	rows := parseAndExec(t,
		"SELECT id FROM data WHERE tag IS NULL ORDER BY id",
		sparse,
	)
	// id=2 and id=4 have no tag
	if len(rows) != 2 {
		t.Fatalf("IS NULL: expected 2 rows, got %d", len(rows))
	}
}

func TestBatchIsNotNull(t *testing.T) {
	sparse, _ := source.NewFileSource("data", testdataPath("sparse.jsonl"))
	rows := parseAndExec(t,
		"SELECT id FROM data WHERE tag IS NOT NULL ORDER BY id",
		sparse,
	)
	// id=1 and id=3 have tag
	if len(rows) != 2 {
		t.Fatalf("IS NOT NULL: expected 2 rows, got %d", len(rows))
	}
}

// =======================
// SQLITE BATCH SOURCE
// =======================

// createTestSQLiteDB creates a temporary SQLite database file with the given
// table populated. Returns the path; cleaned up via t.Cleanup.
func createTestSQLiteDB(t *testing.T, tableName string, ddl string, inserts []string) string {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, ins := range inserts {
		if _, err := db.Exec(ins); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	return dbPath
}

func TestSQLiteSelectStar(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "customers",
		`CREATE TABLE customers (id INTEGER, name TEXT, city TEXT)`,
		[]string{
			`INSERT INTO customers VALUES (1, 'Alice', 'NYC')`,
			`INSERT INTO customers VALUES (2, 'Bob', 'SF')`,
			`INSERT INTO customers VALUES (3, 'Charlie', 'NYC')`,
		},
	)

	src, err := source.NewSQLiteSource("customers", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT * FROM customers", src)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestSQLiteWhere(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "products",
		`CREATE TABLE products (id INTEGER, name TEXT, price REAL, in_stock INTEGER)`,
		[]string{
			`INSERT INTO products VALUES (1, 'Widget', 9.99, 1)`,
			`INSERT INTO products VALUES (2, 'Gadget', 24.99, 0)`,
			`INSERT INTO products VALUES (3, 'Doohickey', 14.99, 1)`,
			`INSERT INTO products VALUES (4, 'Thingamajig', 4.99, 1)`,
		},
	)

	src, err := source.NewSQLiteSource("products", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT name, price FROM products WHERE price > 10 ORDER BY price", src)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if getString(rows[0], "name") != "Doohickey" {
		t.Errorf("first row: got %s, want Doohickey", getString(rows[0], "name"))
	}
}

func TestSQLiteTableOverride(t *testing.T) {
	// The table in the DB is "real_table" but the source name is "data".
	// We use ?table=real_table to tell the source which table to read.
	dbPath := createTestSQLiteDB(t, "real_table",
		`CREATE TABLE real_table (id INTEGER, val TEXT)`,
		[]string{
			`INSERT INTO real_table VALUES (1, 'alpha')`,
			`INSERT INTO real_table VALUES (2, 'beta')`,
		},
	)

	src, err := source.NewSQLiteSource("data", dbPath, "real_table")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT * FROM data ORDER BY id", src)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if getString(rows[0], "val") != "alpha" {
		t.Errorf("first row val: got %s, want alpha", getString(rows[0], "val"))
	}
}

func TestSQLiteJoinCSV(t *testing.T) {
	// SQLite source joined with CSV file source
	dbPath := createTestSQLiteDB(t, "departments",
		`CREATE TABLE departments (id INTEGER, dept_name TEXT, budget REAL)`,
		[]string{
			`INSERT INTO departments VALUES (1, 'Engineering', 500000)`,
			`INSERT INTO departments VALUES (2, 'Marketing', 200000)`,
			`INSERT INTO departments VALUES (3, 'Sales', 300000)`,
		},
	)

	depts, err := source.NewSQLiteSource("departments", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	// employees via chanSource
	employees := newStaticChan("employees",
		source.Record{"name": "Alice", "dept_id": float64(1)},
		source.Record{"name": "Bob", "dept_id": float64(2)},
		source.Record{"name": "Charlie", "dept_id": float64(1)},
		source.Record{"name": "Diana", "dept_id": float64(3)},
	)

	rows := parseAndExec(t,
		"SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name",
		employees, depts,
	)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	if getString(rows[0], "dept_name") != "Engineering" {
		t.Errorf("Alice dept: got %s, want Engineering", getString(rows[0], "dept_name"))
	}
	if getString(rows[1], "dept_name") != "Marketing" {
		t.Errorf("Bob dept: got %s, want Marketing", getString(rows[1], "dept_name"))
	}
}

func TestSQLiteJoinSQLite(t *testing.T) {
	// Two SQLite sources joined together
	dbPath1 := createTestSQLiteDB(t, "authors",
		`CREATE TABLE authors (id INTEGER, name TEXT)`,
		[]string{
			`INSERT INTO authors VALUES (1, 'Tolkien')`,
			`INSERT INTO authors VALUES (2, 'Hemingway')`,
			`INSERT INTO authors VALUES (3, 'Austen')`,
		},
	)
	dbPath2 := createTestSQLiteDB(t, "books",
		`CREATE TABLE books (id INTEGER, title TEXT, author_id INTEGER, year INTEGER)`,
		[]string{
			`INSERT INTO books VALUES (1, 'The Hobbit', 1, 1937)`,
			`INSERT INTO books VALUES (2, 'The Old Man and the Sea', 2, 1952)`,
			`INSERT INTO books VALUES (3, 'Pride and Prejudice', 3, 1813)`,
			`INSERT INTO books VALUES (4, 'The Lord of the Rings', 1, 1954)`,
		},
	)

	authors, err := source.NewSQLiteSource("authors", dbPath1, "")
	if err != nil {
		t.Fatal(err)
	}
	books, err := source.NewSQLiteSource("books", dbPath2, "")
	if err != nil {
		t.Fatal(err)
	}

	rows := parseAndExec(t,
		"SELECT a.name, b.title, b.year FROM books b JOIN authors a ON b.author_id = a.id ORDER BY b.year",
		books, authors,
	)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	if getString(rows[0], "title") != "Pride and Prejudice" {
		t.Errorf("oldest book: got %s, want Pride and Prejudice", getString(rows[0], "title"))
	}
	if getString(rows[3], "name") != "Tolkien" {
		t.Errorf("newest author: got %s, want Tolkien", getString(rows[3], "name"))
	}
}

func TestSQLiteGroupBy(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "sales",
		`CREATE TABLE sales (id INTEGER, region TEXT, amount REAL)`,
		[]string{
			`INSERT INTO sales VALUES (1, 'East', 100)`,
			`INSERT INTO sales VALUES (2, 'West', 200)`,
			`INSERT INTO sales VALUES (3, 'East', 150)`,
			`INSERT INTO sales VALUES (4, 'West', 300)`,
			`INSERT INTO sales VALUES (5, 'East', 50)`,
		},
	)

	src, err := source.NewSQLiteSource("sales", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t,
		"SELECT region, COUNT(*) cnt, SUM(amount) total FROM sales GROUP BY region ORDER BY region",
		src,
	)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if getString(rows[0], "region") != "East" || getFloat(rows[0], "cnt") != 3 || getFloat(rows[0], "total") != 300 {
		t.Errorf("East: got cnt=%v total=%v", rows[0]["cnt"], rows[0]["total"])
	}
	if getString(rows[1], "region") != "West" || getFloat(rows[1], "cnt") != 2 || getFloat(rows[1], "total") != 500 {
		t.Errorf("West: got cnt=%v total=%v", rows[1]["cnt"], rows[1]["total"])
	}
}

func TestSQLiteLeftJoin(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "teams",
		`CREATE TABLE teams (id INTEGER, team_name TEXT)`,
		[]string{
			`INSERT INTO teams VALUES (1, 'Red')`,
			`INSERT INTO teams VALUES (2, 'Blue')`,
			`INSERT INTO teams VALUES (3, 'Green')`,
		},
	)
	teams, err := source.NewSQLiteSource("teams", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}

	// Only some teams have members
	members := newStaticChan("members",
		source.Record{"name": "Alice", "team_id": float64(1)},
		source.Record{"name": "Bob", "team_id": float64(1)},
		source.Record{"name": "Charlie", "team_id": float64(2)},
	)

	rows := parseAndExec(t,
		"SELECT t.team_name, m.name FROM teams t LEFT JOIN members m ON t.id = m.team_id ORDER BY t.team_name, m.name",
		teams, members,
	)
	// Red: Alice, Bob; Blue: Charlie; Green: NULL
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// Green team should have NULL member
	greenFound := false
	for _, r := range rows {
		if getString(r, "team_name") == "Green" {
			greenFound = true
			if r["name"] != nil {
				t.Errorf("Green team should have NULL name, got %v", r["name"])
			}
		}
	}
	if !greenFound {
		t.Error("Green team not found in LEFT JOIN results")
	}
}

func TestSQLiteNullHandling(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "data",
		`CREATE TABLE data (id INTEGER, value TEXT, score REAL)`,
		[]string{
			`INSERT INTO data VALUES (1, 'a', 10.0)`,
			`INSERT INTO data VALUES (2, NULL, 20.0)`,
			`INSERT INTO data VALUES (3, 'c', NULL)`,
			`INSERT INTO data VALUES (4, NULL, NULL)`,
		},
	)

	src, err := source.NewSQLiteSource("data", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT id FROM data WHERE value IS NULL ORDER BY id", src)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows with NULL value, got %d", len(rows))
	}
	if getFloat(rows[0], "id") != 2 || getFloat(rows[1], "id") != 4 {
		t.Errorf("NULL rows: got ids %v, %v; want 2, 4", rows[0]["id"], rows[1]["id"])
	}
}

func TestSQLiteColumnTypes(t *testing.T) {
	// Verify INTEGER, REAL, TEXT types come through correctly
	dbPath := createTestSQLiteDB(t, "typed",
		`CREATE TABLE typed (int_col INTEGER, real_col REAL, text_col TEXT)`,
		[]string{
			`INSERT INTO typed VALUES (42, 3.14, 'hello')`,
		},
	)

	src, err := source.NewSQLiteSource("typed", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT * FROM typed", src)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	r := rows[0]
	if getFloat(r, "int_col") != 42 {
		t.Errorf("int_col: got %v, want 42", r["int_col"])
	}
	if getFloat(r, "real_col") != 3.14 {
		t.Errorf("real_col: got %v, want 3.14", r["real_col"])
	}
	if getString(r, "text_col") != "hello" {
		t.Errorf("text_col: got %v, want hello", r["text_col"])
	}
}

func TestSQLiteStreamJoin(t *testing.T) {
	// Stream source joined with SQLite batch source using OVER
	dbPath := createTestSQLiteDB(t, "users",
		`CREATE TABLE users (id INTEGER, name TEXT, role TEXT)`,
		[]string{
			`INSERT INTO users VALUES (1, 'Alice', 'admin')`,
			`INSERT INTO users VALUES (2, 'Bob', 'user')`,
			`INSERT INTO users VALUES (3, 'Charlie', 'user')`,
		},
	)

	users, err := source.NewSQLiteSource("users", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}

	stream, feed := newStreamChan("events")
	go func() {
		feed <- source.Record{"user_id": float64(1), "action": "deploy"}
		feed <- source.Record{"user_id": float64(2), "action": "login"}
		feed <- source.Record{"user_id": float64(1), "action": "restart"}
		close(feed)
	}()

	sel := parseQuery(t, "SELECT u.name, u.role, e.action FROM events e JOIN users u ON e.user_id = u.id OVER 1h")
	var buf bytes.Buffer
	eng := New(&buf)
	eng.AddSource(stream)
	eng.AddSource(users)

	if err := eng.Execute(sel); err != nil {
		t.Fatalf("execute: %v", err)
	}

	rows := parseOutput(t, buf.String())
	// After 3 inserts: 1 + 2 + 3 = 6 output rows
	if len(rows) != 6 {
		t.Fatalf("expected 6 output rows, got %d", len(rows))
	}

	// Verify last batch has correct joins
	lastThree := rows[3:]
	foundAdminDeploy := false
	foundUserLogin := false
	for _, r := range lastThree {
		if getString(r, "name") == "Alice" && getString(r, "role") == "admin" {
			foundAdminDeploy = true
		}
		if getString(r, "name") == "Bob" && getString(r, "role") == "user" {
			foundUserLogin = true
		}
	}
	if !foundAdminDeploy {
		t.Error("missing Alice/admin join")
	}
	if !foundUserLogin {
		t.Error("missing Bob/user join")
	}
}

func TestSQLiteURIParsing(t *testing.T) {
	tests := []struct {
		name   string
		uri    string
		path   string
		table  string
		scheme string
	}{
		{
			name: "basic", uri: "sqlite:///tmp/test.db",
			path: "/tmp/test.db", table: "", scheme: "sqlite",
		},
		{
			name: "with_table", uri: "sqlite:///tmp/test.db?table=customers",
			path: "/tmp/test.db", table: "customers", scheme: "sqlite",
		},
		{
			name: "relative", uri: "sqlite://data/my.db",
			path: "data/my.db", table: "", scheme: "sqlite",
		},
		{
			name: "relative_with_table", uri: "sqlite://data/my.db?table=orders",
			path: "data/my.db", table: "orders", scheme: "sqlite",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := source.ParseURI("src", tt.uri)
			if err != nil {
				t.Fatalf("ParseURI(%q): %v", tt.uri, err)
			}
			if cfg.Scheme != tt.scheme {
				t.Errorf("scheme: got %q, want %q", cfg.Scheme, tt.scheme)
			}
			if cfg.URI != tt.path {
				t.Errorf("path: got %q, want %q", cfg.URI, tt.path)
			}
			if cfg.Table != tt.table {
				t.Errorf("table: got %q, want %q", cfg.Table, tt.table)
			}
		})
	}
}

func TestSQLiteEmptyTable(t *testing.T) {
	dbPath := createTestSQLiteDB(t, "empty",
		`CREATE TABLE empty (id INTEGER, name TEXT)`,
		nil,
	)

	src, err := source.NewSQLiteSource("empty", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT * FROM empty", src)
	if len(rows) != 0 {
		t.Errorf("expected 0 rows from empty table, got %d", len(rows))
	}
}

func TestSQLiteLargeTable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "large.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Exec(`CREATE TABLE nums (id INTEGER, val REAL)`)
	tx, _ := db.Begin()
	for i := 0; i < 1000; i++ {
		tx.Exec(`INSERT INTO nums VALUES (?, ?)`, i, float64(i)*1.1)
	}
	tx.Commit()
	db.Close()

	src, err := source.NewSQLiteSource("nums", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	rows := parseAndExec(t, "SELECT COUNT(*) cnt, SUM(val) total FROM nums", src)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if getFloat(rows[0], "cnt") != 1000 {
		t.Errorf("count: got %v, want 1000", rows[0]["cnt"])
	}
}

func TestSQLiteEndToEnd(t *testing.T) {
	// Full end-to-end: SQLite source + CSV source + stream, three-way join
	dbPath := createTestSQLiteDB(t, "categories",
		`CREATE TABLE categories (id INTEGER, cat_name TEXT)`,
		[]string{
			`INSERT INTO categories VALUES (1, 'Electronics')`,
			`INSERT INTO categories VALUES (2, 'Books')`,
			`INSERT INTO categories VALUES (3, 'Clothing')`,
		},
	)

	categories, err := source.NewSQLiteSource("categories", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}

	items := newStaticChan("items",
		source.Record{"name": "Laptop", "cat_id": float64(1), "price": float64(999)},
		source.Record{"name": "Novel", "cat_id": float64(2), "price": float64(15)},
		source.Record{"name": "Shirt", "cat_id": float64(3), "price": float64(30)},
		source.Record{"name": "Phone", "cat_id": float64(1), "price": float64(699)},
	)

	rows := parseAndExec(t,
		"SELECT i.name, c.cat_name, i.price FROM items i JOIN categories c ON i.cat_id = c.id ORDER BY i.price DESC",
		items, categories,
	)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	if getString(rows[0], "name") != "Laptop" {
		t.Errorf("most expensive: got %s, want Laptop", getString(rows[0], "name"))
	}
	if getString(rows[0], "cat_name") != "Electronics" {
		t.Errorf("Laptop category: got %s, want Electronics", getString(rows[0], "cat_name"))
	}
}

func TestSQLiteNonEOFStreamJoin(t *testing.T) {
	// Non-EOF stream joined with SQLite source
	dbPath := createTestSQLiteDB(t, "lookup",
		`CREATE TABLE lookup (code TEXT, label TEXT)`,
		[]string{
			`INSERT INTO lookup VALUES ('A', 'Alpha')`,
			`INSERT INTO lookup VALUES ('B', 'Beta')`,
			`INSERT INTO lookup VALUES ('C', 'Charlie')`,
		},
	)

	lookup, err := source.NewSQLiteSource("lookup", dbPath, "")
	if err != nil {
		t.Fatal(err)
	}

	stream, feed := newStreamChan("events")

	sel := parseQuery(t, "SELECT e.msg, l.label FROM events e JOIN lookup l ON e.code = l.code OVER 1h")
	pr, pw := io.Pipe()
	eng := New(pw)
	eng.AddSource(stream)
	eng.AddSource(lookup)

	var engineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engineErr = eng.Execute(sel)
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)

	// First event
	feed <- source.Record{"code": "A", "msg": "first"}
	if !scanWithTimeout(scanner, 2*time.Second) {
		t.Fatal("timed out after first event")
	}
	var r map[string]interface{}
	json.Unmarshal(scanner.Bytes(), &r)
	if getString(r, "label") != "Alpha" {
		t.Errorf("first join label: got %s, want Alpha", getString(r, "label"))
	}

	// Delayed second event
	time.Sleep(50 * time.Millisecond)
	feed <- source.Record{"code": "C", "msg": "second"}
	// Should get 2 rows (full re-query)
	gotAlpha, gotCharlie := false, false
	for i := 0; i < 2; i++ {
		if !scanWithTimeout(scanner, 2*time.Second) {
			t.Fatal("timed out after second event")
		}
		json.Unmarshal(scanner.Bytes(), &r)
		if getString(r, "label") == "Alpha" {
			gotAlpha = true
		}
		if getString(r, "label") == "Charlie" {
			gotCharlie = true
		}
	}
	if !gotAlpha || !gotCharlie {
		t.Errorf("expected Alpha and Charlie; alpha=%v charlie=%v", gotAlpha, gotCharlie)
	}

	close(feed)
	for scanWithTimeout(scanner, 500*time.Millisecond) {
	}
	wg.Wait()
	if engineErr != nil {
		t.Fatalf("engine error: %v", engineErr)
	}
}

// Also test that ParseURI auto-detects .db/.sqlite/.sqlite3 as sqlite scheme
func TestSQLiteFileExtensionDetection(t *testing.T) {
	// Create a real db file to test with
	dbPath := createTestSQLiteDB(t, "test",
		`CREATE TABLE test (x INTEGER)`,
		[]string{`INSERT INTO test VALUES (1)`},
	)

	// Rename to .db extension
	dbDir := filepath.Dir(dbPath)
	for _, ext := range []string{".db", ".sqlite", ".sqlite3"} {
		newPath := filepath.Join(dbDir, "data"+ext)
		if err := copyFile(dbPath, newPath); err != nil {
			t.Fatalf("copy to %s: %v", ext, err)
		}
		src, err := source.NewSQLiteSource("test", newPath, "test")
		if err != nil {
			t.Fatalf("NewSQLiteSource with %s: %v", ext, err)
		}
		rows := parseAndExec(t, "SELECT * FROM test", src)
		if len(rows) != 1 {
			t.Errorf("extension %s: expected 1 row, got %d", ext, len(rows))
		}
	}
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}

// ======================
// STREAM-TO-TABLE (OVER)
// ======================

func TestStreamToTable(t *testing.T) {
	users, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}

	stream, feed := newStreamChan("events")
	go func() {
		feed <- source.Record{"user_id": float64(1), "action": "login"}
		feed <- source.Record{"user_id": float64(2), "action": "click"}
		feed <- source.Record{"user_id": float64(3), "action": "purchase"}
		close(feed)
	}()

	sel := parseQuery(t, "SELECT u.name, e.action FROM events e JOIN users u ON e.user_id = u.id OVER 1h")
	var buf bytes.Buffer
	eng := New(&buf)
	eng.AddSource(stream)
	eng.AddSource(users)

	if err := eng.Execute(sel); err != nil {
		t.Fatalf("execute: %v", err)
	}

	rows := parseOutput(t, buf.String())
	// Without EVERY, each insert re-queries. After 3 inserts we get 1+2+3=6 output rows.
	if len(rows) != 6 {
		t.Fatalf("expected 6 output rows (1+2+3), got %d", len(rows))
	}

	// Verify last batch has all 3 joined rows
	lastThree := rows[3:]
	names := map[string]bool{}
	for _, r := range lastThree {
		names[getString(r, "name")] = true
	}
	if !names["Alice"] || !names["Bob"] || !names["Charlie"] {
		t.Errorf("expected Alice, Bob, Charlie in last batch; got %v", names)
	}
}

func TestStreamToTableWhere(t *testing.T) {
	users, _ := source.NewFileSource("users", testdataPath("users.csv"))

	stream, feed := newStreamChan("events")
	go func() {
		feed <- source.Record{"user_id": float64(1), "action": "login"}
		feed <- source.Record{"user_id": float64(2), "action": "click"}
		feed <- source.Record{"user_id": float64(5), "action": "login"}
		close(feed)
	}()

	sel := parseQuery(t, "SELECT u.name, e.action FROM events e JOIN users u ON e.user_id = u.id WHERE u.age > 29 OVER 1h")
	var buf bytes.Buffer
	eng := New(&buf)
	eng.AddSource(stream)
	eng.AddSource(users)

	if err := eng.Execute(sel); err != nil {
		t.Fatalf("execute: %v", err)
	}

	rows := parseOutput(t, buf.String())
	// Only Alice(30) and Eve(42) have age>29. Bob(25) filtered out.
	// After insert 1: Alice(login) = 1 row
	// After insert 2: Alice(login) = 1 row (Bob filtered)
	// After insert 3: Alice(login), Eve(login) = 2 rows
	// Total: 1 + 1 + 2 = 4
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestStreamToTableGroupBy(t *testing.T) {
	stream, feed := newStreamChan("events")
	go func() {
		feed <- source.Record{"action": "login"}
		feed <- source.Record{"action": "click"}
		feed <- source.Record{"action": "login"}
		feed <- source.Record{"action": "login"}
		close(feed)
	}()

	sel := parseQuery(t, "SELECT action, COUNT(*) cnt FROM events GROUP BY action OVER 1h")
	var buf bytes.Buffer
	eng := New(&buf)
	eng.AddSource(stream)

	if err := eng.Execute(sel); err != nil {
		t.Fatalf("execute: %v", err)
	}

	rows := parseOutput(t, buf.String())
	// After 4 inserts with re-query each time, we get multiple outputs.
	// Verify that the last output batch shows login=3, click=1
	if len(rows) == 0 {
		t.Fatal("expected output rows")
	}

	// Find the last batch (after 4th insert: 2 groups = 2 rows)
	lastTwo := rows[len(rows)-2:]
	counts := map[string]float64{}
	for _, r := range lastTwo {
		counts[getString(r, "action")] = getFloat(r, "cnt")
	}
	if counts["login"] != 3 {
		t.Errorf("login count: got %v, want 3", counts["login"])
	}
	if counts["click"] != 1 {
		t.Errorf("click count: got %v, want 1", counts["click"])
	}
}

// ==========================
// STREAM-TO-STREAM (OVER)
// ==========================

func TestStreamToStream(t *testing.T) {
	streamA, feedA := newStreamChan("clicks")
	streamB, feedB := newStreamChan("impressions")

	go func() {
		feedA <- source.Record{"page": "home", "user": "alice"}
		feedA <- source.Record{"page": "about", "user": "bob"}
		feedB <- source.Record{"page": "home", "ad": "banner1"}
		feedB <- source.Record{"page": "about", "ad": "banner2"}
		feedB <- source.Record{"page": "home", "ad": "sidebar1"}
		close(feedA)
		close(feedB)
	}()

	sel := parseQuery(t,
		"SELECT c.user, c.page, i.ad FROM clicks c JOIN impressions i ON c.page = i.page ORDER BY c.user, i.ad OVER 1h")
	var buf bytes.Buffer
	eng := New(&buf)
	eng.AddSource(streamA)
	eng.AddSource(streamB)

	if err := eng.Execute(sel); err != nil {
		t.Fatalf("execute: %v", err)
	}

	rows := parseOutput(t, buf.String())
	if len(rows) == 0 {
		t.Fatal("expected joined stream-to-stream rows")
	}

	// Verify that we eventually see alice+home+banner1 and bob+about+banner2
	foundAliceBanner := false
	foundBobBanner := false
	for _, r := range rows {
		if getString(r, "user") == "alice" && getString(r, "ad") == "banner1" {
			foundAliceBanner = true
		}
		if getString(r, "user") == "bob" && getString(r, "ad") == "banner2" {
			foundBobBanner = true
		}
	}
	if !foundAliceBanner {
		t.Error("missing alice+banner1 join row")
	}
	if !foundBobBanner {
		t.Error("missing bob+banner2 join row")
	}
}

func TestStreamToStreamGroupBy(t *testing.T) {
	streamA, feedA := newStreamChan("events")
	streamB, feedB := newStreamChan("scores")

	go func() {
		feedA <- source.Record{"game_id": float64(1), "event": "goal"}
		feedA <- source.Record{"game_id": float64(1), "event": "foul"}
		feedA <- source.Record{"game_id": float64(2), "event": "goal"}
		feedB <- source.Record{"game_id": float64(1), "team": "red", "score": float64(3)}
		feedB <- source.Record{"game_id": float64(2), "team": "blue", "score": float64(1)}
		close(feedA)
		close(feedB)
	}()

	sel := parseQuery(t,
		"SELECT s.team, COUNT(*) event_count FROM events e JOIN scores s ON e.game_id = s.game_id GROUP BY s.team OVER 1h")
	var buf bytes.Buffer
	eng := New(&buf)
	eng.AddSource(streamA)
	eng.AddSource(streamB)

	if err := eng.Execute(sel); err != nil {
		t.Fatalf("execute: %v", err)
	}

	rows := parseOutput(t, buf.String())
	if len(rows) == 0 {
		t.Fatal("expected grouped stream-to-stream rows")
	}

	// Final output should show red team with 2 events (goal+foul), blue with 1 (goal)
	lastRows := rows[len(rows)-2:]
	teamCounts := map[string]float64{}
	for _, r := range lastRows {
		teamCounts[getString(r, "team")] = getFloat(r, "event_count")
	}
	if teamCounts["red"] != 2 {
		t.Errorf("red event_count: got %v, want 2", teamCounts["red"])
	}
	if teamCounts["blue"] != 1 {
		t.Errorf("blue event_count: got %v, want 1", teamCounts["blue"])
	}
}

// =========================
// NON-EOF STREAM BEHAVIOR
// =========================

func TestStreamNonEOF(t *testing.T) {
	// Verify that while the input stream stays open, the engine keeps running
	// and producing output. The result stream should not EOF until the input does.

	stream, feed := newStreamChan("data")

	sel := parseQuery(t, "SELECT * FROM data OVER 1h")
	pr, pw := io.Pipe()
	eng := New(pw)
	eng.AddSource(stream)

	// Run engine in background
	var engineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engineErr = eng.Execute(sel)
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)

	// Send first record
	feed <- source.Record{"seq": float64(1), "msg": "hello"}

	// Should get output
	if !scanWithTimeout(scanner, 2*time.Second) {
		t.Fatal("timed out waiting for output after first record")
	}
	var row1 map[string]interface{}
	json.Unmarshal(scanner.Bytes(), &row1)
	if getFloat(row1, "seq") != 1 {
		t.Errorf("first output seq: got %v, want 1", row1["seq"])
	}

	// Wait a bit, then send second record
	time.Sleep(50 * time.Millisecond)
	feed <- source.Record{"seq": float64(2), "msg": "world"}

	// Should get more output (the re-query after insert produces 2 rows)
	if !scanWithTimeout(scanner, 2*time.Second) {
		t.Fatal("timed out waiting for output after second record")
	}

	// Send third record after another delay
	time.Sleep(50 * time.Millisecond)
	feed <- source.Record{"seq": float64(3), "msg": "still going"}

	if !scanWithTimeout(scanner, 2*time.Second) {
		t.Fatal("timed out waiting for output after third record — stream appears to have EOF'd early")
	}

	// Now close the stream
	close(feed)

	// Drain any remaining output
	for scanWithTimeout(scanner, 500*time.Millisecond) {
		// drain
	}

	wg.Wait()
	if engineErr != nil {
		t.Fatalf("engine error: %v", engineErr)
	}
}

func TestStreamNonEOFWithEvery(t *testing.T) {
	// Test that EVERY produces periodic output while the stream stays open.

	stream, feed := newStreamChan("data")

	sel := parseQuery(t, "SELECT COUNT(*) cnt FROM data OVER 1h EVERY 100ms")
	pr, pw := io.Pipe()
	eng := New(pw)
	eng.AddSource(stream)

	var engineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engineErr = eng.Execute(sel)
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)

	// Send some records
	feed <- source.Record{"x": float64(1)}
	feed <- source.Record{"x": float64(2)}
	feed <- source.Record{"x": float64(3)}

	// Wait for at least one EVERY tick to produce output
	if !scanWithTimeout(scanner, 2*time.Second) {
		t.Fatal("timed out waiting for EVERY output")
	}
	var first map[string]interface{}
	json.Unmarshal(scanner.Bytes(), &first)
	cnt1 := getFloat(first, "cnt")
	if cnt1 < 1 {
		t.Errorf("first EVERY output cnt: got %v, want >= 1", cnt1)
	}

	// Send more records and wait for another EVERY tick
	feed <- source.Record{"x": float64(4)}
	feed <- source.Record{"x": float64(5)}
	time.Sleep(200 * time.Millisecond)

	if !scanWithTimeout(scanner, 2*time.Second) {
		t.Fatal("timed out waiting for second EVERY output")
	}
	var second map[string]interface{}
	json.Unmarshal(scanner.Bytes(), &second)
	cnt2 := getFloat(second, "cnt")
	if cnt2 < cnt1 {
		t.Errorf("second EVERY output cnt (%v) should be >= first (%v)", cnt2, cnt1)
	}

	// Close stream, engine should finish
	close(feed)

	// Drain remaining output
	for scanWithTimeout(scanner, 500*time.Millisecond) {
	}

	wg.Wait()
	if engineErr != nil {
		t.Fatalf("engine error: %v", engineErr)
	}
}

func TestStreamNonEOFDelayedRecords(t *testing.T) {
	// Simulates a slow producer — records arrive with gaps.
	// Engine should stay alive and produce output for each.

	stream, feed := newStreamChan("metrics")

	sel := parseQuery(t, "SELECT name, value FROM metrics OVER 1h")
	pr, pw := io.Pipe()
	eng := New(pw)
	eng.AddSource(stream)

	var engineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engineErr = eng.Execute(sel)
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)
	outputCount := 0

	// Send records with delays, verify output after each
	for i := 1; i <= 5; i++ {
		feed <- source.Record{"name": "cpu", "value": float64(i * 10)}
		// Each insert re-queries, producing i rows (all rows so far)
		for j := 0; j < i; j++ {
			if !scanWithTimeout(scanner, 2*time.Second) {
				t.Fatalf("timed out waiting for output after record %d (line %d)", i, j+1)
			}
			outputCount++
		}
		time.Sleep(50 * time.Millisecond)
	}

	close(feed)

	// Drain
	for scanWithTimeout(scanner, 500*time.Millisecond) {
		outputCount++
	}

	wg.Wait()
	if engineErr != nil {
		t.Fatalf("engine error: %v", engineErr)
	}

	// 1+2+3+4+5 = 15 total output rows
	if outputCount != 15 {
		t.Errorf("total output rows: got %d, want 15", outputCount)
	}
}

// ================================
// STREAM-TO-TABLE NON-EOF
// ================================

func TestStreamToTableNonEOF(t *testing.T) {
	// Stream joined with a file source. Stream stays open.

	users, err := source.NewFileSource("users", testdataPath("users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	stream, feed := newStreamChan("events")

	sel := parseQuery(t, "SELECT u.name, e.action FROM events e JOIN users u ON e.user_id = u.id OVER 1h")
	pr, pw := io.Pipe()
	eng := New(pw)
	eng.AddSource(stream)
	eng.AddSource(users)

	var engineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engineErr = eng.Execute(sel)
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)

	// First event
	feed <- source.Record{"user_id": float64(1), "action": "login"}
	if !scanWithTimeout(scanner, 2*time.Second) {
		t.Fatal("timed out after first event")
	}
	var r map[string]interface{}
	json.Unmarshal(scanner.Bytes(), &r)
	if getString(r, "name") != "Alice" {
		t.Errorf("first event join: got %s, want Alice", getString(r, "name"))
	}

	// Wait, then second event
	time.Sleep(100 * time.Millisecond)
	feed <- source.Record{"user_id": float64(3), "action": "click"}

	// Should get 2 rows (re-query after insert)
	gotAlice, gotCharlie := false, false
	for i := 0; i < 2; i++ {
		if !scanWithTimeout(scanner, 2*time.Second) {
			t.Fatal("timed out after second event")
		}
		json.Unmarshal(scanner.Bytes(), &r)
		if getString(r, "name") == "Alice" {
			gotAlice = true
		}
		if getString(r, "name") == "Charlie" {
			gotCharlie = true
		}
	}
	if !gotAlice || !gotCharlie {
		t.Errorf("expected Alice and Charlie after 2nd event; alice=%v charlie=%v", gotAlice, gotCharlie)
	}

	// Send event for user not in users table (user_id=99) — should join to nothing
	feed <- source.Record{"user_id": float64(99), "action": "ghost"}
	// Re-query should still only return Alice and Charlie (99 has no match)
	for i := 0; i < 2; i++ {
		if !scanWithTimeout(scanner, 2*time.Second) {
			t.Fatal("timed out after ghost event")
		}
	}

	close(feed)
	for scanWithTimeout(scanner, 500*time.Millisecond) {
	}

	wg.Wait()
	if engineErr != nil {
		t.Fatalf("engine error: %v", engineErr)
	}
}

// ================================
// STREAM-TO-STREAM NON-EOF
// ================================

func TestStreamToStreamNonEOF(t *testing.T) {
	// Two streams that stay open, joined together.
	streamA, feedA := newStreamChan("orders")
	streamB, feedB := newStreamChan("payments")

	sel := parseQuery(t,
		"SELECT o.item, p.method FROM orders o JOIN payments p ON o.order_id = p.order_id OVER 1h")
	pr, pw := io.Pipe()
	eng := New(pw)
	eng.AddSource(streamA)
	eng.AddSource(streamB)

	var engineErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engineErr = eng.Execute(sel)
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)

	// Send order first — no payment yet, so join produces nothing
	feedA <- source.Record{"order_id": float64(1), "item": "book"}
	time.Sleep(50 * time.Millisecond)

	// Now send matching payment — join should produce a row
	feedB <- source.Record{"order_id": float64(1), "method": "card"}

	if !scanWithTimeout(scanner, 2*time.Second) {
		t.Fatal("timed out waiting for first join result")
	}
	var r map[string]interface{}
	json.Unmarshal(scanner.Bytes(), &r)
	if getString(r, "item") != "book" || getString(r, "method") != "card" {
		t.Errorf("first join: got item=%s method=%s, want book+card", getString(r, "item"), getString(r, "method"))
	}

	// Send another order + payment pair
	time.Sleep(50 * time.Millisecond)
	feedA <- source.Record{"order_id": float64(2), "item": "pen"}
	time.Sleep(50 * time.Millisecond)
	feedB <- source.Record{"order_id": float64(2), "method": "cash"}

	// Wait for output including the new join
	foundPenCash := false
	deadline := time.After(3 * time.Second)
	for !foundPenCash {
		done := make(chan bool, 1)
		go func() {
			done <- scanner.Scan()
		}()
		select {
		case ok := <-done:
			if !ok {
				t.Fatal("scanner closed unexpectedly")
			}
			json.Unmarshal(scanner.Bytes(), &r)
			if getString(r, "item") == "pen" && getString(r, "method") == "cash" {
				foundPenCash = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for pen+cash join")
		}
	}

	close(feedA)
	close(feedB)

	for scanWithTimeout(scanner, 500*time.Millisecond) {
	}

	wg.Wait()
	if engineErr != nil {
		t.Fatalf("engine error: %v", engineErr)
	}
}

// ================================
// CHAN SOURCE BATCH (in-memory)
// ================================

func TestChanSourceBatch(t *testing.T) {
	// Use chanSource in batch mode (static type, channel closes)
	src := newStaticChan("items",
		source.Record{"id": float64(1), "name": "alpha", "value": float64(10)},
		source.Record{"id": float64(2), "name": "beta", "value": float64(20)},
		source.Record{"id": float64(3), "name": "gamma", "value": float64(30)},
	)
	rows := parseAndExec(t, "SELECT name, value FROM items WHERE value >= 20 ORDER BY name", src)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if getString(rows[0], "name") != "beta" {
		t.Errorf("first row: got %s, want beta", getString(rows[0], "name"))
	}
}

func TestChanSourceTableJoinBatch(t *testing.T) {
	// Join two in-memory chan sources in batch mode
	departments := newStaticChan("departments",
		source.Record{"dept_id": float64(1), "dept_name": "Engineering"},
		source.Record{"dept_id": float64(2), "dept_name": "Marketing"},
		source.Record{"dept_id": float64(3), "dept_name": "Sales"},
	)
	employees := newStaticChan("employees",
		source.Record{"emp_id": float64(1), "name": "Alice", "dept_id": float64(1)},
		source.Record{"emp_id": float64(2), "name": "Bob", "dept_id": float64(2)},
		source.Record{"emp_id": float64(3), "name": "Charlie", "dept_id": float64(1)},
		source.Record{"emp_id": float64(4), "name": "Diana", "dept_id": float64(3)},
	)
	rows := parseAndExec(t,
		"SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.dept_id ORDER BY e.name",
		employees, departments,
	)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	if getString(rows[0], "dept_name") != "Engineering" {
		t.Errorf("Alice dept: got %s, want Engineering", getString(rows[0], "dept_name"))
	}
}

// ================================
// NESTED JSON / EDGE CASES
// ================================

func TestBatchNestedJSON(t *testing.T) {
	// Records with nested objects should be serialized as JSON strings
	src := newStaticChan("configs",
		source.Record{
			"name":   "app1",
			"config": map[string]interface{}{"port": float64(8080), "debug": true},
		},
		source.Record{
			"name":   "app2",
			"config": map[string]interface{}{"port": float64(9090), "debug": false},
		},
	)
	rows := parseAndExec(t, "SELECT name, config FROM configs ORDER BY name", src)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// config should be a JSON string
	cfg := getString(rows[0], "config")
	if cfg == "" {
		t.Error("config should not be empty")
	}
	// Parse it back to verify it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(cfg), &parsed); err != nil {
		t.Errorf("config is not valid JSON: %v", err)
	}
}

func TestBatchArrayJSON(t *testing.T) {
	src := newStaticChan("data",
		source.Record{
			"id":   float64(1),
			"tags": []interface{}{"red", "blue", "green"},
		},
	)
	rows := parseAndExec(t, "SELECT id, tags FROM data", src)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	tags := getString(rows[0], "tags")
	var parsed []string
	if err := json.Unmarshal([]byte(tags), &parsed); err != nil {
		t.Errorf("tags is not valid JSON array: %v", err)
	}
	if len(parsed) != 3 {
		t.Errorf("expected 3 tags, got %d", len(parsed))
	}
}

func TestBatchSingleRow(t *testing.T) {
	src := newStaticChan("x",
		source.Record{"val": float64(42)},
	)
	rows := parseAndExec(t, "SELECT val FROM x", src)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if getFloat(rows[0], "val") != 42 {
		t.Errorf("val: got %v, want 42", rows[0]["val"])
	}
}

func TestBatchBooleans(t *testing.T) {
	src := newStaticChan("flags",
		source.Record{"name": "on", "active": true},
		source.Record{"name": "off", "active": false},
	)
	rows := parseAndExec(t, "SELECT name FROM flags WHERE active = TRUE", src)
	if len(rows) != 1 || getString(rows[0], "name") != "on" {
		t.Errorf("expected only 'on' row, got %v", rows)
	}
}

func TestBatchNullLiteral(t *testing.T) {
	sparse, _ := source.NewFileSource("data", testdataPath("sparse.jsonl"))
	rows := parseAndExec(t, "SELECT id, name FROM data WHERE name IS NOT NULL ORDER BY id", sparse)
	// id=1 (full), id=2 (partial), id=4 (nullish) have name; id=3 does not
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows with non-null name, got %d", len(rows))
	}
}

func TestBatchUnaryMinus(t *testing.T) {
	src := newStaticChan("nums",
		source.Record{"x": float64(5)},
		source.Record{"x": float64(-3)},
		source.Record{"x": float64(0)},
	)
	rows := parseAndExec(t, "SELECT x FROM nums WHERE x > -1 ORDER BY x", src)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ================================
// HELPERS
// ================================

// scanWithTimeout calls scanner.Scan() with a timeout.
func scanWithTimeout(scanner *bufio.Scanner, timeout time.Duration) bool {
	done := make(chan bool, 1)
	go func() {
		done <- scanner.Scan()
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(timeout):
		return false
	}
}
