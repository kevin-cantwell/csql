package engine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kevin-cantwell/csql/internal/ast"
	"github.com/kevin-cantwell/csql/internal/source"
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
