# csql

A Unix-native SQL tool for querying data at rest and in motion. Reads CSV files, JSON lines, SQLite databases, and stdin streams. Joins them together with familiar SQL. Outputs JSON lines to stdout.

Single static binary compiled with pure-Go SQLite -- no CGO, no external dependencies.

```
csql [--source name=uri ...] 'SQL query'
```

Any table in the query that isn't bound to an explicit `--source` reads JSON lines from stdin.

## Sources

| Scheme | Type | Example |
|--------|------|---------|
| `file://` | CSV, JSON, JSONL files | `--source users=file://users.csv` |
| `sqlite://` | SQLite database table | `--source orders=sqlite:///var/data/app.db` |
| stdin | JSON lines from stdin | (automatic for unbound table names) |

SQLite sources read the table matching the source name by default. Use `?table=` to override:

```
--source u='sqlite:///app.db?table=users'
```

## Examples

All examples below use the test data files in `testdata/`.

### Query a CSV file

```
$ csql --source users=file://testdata/users.csv \
    'SELECT name, age FROM users WHERE age > 29 ORDER BY age DESC'
{"age":42,"name":"Eve"}
{"age":35,"name":"Charlie"}
{"age":30,"name":"Alice"}
```

### Aggregate functions

```
$ csql --source users=file://testdata/users.csv \
    'SELECT COUNT(*) cnt, MIN(age) youngest, MAX(age) oldest, AVG(age) avg_age FROM users'
{"avg_age":32,"cnt":5,"oldest":42,"youngest":25}
```

### GROUP BY a JSONL file

```
$ csql --source events=file://testdata/events.jsonl \
    'SELECT action, COUNT(*) cnt FROM events GROUP BY action ORDER BY cnt DESC'
{"action":"login","cnt":3}
{"action":"purchase","cnt":1}
{"action":"logout","cnt":1}
{"action":"click","cnt":1}
```

### JOIN a CSV with a JSONL file

```
$ csql \
    --source users=file://testdata/users.csv \
    --source events=file://testdata/events.jsonl \
    'SELECT u.name, e.action FROM events e JOIN users u ON e.user_id = u.id ORDER BY u.name'
{"action":"login","name":"Alice"}
{"action":"purchase","name":"Alice"}
{"action":"click","name":"Bob"}
{"action":"logout","name":"Bob"}
{"action":"login","name":"Charlie"}
{"action":"login","name":"Eve"}
```

### Three-way JOIN across file types

Orders (JSONL) joined to users (CSV) and products (CSV):

```
$ csql \
    --source users=file://testdata/users.csv \
    --source orders=file://testdata/orders.jsonl \
    --source products=file://testdata/products.csv \
    'SELECT u.name, p.name pname, o.quantity
     FROM orders o
     JOIN users u ON o.user_id = u.id
     JOIN products p ON o.product_id = p.id
     ORDER BY o.order_id'
{"name":"Alice","pname":"Widget","quantity":2}
{"name":"Bob","pname":"Thingamajig","quantity":1}
{"name":"Alice","pname":"Gadget","quantity":1}
{"name":"Charlie","pname":"Doohickey","quantity":3}
{"name":"Diana","pname":"Widget","quantity":1}
{"name":"Bob","pname":"Whatchamacallit","quantity":5}
```

### LEFT JOIN (NULLs for unmatched rows)

```
$ csql \
    --source users=file://testdata/users.csv \
    --source orders=file://testdata/orders.jsonl \
    'SELECT u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name'
{"name":"Alice","order_id":1}
{"name":"Alice","order_id":3}
{"name":"Bob","order_id":2}
{"name":"Bob","order_id":6}
{"name":"Charlie","order_id":4}
{"name":"Diana","order_id":5}
{"name":"Eve","order_id":null}
```

### Arithmetic expressions

```
$ csql --source products=file://testdata/products.csv \
    'SELECT name, price * 2 double_price FROM products WHERE price < 10 ORDER BY price'
{"double_price":9.98,"name":"Thingamajig"}
{"double_price":15,"name":"Whatchamacallit"}
{"double_price":19.98,"name":"Widget"}
```

### BETWEEN, IN, LIKE, NOT

```
$ csql --source users=file://testdata/users.csv \
    'SELECT name, age FROM users WHERE age BETWEEN 28 AND 35 ORDER BY name'
{"age":30,"name":"Alice"}
{"age":35,"name":"Charlie"}
{"age":28,"name":"Diana"}
```

```
$ csql --source users=file://testdata/users.csv \
    'SELECT name FROM users WHERE age NOT IN (25, 28) ORDER BY name'
{"name":"Alice"}
{"name":"Charlie"}
{"name":"Eve"}
```

```
$ csql --source users=file://testdata/users.csv \
    "SELECT name FROM users WHERE name LIKE '%li%'"
{"name":"Alice"}
{"name":"Charlie"}
```

### Query a SQLite database

```
$ csql --source people=sqlite:///tmp/people.db \
    'SELECT name, age FROM people WHERE age > 27 ORDER BY age'
{"age":30,"name":"Alice"}
{"age":35,"name":"Charlie"}
```

### Pipe stdin and JOIN with a file

Any table referenced in the query that isn't bound to a `--source` flag reads from stdin as JSON lines:

```
$ echo '{"user_id":1,"action":"login"}
{"user_id":2,"action":"click"}
{"user_id":1,"action":"purchase"}' | csql \
    --source users=file://testdata/users.csv \
    'SELECT u.name, e.action FROM stdin e JOIN users u ON e.user_id = u.id'
{"action":"login","name":"Alice"}
{"action":"click","name":"Bob"}
{"action":"purchase","name":"Alice"}
```

### Pipe stdin and JOIN with a SQLite database

```
$ echo '{"person_id":1,"action":"login"}
{"person_id":2,"action":"click"}' | csql \
    --source 'people=sqlite:///tmp/people.db' \
    'SELECT p.name, s.action FROM stdin s JOIN people p ON s.person_id = p.id'
{"action":"login","name":"Alice"}
{"action":"click","name":"Bob"}
```

### Streaming with OVER (tumbling windows)

`OVER <duration>` switches to streaming mode. The query re-executes after every record inserted into the current time window:

```
$ tail -f /var/log/app.jsonl | csql \
    'SELECT status, COUNT(*) cnt FROM stdin GROUP BY status OVER 5m'
```

### Streaming with EVERY (periodic output)

`EVERY <duration>` throttles output to a fixed interval instead of re-querying after every insert:

```
$ tail -f /var/log/app.jsonl | csql \
    'SELECT status, COUNT(*) cnt FROM stdin GROUP BY status OVER 5m EVERY 10s'
```

### Composable with other Unix tools

Output is JSON lines, so it pipes naturally into `jq`, `grep`, `wc`, etc.:

```
$ csql --source users=file://testdata/users.csv \
    'SELECT name, age FROM users ORDER BY age DESC LIMIT 3' \
  | jq -r '.name'
Eve
Charlie
Alice
```

## SQL Reference

```sql
SELECT [DISTINCT] columns
FROM table [alias]
  [JOIN table [alias] ON condition]
  [LEFT JOIN table [alias] ON condition]
WHERE condition
GROUP BY expressions
ORDER BY expr [ASC|DESC] [, ...]
LIMIT n
OVER duration    -- streaming: tumbling window size (e.g. 5m, 1h)
EVERY duration   -- streaming: output interval (e.g. 10s)
```

### Expressions

| Category | Operators |
|----------|-----------|
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| Comparison | `=`, `!=`, `<`, `<=`, `>`, `>=` |
| Logic | `AND`, `OR`, `NOT` |
| Pattern | `LIKE`, `NOT LIKE` |
| Range | `BETWEEN x AND y`, `NOT BETWEEN x AND y` |
| Set | `IN (...)`, `NOT IN (...)` |
| Null | `IS NULL`, `IS NOT NULL` |
| Aggregates | `COUNT(*)`, `SUM()`, `AVG()`, `MIN()`, `MAX()` |

All SQLite built-in functions (UPPER, LOWER, COALESCE, etc.) are available.

### Duration format

Go duration strings: `5s`, `100ms`, `1m`, `1h`, `2h30m`.

## Building

Requires Go 1.21+. Produces a static binary with no external dependencies.

```
go build -o csql ./cmd/csql
```

For a minimal binary:

```
CGO_ENABLED=0 go build -ldflags="-s -w" -o csql ./cmd/csql
```

## Architecture

The engine is a series of in-memory SQLite databases. In batch mode (no `OVER`), all sources load into a single database. In streaming mode, each time window gets its own database.

Multiple streaming sources are supported -- their record channels are merged and records are routed to the correct table by name.

### Lazy batch source loading

In streaming mode, the engine analyzes the query to choose the most efficient loading strategy for each batch (static) source:

| Strategy | When | How |
|----------|------|-----|
| **ATTACH** | SQLite sources | The original `.db` file is ATTACHed directly to each window database. Zero copying -- SQLite handles indexing and lookup natively. |
| **Indexed** | File/JSONL sources used only via equi-join (`a.col = b.col`) | Records are read into a Go hash map keyed on the join column. Only matching rows are inserted into each window database on demand as streaming records arrive. |
| **Full scan** | File sources in FROM clause, or non-equi-joins | Pre-loaded into a shared static database and ATTACHed to each window (unchanged from before). |

This means a 1M-row SQLite lookup table used in a `JOIN ... ON` never gets copied into memory -- it stays on disk and SQLite queries it directly. A large CSV used in an equi-join only inserts the rows that actually match incoming stream records.

In batch mode (no `OVER`), SQLite sources are also ATTACHed directly rather than copied row-by-row.
