# Continuous SQL

This tool is a PoC for querying one or more streams with SQL. It lexes SQL passed as a command line argument and processes one or more files as structured streams.

## Database
The engine is a series of partitioned SQLite databases. Each database represents a window of time.

Each stream of events is continuously inserted into the appropriate database.

Queries are executed in parallel against 

## Streams