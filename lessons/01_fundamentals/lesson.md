# Module 1: DuckDB Fundamentals

**Duration**: 3 hours
**Objective**: Get comfortable with DuckDB CLI and understand its unique value proposition

## What is DuckDB?

DuckDB is an **in-process SQL OLAP database management system**. Think of it as "SQLite for analytics":

- **In-process**: Runs directly in your application (no server to manage)
- **OLAP**: Optimized for analytical queries (aggregations, scans) rather than transactions
- **SQL**: Full SQL support with modern extensions
- **Fast**: Columnar storage, vectorized execution, parallel query processing

### Why DuckDB?

**Traditional workflow** (loading data into a database):
```bash
# Load CSV into PostgreSQL/MySQL
psql -c "COPY table FROM 'data.csv' CSV HEADER"
# Then query
psql -c "SELECT * FROM table WHERE ..."
```

**DuckDB workflow** (query files directly):
```bash
duckdb -c "SELECT * FROM 'data.csv' WHERE ..."
```

No loading step! DuckDB reads files on-the-fly with excellent performance.

---

## Installation & Setup

### Installation (Ubuntu 24.04)

DuckDB is already installed on your system at `/home/blas/.local/bin/duckdb` (v1.4.1).

To update to the latest version:
```bash
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
mv duckdb ~/.local/bin/duckdb
chmod +x ~/.local/bin/duckdb
```

Verify installation:
```bash
duckdb --version
```

---

## DuckDB CLI Basics

### Starting DuckDB

**In-memory database** (data disappears when you exit):
```bash
duckdb
```

**Persistent database** (data saved to disk):
```bash
duckdb my_database.db
```

**One-off query** (execute and exit):
```bash
duckdb -c "SELECT 42 AS answer"
```

**Read from stdin**:
```bash
echo "SELECT 'hello' AS greeting" | duckdb
```

### Essential CLI Commands

Inside the DuckDB CLI:

| Command | Description |
|---------|-------------|
| `.help` | Show all commands |
| `.quit` or `.exit` | Exit DuckDB |
| `.tables` | List all tables |
| `.schema [table]` | Show table schema |
| `.mode [mode]` | Set output mode (csv, json, markdown, table) |
| `.timer on` | Show query execution time |
| `.read file.sql` | Execute SQL from file |
| `.output file.txt` | Redirect output to file |

**Example session**:
```sql
-- Start DuckDB
$ duckdb

-- Enable timing
.timer on

-- Set output format to markdown
.mode markdown

-- Run a query
SELECT 'Welcome to DuckDB!' AS message;

-- Exit
.quit
```

---

## DuckDB Architecture: Why It's Fast

### 1. Columnar Storage

**Row-oriented** (traditional databases):
```
| id | name  | age | city |
|----|-------|-----|------|
| 1  | Alice | 30  | NYC  |
| 2  | Bob   | 25  | LA   |
```
Stored as: `1,Alice,30,NYC | 2,Bob,25,LA`

**Column-oriented** (DuckDB):
Stored as:
- IDs: `1,2`
- Names: `Alice,Bob`
- Ages: `30,25`
- Cities: `NYC,LA`

**Why it matters**: Analytical queries often touch only a few columns but many rows. Columnar storage means:
- Better compression (similar values together)
- Read only needed columns (skip unused data)
- SIMD vectorization (process multiple values at once)

### 2. Vectorized Execution

Traditional query engines process **one row at a time**:
```
for each row:
    evaluate WHERE clause
    compute aggregation
```

DuckDB processes **batches of rows** (vectors):
```
for each batch of 1024 rows:
    evaluate WHERE clause on all rows at once
    compute aggregation on all rows at once
```

This leverages CPU cache and SIMD instructions for massive speedup.

### 3. Parallel Query Execution

DuckDB automatically parallelizes queries across CPU cores:
```sql
-- This query will use all available cores
SELECT SUM(sales) FROM large_table;
```

No configuration needed!

---

## Reading Data Directly: The DuckDB Superpower

### Query CSV Files Without Loading

**Basic query**:
```sql
SELECT * FROM 'data.csv';
```

**With filters**:
```sql
SELECT *
FROM 'data.csv'
WHERE age > 25;
```

**Specify delimiter and header**:
```sql
SELECT *
FROM read_csv('data.tsv', delim='\t', header=true);
```

**Auto-detect vs explicit schema**:
```sql
-- Auto-detect (DuckDB guesses types)
SELECT * FROM 'data.csv';

-- Explicit schema
SELECT * FROM read_csv('data.csv',
    columns={'id': 'INTEGER', 'name': 'VARCHAR', 'age': 'INTEGER'}
);
```

### Supported File Formats

| Format | Extension | Example |
|--------|-----------|---------|
| CSV | `.csv` | `SELECT * FROM 'data.csv'` |
| Parquet | `.parquet` | `SELECT * FROM 'data.parquet'` |
| JSON | `.json` | `SELECT * FROM 'data.json'` |
| Excel | `.xlsx` | `INSTALL spatial; SELECT * FROM st_read('data.xlsx')` |
| Gzip/Zstd | `.csv.gz`, `.parquet.zst` | Auto-decompression |

**Example**:
```sql
-- Query compressed CSV
SELECT * FROM 'large_data.csv.gz' LIMIT 10;

-- Query Parquet file
SELECT COUNT(*) FROM 'dataset.parquet';

-- Query JSON
SELECT * FROM 'api_response.json';
```

---

## Quick Data Exploration Commands

### DESCRIBE: Show Schema

```sql
DESCRIBE SELECT * FROM 'data.csv';
```

Output:
```
column_name | column_type | null | key | default | extra
------------|-------------|------|-----|---------|------
id          | INTEGER     | YES  |     |         |
name        | VARCHAR     | YES  |     |         |
age         | INTEGER     | YES  |     |         |
```

### SUMMARIZE: Get Statistics

```sql
SUMMARIZE SELECT * FROM 'data.csv';
```

Output: min, max, avg, std dev, quartiles, null count for each column.

**Pro tip**: Use `SUMMARIZE` as your first exploration step on new datasets!

---

## Hands-on Exercises

### Exercise 1: First DuckDB Session
1. Start DuckDB in-memory mode
2. Run `SELECT 'Hello DuckDB!' AS greeting;`
3. Enable timing with `.timer on`
4. Set output to markdown with `.mode markdown`
5. Exit with `.quit`

### Exercise 2: Query a CSV File
A sample CSV file is in `datasets/raw/employees.csv`.

Tasks:
1. Query all records: `SELECT * FROM 'datasets/raw/employees.csv';`
2. Count total rows: `SELECT COUNT(*) FROM 'datasets/raw/employees.csv';`
3. Get schema: `DESCRIBE SELECT * FROM 'datasets/raw/employees.csv';`
4. Get statistics: `SUMMARIZE SELECT * FROM 'datasets/raw/employees.csv';`

### Exercise 3: CSV vs Parquet Performance
Compare query performance on the same data in different formats.

Files:
- `datasets/raw/sales.csv` (100,000 rows)
- `datasets/raw/sales.parquet` (same data)

Tasks:
1. Enable timing: `.timer on`
2. Query CSV: `SELECT AVG(amount) FROM 'datasets/raw/sales.csv';`
3. Query Parquet: `SELECT AVG(amount) FROM 'datasets/raw/sales.parquet';`
4. Note the time difference (Parquet should be 5-10x faster)

### Exercise 4: Multiple Files at Once
Query multiple CSV files using wildcards:

```sql
-- Combine all CSV files in a directory
SELECT * FROM 'datasets/raw/logs_*.csv';
```

See `exercises/01_fundamentals.sql` for complete exercises with solutions.

---

## Key Takeaways

✅ **DuckDB is "SQLite for analytics"** - in-process, no server, fast analytical queries
✅ **Query files directly** - no loading required, works with CSV, Parquet, JSON, etc.
✅ **Columnar storage + vectorization** - why DuckDB is fast on analytical workloads
✅ **Use DESCRIBE and SUMMARIZE** - essential for quick data exploration
✅ **In-memory vs persistent** - choose based on your needs

---

## Reference Materials

- **CLI Cheat Sheet**: `reference/cheatsheets/cli_basics.md`
- **File Format Guide**: `reference/cheatsheets/file_formats.md`
- **Official Docs**: https://duckdb.org/docs/

---

## Next Module

**Module 2: Modern SQL with DuckDB** - Window functions, CTEs, and advanced analytical queries.
