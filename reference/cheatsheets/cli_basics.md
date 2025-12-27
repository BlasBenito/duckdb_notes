# DuckDB CLI Quick Reference

## Starting DuckDB

```bash
# In-memory database (data lost on exit)
duckdb

# Persistent database
duckdb my_database.db

# Execute single query and exit
duckdb -c "SELECT 42"

# Execute query from stdin
echo "SELECT 'hello'" | duckdb

# Read SQL file
duckdb < queries.sql

# Interactive mode with database
duckdb my_database.db
```

## Essential Commands

| Command | Description | Example |
|---------|-------------|---------|
| `.help` | Show all commands | `.help` |
| `.quit` or `.exit` | Exit DuckDB | `.quit` |
| `.tables` | List all tables | `.tables` |
| `.schema [table]` | Show table schema | `.schema employees` |
| `.mode [mode]` | Set output format | `.mode markdown` |
| `.timer on/off` | Toggle query timing | `.timer on` |
| `.read file.sql` | Execute SQL from file | `.read queries.sql` |
| `.output file.txt` | Redirect output to file | `.output results.txt` |
| `.output stdout` | Reset output to terminal | `.output stdout` |
| `.shell cmd` | Run shell command | `.shell ls -la` |
| `.open db.db` | Open different database | `.open another.db` |

## Output Modes

```sql
.mode table       -- ASCII table (default)
.mode markdown    -- Markdown table
.mode csv         -- CSV output
.mode json        -- JSON output
.mode line        -- One value per line
.mode list        -- Values delimited by "|"
.mode box         -- Unicode box drawing
.mode html        -- HTML table
```

## Reading Files

```sql
-- CSV files
SELECT * FROM 'file.csv';
SELECT * FROM read_csv('file.csv', delim=',', header=true);

-- Parquet files
SELECT * FROM 'file.parquet';

-- JSON files
SELECT * FROM 'file.json';

-- Multiple files (glob patterns)
SELECT * FROM 'data/*.csv';
SELECT * FROM 'data/**.parquet';  -- Recursive

-- Compressed files (auto-detected)
SELECT * FROM 'file.csv.gz';
SELECT * FROM 'file.parquet.zst';

-- Remote files
SELECT * FROM 'https://example.com/data.csv';
```

## Data Exploration

```sql
-- Get schema information
DESCRIBE SELECT * FROM 'file.csv';

-- Get statistical summary
SUMMARIZE SELECT * FROM 'file.csv';

-- Show table info
PRAGMA table_info('table_name');

-- List available functions
SELECT * FROM duckdb_functions();

-- Show loaded extensions
SELECT * FROM duckdb_extensions();
```

## Creating Tables

```sql
-- From CSV
CREATE TABLE my_table AS
SELECT * FROM 'data.csv';

-- From query
CREATE TABLE summary AS
SELECT department, COUNT(*) AS count
FROM employees
GROUP BY department;

-- From Parquet
CREATE TABLE my_table AS
SELECT * FROM 'data.parquet';

-- With explicit schema
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    salary DOUBLE
);
```

## Exporting Data

```sql
-- Export to CSV
COPY (SELECT * FROM table) TO 'output.csv' (HEADER, DELIMITER ',');

-- Export to Parquet
COPY table TO 'output.parquet' (FORMAT PARQUET);

-- Export to JSON
COPY (SELECT * FROM table) TO 'output.json';

-- Using file extension (auto-detect format)
COPY table TO 'output.csv';
COPY table TO 'output.parquet';
```

## Common Patterns

### Save query results to file
```sql
.output results.csv
.mode csv
SELECT * FROM my_query;
.output stdout
```

### Execute script and save output
```bash
duckdb -c ".read analysis.sql" > output.txt
```

### Create database from CSV files
```bash
duckdb my_db.db << EOF
CREATE TABLE table1 AS SELECT * FROM 'data1.csv';
CREATE TABLE table2 AS SELECT * FROM 'data2.csv';
EOF
```

### Run query on multiple files
```bash
duckdb -c "SELECT COUNT(*) FROM 'data/*.csv'"
```

## Performance Tips

```sql
-- Enable timing to measure query speed
.timer on

-- Use EXPLAIN to see query plan
EXPLAIN SELECT * FROM large_table WHERE condition;

-- Use EXPLAIN ANALYZE to see actual timings
EXPLAIN ANALYZE SELECT * FROM large_table WHERE condition;

-- Check memory usage
SELECT * FROM pragma_database_size();

-- Limit memory (in bytes)
SET memory_limit='4GB';

-- Set number of threads
SET threads=4;
```

## Configuration

```sql
-- Show all settings
SELECT * FROM duckdb_settings();

-- Set memory limit
SET memory_limit='8GB';

-- Set number of threads
SET threads=8;

-- Set temporary directory
SET temp_directory='/path/to/temp';

-- Enable profiling
SET enable_profiling='json';
SET profiling_output='profile.json';

-- Show progress bars for long queries
SET enable_progress_bar=true;
```

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+C` | Cancel current query |
| `Ctrl+D` | Exit DuckDB |
| `Ctrl+L` | Clear screen |
| `↑/↓` | Navigate command history |
| `Tab` | Auto-complete (table/column names) |

## Debugging

```sql
-- Show warnings
.warnings

-- Verbose error messages
SET debug_mode=true;

-- Show query plan
EXPLAIN SELECT ...;

-- Show query plan with timings
EXPLAIN ANALYZE SELECT ...;

-- Show query profiling
SET enable_profiling='query_tree';
SELECT ...;
```

## Common Use Cases

### Quick data exploration
```bash
duckdb -c "SUMMARIZE SELECT * FROM 'data.csv'"
```

### One-off data transformation
```bash
duckdb -c "COPY (SELECT * FROM 'input.csv' WHERE x > 10) TO 'output.parquet'"
```

### Interactive analysis
```bash
duckdb
.timer on
.mode markdown
SELECT * FROM 'data.csv' WHERE condition LIMIT 10;
```

### Create analytical database
```bash
duckdb analytics.db << EOF
CREATE TABLE sales AS SELECT * FROM 'sales_*.csv';
CREATE TABLE customers AS SELECT * FROM 'customers.parquet';
CREATE VIEW monthly_sales AS
  SELECT DATE_TRUNC('month', date) AS month, SUM(amount) AS total
  FROM sales GROUP BY month;
EOF
```
