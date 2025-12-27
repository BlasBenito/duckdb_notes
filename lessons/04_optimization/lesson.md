# Module 4: Query Optimization & Performance

**Duration**: 3 hours
**Objective**: Write efficient queries and understand DuckDB's execution engine

## Understanding Query Performance

### The Query Execution Pipeline

```
SQL Query → Parser → Planner → Optimizer → Execution → Results
```

DuckDB's optimizer automatically:
- Pushes filters down to file readers
- Prunes unnecessary columns (projection pushdown)
- Parallelizes execution
- Chooses optimal join strategies

## EXPLAIN: Your Performance Debugging Tool

### Basic EXPLAIN

```sql
EXPLAIN SELECT * FROM sales WHERE amount > 1000;
```

Shows the query plan without executing.

### EXPLAIN ANALYZE

```sql
EXPLAIN ANALYZE SELECT * FROM sales WHERE amount > 1000;
```

Executes query and shows actual timings.

### Reading Query Plans

Key elements to look for:
- **SEQ_SCAN**: Full table/file scan
- **FILTER**: WHERE clause application
- **PROJECTION**: Column selection
- **HASH_JOIN / MERGE_JOIN**: Join operations
- **HASH_GROUP_BY**: Aggregation
- **Cardinality**: Estimated/actual row counts

Example:
```sql
EXPLAIN ANALYZE
SELECT product_category, SUM(amount)
FROM 'datasets/raw/sales.parquet'
WHERE amount > 100
GROUP BY product_category;
```

Look for:
- Filter pushdown (filter applied during scan)
- Estimated vs actual row counts
- Join types and order
- Parallel execution indicators

## Filter and Projection Pushdown

### Filter Pushdown

DuckDB pushes WHERE filters to the file reader:

```sql
-- Filter applied DURING file read (fast)
SELECT * FROM 'sales.parquet'
WHERE year = 2024;

-- vs Filter applied AFTER reading (slower)
SELECT * FROM (SELECT * FROM 'sales.parquet')
WHERE year = 2024;
```

Verify with EXPLAIN - look for filters in PARQUET_SCAN.

### Projection Pushdown

Only read needed columns:

```sql
-- Only reads 2 columns from Parquet (fast)
SELECT product_category, amount
FROM 'sales.parquet';

-- Reads ALL columns then selects (slower)
SELECT product_category, amount
FROM (SELECT * FROM 'sales.parquet');
```

Parquet format excels here - columnar storage.

## Query Optimization Techniques

### 1. Use Appropriate File Formats

```sql
-- Slow: CSV scan
SELECT AVG(metric_10) FROM 'wide_table.csv';

-- Fast: Parquet with column pruning
SELECT AVG(metric_10) FROM 'wide_table.parquet';
```

**Why**: Parquet only reads metric_10 column.

### 2. Filter Early and Often

```sql
-- Bad: Filter after aggregation
SELECT * FROM (
    SELECT year, product_category, SUM(amount) AS total
    FROM sales
    GROUP BY year, product_category
) WHERE year = 2024;

-- Good: Filter before aggregation
SELECT year, product_category, SUM(amount) AS total
FROM sales
WHERE year = 2024
GROUP BY year, product_category;
```

### 3. Partition Your Data

```sql
-- Without partitioning: Scans all files
SELECT SUM(amount)
FROM 'sales/**/*.parquet'
WHERE year = 2024;

-- With partitioning: Only scans year=2024 directory
SELECT SUM(amount)
FROM 'sales_partitioned/**/*.parquet'
WHERE year = 2024;
```

### 4. Use Sampling for Development

```sql
-- Develop on sample
SELECT * FROM 'huge_table.parquet'
USING SAMPLE 1 PERCENT;

-- Run full query when ready
SELECT * FROM 'huge_table.parquet';
```

### 5. Optimize Joins

**Join order matters**:
```sql
-- Better: Small table first
SELECT *
FROM small_table s
JOIN large_table l ON s.id = l.id;
```

**Join types**:
- HASH JOIN: Default, good for most cases
- MERGE JOIN: For pre-sorted data
- NESTED LOOP: For very small tables

View with EXPLAIN to see chosen strategy.

## Memory Management

### Check Memory Usage

```sql
SELECT * FROM pragma_database_size();
```

### Set Memory Limit

```sql
SET memory_limit='4GB';
```

### Spilling to Disk

When data exceeds memory, DuckDB spills to disk automatically.

Configure temp directory:
```sql
SET temp_directory='/path/to/fast/disk';
```

## Parallel Execution

DuckDB parallelizes automatically.

### Control Thread Count

```sql
-- Check current setting
SELECT current_setting('threads');

-- Set thread count
SET threads=8;
```

**Guidelines**:
- Default: All CPU cores
- For shared systems: Limit to avoid contention
- For I/O-bound queries: More threads may not help

## Sampling Techniques

### Random Sampling

```sql
-- 10% random sample
SELECT * FROM large_table
USING SAMPLE 10 PERCENT;

-- Fixed number of rows (reservoir sampling)
SELECT * FROM large_table
USING SAMPLE 1000 ROWS;
```

### Deterministic Sampling

```sql
-- Reproducible sample
SELECT * FROM large_table
USING SAMPLE 10 PERCENT (bernoulli, 42);  -- 42 is seed
```

### Stratified Sampling

```sql
-- Sample from each group
SELECT *
FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY RANDOM()) <= 100;
```

## Indexes in DuckDB

DuckDB is primarily a **columnar** database - traditional row-based indexes are less important.

### When Indexes Help

DuckDB automatically creates:
- Statistics for each column (min, max, null count)
- Lightweight metadata for pruning

Explicit indexes rarely needed, but can help for:
- Repeated point lookups on large tables
- Foreign key constraints

### Creating Indexes

```sql
CREATE INDEX idx_customer_id ON orders(customer_id);
```

**Note**: Most analytical queries don't benefit from indexes. Focus on:
- Partitioning
- Parquet format
- Filter/projection pushdown

## Performance Troubleshooting Checklist

1. **Use EXPLAIN ANALYZE** - Find bottlenecks
2. **Check file format** - Use Parquet for analytics
3. **Verify filter pushdown** - Filters in SCAN operation?
4. **Check partition pruning** - Only relevant files scanned?
5. **Review join order** - Small tables first?
6. **Monitor memory** - Spilling to disk?
7. **Consider sampling** - For development/testing
8. **Check parallelization** - Using all cores?

## Practical Examples

### Example 1: Slow Query Diagnosis

```sql
-- Slow query
EXPLAIN ANALYZE
SELECT *
FROM 'huge_sales.csv'
WHERE year = 2024;

-- Issues found:
-- 1. CSV format (slow scan)
-- 2. No partitioning
-- 3. year column not indexed

-- Optimized:
-- Step 1: Convert to Parquet with partitioning
COPY (
    SELECT *, EXTRACT(YEAR FROM sale_date) AS year
    FROM 'huge_sales.csv'
) TO 'sales_optimized' (
    FORMAT PARQUET,
    PARTITION_BY (year),
    COMPRESSION 'ZSTD'
);

-- Step 2: Fast query
EXPLAIN ANALYZE
SELECT *
FROM 'sales_optimized/**/*.parquet'
WHERE year = 2024;
-- Now with partition pruning!
```

### Example 2: Join Optimization

```sql
-- Analyze join performance
EXPLAIN ANALYZE
SELECT o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= '2024-01-01';

-- Check:
-- - Join type (HASH_JOIN usually best)
-- - Filter applied before join?
-- - Cardinality estimates accurate?
```

## Key Takeaways

✅ **EXPLAIN ANALYZE** is your best friend for performance debugging
✅ **Parquet + partitioning** beats CSV by 10-100x
✅ **Filter/projection pushdown** happens automatically
✅ **DuckDB parallelizes** without configuration
✅ **Indexes rarely needed** - focus on format and partitioning
✅ **Sample for development** - test on small data first

## Practice Exercises

See `exercises/04_optimization.sql` for hands-on performance tuning exercises.

## Next Module

**Module 5: DuckDB Extensions & Advanced Features** - httpfs, spatial, and extension ecosystem.
