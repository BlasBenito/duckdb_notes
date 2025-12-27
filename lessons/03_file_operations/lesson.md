# Module 3: Advanced File Operations

**Duration**: 3 hours
**Objective**: Master DuckDB's file reading/writing capabilities for data workflows

## Overview

DuckDB's killer feature is querying files directly without loading. This module covers:
- Glob patterns for reading multiple files
- File format conversions
- Partitioned datasets
- Remote file access
- Export operations with COPY

---

## Part 1: Glob Patterns and Multiple Files (45 minutes)

### Basic Glob Patterns

Query multiple files matching a pattern:

```sql
-- All CSV files in directory
SELECT * FROM 'data/*.csv';

-- All Parquet files recursively
SELECT * FROM 'data/**.parquet';

-- Specific pattern
SELECT * FROM 'logs/2024-*.csv';
```

### Glob Pattern Syntax

| Pattern | Matches | Example |
|---------|---------|---------|
| `*` | Any characters (single directory) | `data/*.csv` |
| `**` | Any characters (recursive) | `data/**/*.csv` |
| `?` | Single character | `log_?.csv` |
| `[abc]` | One of a, b, or c | `file_[123].csv` |
| `[a-z]` | Character range | `data_[a-z].csv` |
| `{a,b}` | Alternatives | `{sales,revenue}.csv` |

### Examples

```sql
-- All CSV files in any subdirectory
SELECT * FROM 'data/**/*.csv';

-- All files for year 2024
SELECT * FROM 'logs/2024-*.csv';

-- Multiple specific files
SELECT * FROM 'data/{sales,revenue,costs}.parquet';

-- Pattern with character class
SELECT * FROM 'logs/log_[0-9].csv';
```

### Getting File Metadata

DuckDB adds `filename` pseudo-column:

```sql
SELECT
    filename,
    COUNT(*) AS records_per_file,
    SUM(amount) AS total_per_file
FROM 'data/*.csv'
GROUP BY filename;
```

### Union Multiple File Types

```sql
-- Combine CSV and Parquet files
SELECT 'csv' AS source_type, * FROM 'data/*.csv'
UNION ALL
SELECT 'parquet', * FROM 'data/*.parquet';
```

---

## Part 2: File Format Conversions (45 minutes)

### Why Convert Formats?

- **CSV → Parquet**: 5-10x smaller, 10-100x faster queries
- **JSON → Parquet**: Structured format, better performance
- **Parquet → CSV**: Human-readable, broader compatibility

### CSV to Parquet

```sql
-- Simple conversion
COPY (SELECT * FROM 'input.csv')
TO 'output.parquet' (FORMAT PARQUET);

-- With compression
COPY (SELECT * FROM 'input.csv')
TO 'output.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- Batch convert multiple files
COPY (SELECT * FROM 'data/*.csv')
TO 'combined.parquet' (FORMAT PARQUET);
```

### Parquet to CSV

```sql
COPY (SELECT * FROM 'data.parquet')
TO 'data.csv' (HEADER, DELIMITER ',');
```

### JSON to Parquet

```sql
COPY (SELECT * FROM 'data.json')
TO 'data.parquet' (FORMAT PARQUET);
```

### Compression Options

| Codec | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| `UNCOMPRESSED` | Fastest | 1x | Fast writes, temp data |
| `SNAPPY` | Fast | 2-3x | Balanced (default) |
| `GZIP` | Slow | 3-4x | Maximum compatibility |
| `ZSTD` | Balanced | 3-5x | Best overall (recommended) |
| `LZ4` | Very fast | 2x | Speed priority |

```sql
-- Different compression codecs
COPY (SELECT * FROM 'data.csv')
TO 'data_snappy.parquet' (FORMAT PARQUET, COMPRESSION 'SNAPPY');

COPY (SELECT * FROM 'data.csv')
TO 'data_zstd.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD');
```

### File Format Comparison

Create test datasets to see size/performance differences:

```sql
-- Original CSV
COPY (SELECT * FROM 'large_dataset.csv')
TO 'test.csv' (HEADER, DELIMITER ',');

-- Parquet (Snappy)
COPY (SELECT * FROM 'large_dataset.csv')
TO 'test_snappy.parquet' (FORMAT PARQUET, COMPRESSION 'SNAPPY');

-- Parquet (ZSTD)
COPY (SELECT * FROM 'large_dataset.csv')
TO 'test_zstd.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- Compare file sizes
.shell ls -lh test*
```

---

## Part 3: Partitioned Datasets (1 hour)

### What is Partitioning?

Organize data into subdirectories based on column values:

```
data/
├── year=2023/
│   ├── month=01/
│   │   └── data.parquet
│   └── month=02/
│       └── data.parquet
└── year=2024/
    ├── month=01/
    │   └── data.parquet
    └── month=02/
        └── data.parquet
```

### Benefits

✅ **Query only relevant files** (partition pruning)
✅ **Faster queries** on partitioned columns
✅ **Organize large datasets** logically
✅ **Incremental updates** easier

### Reading Partitioned Data

DuckDB automatically detects Hive-style partitions:

```sql
-- Reads all partitions, extracts partition columns
SELECT * FROM 'data/**/*.parquet';

-- Partition columns automatically available
SELECT year, month, COUNT(*) AS records
FROM 'data/**/*.parquet'
GROUP BY year, month;
```

### Partition Pruning

When you filter on partition columns, DuckDB only reads relevant files:

```sql
-- Only reads files in year=2024/month=01/
SELECT * FROM 'data/**/*.parquet'
WHERE year = 2024 AND month = 1;
```

Verify with EXPLAIN:

```sql
EXPLAIN SELECT * FROM 'data/**/*.parquet'
WHERE year = 2024;
-- Look for "Filters: year=2024" and reduced file count
```

### Writing Partitioned Data

```sql
-- Create partitioned dataset
COPY (
    SELECT
        *,
        EXTRACT(YEAR FROM sale_date) AS year,
        EXTRACT(MONTH FROM sale_date) AS month
    FROM 'sales.csv'
) TO 'partitioned_sales' (
    FORMAT PARQUET,
    PARTITION_BY (year, month)
);
```

This creates:
```
partitioned_sales/
├── year=2023/
│   ├── month=1/
│   ├── month=2/
│   └── ...
└── year=2024/
    ├── month=1/
    └── ...
```

### Partition Column Selection

Choose partition columns wisely:

✅ **Good partition columns**:
- Low cardinality (date parts, categories, regions)
- Frequently filtered in queries
- Data naturally organized by this dimension

❌ **Bad partition columns**:
- High cardinality (user_id, transaction_id)
- Never used in WHERE clauses
- Creates too many small files

### Example: Partitioned Sales Data

```sql
-- Create partitioned dataset by year and product category
COPY (
    SELECT
        *,
        EXTRACT(YEAR FROM sale_date) AS year
    FROM 'datasets/raw/sales.parquet'
) TO 'datasets/processed/sales_partitioned' (
    FORMAT PARQUET,
    PARTITION_BY (year, product_category),
    COMPRESSION 'ZSTD'
);

-- Query only Electronics sales in 2023
SELECT SUM(amount)
FROM 'datasets/processed/sales_partitioned/**/*.parquet'
WHERE year = 2023 AND product_category = 'Electronics';
-- Only reads relevant partition!
```

---

## Part 4: Remote Files (30 minutes)

### HTTP/HTTPS Files

Query files directly from URLs:

```sql
-- Read CSV from URL
SELECT * FROM 'https://example.com/data.csv';

-- Read Parquet from URL
SELECT * FROM 'https://example.com/data.parquet';
```

### S3-Compatible Storage

Requires `httpfs` extension:

```sql
-- Install extension
INSTALL httpfs;
LOAD httpfs;

-- Configure credentials (if needed)
SET s3_region='us-east-1';
SET s3_access_key_id='your_key';
SET s3_secret_access_key='your_secret';

-- Query S3 file
SELECT * FROM 's3://bucket/path/to/data.parquet';

-- Query with glob pattern
SELECT * FROM 's3://bucket/data/*.parquet';
```

### Public Datasets

Many public datasets available without credentials:

```sql
-- AWS Open Data Registry example
INSTALL httpfs;
LOAD httpfs;

-- Query public Parquet files
SELECT * FROM 's3://bucket-name/public-data/*.parquet';
```

### Performance Considerations

- **Network latency**: Remote queries slower than local
- **Bandwidth**: Large file transfers take time
- **Caching**: DuckDB may cache metadata
- **Parquet advantage**: Columnar format reduces data transfer

---

## Part 5: COPY Statement Deep Dive (30 minutes)

### Basic COPY Syntax

```sql
COPY source TO 'destination' (options);
```

### Copy from Query

```sql
COPY (
    SELECT *
    FROM sales
    WHERE amount > 1000
) TO 'high_value_sales.parquet' (FORMAT PARQUET);
```

### Copy from Table

```sql
-- If you have a table in DuckDB
CREATE TABLE results AS SELECT ...;

COPY results TO 'results.csv' (HEADER, DELIMITER ',');
```

### CSV Export Options

```sql
COPY (SELECT * FROM data) TO 'output.csv' (
    HEADER true,              -- Include column names
    DELIMITER ',',            -- Field separator
    QUOTE '"',                -- Quote character
    ESCAPE '\',               -- Escape character
    NULL 'NULL',              -- NULL representation
    DATEFORMAT '%Y-%m-%d',    -- Date format
    TIMESTAMPFORMAT '%Y-%m-%d %H:%M:%S'  -- Timestamp format
);
```

### Parquet Export Options

```sql
COPY (SELECT * FROM data) TO 'output.parquet' (
    FORMAT PARQUET,
    COMPRESSION 'ZSTD',       -- Compression codec
    ROW_GROUP_SIZE 122880     -- Rows per row group
);
```

### JSON Export

```sql
-- Array of objects
COPY (SELECT * FROM data) TO 'output.json';

-- Newline-delimited JSON
COPY (SELECT * FROM data) TO 'output.ndjson' (FORMAT JSON, ARRAY false);
```

### Export to stdout

```sql
-- Print to console
COPY (SELECT * FROM data LIMIT 10) TO '/dev/stdout' (FORMAT CSV);
```

---

## Part 6: Practical Workflows (30 minutes)

### Workflow 1: Data Lake Ingestion

```sql
-- 1. Read all raw CSV files
-- 2. Clean and transform
-- 3. Write as partitioned Parquet

COPY (
    SELECT
        *,
        EXTRACT(YEAR FROM sale_date) AS year,
        EXTRACT(MONTH FROM sale_date) AS month
    FROM 'raw_data/**/*.csv'
    WHERE amount IS NOT NULL
      AND amount > 0
) TO 'clean_data' (
    FORMAT PARQUET,
    PARTITION_BY (year, month),
    COMPRESSION 'ZSTD'
);
```

### Workflow 2: Incremental Updates

```sql
-- Process only new files (by date pattern)
COPY (
    SELECT * FROM 'raw_data/2024-12-*.csv'
) TO 'processed/data_2024_12.parquet' (FORMAT PARQUET);
```

### Workflow 3: Format Migration

```sql
-- Migrate entire dataset from CSV to Parquet
COPY (
    SELECT * FROM 'legacy_data/**/*.csv'
) TO 'modern_data' (
    FORMAT PARQUET,
    PARTITION_BY (year, region),
    COMPRESSION 'ZSTD'
);
```

### Workflow 4: Data Sampling for Development

```sql
-- Create small sample for testing
COPY (
    SELECT * FROM 'production_data/*.parquet'
    USING SAMPLE 1%  -- 1% sample
) TO 'sample_data.parquet' (FORMAT PARQUET);
```

### Workflow 5: Daily ETL Pipeline

```bash
#!/bin/bash
# daily_etl.sh

duckdb << EOF
-- Extract from multiple sources
CREATE TEMP TABLE raw_data AS
SELECT * FROM 'daily_feeds/*.csv';

-- Transform
CREATE TEMP TABLE transformed AS
SELECT
    DATE_TRUNC('day', timestamp) AS date,
    user_id,
    SUM(amount) AS daily_total
FROM raw_data
GROUP BY date, user_id;

-- Load to partitioned storage
COPY (
    SELECT *, EXTRACT(YEAR FROM date) AS year,
              EXTRACT(MONTH FROM date) AS month
    FROM transformed
) TO 'data_warehouse/sales' (
    FORMAT PARQUET,
    PARTITION_BY (year, month),
    COMPRESSION 'ZSTD'
);
EOF
```

---

## Key Takeaways

✅ **Glob patterns** enable querying multiple files in one query
✅ **Parquet format** is 10-100x faster than CSV for analytics
✅ **Partitioning** dramatically speeds up queries on large datasets
✅ **Remote files** can be queried directly (HTTP, S3)
✅ **COPY statement** is the Swiss Army knife for data export
✅ **Format conversions** are trivial with DuckDB

---

## Best Practices

1. **Always use Parquet for large datasets** - Faster and smaller
2. **Partition by frequently-filtered columns** - Year, month, category
3. **Use ZSTD compression** - Best balance of speed and size
4. **Glob patterns for batch processing** - Process many files at once
5. **Test on samples first** - Use LIMIT when developing queries
6. **Monitor file sizes** - Too many tiny files is bad for performance

---

## Practice Exercises

See `exercises/03_file_operations.sql` for hands-on exercises.

## Next Module

**Module 4: Query Optimization & Performance** - EXPLAIN, indexes, and making queries fast.
