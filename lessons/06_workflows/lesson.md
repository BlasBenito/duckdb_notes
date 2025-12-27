# Module 6: Data Analysis Workflows

**Duration**: 2 hours
**Objective**: Build complete analytical workflows using DuckDB

## Philosophy: DuckDB in Analysis Pipelines

DuckDB excels at:
- **ETL/ELT**: Extract, Transform, Load data
- **Ad-hoc analysis**: Quick queries on raw files
- **Data pipelines**: Multi-step transformations
- **Reproducible analysis**: SQL scripts under version control

## Workflow 1: Daily ETL Pipeline

### Scenario
Process daily sales files, clean, aggregate, and export.

### Script: `daily_sales_etl.sh`

```bash
#!/bin/bash
DATE=$(date +%Y-%m-%d)

duckdb << EOF
-- Extract: Read today's raw files
CREATE TEMP TABLE raw_sales AS
SELECT * FROM 'raw_data/sales_${DATE}_*.csv';

-- Transform: Clean and enrich
CREATE TEMP TABLE clean_sales AS
SELECT
    *,
    ROUND(amount, 2) AS amount_clean,
    EXTRACT(YEAR FROM sale_date) AS year,
    EXTRACT(MONTH FROM sale_date) AS month,
    EXTRACT(DOW FROM sale_date) AS day_of_week
FROM raw_sales
WHERE amount > 0
  AND sale_date IS NOT NULL;

-- Load: Export to data warehouse
COPY (
    SELECT * FROM clean_sales
) TO 'warehouse/sales' (
    FORMAT PARQUET,
    PARTITION_BY (year, month),
    COMPRESSION 'ZSTD'
);

-- Create daily summary
COPY (
    SELECT
        sale_date,
        product_category,
        COUNT(*) AS transactions,
        SUM(amount_clean) AS total_sales,
        AVG(amount_clean) AS avg_transaction
    FROM clean_sales
    GROUP BY sale_date, product_category
) TO 'summaries/daily_sales_${DATE}.csv' (HEADER true);

PRINT 'ETL completed for ${DATE}';
EOF
```

## Workflow 2: Exploratory Data Analysis

### Multi-Step Analysis with Views

```sql
-- Step 1: Create base view
CREATE VIEW sales_clean AS
SELECT * FROM 'raw/sales.parquet'
WHERE amount > 0;

-- Step 2: Create analytical views
CREATE VIEW daily_sales AS
SELECT
    sale_date,
    product_category,
    SUM(amount) AS total_sales,
    COUNT(*) AS transactions
FROM sales_clean
GROUP BY sale_date, product_category;

CREATE VIEW sales_with_trends AS
SELECT
    *,
    AVG(total_sales) OVER (
        PARTITION BY product_category
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma_7day,
    LAG(total_sales, 1) OVER (
        PARTITION BY product_category
        ORDER BY sale_date
    ) AS prev_day_sales
FROM daily_sales;

-- Step 3: Analysis queries
SELECT * FROM sales_with_trends
WHERE total_sales > ma_7day * 1.5;
```

## Workflow 3: Reproducible Analysis Script

### Analysis Template: `analysis.sql`

```sql
-- ============================================================================
-- Sales Analysis Report
-- Purpose: Monthly sales performance analysis
-- Author: Your Name
-- Date: 2024-01-01
-- ============================================================================

-- Configuration
.timer on
.mode markdown

-- Load extensions if needed
-- INSTALL httpfs;
-- LOAD httpfs;

-- ============================================================================
-- 1. DATA LOADING
-- ============================================================================
CREATE TEMP TABLE sales AS
SELECT * FROM 'data/sales_2024_*.parquet'
WHERE sale_date >= '2024-01-01';

-- ============================================================================
-- 2. DATA QUALITY CHECKS
-- ============================================================================
.print '=== Data Quality Report ==='

SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT id) AS unique_ids,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amounts,
    MIN(sale_date) AS first_date,
    MAX(sale_date) AS last_date
FROM sales;

-- ============================================================================
-- 3. CORE ANALYSIS
-- ============================================================================
.print ''
.print '=== Monthly Sales by Category ==='

SELECT
    DATE_TRUNC('month', sale_date) AS month,
    product_category,
    COUNT(*) AS transactions,
    ROUND(SUM(amount), 2) AS total_sales,
    ROUND(AVG(amount), 2) AS avg_transaction
FROM sales
GROUP BY month, product_category
ORDER BY month, total_sales DESC;

-- ============================================================================
-- 4. EXPORT RESULTS
-- ============================================================================
COPY (
    SELECT * FROM sales
) TO 'output/sales_analysis_2024.parquet' (FORMAT PARQUET);
```

Run with: `duckdb < analysis.sql > report.txt`

## Workflow 4: Parameterized Analysis

### Using Shell Variables

```bash
#!/bin/bash
YEAR=$1
CATEGORY=$2

duckdb << EOF
SELECT
    DATE_TRUNC('month', sale_date) AS month,
    SUM(amount) AS total_sales
FROM 'sales.parquet'
WHERE EXTRACT(YEAR FROM sale_date) = ${YEAR}
  AND product_category = '${CATEGORY}'
GROUP BY month
ORDER BY month;
EOF
```

Usage: `./analyze.sh 2024 Electronics`

## Workflow 5: Data Pipeline with Error Handling

```bash
#!/bin/bash
set -e  # Exit on error

INPUT_DIR="raw_data"
OUTPUT_DIR="processed_data"
LOG_FILE="pipeline_$(date +%Y%m%d).log"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "Starting data pipeline"

# Check input files exist
if [ ! -d "$INPUT_DIR" ]; then
    log "ERROR: Input directory not found"
    exit 1
fi

# Run DuckDB pipeline
duckdb << EOF 2>&1 | tee -a "$LOG_FILE"
-- Pipeline execution
CREATE TEMP TABLE staging AS
SELECT * FROM '${INPUT_DIR}/*.csv';

-- Validate
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN ERROR('No records loaded')
        WHEN SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) > 0
            THEN ERROR('Negative amounts found')
        ELSE 'Validation passed'
    END AS status
FROM staging;

-- Process and export
COPY (
    SELECT * FROM staging WHERE amount > 0
) TO '${OUTPUT_DIR}/clean_data.parquet' (FORMAT PARQUET);
EOF

if [ $? -eq 0 ]; then
    log "Pipeline completed successfully"
else
    log "ERROR: Pipeline failed"
    exit 1
fi
```

## Combining DuckDB with Shell Tools

### Example: Pipeline with Unix Tools

```bash
# Extract unique categories
duckdb -c "SELECT DISTINCT product_category FROM 'sales.parquet'" | \
    tail -n +2 | \
    sort > categories.txt

# Process each category
while read category; do
    duckdb -c "COPY (
        SELECT * FROM 'sales.parquet'
        WHERE product_category = '$category'
    ) TO 'output/${category}.parquet' (FORMAT PARQUET)"
done < categories.txt
```

## Creating Reusable Templates

### Template: `etl_template.sql`

```sql
-- Parameterized ETL Template
-- Usage: Replace {{TABLE}}, {{DATE}}, etc. with actual values

-- Extract
CREATE TEMP TABLE raw AS
SELECT * FROM '{{INPUT_PATH}}/{{DATE}}_*.csv';

-- Transform
CREATE TEMP TABLE transformed AS
SELECT
    {{TRANSFORMATIONS}}
FROM raw
WHERE {{FILTERS}};

-- Load
COPY transformed TO '{{OUTPUT_PATH}}/{{TABLE}}.parquet'
(FORMAT PARQUET, COMPRESSION 'ZSTD');
```

Use with sed or envsubst:
```bash
export TABLE=sales
export DATE=2024-01-01
envsubst < etl_template.sql | duckdb
```

## Documentation Best Practices

1. **Comment your queries**
   ```sql
   -- Calculate 7-day moving average for trend analysis
   SELECT ..., AVG(sales) OVER (...) AS ma_7day
   ```

2. **Use meaningful names**
   ```sql
   -- Good
   CREATE VIEW monthly_sales_by_category AS ...

   -- Bad
   CREATE VIEW v1 AS ...
   ```

3. **Version control your SQL**
   - Git repository for analysis scripts
   - Document data sources
   - Track schema changes

4. **Include metadata**
   ```sql
   .print 'Analysis: Monthly Sales Trends'
   .print 'Generated: ' || CURRENT_TIMESTAMP
   .print 'Data Range: ' || (SELECT MIN(sale_date) || ' to ' || MAX(sale_date) FROM sales)
   ```

## Key Takeaways

✅ **Script your analyses** - Reproducible and maintainable
✅ **Use views for complex logic** - Modular and reusable
✅ **Combine with shell tools** - Unix philosophy
✅ **Error handling** - Robust pipelines
✅ **Document everything** - Future you will thank you

## Practice Exercises

See `exercises/06_workflows.sql` for building complete analysis workflows.

## Next Module

**Module 7: R Integration** - Using DuckDB from R with dplyr.
