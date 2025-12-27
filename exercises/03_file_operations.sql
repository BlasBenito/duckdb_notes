-- ============================================================================
-- Module 3: Advanced File Operations - Hands-on Exercises
-- ============================================================================
-- Duration: ~1.5 hours of hands-on practice
-- Prerequisites: Completed Modules 1-2
-- ============================================================================

-- ============================================================================
-- PART 1: GLOB PATTERNS AND MULTIPLE FILES
-- ============================================================================

-- Exercise 1.1: Read all log files at once
SELECT COUNT(*) AS total_records
FROM 'datasets/raw/logs_*.csv';

-- Exercise 1.2: Get record count per file
SELECT
    filename,
    COUNT(*) AS records,
    MIN(timestamp) AS earliest,
    MAX(timestamp) AS latest
FROM 'datasets/raw/logs_*.csv'
GROUP BY filename
ORDER BY filename;

-- Exercise 1.3: Combine files with source tracking
SELECT
    filename,
    event_type,
    COUNT(*) AS event_count
FROM 'datasets/raw/logs_*.csv'
GROUP BY filename, event_type
ORDER BY filename, event_type;

-- Exercise 1.4: Recursive glob pattern
-- Create subdirectories first for testing:
-- mkdir -p datasets/raw/nested/2024/01
-- Then query recursively
SELECT COUNT(*) FROM 'datasets/raw/**/*.csv';

-- ============================================================================
-- PART 2: FILE FORMAT CONVERSIONS
-- ============================================================================

-- Exercise 2.1: Convert employees CSV to Parquet
COPY (SELECT * FROM 'datasets/raw/employees.csv')
TO 'datasets/processed/employees.parquet' (FORMAT PARQUET);

-- Verify conversion
SELECT COUNT(*) FROM 'datasets/processed/employees.parquet';

-- Exercise 2.2: Convert with different compressions
COPY (SELECT * FROM 'datasets/raw/wide_table.csv')
TO 'datasets/processed/wide_table_snappy.parquet'
(FORMAT PARQUET, COMPRESSION 'SNAPPY');

COPY (SELECT * FROM 'datasets/raw/wide_table.csv')
TO 'datasets/processed/wide_table_zstd.parquet'
(FORMAT PARQUET, COMPRESSION 'ZSTD');

COPY (SELECT * FROM 'datasets/raw/wide_table.csv')
TO 'datasets/processed/wide_table_uncompressed.parquet'
(FORMAT PARQUET, COMPRESSION 'UNCOMPRESSED');

-- Check file sizes
.shell ls -lh datasets/processed/wide_table*.parquet

-- Exercise 2.3: Combine multiple CSV files into single Parquet
COPY (SELECT * FROM 'datasets/raw/logs_*.csv')
TO 'datasets/processed/all_logs.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- Exercise 2.4: Convert Parquet to CSV
COPY (SELECT * FROM 'datasets/raw/sales.parquet' LIMIT 1000)
TO 'datasets/processed/sales_sample.csv' (HEADER true, DELIMITER ',');

-- Exercise 2.5: JSON to Parquet
COPY (SELECT * FROM 'datasets/raw/users.json')
TO 'datasets/processed/users.parquet' (FORMAT PARQUET);

-- ============================================================================
-- PART 3: PARTITIONED DATASETS
-- ============================================================================

-- Exercise 3.1: Create partitioned dataset by year
COPY (
    SELECT
        *,
        EXTRACT(YEAR FROM sale_date) AS year
    FROM 'datasets/raw/sales.parquet'
) TO 'datasets/processed/sales_by_year' (
    FORMAT PARQUET,
    PARTITION_BY (year),
    COMPRESSION 'ZSTD'
);

-- Verify partition structure
.shell ls -R datasets/processed/sales_by_year/

-- Exercise 3.2: Create multi-level partitions (year + category)
COPY (
    SELECT
        *,
        EXTRACT(YEAR FROM sale_date) AS year
    FROM 'datasets/raw/sales.parquet'
) TO 'datasets/processed/sales_by_year_category' (
    FORMAT PARQUET,
    PARTITION_BY (year, product_category),
    COMPRESSION 'ZSTD'
);

-- Verify structure
.shell tree datasets/processed/sales_by_year_category/

-- Exercise 3.3: Query partitioned data with pruning
-- Enable timing to see performance benefit
.timer on

-- Query specific partition (should be fast)
SELECT
    product_category,
    COUNT(*) AS transactions,
    SUM(amount) AS total_sales
FROM 'datasets/processed/sales_by_year_category/**/*.parquet'
WHERE year = 2023 AND product_category = 'Electronics'
GROUP BY product_category;

-- Use EXPLAIN to verify partition pruning
EXPLAIN SELECT COUNT(*)
FROM 'datasets/processed/sales_by_year_category/**/*.parquet'
WHERE year = 2023;

-- Exercise 3.4: Create date-partitioned logs
COPY (
    SELECT
        *,
        EXTRACT(YEAR FROM timestamp) AS year,
        EXTRACT(MONTH FROM timestamp) AS month,
        EXTRACT(DAY FROM timestamp) AS day
    FROM 'datasets/raw/logs_*.csv'
) TO 'datasets/processed/logs_partitioned' (
    FORMAT PARQUET,
    PARTITION_BY (year, month, day),
    COMPRESSION 'ZSTD'
);

-- ============================================================================
-- PART 4: DATA TRANSFORMATIONS WITH COPY
-- ============================================================================

-- Exercise 4.1: Export aggregated data
COPY (
    SELECT
        department,
        COUNT(*) AS employee_count,
        AVG(salary) AS avg_salary,
        MIN(salary) AS min_salary,
        MAX(salary) AS max_salary
    FROM 'datasets/raw/employees.csv'
    GROUP BY department
) TO 'datasets/processed/department_summary.csv' (HEADER true);

-- Exercise 4.2: Export filtered data
COPY (
    SELECT * FROM 'datasets/raw/sales.parquet'
    WHERE amount > 1000
      AND product_category = 'Electronics'
) TO 'datasets/processed/high_value_electronics.parquet' (FORMAT PARQUET);

-- Exercise 4.3: Export with transformations
COPY (
    SELECT
        UPPER(name) AS name,
        department,
        ROUND(salary / 12, 2) AS monthly_salary,
        DATE_DIFF('year', hire_date, CURRENT_DATE) AS years_employed
    FROM 'datasets/raw/employees.csv'
) TO 'datasets/processed/employees_transformed.csv' (HEADER true);

-- Exercise 4.4: Export to JSON
COPY (
    SELECT
        department,
        LIST({'name': name, 'salary': salary} ORDER BY salary DESC) AS employees
    FROM 'datasets/raw/employees.csv'
    GROUP BY department
) TO 'datasets/processed/employees_by_dept.json';

-- Exercise 4.5: Export wide table to long format
COPY (
    SELECT id, 'metric_1' AS metric_name, metric_1 AS value
    FROM 'datasets/raw/wide_table.csv'
    UNION ALL
    SELECT id, 'metric_2', metric_2
    FROM 'datasets/raw/wide_table.csv'
    UNION ALL
    SELECT id, 'metric_3', metric_3
    FROM 'datasets/raw/wide_table.csv'
) TO 'datasets/processed/long_format.parquet' (FORMAT PARQUET);

-- ============================================================================
-- PART 5: PRACTICAL WORKFLOWS
-- ============================================================================

-- Workflow 1: Data Cleaning Pipeline
-- Clean, validate, and export sales data

COPY (
    SELECT
        -- Clean data
        *,
        -- Add derived columns
        EXTRACT(YEAR FROM sale_date) AS year,
        EXTRACT(MONTH FROM sale_date) AS month,
        EXTRACT(DAY FROM sale_date) AS day,
        DATE_TRUNC('week', sale_date) AS week_start,
        -- Categorize amounts
        CASE
            WHEN amount < 100 THEN 'Small'
            WHEN amount < 1000 THEN 'Medium'
            ELSE 'Large'
        END AS transaction_size
    FROM 'datasets/raw/sales.parquet'
    WHERE
        -- Validation filters
        amount IS NOT NULL
        AND amount > 0
        AND sale_date IS NOT NULL
        AND product_category IS NOT NULL
) TO 'datasets/processed/sales_cleaned' (
    FORMAT PARQUET,
    PARTITION_BY (year, month),
    COMPRESSION 'ZSTD'
);

-- Workflow 2: Create Analytical Aggregates
-- Daily sales summary

COPY (
    SELECT
        sale_date,
        product_category,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_sales,
        AVG(amount) AS avg_transaction,
        MIN(amount) AS min_transaction,
        MAX(amount) AS max_transaction,
        STDDEV(amount) AS stddev_transaction
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date, product_category
) TO 'datasets/processed/daily_sales_summary.parquet' (FORMAT PARQUET);

-- Workflow 3: Create Sample Dataset for Testing
-- 1% sample of sales data

COPY (
    SELECT * FROM 'datasets/raw/sales.parquet'
    USING SAMPLE 1 PERCENT
) TO 'datasets/processed/sales_sample_1pct.parquet' (FORMAT PARQUET);

-- Workflow 4: Merge and Deduplicate
-- Combine logs and remove duplicates

COPY (
    SELECT DISTINCT *
    FROM 'datasets/raw/logs_*.csv'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY log_id
        ORDER BY timestamp DESC
    ) = 1
) TO 'datasets/processed/logs_deduplicated.parquet' (FORMAT PARQUET);

-- ============================================================================
-- CHALLENGE EXERCISES
-- ============================================================================

-- Challenge 1: Multi-format data consolidation
-- Read from multiple formats, combine, and export

COPY (
    SELECT 'employees_csv' AS source, COUNT(*) AS record_count
    FROM 'datasets/raw/employees.csv'
    UNION ALL
    SELECT 'sales_parquet', COUNT(*)
    FROM 'datasets/raw/sales.parquet'
    UNION ALL
    SELECT 'users_json', COUNT(*)
    FROM 'datasets/raw/users.json'
    UNION ALL
    SELECT 'logs_csv_all', COUNT(*)
    FROM 'datasets/raw/logs_*.csv'
) TO 'datasets/processed/data_inventory.csv' (HEADER true);

-- Challenge 2: Create time-series optimized dataset
-- Partition by date for time-series analysis

COPY (
    WITH daily_metrics AS (
        SELECT
            sale_date,
            product_category,
            SUM(amount) AS daily_total,
            COUNT(*) AS transaction_count,
            AVG(amount) AS avg_transaction
        FROM 'datasets/raw/sales.parquet'
        GROUP BY sale_date, product_category
    ),
    with_moving_averages AS (
        SELECT
            *,
            AVG(daily_total) OVER (
                PARTITION BY product_category
                ORDER BY sale_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS ma_7day,
            AVG(daily_total) OVER (
                PARTITION BY product_category
                ORDER BY sale_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS ma_30day
        FROM daily_metrics
    )
    SELECT
        *,
        EXTRACT(YEAR FROM sale_date) AS year,
        EXTRACT(MONTH FROM sale_date) AS month
    FROM with_moving_averages
) TO 'datasets/processed/sales_time_series' (
    FORMAT PARQUET,
    PARTITION_BY (year, month),
    COMPRESSION 'ZSTD'
);

-- Challenge 3: Create data quality report
-- Analyze and export data quality metrics

COPY (
    SELECT
        'employees' AS table_name,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT id) AS unique_ids,
        SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_names,
        SUM(CASE WHEN salary IS NULL THEN 1 ELSE 0 END) AS null_salaries,
        SUM(CASE WHEN salary < 0 THEN 1 ELSE 0 END) AS negative_salaries
    FROM 'datasets/raw/employees.csv'

    UNION ALL

    SELECT
        'sales',
        COUNT(*),
        COUNT(DISTINCT id),
        SUM(CASE WHEN sale_date IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END),
        SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END)
    FROM 'datasets/raw/sales.parquet'
) TO 'datasets/processed/data_quality_report.csv' (HEADER true);

-- ============================================================================
-- PERFORMANCE COMPARISON
-- ============================================================================

-- Compare query performance: CSV vs Parquet
.timer on

-- Query CSV
SELECT
    product_category,
    AVG(amount) AS avg_amount
FROM 'datasets/raw/sales.csv'
GROUP BY product_category;

-- Query Parquet
SELECT
    product_category,
    AVG(amount) AS avg_amount
FROM 'datasets/raw/sales.parquet'
GROUP BY product_category;

-- Query partitioned Parquet (with partition pruning)
SELECT
    product_category,
    AVG(amount) AS avg_amount
FROM 'datasets/processed/sales_by_year_category/**/*.parquet'
WHERE year = 2023
GROUP BY product_category;

.timer off

-- ============================================================================
-- SOLUTIONS NOTES
-- ============================================================================
-- All solutions are provided inline above
-- Key concepts covered:
-- ✅ Glob patterns for multiple files
-- ✅ File format conversions (CSV, Parquet, JSON)
-- ✅ Compression codecs
-- ✅ Partitioned datasets for performance
-- ✅ Data cleaning and transformation workflows
-- ✅ Aggregation and export
-- ✅ Performance optimization techniques
-- ============================================================================
