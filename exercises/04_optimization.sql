-- ============================================================================
-- Module 4: Query Optimization & Performance - Exercises
-- ============================================================================

-- Enable timing for all exercises
.timer on

-- ============================================================================
-- PART 1: EXPLAIN and Query Plans
-- ============================================================================

-- Exercise 1.1: Basic EXPLAIN
EXPLAIN SELECT * FROM 'datasets/raw/sales.parquet' WHERE amount > 1000;

-- Exercise 1.2: EXPLAIN ANALYZE (with actual execution)
EXPLAIN ANALYZE
SELECT product_category, SUM(amount) AS total
FROM 'datasets/raw/sales.parquet'
WHERE amount > 500
GROUP BY product_category;

-- Exercise 1.3: Compare query plans
-- Query 1: Filter before aggregation
EXPLAIN
SELECT product_category, AVG(amount)
FROM 'datasets/raw/sales.parquet'
WHERE amount > 100
GROUP BY product_category;

-- Query 2: Filter after aggregation (less efficient)
EXPLAIN
SELECT *
FROM (
    SELECT product_category, AVG(amount) AS avg_amount
    FROM 'datasets/raw/sales.parquet'
    GROUP BY product_category
)
WHERE avg_amount > 100;

-- ============================================================================
-- PART 2: Format Performance Comparison
-- ============================================================================

-- Exercise 2.1: CSV vs Parquet read performance
SELECT COUNT(*), AVG(amount)
FROM 'datasets/raw/sales.csv';

SELECT COUNT(*), AVG(amount)
FROM 'datasets/raw/sales.parquet';

-- Exercise 2.2: Column projection with Parquet
-- Read all columns
SELECT COUNT(*) FROM 'datasets/raw/wide_table.parquet';

-- Read only one column (should be much faster)
SELECT COUNT(metric_1) FROM 'datasets/raw/wide_table.parquet';

-- Read specific columns
SELECT AVG(metric_1), AVG(metric_5), AVG(metric_10)
FROM 'datasets/raw/wide_table.parquet';

-- ============================================================================
-- PART 3: Filter Pushdown
-- ============================================================================

-- Exercise 3.1: Verify filter pushdown
EXPLAIN
SELECT * FROM 'datasets/raw/sales.parquet'
WHERE product_category = 'Electronics';
-- Look for FILTER in PARQUET_SCAN

-- Exercise 3.2: Compare with partitioned data
-- Create partitioned version first (from Module 3)
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM 'datasets/processed/sales_by_year_category/**/*.parquet'
WHERE year = 2023 AND product_category = 'Electronics';
-- Check for partition pruning

-- ============================================================================
-- PART 4: Join Optimization
-- ============================================================================

-- Exercise 4.1: Join performance analysis
EXPLAIN ANALYZE
SELECT e.name, e.department, d.total_budget
FROM 'datasets/raw/employees.csv' e
JOIN (
    SELECT department, SUM(salary) AS total_budget
    FROM 'datasets/raw/employees.csv'
    GROUP BY department
) d ON e.department = d.department;

-- Exercise 4.2: Small table first
-- Good practice: small dimension table first
EXPLAIN
SELECT *
FROM (SELECT DISTINCT department FROM 'datasets/raw/employees.csv') d
JOIN 'datasets/raw/employees.csv' e ON d.department = e.department;

-- ============================================================================
-- PART 5: Sampling for Development
-- ============================================================================

-- Exercise 5.1: Random sampling
SELECT * FROM 'datasets/raw/sales.parquet'
USING SAMPLE 1 PERCENT;

-- Exercise 5.2: Fixed row count
SELECT * FROM 'datasets/raw/sales.parquet'
USING SAMPLE 1000 ROWS;

-- Exercise 5.3: Reproducible sampling
SELECT * FROM 'datasets/raw/sales.parquet'
USING SAMPLE 5 PERCENT (bernoulli, 12345);

-- Exercise 5.4: Develop on sample, then run on full data
-- Development query on sample
SELECT
    product_category,
    DATE_TRUNC('month', sale_date) AS month,
    SUM(amount) AS total
FROM 'datasets/raw/sales.parquet'
USING SAMPLE 1 PERCENT
GROUP BY product_category, month
ORDER BY product_category, month;

-- Production query (remove USING SAMPLE)
-- (commented out to avoid long execution)
-- SELECT ... (same query without USING SAMPLE)

-- ============================================================================
-- PART 6: Aggregation Optimization
-- ============================================================================

-- Exercise 6.1: Efficient aggregation with filters
EXPLAIN ANALYZE
SELECT
    product_category,
    COUNT(*) AS total_transactions,
    COUNT(*) FILTER (WHERE amount > 1000) AS high_value_transactions,
    AVG(amount) AS avg_amount,
    SUM(amount) FILTER (WHERE sale_date >= '2023-06-01') AS h2_2023_sales
FROM 'datasets/raw/sales.parquet'
GROUP BY product_category;

-- Exercise 6.2: Incremental aggregation
-- Better to filter before grouping
SELECT DATE_TRUNC('day', sale_date) AS day, SUM(amount)
FROM 'datasets/raw/sales.parquet'
WHERE sale_date BETWEEN '2023-01-01' AND '2023-01-31'
GROUP BY day;

-- ============================================================================
-- PART 7: Memory and Threading
-- ============================================================================

-- Exercise 7.1: Check current settings
SELECT current_setting('memory_limit');
SELECT current_setting('threads');
SELECT current_setting('temp_directory');

-- Exercise 7.2: Adjust settings for large query
SET memory_limit='2GB';
SET threads=4;

-- Run memory-intensive query
SELECT
    product_category,
    sale_date,
    amount,
    AVG(amount) OVER (PARTITION BY product_category ORDER BY sale_date ROWS BETWEEN 999 PRECEDING AND CURRENT ROW) AS ma_1000
FROM 'datasets/raw/sales.parquet';

-- Reset to defaults
SET memory_limit='80%';
SET threads TO DEFAULT;

-- ============================================================================
-- CHALLENGE: Query Optimization Project
-- ============================================================================

-- Challenge: Optimize this slow query

-- Original (potentially slow)
SELECT
    e.name,
    e.department,
    e.salary,
    dept_avg.avg_salary,
    company_avg.overall_avg
FROM 'datasets/raw/employees.csv' e
CROSS JOIN (
    SELECT AVG(salary) AS avg_salary, department
    FROM 'datasets/raw/employees.csv'
    GROUP BY department
) dept_avg
CROSS JOIN (
    SELECT AVG(salary) AS overall_avg
    FROM 'datasets/raw/employees.csv'
) company_avg
WHERE e.department = dept_avg.department;

-- Optimized version using window functions
SELECT
    name,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) AS avg_salary,
    AVG(salary) OVER () AS overall_avg
FROM 'datasets/raw/employees.csv';

-- Compare execution times with EXPLAIN ANALYZE

.timer off

-- ============================================================================
-- KEY OPTIMIZATION TECHNIQUES PRACTICED
-- ============================================================================
-- ✅ Using EXPLAIN and EXPLAIN ANALYZE
-- ✅ Comparing CSV vs Parquet performance
-- ✅ Verifying filter and projection pushdown
-- ✅ Analyzing join strategies
-- ✅ Using sampling for development
-- ✅ Optimizing aggregations
-- ✅ Memory and thread management
-- ============================================================================
