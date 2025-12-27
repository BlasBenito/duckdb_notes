-- ============================================================================
-- Module 1: DuckDB Fundamentals - Hands-on Exercises
-- ============================================================================
-- Duration: ~1.5 hours of hands-on practice
-- Prerequisites: DuckDB installed, datasets in datasets/raw/
-- ============================================================================

-- ============================================================================
-- EXERCISE 1: CLI Basics and First Queries
-- ============================================================================
-- Goal: Get comfortable with DuckDB CLI and basic SQL queries

-- 1.1: Start DuckDB and run a simple query
-- Command: duckdb
-- Then run:
SELECT 'Hello DuckDB!' AS greeting;

-- 1.2: Enable timing to see query performance
.timer on

-- 1.3: Try different output modes
.mode markdown
SELECT 'Markdown' AS format;

.mode csv
SELECT 'CSV' AS format;

.mode json
SELECT 'JSON' AS format;

.mode table
SELECT 'Table' AS format;

-- 1.4: Basic arithmetic and functions
SELECT
    42 AS answer,
    'DuckDB' AS database,
    CURRENT_DATE AS today,
    CURRENT_TIMESTAMP AS now,
    2 + 2 AS addition,
    10 * 5 AS multiplication,
    POWER(2, 10) AS power_of_two,
    SQRT(16) AS square_root;

-- 1.5: Working with lists (arrays)
SELECT
    [1, 2, 3, 4, 5] AS numbers,
    ['apple', 'banana', 'cherry'] AS fruits,
    LIST_SUM([1, 2, 3, 4, 5]) AS sum_of_numbers,
    LIST_AVG([1, 2, 3, 4, 5]) AS avg_of_numbers,
    LEN(['apple', 'banana', 'cherry']) AS fruit_count;

-- ============================================================================
-- EXERCISE 2: Reading CSV Files Directly
-- ============================================================================
-- Goal: Master querying CSV files without loading them

-- 2.1: Basic CSV query
SELECT * FROM 'datasets/raw/employees.csv' LIMIT 5;

-- 2.2: Get row count
SELECT COUNT(*) AS total_employees
FROM 'datasets/raw/employees.csv';

-- 2.3: Describe the schema
DESCRIBE SELECT * FROM 'datasets/raw/employees.csv';

-- 2.4: Get statistical summary
SUMMARIZE SELECT * FROM 'datasets/raw/employees.csv';

-- 2.5: Filter and aggregate
SELECT
    department,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary,
    MAX(salary) AS max_salary,
    MIN(salary) AS min_salary
FROM 'datasets/raw/employees.csv'
GROUP BY department
ORDER BY avg_salary DESC;

-- 2.6: String operations
SELECT
    name,
    UPPER(name) AS name_upper,
    LOWER(name) AS name_lower,
    LENGTH(name) AS name_length,
    department
FROM 'datasets/raw/employees.csv'
WHERE LENGTH(name) > 8;

-- ============================================================================
-- EXERCISE 3: Format Performance Comparison
-- ============================================================================
-- Goal: Understand the performance difference between CSV and Parquet

-- 3.1: Query CSV file (note the timing)
.timer on
SELECT
    AVG(amount) AS avg_amount,
    SUM(amount) AS total_amount,
    COUNT(*) AS transaction_count
FROM 'datasets/raw/sales.csv';

-- 3.2: Same query on Parquet file (should be much faster)
SELECT
    AVG(amount) AS avg_amount,
    SUM(amount) AS total_amount,
    COUNT(*) AS transaction_count
FROM 'datasets/raw/sales.parquet';

-- 3.3: More complex aggregation on CSV
SELECT
    product_category,
    DATE_TRUNC('month', sale_date) AS month,
    SUM(amount) AS monthly_sales,
    COUNT(*) AS transaction_count
FROM 'datasets/raw/sales.csv'
GROUP BY product_category, DATE_TRUNC('month', sale_date)
ORDER BY product_category, month;

-- 3.4: Same query on Parquet (compare timing)
SELECT
    product_category,
    DATE_TRUNC('month', sale_date) AS month,
    SUM(amount) AS monthly_sales,
    COUNT(*) AS transaction_count
FROM 'datasets/raw/sales.parquet'
GROUP BY product_category, DATE_TRUNC('month', sale_date)
ORDER BY product_category, month;

-- 3.5: Projection pushdown demonstration
-- Only read specific columns (Parquet excels here)
SELECT product_category, amount
FROM 'datasets/raw/sales.parquet'
WHERE amount > 1000;

-- ============================================================================
-- EXERCISE 4: Working with Multiple Files
-- ============================================================================
-- Goal: Query multiple files simultaneously using glob patterns

-- 4.1: Query all CSV files in a directory
SELECT * FROM 'datasets/raw/logs_*.csv' LIMIT 10;

-- 4.2: Count records across all log files
SELECT COUNT(*) AS total_log_entries
FROM 'datasets/raw/logs_*.csv';

-- 4.3: Aggregate across multiple files
SELECT
    DATE_TRUNC('day', timestamp) AS day,
    COUNT(*) AS daily_events,
    COUNT(DISTINCT user_id) AS unique_users
FROM 'datasets/raw/logs_*.csv'
GROUP BY DATE_TRUNC('day', timestamp)
ORDER BY day;

-- 4.4: Query with filename metadata
-- See which file each record came from
SELECT
    filename,
    COUNT(*) AS records_per_file
FROM 'datasets/raw/logs_*.csv'
GROUP BY filename;

-- ============================================================================
-- EXERCISE 5: Creating Persistent Databases
-- ============================================================================
-- Goal: Understand when to use persistent vs in-memory databases

-- 5.1: Create a persistent database
-- Command line: duckdb my_analytics.db

-- 5.2: Create a table from a CSV file
CREATE TABLE employees AS
SELECT * FROM 'datasets/raw/employees.csv';

-- 5.3: Verify table was created
.tables

-- 5.4: Query the persistent table
SELECT * FROM employees LIMIT 5;

-- 5.5: Create an aggregated table
CREATE TABLE department_summary AS
SELECT
    department,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary,
    MAX(salary) AS max_salary,
    MIN(salary) AS min_salary
FROM employees
GROUP BY department;

-- 5.6: Query the summary table
SELECT * FROM department_summary ORDER BY avg_salary DESC;

-- 5.7: Exit and reopen database to verify persistence
-- .quit
-- duckdb my_analytics.db
-- .tables
-- SELECT * FROM department_summary;

-- ============================================================================
-- EXERCISE 6: Data Type Exploration
-- ============================================================================
-- Goal: Understand DuckDB's type system and type inference

-- 6.1: Explicit type specification
SELECT * FROM read_csv(
    'datasets/raw/employees.csv',
    columns={
        'id': 'INTEGER',
        'name': 'VARCHAR',
        'age': 'INTEGER',
        'department': 'VARCHAR',
        'salary': 'DOUBLE',
        'hire_date': 'DATE'
    }
);

-- 6.2: Date and time operations
SELECT
    hire_date,
    EXTRACT(YEAR FROM hire_date) AS hire_year,
    EXTRACT(MONTH FROM hire_date) AS hire_month,
    DATE_DIFF('day', hire_date, CURRENT_DATE) AS days_employed,
    DATE_DIFF('year', hire_date, CURRENT_DATE) AS years_employed
FROM 'datasets/raw/employees.csv'
ORDER BY hire_date;

-- 6.3: Type casting
SELECT
    salary,
    CAST(salary AS INTEGER) AS salary_int,
    ROUND(salary, 0) AS salary_rounded,
    salary::VARCHAR AS salary_string,
    FORMAT('{:,.2f}', salary) AS salary_formatted
FROM 'datasets/raw/employees.csv'
LIMIT 5;

-- ============================================================================
-- EXERCISE 7: JSON Data Exploration
-- ============================================================================
-- Goal: Query semi-structured JSON data

-- 7.1: Read JSON file
SELECT * FROM 'datasets/raw/users.json' LIMIT 5;

-- 7.2: Access nested fields
SELECT
    name,
    email,
    address.city AS city,
    address.country AS country
FROM 'datasets/raw/users.json';

-- 7.3: Unnest arrays in JSON
SELECT
    name,
    UNNEST(tags) AS tag
FROM 'datasets/raw/users.json';

-- 7.4: Aggregate JSON data
SELECT
    address.country AS country,
    COUNT(*) AS user_count
FROM 'datasets/raw/users.json'
GROUP BY address.country
ORDER BY user_count DESC;

-- ============================================================================
-- EXERCISE 8: Exporting Data
-- ============================================================================
-- Goal: Export query results to different formats

-- 8.1: Export to CSV
COPY (
    SELECT department, COUNT(*) AS count, AVG(salary) AS avg_salary
    FROM 'datasets/raw/employees.csv'
    GROUP BY department
) TO 'datasets/processed/department_summary.csv' (HEADER, DELIMITER ',');

-- 8.2: Export to Parquet
COPY (
    SELECT *
    FROM 'datasets/raw/sales.csv'
    WHERE amount > 1000
) TO 'datasets/processed/high_value_sales.parquet' (FORMAT PARQUET);

-- 8.3: Export to JSON
COPY (
    SELECT department, COUNT(*) AS employee_count
    FROM 'datasets/raw/employees.csv'
    GROUP BY department
) TO 'datasets/processed/department_counts.json';

-- ============================================================================
-- CHALLENGE EXERCISES
-- ============================================================================
-- These exercises combine multiple concepts

-- Challenge 1: Find top 3 highest paid employees per department
WITH ranked_employees AS (
    SELECT
        name,
        department,
        salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM 'datasets/raw/employees.csv'
)
SELECT name, department, salary
FROM ranked_employees
WHERE rank <= 3
ORDER BY department, salary DESC;

-- Challenge 2: Calculate rolling 7-day average of sales
SELECT
    sale_date,
    amount,
    AVG(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
FROM 'datasets/raw/sales.parquet'
ORDER BY sale_date;

-- Challenge 3: Convert wide CSV to tall format
-- If you have a wide table with multiple value columns
SELECT
    id,
    'metric_1' AS metric_name,
    metric_1 AS value
FROM 'datasets/raw/wide_table.csv'
UNION ALL
SELECT
    id,
    'metric_2' AS metric_name,
    metric_2 AS value
FROM 'datasets/raw/wide_table.csv'
UNION ALL
SELECT
    id,
    'metric_3' AS metric_name,
    metric_3 AS value
FROM 'datasets/raw/wide_table.csv'
ORDER BY id, metric_name;

-- ============================================================================
-- SOLUTIONS NOTES
-- ============================================================================
-- All solutions are provided inline above
-- Key concepts covered:
-- ✅ CLI commands and output modes
-- ✅ Querying CSV files directly
-- ✅ DESCRIBE and SUMMARIZE for exploration
-- ✅ CSV vs Parquet performance
-- ✅ Multiple file queries with glob patterns
-- ✅ Creating persistent databases
-- ✅ Type system and casting
-- ✅ JSON querying
-- ✅ Data export (COPY command)
-- ✅ Window functions and CTEs (preview of Module 2)
-- ============================================================================
