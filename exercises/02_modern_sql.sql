-- ============================================================================
-- Module 2: Modern SQL with DuckDB - Hands-on Exercises
-- ============================================================================
-- Duration: ~3 hours of hands-on practice
-- Prerequisites: Completed Module 1
-- ============================================================================

-- ============================================================================
-- PART 1: WINDOW FUNCTIONS - RANKING
-- ============================================================================

-- Exercise 1.1: Basic ranking
-- Find the top 5 highest-paid employees overall
SELECT
    name,
    department,
    salary,
    RANK() OVER (ORDER BY salary DESC) AS salary_rank
FROM 'datasets/raw/employees.csv'
QUALIFY salary_rank <= 5;

-- Exercise 1.2: Ranking within groups
-- Find the top 3 highest-paid employees in each department
SELECT
    name,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank
FROM 'datasets/raw/employees.csv'
QUALIFY dept_rank <= 3
ORDER BY department, dept_rank;

-- Exercise 1.3: Compare ROW_NUMBER, RANK, and DENSE_RANK
SELECT
    name,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS row_num,
    RANK() OVER (ORDER BY salary DESC) AS rank,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank
FROM 'datasets/raw/employees.csv'
ORDER BY salary DESC;

-- Exercise 1.4: NTILE for percentiles
-- Divide employees into salary quartiles
SELECT
    name,
    department,
    salary,
    NTILE(4) OVER (ORDER BY salary) AS salary_quartile,
    CASE NTILE(4) OVER (ORDER BY salary)
        WHEN 1 THEN 'Bottom 25%'
        WHEN 2 THEN '25-50%'
        WHEN 3 THEN '50-75%'
        WHEN 4 THEN 'Top 25%'
    END AS quartile_label
FROM 'datasets/raw/employees.csv'
ORDER BY salary;

-- Exercise 1.5: Top N per category in sales data
-- Find top 5 sales transactions for each product category
SELECT
    sale_date,
    product_category,
    amount,
    ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY amount DESC) AS rank_in_category
FROM 'datasets/raw/sales.parquet'
QUALIFY rank_in_category <= 5
ORDER BY product_category, rank_in_category;

-- ============================================================================
-- PART 2: WINDOW FUNCTIONS - OFFSET FUNCTIONS
-- ============================================================================

-- Exercise 2.1: Day-over-day sales change
-- Calculate daily sales and day-over-day change
WITH daily_sales AS (
    SELECT
        sale_date,
        SUM(amount) AS daily_total
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date
)
SELECT
    sale_date,
    daily_total,
    LAG(daily_total, 1) OVER (ORDER BY sale_date) AS previous_day,
    daily_total - LAG(daily_total, 1) OVER (ORDER BY sale_date) AS day_over_day_change,
    ROUND(100.0 * (daily_total - LAG(daily_total, 1) OVER (ORDER BY sale_date)) /
          LAG(daily_total, 1) OVER (ORDER BY sale_date), 2) AS pct_change
FROM daily_sales
ORDER BY sale_date;

-- Exercise 2.2: Compare with next and previous values
SELECT
    name,
    department,
    salary,
    LAG(salary, 1) OVER (PARTITION BY department ORDER BY salary) AS lower_salary,
    LEAD(salary, 1) OVER (PARTITION BY department ORDER BY salary) AS higher_salary,
    salary - LAG(salary, 1) OVER (PARTITION BY department ORDER BY salary) AS diff_from_lower
FROM 'datasets/raw/employees.csv'
ORDER BY department, salary;

-- Exercise 2.3: FIRST_VALUE and LAST_VALUE
-- Compare each employee's salary to department min and max
SELECT
    name,
    department,
    salary,
    FIRST_VALUE(salary) OVER (
        PARTITION BY department
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS dept_max_salary,
    LAST_VALUE(salary) OVER (
        PARTITION BY department
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS dept_min_salary,
    salary - FIRST_VALUE(salary) OVER (
        PARTITION BY department
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS diff_from_max
FROM 'datasets/raw/employees.csv'
ORDER BY department, salary DESC;

-- Exercise 2.4: Detect streaks
-- Find consecutive days with sales above average
WITH daily_stats AS (
    SELECT
        sale_date,
        SUM(amount) AS daily_total,
        AVG(SUM(amount)) OVER () AS overall_avg
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date
),
with_flags AS (
    SELECT
        sale_date,
        daily_total,
        overall_avg,
        CASE WHEN daily_total > overall_avg THEN 1 ELSE 0 END AS above_avg,
        LAG(CASE WHEN daily_total > overall_avg THEN 1 ELSE 0 END, 1, 0)
            OVER (ORDER BY sale_date) AS prev_above_avg
    FROM daily_stats
)
SELECT
    sale_date,
    daily_total,
    overall_avg,
    above_avg,
    CASE
        WHEN above_avg = 1 AND prev_above_avg = 0 THEN 'Start of streak'
        WHEN above_avg = 1 AND prev_above_avg = 1 THEN 'Continues'
        WHEN above_avg = 0 AND prev_above_avg = 1 THEN 'End of streak'
        ELSE 'Below average'
    END AS streak_status
FROM with_flags
ORDER BY sale_date;

-- ============================================================================
-- PART 3: WINDOW FUNCTIONS - AGGREGATES & MOVING CALCULATIONS
-- ============================================================================

-- Exercise 3.1: Running totals
-- Calculate cumulative sales over time
WITH daily_sales AS (
    SELECT
        sale_date,
        SUM(amount) AS daily_total
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date
)
SELECT
    sale_date,
    daily_total,
    SUM(daily_total) OVER (ORDER BY sale_date) AS running_total,
    AVG(daily_total) OVER (ORDER BY sale_date) AS cumulative_avg
FROM daily_sales
ORDER BY sale_date;

-- Exercise 3.2: Moving averages (multiple windows)
WITH daily_sales AS (
    SELECT
        sale_date,
        SUM(amount) AS daily_total
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date
)
SELECT
    sale_date,
    daily_total,
    AVG(daily_total) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS ma_3day,
    AVG(daily_total) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma_7day,
    AVG(daily_total) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS ma_30day
FROM daily_sales
ORDER BY sale_date;

-- Exercise 3.3: Group statistics without collapsing rows
-- Add department statistics to each employee row
SELECT
    name,
    department,
    salary,
    COUNT(*) OVER (PARTITION BY department) AS dept_employee_count,
    AVG(salary) OVER (PARTITION BY department) AS dept_avg_salary,
    MIN(salary) OVER (PARTITION BY department) AS dept_min_salary,
    MAX(salary) OVER (PARTITION BY department) AS dept_max_salary,
    ROUND(100.0 * (salary - AVG(salary) OVER (PARTITION BY department)) /
          AVG(salary) OVER (PARTITION BY department), 2) AS pct_diff_from_avg
FROM 'datasets/raw/employees.csv'
ORDER BY department, salary DESC;

-- Exercise 3.4: Percentage of total
-- Calculate each transaction's percentage of category total
SELECT
    product_category,
    amount,
    SUM(amount) OVER (PARTITION BY product_category) AS category_total,
    ROUND(100.0 * amount / SUM(amount) OVER (PARTITION BY product_category), 2) AS pct_of_category,
    ROUND(100.0 * amount / SUM(amount) OVER (), 2) AS pct_of_grand_total
FROM 'datasets/raw/sales.parquet'
ORDER BY product_category, amount DESC
LIMIT 20;

-- ============================================================================
-- PART 4: COMMON TABLE EXPRESSIONS (CTEs)
-- ============================================================================

-- Exercise 4.1: Basic CTE for readability
-- Calculate department budgets and identify high-budget departments
WITH department_budgets AS (
    SELECT
        department,
        SUM(salary) AS total_budget,
        AVG(salary) AS avg_salary,
        COUNT(*) AS employee_count
    FROM 'datasets/raw/employees.csv'
    GROUP BY department
)
SELECT
    department,
    total_budget,
    avg_salary,
    employee_count,
    CASE
        WHEN total_budget > 350000 THEN 'High Budget'
        WHEN total_budget > 250000 THEN 'Medium Budget'
        ELSE 'Low Budget'
    END AS budget_category
FROM department_budgets
ORDER BY total_budget DESC;

-- Exercise 4.2: Chained CTEs for multi-step analysis
-- Analyze sales trends with multiple processing steps
WITH
-- Step 1: Daily aggregates
daily_sales AS (
    SELECT
        sale_date,
        product_category,
        SUM(amount) AS daily_total,
        COUNT(*) AS transaction_count
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date, product_category
),
-- Step 2: Add moving averages
with_moving_avg AS (
    SELECT
        sale_date,
        product_category,
        daily_total,
        transaction_count,
        AVG(daily_total) OVER (
            PARTITION BY product_category
            ORDER BY sale_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7day
    FROM daily_sales
),
-- Step 3: Identify outlier days
outliers AS (
    SELECT
        *,
        CASE
            WHEN daily_total > ma_7day * 1.5 THEN 'High'
            WHEN daily_total < ma_7day * 0.5 THEN 'Low'
            ELSE 'Normal'
        END AS performance
    FROM with_moving_avg
)
-- Final: Show only outlier days
SELECT *
FROM outliers
WHERE performance != 'Normal'
ORDER BY product_category, sale_date;

-- Exercise 4.3: Recursive CTE - Generate date series
-- Create a calendar table for all dates in 2024
WITH RECURSIVE date_series AS (
    -- Base case: Start date
    SELECT DATE '2024-01-01' AS date

    UNION ALL

    -- Recursive case: Add one day
    SELECT date + INTERVAL 1 DAY
    FROM date_series
    WHERE date < DATE '2024-12-31'
)
SELECT
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    DAYNAME(date) AS day_name,
    CASE WHEN DAYOFWEEK(date) IN (6, 7) THEN true ELSE false END AS is_weekend
FROM date_series
LIMIT 20;

-- Exercise 4.4: Recursive CTE - Number sequence with calculations
-- Generate Fibonacci sequence
WITH RECURSIVE fibonacci AS (
    -- Base cases
    SELECT 1 AS n, 0 AS fib_n, 1 AS fib_n_plus_1

    UNION ALL

    -- Recursive case
    SELECT
        n + 1,
        fib_n_plus_1,
        fib_n + fib_n_plus_1
    FROM fibonacci
    WHERE n < 20
)
SELECT n, fib_n AS fibonacci_number
FROM fibonacci;

-- ============================================================================
-- PART 5: ADVANCED GROUPING
-- ============================================================================

-- Exercise 5.1: GROUPING SETS
-- Calculate sales totals at multiple aggregation levels
SELECT
    product_category,
    DATE_TRUNC('month', sale_date) AS month,
    SUM(amount) AS total_sales,
    COUNT(*) AS transaction_count
FROM 'datasets/raw/sales.parquet'
GROUP BY GROUPING SETS (
    (product_category, DATE_TRUNC('month', sale_date)),  -- By category and month
    (product_category),                                   -- By category only
    (DATE_TRUNC('month', sale_date)),                     -- By month only
    ()                                                    -- Grand total
)
ORDER BY product_category, month;

-- Exercise 5.2: ROLLUP
-- Create hierarchical subtotals
SELECT
    product_category,
    DATE_TRUNC('quarter', sale_date) AS quarter,
    SUM(amount) AS total_sales,
    GROUPING(product_category) AS is_category_total,
    GROUPING(DATE_TRUNC('quarter', sale_date)) AS is_grand_total
FROM 'datasets/raw/sales.parquet'
GROUP BY ROLLUP (product_category, DATE_TRUNC('quarter', sale_date))
ORDER BY product_category, quarter;

-- Exercise 5.3: CUBE
-- All possible aggregation combinations
SELECT
    department,
    CASE WHEN age < 30 THEN '<30'
         WHEN age < 40 THEN '30-39'
         ELSE '40+' END AS age_group,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary
FROM 'datasets/raw/employees.csv'
GROUP BY CUBE (
    department,
    CASE WHEN age < 30 THEN '<30'
         WHEN age < 40 THEN '30-39'
         ELSE '40+' END
)
ORDER BY department, age_group;

-- Exercise 5.4: FILTER clause
-- Conditional aggregations made clean
SELECT
    department,
    COUNT(*) AS total_employees,
    COUNT(*) FILTER (WHERE salary > 85000) AS high_earners,
    COUNT(*) FILTER (WHERE age < 30) AS young_employees,
    COUNT(*) FILTER (WHERE hire_date >= '2020-01-01') AS recent_hires,
    AVG(salary) AS avg_salary,
    AVG(salary) FILTER (WHERE age < 35) AS avg_salary_young,
    AVG(salary) FILTER (WHERE hire_date >= '2020-01-01') AS avg_salary_recent
FROM 'datasets/raw/employees.csv'
GROUP BY department
ORDER BY department;

-- ============================================================================
-- PART 6: PIVOT AND UNPIVOT
-- ============================================================================

-- Exercise 6.1: PIVOT - Department metrics as columns
-- Transform department stats from rows to columns
PIVOT (
    SELECT
        EXTRACT(YEAR FROM hire_date) AS hire_year,
        department,
        COUNT(*) AS hires
    FROM 'datasets/raw/employees.csv'
    WHERE hire_date >= '2015-01-01'
    GROUP BY hire_year, department
)
ON department
USING SUM(hires)
ORDER BY hire_year;

-- Exercise 6.2: UNPIVOT - Transform wide table to long format
-- Take first 10 rows and unpivot metrics
SELECT *
FROM (
    SELECT id, metric_1, metric_2, metric_3, metric_4, metric_5
    FROM 'datasets/raw/wide_table.csv'
    LIMIT 10
)
UNPIVOT (
    value FOR metric_name IN (metric_1, metric_2, metric_3, metric_4, metric_5)
);

-- Exercise 6.3: Manual unpivot with UNION ALL
-- Alternative approach without UNPIVOT keyword
SELECT id, 'metric_1' AS metric, metric_1 AS value
FROM 'datasets/raw/wide_table.csv'
LIMIT 5
UNION ALL
SELECT id, 'metric_2', metric_2
FROM 'datasets/raw/wide_table.csv'
LIMIT 5
UNION ALL
SELECT id, 'metric_3', metric_3
FROM 'datasets/raw/wide_table.csv'
LIMIT 5
ORDER BY id, metric;

-- ============================================================================
-- PART 7: MODERN SQL FEATURES
-- ============================================================================

-- Exercise 7.1: List comprehensions
SELECT
    [x * x FOR x IN RANGE(1, 11)] AS squares,
    [x FOR x IN RANGE(1, 21) IF x % 2 = 0] AS even_numbers,
    [UPPER(name) FOR name IN ['alice', 'bob', 'charlie']] AS uppercase_names;

-- Exercise 7.2: LIKE ANY pattern matching
SELECT
    name,
    department
FROM 'datasets/raw/employees.csv'
WHERE name LIKE ANY ('%son', '%lez', 'A%', '%Lee');

-- Exercise 7.3: EXCLUDE columns
-- Select all columns except sensitive ones
SELECT * EXCLUDE (salary, hire_date)
FROM 'datasets/raw/employees.csv'
LIMIT 5;

-- Exercise 7.4: REPLACE columns
-- Transform columns in SELECT *
SELECT * REPLACE (
    UPPER(name) AS name,
    ROUND(salary / 12, 2) AS salary,
    department || ' Dept' AS department
)
FROM 'datasets/raw/employees.csv'
LIMIT 5;

-- Exercise 7.5: Struct operations
SELECT
    department,
    {'count': COUNT(*), 'avg_salary': AVG(salary), 'total_budget': SUM(salary)} AS dept_stats
FROM 'datasets/raw/employees.csv'
GROUP BY department;

-- Exercise 7.6: Array aggregation with LIST
SELECT
    department,
    LIST(name ORDER BY salary DESC) AS employees_by_salary,
    LIST(salary ORDER BY salary DESC) AS salaries,
    LIST(name ORDER BY hire_date) AS employees_by_seniority
FROM 'datasets/raw/employees.csv'
GROUP BY department;

-- ============================================================================
-- CHALLENGE EXERCISES
-- ============================================================================

-- Challenge 1: Cohort analysis
-- For each month, show how many employees were hired and still employed
WITH hire_cohorts AS (
    SELECT
        DATE_TRUNC('month', hire_date) AS cohort_month,
        COUNT(*) AS cohort_size
    FROM 'datasets/raw/employees.csv'
    GROUP BY cohort_month
),
cohort_grid AS (
    SELECT
        c1.cohort_month,
        c2.cohort_month AS analysis_month,
        c1.cohort_size
    FROM hire_cohorts c1
    CROSS JOIN hire_cohorts c2
    WHERE c2.cohort_month >= c1.cohort_month
)
SELECT
    cohort_month,
    cohort_size,
    analysis_month,
    DATE_DIFF('month', cohort_month, analysis_month) AS months_since_hire
FROM cohort_grid
ORDER BY cohort_month, analysis_month
LIMIT 20;

-- Challenge 2: Running total with percentage
-- Show cumulative sales and what percentage of total each day represents
WITH daily_sales AS (
    SELECT
        sale_date,
        SUM(amount) AS daily_total
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date
),
with_running_total AS (
    SELECT
        sale_date,
        daily_total,
        SUM(daily_total) OVER (ORDER BY sale_date) AS running_total,
        SUM(daily_total) OVER () AS grand_total
    FROM daily_sales
)
SELECT
    sale_date,
    daily_total,
    running_total,
    ROUND(100.0 * running_total / grand_total, 2) AS pct_of_total_achieved
FROM with_running_total
ORDER BY sale_date
LIMIT 30;

-- Challenge 3: Gap detection
-- Find gaps in the sales date sequence (days with no sales)
WITH RECURSIVE all_dates AS (
    SELECT MIN(sale_date) AS date, MAX(sale_date) AS max_date
    FROM 'datasets/raw/sales.parquet'

    UNION ALL

    SELECT date + INTERVAL 1 DAY, max_date
    FROM all_dates
    WHERE date < max_date
),
sales_by_date AS (
    SELECT DISTINCT sale_date
    FROM 'datasets/raw/sales.parquet'
)
SELECT ad.date AS missing_date
FROM all_dates ad
LEFT JOIN sales_by_date sbd ON ad.date = sbd.sale_date
WHERE sbd.sale_date IS NULL
ORDER BY ad.date
LIMIT 20;

-- Challenge 4: Product category growth rate
-- Calculate month-over-month growth rate by category
WITH monthly_sales AS (
    SELECT
        product_category,
        DATE_TRUNC('month', sale_date) AS month,
        SUM(amount) AS monthly_total
    FROM 'datasets/raw/sales.parquet'
    GROUP BY product_category, DATE_TRUNC('month', sale_date)
)
SELECT
    product_category,
    month,
    monthly_total,
    LAG(monthly_total) OVER (PARTITION BY product_category ORDER BY month) AS prev_month,
    ROUND(100.0 * (monthly_total - LAG(monthly_total) OVER (PARTITION BY product_category ORDER BY month)) /
          LAG(monthly_total) OVER (PARTITION BY product_category ORDER BY month), 2) AS growth_rate_pct
FROM monthly_sales
ORDER BY product_category, month;

-- ============================================================================
-- SOLUTIONS NOTES
-- ============================================================================
-- All solutions are provided inline above
-- Key concepts covered:
-- ✅ Window functions: ranking, offset, aggregates, frames
-- ✅ QUALIFY for filtering window results
-- ✅ CTEs for readable multi-step queries
-- ✅ Recursive CTEs for sequences and hierarchies
-- ✅ GROUPING SETS, ROLLUP, CUBE for multi-level aggregations
-- ✅ FILTER clause for conditional aggregations
-- ✅ PIVOT and UNPIVOT for reshaping data
-- ✅ Modern SQL features: list comprehensions, EXCLUDE, REPLACE, structs, arrays
-- ============================================================================
