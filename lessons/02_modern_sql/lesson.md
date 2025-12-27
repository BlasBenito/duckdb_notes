# Module 2: Modern SQL with DuckDB

**Duration**: 6 hours
**Objective**: Master DuckDB's enhanced SQL syntax and analytical capabilities

## Overview

This module covers advanced SQL features that are essential for analytical work:
- Window functions (ranking, offset, aggregates)
- Common Table Expressions (CTEs) and recursive queries
- Advanced grouping (GROUPING SETS, ROLLUP, CUBE)
- DuckDB-specific modern SQL features

---

## Part 1: Window Functions Deep Dive (2.5 hours)

### What are Window Functions?

Window functions perform calculations **across rows** related to the current row, without collapsing rows (unlike GROUP BY).

**GROUP BY example** (collapses rows):
```sql
SELECT department, AVG(salary)
FROM employees
GROUP BY department;
-- Result: 1 row per department
```

**Window function example** (preserves rows):
```sql
SELECT
    name,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) AS dept_avg_salary
FROM employees;
-- Result: All rows preserved, with dept average added
```

### Window Function Syntax

```sql
function_name(arg1, arg2, ...) OVER (
    [PARTITION BY column1, column2, ...]
    [ORDER BY column3, column4, ...]
    [ROWS/RANGE window_frame]
)
```

- **PARTITION BY**: Divides data into groups (like GROUP BY but doesn't collapse)
- **ORDER BY**: Defines ordering within each partition
- **Frame clause**: Defines which rows to include in calculation

---

### 1.1 Ranking Functions

#### ROW_NUMBER()

Assigns unique sequential integer to rows, starting from 1.

```sql
SELECT
    name,
    department,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS overall_rank,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank
FROM 'datasets/raw/employees.csv';
```

**Use case**: Assign unique ranks, even for ties (ties get different numbers).

#### RANK()

Assigns ranks with gaps for ties (1, 2, 2, 4, 5...).

```sql
SELECT
    name,
    department,
    salary,
    RANK() OVER (ORDER BY salary DESC) AS salary_rank
FROM 'datasets/raw/employees.csv';
```

**Use case**: Competition-style ranking (Olympic medals: gold, silver, silver, bronze=4th place).

#### DENSE_RANK()

Assigns ranks without gaps for ties (1, 2, 2, 3, 4...).

```sql
SELECT
    name,
    department,
    salary,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS salary_dense_rank
FROM 'datasets/raw/employees.csv';
```

**Use case**: Ranking where you want consecutive integers despite ties.

#### NTILE(n)

Divides rows into n approximately equal groups (quartiles, deciles, etc.).

```sql
SELECT
    name,
    salary,
    NTILE(4) OVER (ORDER BY salary) AS salary_quartile,
    NTILE(10) OVER (ORDER BY salary) AS salary_decile
FROM 'datasets/raw/employees.csv';
```

**Use case**: Segmentation analysis (top 10%, bottom quartile, etc.).

#### Comparison: Which to use?

| Function | Ties Behavior | Use Case |
|----------|---------------|----------|
| `ROW_NUMBER()` | Unique (1,2,3,4,5) | Need unique IDs |
| `RANK()` | Gaps (1,2,2,4,5) | Competition-style |
| `DENSE_RANK()` | No gaps (1,2,2,3,4) | Consecutive ranks |
| `NTILE(n)` | Even distribution | Percentiles/quartiles |

---

### 1.2 Offset Functions

Access values from other rows relative to the current row.

#### LAG() and LEAD()

**LAG(column, offset, default)**: Access previous row
**LEAD(column, offset, default)**: Access next row

```sql
SELECT
    sale_date,
    amount,
    LAG(amount, 1) OVER (ORDER BY sale_date) AS previous_day_amount,
    LEAD(amount, 1) OVER (ORDER BY sale_date) AS next_day_amount,
    amount - LAG(amount, 1, 0) OVER (ORDER BY sale_date) AS day_over_day_change
FROM 'datasets/raw/sales.parquet'
ORDER BY sale_date
LIMIT 10;
```

**Use cases**:
- Calculate day-over-day/month-over-month changes
- Detect trends (is current value higher than previous?)
- Time series analysis

#### FIRST_VALUE() and LAST_VALUE()

**FIRST_VALUE(column)**: First value in window
**LAST_VALUE(column)**: Last value in window

```sql
SELECT
    name,
    department,
    salary,
    FIRST_VALUE(salary) OVER (
        PARTITION BY department
        ORDER BY salary DESC
    ) AS highest_salary_in_dept,
    LAST_VALUE(salary) OVER (
        PARTITION BY department
        ORDER BY salary DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS lowest_salary_in_dept
FROM 'datasets/raw/employees.csv';
```

**Important**: `LAST_VALUE` requires explicit frame clause to see all rows (see Window Frames below).

---

### 1.3 Aggregate Window Functions

Use regular aggregates (SUM, AVG, COUNT, etc.) as window functions.

#### Running Totals

```sql
SELECT
    sale_date,
    amount,
    SUM(amount) OVER (ORDER BY sale_date) AS running_total
FROM 'datasets/raw/sales.parquet'
ORDER BY sale_date;
```

#### Moving Averages

```sql
SELECT
    sale_date,
    amount,
    AVG(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7days
FROM 'datasets/raw/sales.parquet'
ORDER BY sale_date;
```

#### Group-level Aggregates (without collapsing)

```sql
SELECT
    name,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) AS dept_avg,
    salary - AVG(salary) OVER (PARTITION BY department) AS diff_from_avg,
    COUNT(*) OVER (PARTITION BY department) AS dept_size
FROM 'datasets/raw/employees.csv';
```

---

### 1.4 Custom Window Frames

Control which rows are included in window calculation.

**Syntax**:
```sql
{ROWS | RANGE} BETWEEN frame_start AND frame_end
```

**Frame boundaries**:
- `UNBOUNDED PRECEDING`: First row of partition
- `n PRECEDING`: n rows before current
- `CURRENT ROW`: Current row
- `n FOLLOWING`: n rows after current
- `UNBOUNDED FOLLOWING`: Last row of partition

#### ROWS vs RANGE

**ROWS**: Physical row offset (count of rows)
**RANGE**: Logical offset (based on value)

```sql
-- ROWS: Last 3 physical rows
SELECT
    sale_date,
    amount,
    AVG(amount) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS avg_last_3_rows
FROM 'datasets/raw/sales.parquet';

-- RANGE: All rows within 7 days
SELECT
    sale_date,
    amount,
    AVG(amount) OVER (
        ORDER BY sale_date
        RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW
    ) AS avg_last_7_days
FROM 'datasets/raw/sales.parquet';
```

**Default frame**:
- With ORDER BY: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
- Without ORDER BY: All rows in partition

---

### 1.5 QUALIFY Clause (DuckDB-specific)

Filter results based on window function values (like HAVING for window functions).

**Without QUALIFY** (verbose):
```sql
SELECT * FROM (
    SELECT
        name,
        department,
        salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM employees
) WHERE rank <= 3;
```

**With QUALIFY** (concise):
```sql
SELECT
    name,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM 'datasets/raw/employees.csv'
QUALIFY rank <= 3;
```

**Use cases**:
- Top N per group
- Filter on moving averages
- Get first/last occurrence per group

---

## Part 2: Common Table Expressions (CTEs) (1.5 hours)

### What are CTEs?

CTEs (WITH clauses) create temporary named result sets for cleaner, more readable queries.

### 2.1 Basic CTEs

**Without CTE** (nested subquery):
```sql
SELECT *
FROM (
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
)
WHERE avg_salary > 80000;
```

**With CTE** (cleaner):
```sql
WITH department_stats AS (
    SELECT department, AVG(salary) AS avg_salary
    FROM 'datasets/raw/employees.csv'
    GROUP BY department
)
SELECT *
FROM department_stats
WHERE avg_salary > 80000;
```

### 2.2 Chaining Multiple CTEs

Break complex logic into digestible steps:

```sql
WITH
-- Step 1: Calculate daily sales
daily_sales AS (
    SELECT
        sale_date,
        SUM(amount) AS total_sales
    FROM 'datasets/raw/sales.parquet'
    GROUP BY sale_date
),
-- Step 2: Add moving average
sales_with_ma AS (
    SELECT
        sale_date,
        total_sales,
        AVG(total_sales) OVER (
            ORDER BY sale_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7day
    FROM daily_sales
),
-- Step 3: Identify days above average
above_avg_days AS (
    SELECT *
    FROM sales_with_ma
    WHERE total_sales > ma_7day
)
-- Final query
SELECT * FROM above_avg_days
ORDER BY sale_date;
```

**Benefits**:
- Readable (each step has a name)
- Debuggable (can test each CTE independently)
- Reusable (reference CTE multiple times)

### 2.3 Recursive CTEs

Query hierarchical data (organization charts, graphs, etc.).

**Syntax**:
```sql
WITH RECURSIVE cte_name AS (
    -- Base case (anchor)
    SELECT ...

    UNION ALL

    -- Recursive case
    SELECT ...
    FROM cte_name
    JOIN ...
)
SELECT * FROM cte_name;
```

**Example: Generate number sequence**:
```sql
WITH RECURSIVE numbers AS (
    -- Base case
    SELECT 1 AS n

    UNION ALL

    -- Recursive case
    SELECT n + 1
    FROM numbers
    WHERE n < 10
)
SELECT * FROM numbers;
-- Result: 1, 2, 3, ..., 10
```

**Example: Organization hierarchy**:
```sql
WITH RECURSIVE org_tree AS (
    -- Base: CEO (no manager)
    SELECT id, name, manager_id, 1 AS level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive: Employees reporting to already-found employees
    SELECT e.id, e.name, e.manager_id, ot.level + 1
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.id
)
SELECT * FROM org_tree
ORDER BY level, name;
```

---

## Part 3: Advanced Grouping (1 hour)

### 3.1 GROUPING SETS

Compute multiple GROUP BY combinations in one query.

**Manual approach** (slow):
```sql
-- Total by category
SELECT product_category, NULL AS region, SUM(amount)
FROM sales
GROUP BY product_category

UNION ALL

-- Total by region
SELECT NULL, region, SUM(amount)
FROM sales
GROUP BY region

UNION ALL

-- Grand total
SELECT NULL, NULL, SUM(amount)
FROM sales;
```

**GROUPING SETS** (efficient):
```sql
SELECT
    product_category,
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY GROUPING SETS (
    (product_category),
    (region),
    ()  -- grand total
);
```

### 3.2 ROLLUP

Hierarchical totals (subtotals + grand total).

```sql
SELECT
    product_category,
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY ROLLUP (product_category, region);
```

Produces:
- Grand total (all rows)
- Totals per product_category
- Totals per (product_category, region)

### 3.3 CUBE

All possible combinations of grouping columns.

```sql
SELECT
    product_category,
    region,
    SUM(amount) AS total_sales
FROM sales
GROUP BY CUBE (product_category, region);
```

Produces totals for:
- ()
- (product_category)
- (region)
- (product_category, region)

### 3.4 FILTER Clause in Aggregates

Conditional aggregation without CASE.

**Without FILTER** (verbose):
```sql
SELECT
    department,
    COUNT(*) AS total_employees,
    SUM(CASE WHEN salary > 85000 THEN 1 ELSE 0 END) AS high_earners
FROM employees
GROUP BY department;
```

**With FILTER** (clean):
```sql
SELECT
    department,
    COUNT(*) AS total_employees,
    COUNT(*) FILTER (WHERE salary > 85000) AS high_earners,
    AVG(salary) FILTER (WHERE hire_date >= '2020-01-01') AS avg_recent_hires
FROM 'datasets/raw/employees.csv'
GROUP BY department;
```

---

## Part 4: PIVOT and UNPIVOT (45 minutes)

### 4.1 PIVOT (Columns → Rows)

Transform row values into columns.

**Long format**:
```
department | metric      | value
-----------|-------------|------
Sales      | count       | 5
Sales      | avg_salary  | 85000
Marketing  | count       | 4
Marketing  | avg_salary  | 75000
```

**Pivoted**:
```
department | count | avg_salary
-----------|-------|----------
Sales      | 5     | 85000
Marketing  | 4     | 75000
```

**DuckDB PIVOT syntax**:
```sql
PIVOT (
    SELECT department, metric, value
    FROM stats
)
ON metric
USING SUM(value);
```

### 4.2 UNPIVOT (Rows → Columns)

Transform columns into rows (opposite of PIVOT).

**Wide format**:
```
id | metric_1 | metric_2 | metric_3
---|----------|----------|----------
1  | 10       | 20       | 30
2  | 15       | 25       | 35
```

**Unpivoted**:
```
id | metric_name | value
---|-------------|------
1  | metric_1    | 10
1  | metric_2    | 20
1  | metric_3    | 30
2  | metric_1    | 15
...
```

**DuckDB UNPIVOT syntax**:
```sql
UNPIVOT wide_table
ON metric_1, metric_2, metric_3
INTO
    NAME metric_name
    VALUE value;
```

---

## Part 5: Advanced DuckDB Features (45 minutes)

### 5.1 List Comprehensions

Python-style list transformations.

```sql
SELECT
    [x * 2 FOR x IN [1, 2, 3, 4, 5]] AS doubled,
    [x FOR x IN [1, 2, 3, 4, 5] IF x > 2] AS filtered,
    [UPPER(x) FOR x IN ['a', 'b', 'c']] AS uppercase;
```

### 5.2 Pattern Matching: LIKE ANY

Match against multiple patterns.

```sql
SELECT name
FROM 'datasets/raw/employees.csv'
WHERE name LIKE ANY ('%son', '%lez', 'A%');
```

### 5.3 EXCLUDE and REPLACE

Modify SELECT * queries.

**EXCLUDE** (remove columns):
```sql
SELECT * EXCLUDE (salary, hire_date)
FROM 'datasets/raw/employees.csv';
```

**REPLACE** (transform columns):
```sql
SELECT * REPLACE (UPPER(name) AS name, salary / 12 AS salary)
FROM 'datasets/raw/employees.csv';
```

### 5.4 Struct and Array Operations

**Struct creation**:
```sql
SELECT
    {'name': name, 'salary': salary} AS employee_struct
FROM 'datasets/raw/employees.csv';
```

**Array aggregation**:
```sql
SELECT
    department,
    LIST(name) AS employees,
    LIST(salary ORDER BY salary DESC) AS salaries
FROM 'datasets/raw/employees.csv'
GROUP BY department;
```

---

## Key Takeaways

✅ **Window functions** preserve rows while adding aggregate context
✅ **QUALIFY** filters on window function results (DuckDB-specific)
✅ **CTEs** make complex queries readable and debuggable
✅ **Recursive CTEs** handle hierarchical data
✅ **GROUPING SETS/ROLLUP/CUBE** compute multiple aggregation levels efficiently
✅ **FILTER clause** enables clean conditional aggregation
✅ **PIVOT/UNPIVOT** reshape data between wide and long formats
✅ **Modern SQL features** (list comprehensions, EXCLUDE, LIKE ANY) make queries concise

---

## Practice Exercises

See `exercises/02_modern_sql.sql` for comprehensive hands-on exercises covering all topics.

## Reference Materials

- **Window Functions Reference**: `reference/cheatsheets/window_functions.md`
- **CTEs Patterns**: `reference/cheatsheets/cte_patterns.md`

---

## Next Module

**Module 3: Advanced File Operations** - Glob patterns, partitioned datasets, remote files, and format conversions.
