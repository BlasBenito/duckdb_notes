# Window Functions Quick Reference

## Basic Syntax

```sql
function_name(args) OVER (
    [PARTITION BY col1, col2]
    [ORDER BY col3, col4]
    [ROWS/RANGE frame_clause]
)
```

## Ranking Functions

| Function | Ties Behavior | Use Case | Example |
|----------|---------------|----------|---------|
| `ROW_NUMBER()` | Unique (1,2,3,4) | Unique row IDs | Pagination |
| `RANK()` | Gaps (1,2,2,4) | Competition ranking | Olympic medals |
| `DENSE_RANK()` | No gaps (1,2,2,3) | Consecutive ranks | Grade levels |
| `NTILE(n)` | Even groups | Percentiles | Top 10%, quartiles |

### Examples

```sql
-- Top 3 per department
SELECT name, department, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM employees
QUALIFY rank <= 3;

-- Quartiles
SELECT name, salary,
       NTILE(4) OVER (ORDER BY salary) AS quartile
FROM employees;
```

## Offset Functions

| Function | Description | Common Use |
|----------|-------------|------------|
| `LAG(col, n, default)` | Previous row value | Period-over-period change |
| `LEAD(col, n, default)` | Next row value | Forward-looking metrics |
| `FIRST_VALUE(col)` | First value in window | Baseline comparison |
| `LAST_VALUE(col)` | Last value in window | End-of-period value |

### Examples

```sql
-- Day-over-day change
SELECT date, sales,
       sales - LAG(sales) OVER (ORDER BY date) AS daily_change
FROM daily_sales;

-- Compare to first/last in group
SELECT product, date, sales,
       FIRST_VALUE(sales) OVER (PARTITION BY product ORDER BY date) AS first_sale,
       LAST_VALUE(sales) OVER (
           PARTITION BY product ORDER BY date
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS last_sale
FROM sales;
```

## Aggregate Window Functions

| Function | Description |
|----------|-------------|
| `SUM() OVER (...)` | Running/moving sum |
| `AVG() OVER (...)` | Running/moving average |
| `COUNT() OVER (...)` | Running/moving count |
| `MIN() OVER (...)` | Minimum in window |
| `MAX() OVER (...)` | Maximum in window |

### Examples

```sql
-- Running total
SELECT date, amount,
       SUM(amount) OVER (ORDER BY date) AS running_total
FROM transactions;

-- 7-day moving average
SELECT date, value,
       AVG(value) OVER (
           ORDER BY date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS ma_7day
FROM metrics;

-- Group stats without collapsing
SELECT name, department, salary,
       AVG(salary) OVER (PARTITION BY department) AS dept_avg,
       salary - AVG(salary) OVER (PARTITION BY department) AS diff_from_avg
FROM employees;
```

## Window Frame Clauses

### Frame Types

**ROWS**: Physical row offset
**RANGE**: Logical offset (by value)

### Frame Boundaries

| Boundary | Description |
|----------|-------------|
| `UNBOUNDED PRECEDING` | Start of partition |
| `n PRECEDING` | n rows/values before current |
| `CURRENT ROW` | Current row |
| `n FOLLOWING` | n rows/values after current |
| `UNBOUNDED FOLLOWING` | End of partition |

### Frame Syntax

```sql
{ROWS | RANGE} BETWEEN frame_start AND frame_end
```

### Common Frame Patterns

```sql
-- Last 7 rows (including current)
ROWS BETWEEN 6 PRECEDING AND CURRENT ROW

-- All rows up to current (cumulative)
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

-- All rows in partition
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING

-- Centered window (3 before, current, 3 after)
ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING

-- Last 7 days (time-based)
RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW
```

### ROWS vs RANGE Example

```sql
-- ROWS: Last 3 physical rows
SELECT date, sales,
       AVG(sales) OVER (
           ORDER BY date
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ) AS ma_3rows
FROM daily_sales;

-- RANGE: All rows within 7 days
SELECT date, sales,
       AVG(sales) OVER (
           ORDER BY date
           RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW
       ) AS ma_7days
FROM daily_sales;
```

## QUALIFY Clause (DuckDB-specific)

Filter based on window function results (like HAVING for windows).

```sql
-- Without QUALIFY (verbose)
SELECT * FROM (
    SELECT name, salary,
           RANK() OVER (ORDER BY salary DESC) AS rank
    FROM employees
) WHERE rank <= 10;

-- With QUALIFY (concise)
SELECT name, salary,
       RANK() OVER (ORDER BY salary DESC) AS rank
FROM employees
QUALIFY rank <= 10;
```

### Common QUALIFY Patterns

```sql
-- Top N per group
SELECT * FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) <= 5;

-- First occurrence per group
SELECT * FROM events
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp) = 1;

-- Above moving average
SELECT * FROM metrics
QUALIFY value > AVG(value) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);

-- Outlier detection
SELECT * FROM sales
QUALIFY ABS(amount - AVG(amount) OVER ()) > 2 * STDDEV(amount) OVER ();
```

## Common Patterns

### Top N per Group

```sql
SELECT *
FROM sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) <= 3;
```

### Running Total

```sql
SELECT date, amount,
       SUM(amount) OVER (ORDER BY date) AS running_total
FROM transactions;
```

### Moving Average

```sql
SELECT date, value,
       AVG(value) OVER (
           ORDER BY date
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) AS ma_7day
FROM metrics;
```

### Period-over-Period Change

```sql
SELECT date, sales,
       LAG(sales) OVER (ORDER BY date) AS prev_period,
       sales - LAG(sales) OVER (ORDER BY date) AS change,
       100.0 * (sales - LAG(sales) OVER (ORDER BY date)) / LAG(sales) OVER (ORDER BY date) AS pct_change
FROM period_sales;
```

### Percentage of Total

```sql
SELECT category, amount,
       100.0 * amount / SUM(amount) OVER () AS pct_of_total,
       100.0 * amount / SUM(amount) OVER (PARTITION BY category) AS pct_of_category
FROM sales;
```

### Cumulative Distribution

```sql
SELECT value,
       CUME_DIST() OVER (ORDER BY value) AS cumulative_pct,
       PERCENT_RANK() OVER (ORDER BY value) AS percentile
FROM data;
```

### Gap and Island Detection

```sql
-- Find gaps in sequence
WITH numbered AS (
    SELECT id,
           id - ROW_NUMBER() OVER (ORDER BY id) AS grp
    FROM data
)
SELECT MIN(id) AS island_start, MAX(id) AS island_end, COUNT(*) AS island_size
FROM numbered
GROUP BY grp;
```

## Performance Tips

1. **Index on ORDER BY columns** - Helps with window sorting
2. **Limit PARTITION BY cardinality** - Too many partitions can slow down
3. **Use QUALIFY instead of subqueries** - More efficient filtering
4. **Combine window functions** - Reuse same OVER clause with named windows:

```sql
SELECT name, salary,
       RANK() OVER w AS rank,
       DENSE_RANK() OVER w AS dense_rank,
       PERCENT_RANK() OVER w AS percentile
FROM employees
WINDOW w AS (ORDER BY salary DESC);
```

## Named Windows

Reuse window definitions:

```sql
SELECT
    name,
    department,
    salary,
    AVG(salary) OVER dept_window AS dept_avg,
    RANK() OVER dept_window AS dept_rank
FROM employees
WINDOW dept_window AS (PARTITION BY department ORDER BY salary DESC);
```

## Debugging Tips

1. **Start simple** - Add window functions incrementally
2. **Check frame defaults** - Default frame depends on ORDER BY presence
3. **LAST_VALUE gotcha** - Always specify frame to UNBOUNDED FOLLOWING
4. **Use QUALIFY** - Cleaner than WHERE on window results
5. **Named windows** - More readable when reusing OVER clauses
